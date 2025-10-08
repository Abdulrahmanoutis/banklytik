import time
import boto3
import pandas as pd
from collections import defaultdict
from django.conf import settings


def get_textract_client():
    return boto3.client(
        "textract",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
    )


def start_textract_job(s3_key: str) -> str:
    client = get_textract_client()
    response = client.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": settings.AWS_S3_BUCKET, "Name": s3_key}},
        FeatureTypes=["TABLES"],
    )
    return response["JobId"]


def wait_for_job(job_id: str):
    client = get_textract_client()
    while True:
        result = client.get_document_analysis(JobId=job_id)
        status = result["JobStatus"]
        if status in ("SUCCEEDED", "FAILED"):
            if status == "FAILED":
                raise RuntimeError("Textract job failed.")
            return
        time.sleep(5)


def get_all_blocks(job_id: str) -> list:
    client = get_textract_client()
    blocks = []
    next_token = None

    while True:
        if next_token:
            response = client.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            response = client.get_document_analysis(JobId=job_id)
        blocks.extend(response.get("Blocks", []))
        next_token = response.get("NextToken")
        if not next_token:
            break

    return blocks


def extract_tables_as_json(blocks: list) -> list:
    """
    Convert Textract blocks into a JSON-friendly list of tables.
    Each table = list of rows, where each row = list of cell strings.
    """
    block_map = {b["Id"]: b for b in blocks}
    table_blocks = [b for b in blocks if b["BlockType"] == "TABLE"]
    results = []

    for table in table_blocks:
        rows = {}
        for rel in table.get("Relationships", []):
            if rel["Type"] == "CHILD":
                for cid in rel["Ids"]:
                    cell = block_map.get(cid)
                    if cell and cell["BlockType"] == "CELL":
                        row_idx = cell["RowIndex"]
                        col_idx = cell["ColumnIndex"]

                        text = ""
                        for subrel in cell.get("Relationships", []):
                            for wid in subrel.get("Ids", []):
                                word = block_map.get(wid)
                                if word["BlockType"] == "WORD":
                                    text += word.get("Text", "") + " "
                                elif word["BlockType"] == "SELECTION_ELEMENT":
                                    if word.get("SelectionStatus") == "SELECTED":
                                        text += "[X] "
                        rows.setdefault(row_idx, {})
                        rows[row_idx][col_idx] = text.strip()

        # Sort rows and cols
        table_data = []
        for r in sorted(rows.keys()):
            row_data = []
            for c in sorted(rows[r].keys()):
                row_data.append(rows[r][c])
            table_data.append(row_data)

        results.append(table_data)

    return results


def extract_combined_table(blocks: list) -> pd.DataFrame:
    """
    Combine all TABLE blocks from Textract into one pandas DataFrame.
    Falls back to LINE blocks if no TABLES are detected.
    """
    block_map = {b["Id"]: b for b in blocks}
    table_blocks = [b for b in blocks if b.get("BlockType") == "TABLE"]
    table_blocks.sort(key=lambda b: b.get("Page", 0))

    all_rows = []
    for table in table_blocks:
        cells = []
        for rel in table.get("Relationships", []) or []:
            if rel.get("Type") == "CHILD":
                for cid in rel.get("Ids", []):
                    cell = block_map.get(cid)
                    if not cell or cell.get("BlockType") != "CELL":
                        continue
                    row = cell["RowIndex"]
                    col = cell["ColumnIndex"]
                    text = ""
                    for crel in cell.get("Relationships", []) or []:
                        if crel.get("Type") == "CHILD":
                            for wid in crel.get("Ids", []):
                                w = block_map.get(wid)
                                if not w:
                                    continue
                                if w.get("BlockType") == "WORD":
                                    text += (w.get("Text") or "") + " "
                                elif w.get("BlockType") == "SELECTION_ELEMENT":
                                    if w.get("SelectionStatus") == "SELECTED":
                                        text += "[X] "
                    cells.append({"row": row, "col": col, "text": text.strip()})

        if not cells:
            continue

        max_row = max(c["row"] for c in cells)
        max_col = max(c["col"] for c in cells)
        table_map = defaultdict(dict)
        for c in cells:
            table_map[c["row"]][c["col"]] = c["text"]

        for r in range(1, max_row + 1):
            row_vals = [table_map[r].get(c, "") for c in range(1, max_col + 1)]
            all_rows.append(row_vals)

    # ✅ Fallback: if no TABLE blocks, try LINE blocks
    if not all_rows:
        line_blocks = [b for b in blocks if b.get("BlockType") == "LINE"]
        if line_blocks:
            all_rows = [[b.get("Text", "")] for b in line_blocks]

    if not all_rows:
        return pd.DataFrame()

    return pd.DataFrame(all_rows)


def process_textract_to_json(s3_key: str) -> dict:
    """
    Main entrypoint:
    1. Run Textract on S3 PDF
    2. Collect blocks
    3. Return structured JSON tables
    """
    job_id = start_textract_job(s3_key)
    wait_for_job(job_id)
    blocks = get_all_blocks(job_id)
    tables = extract_tables_as_json(blocks)
    return {"tables": tables}


def sample_representative_pages(blocks: list) -> list:
    """
    Extract representative pages (first, second, and last) from Textract blocks.
    Handles cases where the document has only 1 or 2 pages.
    """
    pages = sorted({b.get("Page", 0) for b in blocks if "Page" in b})

    if not pages:
        return []

    selected_pages = [pages[0]]

    if len(pages) > 1:
        selected_pages.append(pages[1])

    if len(pages) > 2:
        selected_pages.append(pages[-1])

    sampled_blocks = [b for b in blocks if b.get("Page") in selected_pages]
    return sampled_blocks
