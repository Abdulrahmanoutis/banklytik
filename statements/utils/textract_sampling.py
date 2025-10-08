import re
import random
from collections import defaultdict

# ---------------------------------------------------------------------
# Utility: Convert Textract blocks into a per-page structure
# ---------------------------------------------------------------------
def organize_blocks_by_page(blocks):
    pages = defaultdict(lambda: {"tables": [], "lines": []})
    for b in blocks:
        if b.get("BlockType") == "PAGE":
            continue
        page_num = b.get("Page", 1)
        if b["BlockType"] == "TABLE":
            pages[page_num]["tables"].append(b)
        elif b["BlockType"] == "LINE" and "Text" in b:
            pages[page_num]["lines"].append(b["Text"])
    return pages


# ---------------------------------------------------------------------
# Utility: Convert Textract TABLE blocks into simple row/column text
# ---------------------------------------------------------------------
def convert_table_to_compact(block):
    table = {"rows": [], "summary": ""}
    # Some Textract blocks may have embedded cells
    if "Children" in block:
        for cell in block["Children"]:
            r, c = cell.get("RowIndex", 1), cell.get("ColumnIndex", 1)
            text = cell.get("Text", "")
            while len(table["rows"]) < r:
                table["rows"].append([])
            while len(table["rows"][r - 1]) < c:
                table["rows"][r - 1].append("")
            table["rows"][r - 1][c - 1] = text

    # Create a small preview of the table
    joined_lines = [" | ".join(r) for r in table["rows"][:3]]
    table["summary"] = "\n".join(joined_lines)
    return table


# ---------------------------------------------------------------------
# Step 1: Select representative pages for DeepSeek
# ---------------------------------------------------------------------
def sample_representative_pages(blocks, max_pages=3):
    pages = sorted({b.get("Page") for b in blocks if "Page" in b})
    if not pages:
        return []

    if len(pages) <= max_pages:
        return pages

    mid = pages[len(pages) // 2]
    selected = [pages[0], mid, pages[-1]]
    return selected[:max_pages]


# ---------------------------------------------------------------------
# NEW: Heuristic to detect transaction-like tables
# ---------------------------------------------------------------------
def looks_like_transaction_table(header_row, sample_rows=None):
    """
    Decide if a table looks like a bank transaction table based on headers and sample content.
    """

    if not header_row:
        return False

    header_text = [re.sub(r"[^a-zA-Z0-9]", "", h).lower() for h in header_row]
    text_joined = " ".join(header_text)

    # --- Keyword-based scoring ---
    score = 0
    transaction_keywords = ["debit", "credit", "balance", "amount"]
    context_keywords = ["date", "description", "reference", "channel"]

    # 1. Keyword matches
    for kw in transaction_keywords:
        if kw in text_joined:
            score += 1
    for kw in context_keywords:
        if kw in text_joined:
            score += 1

    # 2. Combined columns like "debitcredit" or "debitcredit₦"
    if "debitcredit" in text_joined or "debitcreditn" in text_joined:
        score += 2  # strong signal

    # 3. Many numeric entries in rows
    if sample_rows:
        numeric_cells = 0
        total_cells = 0
        for row in sample_rows:
            for cell in row:
                total_cells += 1
                if re.search(r"[-+]?\d+[,.]?\d*", cell) or "₦" in cell:
                    numeric_cells += 1
        if total_cells > 0 and (numeric_cells / total_cells) > 0.3:
            score += 1

    # 4. Enough columns (typical statement tables have >5)
    if len(header_row) >= 5:
        score += 1

    # 5. Presence of "time" or "value date" or "trans"
    if any(k in text_joined for k in ["time", "valuedate", "trans"]):
        score += 1

    # Final decision
    return score >= 3


# ---------------------------------------------------------------------
# Step 2: Build the DeepSeek payload
# ---------------------------------------------------------------------
def build_deepseek_sampling_payload(blocks, max_pages=3):
    """
    Creates a compact JSON-friendly payload to send to DeepSeek.
    Includes heuristic-based hints for transaction tables.
    """
    pages_data = organize_blocks_by_page(blocks)
    selected_pages = sample_representative_pages(blocks, max_pages)

    print(f"DEBUG: Organized {len(blocks)} blocks into {len(pages_data)} pages")
    print(f"DEBUG: sample_representative_pages → {selected_pages}")
    print(f"DEBUG: type of pages_data: {type(pages_data)}")

    payload = {"sample_pages": []}
    transaction_pages = []

    for p in selected_pages:
        page_info = pages_data[p]
        tables = []
        for t in page_info["tables"]:
            compact = convert_table_to_compact(t)
            tables.append(compact)

            # Detect if it's a transaction-like table
            if compact["rows"]:
                header = compact["rows"][0]
                sample = compact["rows"][1:6]
                if looks_like_transaction_table(header, sample):
                    transaction_pages.append(p)

        payload["sample_pages"].append({
            "page_number": p,
            "num_tables": len(tables),
            "num_lines": len(page_info["lines"]),
            "lines_sample": page_info["lines"][:10],
            "tables_sample": tables[:2],
        })

    print(f"DEBUG: Transaction-like pages identified: {transaction_pages}")

    payload["metadata"] = {
        "total_blocks": len(blocks),
        "selected_pages": selected_pages,
        "transaction_pages": transaction_pages,
        "total_pages": len(pages_data),
    }

    print("DEBUG: Payload built successfully with", len(payload["sample_pages"]), "pages")
    return payload
