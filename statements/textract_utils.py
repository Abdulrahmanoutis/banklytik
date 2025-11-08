# statements/textract_utils.py
"""
Robust Textract utilities for Banklytik.

This version maps blocks by Id and resolves TABLE -> CELL -> WORD relationships
so we extract all tables across pages reliably (no assumptions about block order).
It preserves the previous public API:
  - extract_all_tables(blocks) -> list of {"table_id","page","df","blocks","raw_table_block"}
  - table_blocks_to_dataframe(cell_blocks, all_blocks_dict) -> DataFrame
  - extract_combined_table(blocks) -> DataFrame (fallback)
  - sample_representative_pages(blocks, max_pages)
  - build_deepseek_sampling_payload(blocks)
  - detect_table_structure(df)
"""

import boto3
import time
import json
import pandas as pd
from django.conf import settings

import re
from typing import List, Dict, Any
from collections import defaultdict

# Textract client setup (unchanged)
def get_textract_client():
    return boto3.client(
        "textract",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
    )

def start_textract_job(s3_key):
    textract = get_textract_client()
    response = textract.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": settings.AWS_S3_BUCKET, "Name": s3_key}},
        FeatureTypes=["TABLES"],
    )
    return response["JobId"]

def wait_for_job(job_id, poll_interval=5):
    textract = get_textract_client()
    while True:
        response = textract.get_document_analysis(JobId=job_id)
        status = response["JobStatus"]
        if status in ["SUCCEEDED", "FAILED"]:
            return status
        time.sleep(poll_interval)

def get_all_blocks(job_id):
    textract = get_textract_client()
    blocks = []
    next_token = None
    while True:
        if next_token:
            response = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            response = textract.get_document_analysis(JobId=job_id)
        blocks.extend(response.get("Blocks", []))
        next_token = response.get("NextToken")
        if not next_token:
            break
    return blocks

# -------------------------
# Internal helpers
# -------------------------
def _map_blocks(blocks: List[Dict[str, Any]]):
    """Return id_map and type_map for quick lookup."""
    id_map = {}
    type_map = defaultdict(list)
    for b in blocks:
        bid = b.get("Id")
        if not bid:
            continue
        id_map[bid] = b
        bt = b.get("BlockType")
        if bt:
            type_map[bt].append(b)
    return id_map, type_map

def _get_child_ids(block: Dict[str, Any]) -> List[str]:
    """Return child Ids from Relationships of a block (or empty list)."""
    rels = block.get("Relationships") or []
    for r in rels:
        if r.get("Type") == "CHILD":
            return [x for x in r.get("Ids", [])]
    return []

def _extract_cell_text(cell_block: Dict[str, Any], id_map: Dict[str, Dict[str, Any]]) -> str:
    """Concatenate WORD and SELECTION children for a CELL block."""
    texts = []
    for cid in _get_child_ids(cell_block):
        child = id_map.get(cid)
        if not child:
            continue
        ctype = child.get("BlockType")
        if ctype == "WORD":
            texts.append(child.get("Text", ""))
        elif ctype == "SELECTION_ELEMENT":
            sel = child.get("SelectionStatus") or child.get("SelectionStatus".lower())
            if sel == "SELECTED":
                texts.append("[X]")
        elif ctype == "LINE":
            # Some outputs place lines as children - be tolerant
            texts.append(child.get("Text", ""))
    return " ".join([t for t in texts if t is not None and str(t).strip() != ""]).strip()

def _table_block_to_matrix(table_block: Dict[str, Any], id_map: Dict[str, Dict[str, Any]]):
    """
    For a TABLE block, find all child CELL blocks and reconstruct a matrix of their texts.
    Returns: matrix (list of rows), max_row, max_col, list_of_cell_blocks
    """
    cell_ids = []
    # Gather child ids from table block (may reference CELL ids)
    for cid in _get_child_ids(table_block):
        cell_ids.append(cid)

    cell_blocks = []
    for cid in cell_ids:
        b = id_map.get(cid)
        if not b:
            continue
        # If the relationship points to a ROW or CELL container, attempt to find nested children
        if b.get("BlockType") == "CELL":
            cell_blocks.append(b)
        else:
            # in some responses relationships can be nested; inspect CHILD of this block
            for nested in _get_child_ids(b):
                nb = id_map.get(nested)
                if nb and nb.get("BlockType") == "CELL":
                    cell_blocks.append(nb)

    # Build grid indexed by RowIndex/ColumnIndex
    grid = {}
    max_row = 0
    max_col = 0
    for cell in cell_blocks:
        row_index = cell.get("RowIndex") or cell.get("RowIndex", 0)
        col_index = cell.get("ColumnIndex") or cell.get("ColumnIndex", 0)
        if row_index is None or col_index is None:
            continue
        row_index = int(row_index)
        col_index = int(col_index)
        max_row = max(max_row, row_index)
        max_col = max(max_col, col_index)
        grid.setdefault(row_index, {})[col_index] = _extract_cell_text(cell, id_map)

    # Build matrix
    matrix = []
    for r in range(1, max_row + 1):
        row = []
        for c in range(1, max_col + 1):
            row.append(grid.get(r, {}).get(c, ""))
        matrix.append(row)
    return matrix, max_row, max_col, cell_blocks

def table_matrix_to_dataframe(matrix: List[List[str]]):
    """Convert matrix to DataFrame; drop fully empty rows/cols."""
    if not matrix:
        return pd.DataFrame()
    df = pd.DataFrame(matrix)
    # drop empty rows
    df = df.loc[~(df.map(lambda x: str(x).strip() == "").all(axis=1))].reset_index(drop=True)
    # drop empty columns
    nonempty_cols = [i for i in df.columns if not (df[i].astype(str).str.strip() == "").all()]
    if not nonempty_cols:
        return pd.DataFrame()
    df = df[nonempty_cols]
    df.columns = [str(c) for c in df.columns]
    return df

# -------------------------
# Public API
# -------------------------
def table_blocks_to_dataframe(cell_blocks: List[Dict[str, Any]], all_blocks_dict: Dict[str, Dict[str, Any]]):
    """
    Convert a list of CELL blocks (and id map) into a pandas DataFrame.
    This preserves the signature you had: it accepts the cell blocks and an id map.
    """
    # Group cells by row/col
    rows = {}
    for cell in cell_blocks:
        if cell.get("BlockType") != "CELL":
            continue
        r = cell.get("RowIndex", 0)
        c = cell.get("ColumnIndex", 0)
        text = _extract_cell_text(cell, all_blocks_dict)
        rows.setdefault(int(r), {})[int(c)] = text

    if not rows:
        return pd.DataFrame()

    max_cols = max(max(cols.keys()) for cols in rows.values()) if rows else 0
    data = []
    for rid in sorted(rows.keys()):
        row_data = [rows[rid].get(ci, "") for ci in range(1, max_cols + 1)]
        data.append(row_data)
    return pd.DataFrame(data)

def extract_all_tables(blocks: List[Dict[str, Any]]):
    """
    Extract all TABLE blocks across pages and return list of dicts:
      {"table_id": int, "page": int|None, "df": DataFrame, "blocks": [cell_blocks], "raw_table_block": <block>}
    """
    if not blocks:
        return []

    id_map, type_map = _map_blocks(blocks)
    table_blocks = type_map.get("TABLE", [])

    tables = []
    table_counter = 0

    print(f"DEBUG: Processing {len(blocks)} blocks")

    for tb in table_blocks:
        try:
            matrix, max_row, max_col, cell_blocks = _table_block_to_matrix(tb, id_map)
            df = table_matrix_to_dataframe(matrix)
            page = tb.get("Page") or tb.get("PageNumber") or None
            table_counter += 1
            tables.append({
                "table_id": table_counter,
                "page": page,
                "df": df,
                "blocks": cell_blocks,
                "raw_table_block": tb
            })
            print(f"DEBUG: Found table {table_counter} with {len(cell_blocks)} blocks (page={page})")
        except Exception as e:
            print(f"DEBUG: Failed to parse one TABLE block: {e}")
            continue

    # Fallback: if no TABLE blocks found, attempt combined extraction
    if not tables:
        try:
            df_combined = extract_combined_table(blocks)
            if not df_combined.empty:
                tables = [{"table_id": 1, "page": None, "df": df_combined, "blocks": [], "raw_table_block": None}]
                print(f"DEBUG: Fallback combined into shape: {df_combined.shape}")
        except Exception as e:
            print(f"DEBUG: extract_combined_table fallback failed: {e}")

    print(f"DEBUG: Extracted {len(tables)} tables from Textract")
    return tables

def extract_combined_table(blocks: List[Dict[str, Any]]):
    """
    Fallback combining approach. Attempts to build rows from LINE blocks if TABLEs are not present.
    """
    print("DEBUG: Using original extract_combined_table (fallback)")
    id_map, type_map = _map_blocks(blocks)
    line_blocks = type_map.get("LINE", []) or []

    lines_text = [lb.get("Text", "").strip() for lb in line_blocks][:500]
    rows = []
    for ln in lines_text:
        if not ln:
            continue
        parts = re.split(r'\s{2,}|\t|\s\|\s', ln)
        parts = [p.strip() for p in parts if p.strip() != ""]
        rows.append(parts)
    if not rows:
        return pd.DataFrame()
    max_cols = max(len(r) for r in rows)
    norm_rows = [r + [""] * (max_cols - len(r)) for r in rows]
    df = pd.DataFrame(norm_rows)
    print(f"DEBUG: Combined {len(rows)} lines into shape: {df.shape}")
    return df

def sample_representative_pages(blocks, max_pages=3):
    pages = {}
    for block in blocks:
        page_num = block.get("Page", 1)
        pages.setdefault(page_num, []).append(block)
    sampled_pages = []
    for page_num in sorted(pages.keys())[:max_pages]:
        sampled_pages.append({
            "page": page_num,
            "blocks": pages[page_num][:50]
        })
    return sampled_pages

def build_deepseek_sampling_payload(blocks, sampled_pages=3):
    pages = sample_representative_pages(blocks, sampled_pages)
    return {"sampled_pages": pages}

def detect_table_structure(df):
    """
    Detect whether a Textract-extracted table has headers or starts with data.
    Returns: "with_headers" or "data_only" or "empty"
    """
    if df is None or df.empty:
        return "empty"
    first_row = df.iloc[0].astype(str).str.lower().tolist()
    first_row_str = " ".join(first_row)
    header_indicators = ["trans", "date", "desc", "value", "debit", "credit", "balance", "channel", "reference"]
    has_headers = any(h in first_row_str for h in header_indicators)
    return "with_headers" if has_headers else "data_only"
