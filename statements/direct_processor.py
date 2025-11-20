# statements/direct_processor.py
import os
import pandas as pd
from datetime import datetime
from django.conf import settings

DEBUG_DIR = os.path.join(getattr(settings, "BASE_DIR", "."), "debug_exports")
os.makedirs(DEBUG_DIR, exist_ok=True)


def process_tables_directly(tables):
    """
    Safely process Textract tables into a unified DataFrame.
    Handles headers, row cleanup, and merges multi-page tables.
    """
    print("DEBUG: 🧠 Running process_tables_directly()...")

    if not tables:
        print("⚠️ No tables passed into direct processor.")
        return pd.DataFrame()

    all_frames = []

    for t in tables:
        df_table = t.get("df")
        page = t.get("page")
        table_id = t.get("table_id")

        print(f"\nDEBUG: Processing TABLE {table_id} (page={page})")

        if df_table is None or df_table.empty:
            print(f"⚠️ Table {table_id} is empty — skipping.")
            continue

        print(f"DEBUG: Raw shape = {df_table.shape}")
        print(f"DEBUG: Raw columns = {df_table.columns.tolist()}")
        print(df_table.head(3))

        df_table = df_table.reset_index(drop=True)
        df_table = df_table.loc[:, ~df_table.columns.duplicated()].copy()

        # Header detection
        first_row = " ".join(df_table.iloc[0].astype(str).str.lower().tolist())
        header_keywords = ["date", "time", "desc", "amount", "debit", "credit", "balance"]
        is_header = any(k in first_row for k in header_keywords)

        if is_header:
            headers = df_table.iloc[0].astype(str).tolist()
            df_table.columns = headers
            df_table = df_table.iloc[1:].reset_index(drop=True)
            print("DEBUG: Header row detected and applied")
        else:
            df_table.columns = [f"col_{i}" for i in range(len(df_table.columns))]
            print("DEBUG: No header detected — using positional headers")

        # Clean text values
        df_table = df_table.map(lambda v: str(v).strip() if pd.notna(v) else "")

        # Drop fully empty rows
        before = len(df_table)
        df_table = df_table[~df_table.apply(lambda row: all(v == "" for v in row), axis=1)]
        after = len(df_table)
        print(f"DEBUG: Removed {before - after} empty rows")

        if after > 0:
            all_frames.append(df_table)
            print(f"DEBUG: Added cleaned TABLE {table_id} with shape {df_table.shape}")
        else:
            print(f"⚠️ TABLE {table_id} has no usable rows after cleaning")

    # Merge all tables
    if not all_frames:
        print("❌ No usable tables found.")
        return pd.DataFrame()

    merged = pd.concat(all_frames, ignore_index=True)
    merged = merged.loc[:, ~merged.columns.duplicated()].copy()
    print(f"\n✅ Final merged shape = {merged.shape}")
    print("DEBUG: Merged head:\n", merged.head(10))

    return merged
