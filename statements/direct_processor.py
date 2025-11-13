# statements/direct_processor.py
import os
import traceback
import json
import pandas as pd
import re
from datetime import datetime
from django.conf import settings

# Debug folder
DEBUG_DIR = os.path.join(getattr(settings, "BASE_DIR", "."), "debug_exports")
os.makedirs(DEBUG_DIR, exist_ok=True)




# Put at top of direct_processor.py:


def _debug_write_csv(df, name):
    try:
        DEBUG_DIR = os.path.join(os.getcwd(), "debug_exports")
        os.makedirs(DEBUG_DIR, exist_ok=True)
        stamp = int(datetime.utcnow().timestamp())
        path = os.path.join(DEBUG_DIR, f"{name}_{stamp}.csv")
        df.to_csv(path, index=False)
        print(f"✅ Saved debug CSV: {path}")
    except Exception as e:
        print(f"⚠️ Could not save debug CSV {name}: {e}")

# put this function into statements/direct_processor.py (replace existing process_tables_directly)


def process_tables_directly(tables):
    """
    Process tables extracted from Textract into a unified cleaned DataFrame.

    This version is forgiving of header length mismatches, duplicate columns,
    and inconsistent cell counts. It prints rich debug information and writes
    a preview CSV to debug_exports for inspection.
    """
    print("DEBUG: 🧠 Running process_tables_directly()...")
    if not tables:
        print("⚠️ No tables provided to process_tables_directly().")
        return pd.DataFrame()

    all_frames = []
    debug_dir = os.path.join(getattr(settings, "BASE_DIR", "."), "debug_exports")
    os.makedirs(debug_dir, exist_ok=True)

    for t in tables:
        try:
            df_table = t.get("df")
            tid = t.get("table_id", "unknown")
            page = t.get("page", None)
            if df_table is None or df_table.empty:
                print(f"⚠️ Table {tid} (page {page}) is empty — skipping.")
                continue

            print(f"DEBUG: Cleaning table {tid} (page={page}) shape={df_table.shape}")
            print(f"DEBUG: Raw columns: {df_table.columns.tolist()}")

            # Reset index and ensure unique column names
            df_table = df_table.reset_index(drop=True)

            # If duplicate columns exist, keep the first occurrence and drop later duplicates
            if df_table.columns.duplicated().any():
                print(f"DEBUG: Duplicate column names detected for table {tid}. Deduping.")
                df_table = df_table.loc[:, ~df_table.columns.duplicated()]

            # Ensure column names are strings
            df_table.columns = [str(c) for c in df_table.columns]

            # --- Improved Header Detection ---
            header_row_str = " ".join(df_table.iloc[0].astype(str).str.lower().tolist())
            header_indicators = ["date", "time", "desc", "description", "amount", "debit", "credit", "balance", "trans", "reference"]
            is_header_row = any(ind in header_row_str for ind in header_indicators)

            if is_header_row and len(df_table) > 1:
                try:
                    new_headers = [str(x).strip() for x in df_table.iloc[0].tolist()]
                    # pad or trim to match actual number of columns
                    if len(new_headers) < df_table.shape[1]:
                        new_headers += [f"col_{i}" for i in range(len(new_headers), df_table.shape[1])]
                    elif len(new_headers) > df_table.shape[1]:
                        new_headers = new_headers[: df_table.shape[1]]

                    df_table.columns = new_headers
                    df_table = df_table.iloc[1:].reset_index(drop=True)
                    print(f"✅ Applied flexible header row for table {tid}: {df_table.columns.tolist()}")
                except Exception as e:
                    print(f"⚠️ Header assignment failed for table {tid}: {e}")
                    traceback.print_exc()
                    df_table.columns = [f"col_{i}" for i in range(df_table.shape[1])]
            else:
                # positional headers fallback
                df_table.columns = [f"col_{i}" for i in range(df_table.shape[1])]
                print(f"DEBUG: Using positional headers for table {tid}: {df_table.columns.tolist()}")

            # Normalize every cell to string, strip whitespace
            try:
                df_table = df_table.map(lambda v: str(v).strip() if pd.notna(v) else "")
            except Exception:
                # map may fail on mixed dtypes in older pandas; fallback to astype(str)
                df_table = df_table.astype(str).applymap(lambda v: v.strip() if isinstance(v, str) else str(v))

            # Drop fully empty rows (all columns blank)
            before_drop = len(df_table)
            try:
                non_empty_mask = ~(df_table.applymap(lambda x: str(x).strip() == "").all(axis=1))
            except Exception:
                non_empty_mask = ~(df_table.map(lambda x: str(x).strip()).eq("").all(axis=1))
            df_table = df_table[non_empty_mask].reset_index(drop=True)
            after_drop = len(df_table)
            print(f"DEBUG: Dropped {before_drop - after_drop} empty rows from page {page} (table {tid}).")

            # Save preview for this table
            try:
                stamp = int(datetime.utcnow().timestamp())
                preview_path = os.path.join(debug_dir, f"direct_table_preview_t{tid}_p{page}_{stamp}.csv")
                df_table.to_csv(preview_path, index=False)
                print(f"DEBUG: Saved direct table preview → {preview_path}")
            except Exception as e:
                print(f"⚠️ Failed to save direct table preview for table {tid}: {e}")

            if len(df_table) > 0:
                all_frames.append(df_table)
                print(f"DEBUG: Table {tid} contributed {len(df_table)} rows")
            else:
                print(f"⚠️ After cleaning, table {tid} is empty — skipping.")

        except Exception as e:
            print(f"⚠️ Unexpected error processing table {t.get('table_id')}: {e}")
            traceback.print_exc()
            continue

    if not all_frames:
        print("⚠️ No valid tables were processed by direct processor.")
        return pd.DataFrame()

    # Concatenate all tables vertically; do not attempt to align columns by name here (we want positional concat)
    try:
        combined_df = pd.concat(all_frames, ignore_index=True, sort=False)
        # Save combined preview
        try:
            stamp = int(datetime.utcnow().timestamp())
            combined_path = os.path.join(debug_dir, f"combined_preview_{stamp}.csv")
            combined_df.to_csv(combined_path, index=False)
            print(f"✅ Successfully merged {len(all_frames)} tables into shape: {combined_df.shape}")
            print(f"DEBUG: Combined preview saved → {combined_path}")
        except Exception as e:
            print(f"⚠️ Failed to save combined preview: {e}")
        return combined_df
    except Exception as e:
        print(f"⚠️ Failed to merge tables: {e}")
        traceback.print_exc()
        return pd.DataFrame()

# ---------- Helper utilities (local, small, self-contained) ----------
def safe_save(obj, filename):
    path = os.path.join(DEBUG_DIR, filename)
    try:
        with open(path, "w", encoding="utf-8") as f:
            if isinstance(obj, (dict, list)):
                json.dump(obj, f, indent=2, default=str)
            else:
                f.write(str(obj))
    except Exception as e:
        print("DEBUG: Failed to write debug file", path, e)


def detect_table_structure(df: pd.DataFrame) -> str:
    """Return 'with_headers' or 'data_only' or 'empty'."""
    if df is None or df.empty:
        return "empty"
    first_row = df.iloc[0].astype(str).str.lower().tolist()
    first_row_str = " ".join(first_row)
    header_indicators = ["trans", "date", "description", "debit", "credit", "balance", "value date"]
    has_headers = any(h in first_row_str for h in header_indicators)
    return "with_headers" if has_headers else "data_only"


# ---------- Main processing function ----------

def process_tables_directly(tables):
    """
    Process tables extracted from Textract into a unified cleaned DataFrame.
    Handles:
      - Multi-page tables (e.g. Kuda statements)
      - Duplicate columns
      - Missing headers
      - Date and amount normalization
    """
    print("DEBUG: 🧠 Running process_tables_directly()...")
    if not tables:
        print("⚠️ No tables provided to process_tables_directly().")
        return pd.DataFrame()

    all_frames = []

    for t in tables:
        df_table = t.get("df")
        page = t.get("page")
        if df_table is None or df_table.empty:
            print(f"⚠️ Table {t.get('table_id')} (page {page}) is empty — skipping.")
            continue

        print(f"DEBUG: Cleaning table {t.get('table_id')} (page={page}) type={type(df_table)}")
        print(f"DEBUG: df_table shape={df_table.shape}, columns={df_table.columns.tolist()}")
        print(f"DEBUG: First 3 rows preview:\n{df_table.head(3)}")

        # --- Reset index and drop duplicates safely ---
        df_table = df_table.reset_index(drop=True)
        df_table = df_table.loc[:, ~df_table.columns.duplicated()].copy()

        # --- Try to detect header row dynamically ---
        first_row = " ".join(df_table.iloc[0].astype(str).str.lower().tolist())
        header_indicators = ["date", "time", "desc", "amount", "debit", "credit", "balance"]
        is_header_row = any(x in first_row for x in header_indicators)

        if is_header_row:
            df_table.columns = df_table.iloc[0].astype(str).tolist()
            df_table = df_table[1:].reset_index(drop=True)
        else:
            # Assign fallback headers if none detected
            base_headers = [
                "date", "credit", "debit", "channel", "description", "balance"
            ]
            df_table.columns = base_headers[: len(df_table.columns)]

        # --- Normalize text values ---
        df_table = df_table.map(lambda x: str(x).strip() if pd.notna(x) else "")

        # --- Handle duplicate column names safely again after renaming ---
        df_table = df_table.loc[:, ~df_table.columns.duplicated()].copy()

        # --- Drop fully empty rows ---
        before_drop = len(df_table)
        df_table = df_table.loc[~(df_table.applymap(lambda x: str(x).strip() == "").all(axis=1))]
        after_drop = len(df_table)
        print(f"DEBUG: Dropped {before_drop - after_drop} empty rows from page {page}.")

        # --- Save snapshot for debugging ---
        try:
            import os
            DEBUG_DIR = os.path.join("debug_exports")
            os.makedirs(DEBUG_DIR, exist_ok=True)
            stamp = int(datetime.utcnow().timestamp())
            path = os.path.join(DEBUG_DIR, f"cleaned_table_snapshot_{stamp}.csv")
            df_table.to_csv(path, index=False)
            print(f"✅ Saved cleaned snapshot to {path}")
        except Exception as e:
            print(f"⚠️ Could not save cleaned snapshot for page {page}: {e}")

        # --- Log contribution ---
        print(f"DEBUG: Table {t.get('table_id')} contributed {len(df_table)} transactions (bank=UNKNOWN)")
        all_frames.append(df_table)

    # --- Merge all tables ---
    if not all_frames:
        print("⚠️ No valid tables were processed.")
        return pd.DataFrame()

    try:
        combined_df = pd.concat(all_frames, ignore_index=True)
        combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()].copy()
        combined_df = combined_df.reset_index(drop=True)
        print(f"✅ Successfully merged all tables into shape={combined_df.shape}")
        print(f"DEBUG: Combined columns: {combined_df.columns.tolist()}")
        return combined_df
    except Exception as e:
        print(f"⚠️ Failed to merge all tables directly: {e}")
        return pd.DataFrame()


# ---------- Internal cleaning helper ----------
def _clean_table_dataframe(df_table, normalize_text, parse_date_str, clean_amount, extract_channel, **kwargs):
    """
    Clean one table DataFrame in lossless debug mode.
    Keeps invalid rows and marks errors in 'row_issue'.
    Adds user-friendly parsed/unparsed display for UI.
    """
    import pandas as pd
    import os
    from datetime import datetime

    # --- Ensure unique column names to avoid reindexing errors (dedupe identical headers) ---
    def _dedupe_columns(cols):
        seen = {}
        out = []
        for c in cols:
            key = str(c)
            if key in seen:
                seen[key] += 1
                out.append(f"{key}.{seen[key]}")
            else:
                seen[key] = 0
                out.append(key)
        return out

    # apply dedupe
    df = df_table.copy()
    try:
        df.columns = _dedupe_columns(df.columns.tolist())
    except Exception:
        # fallback: keep original if anything unexpected happens
        df = df_table.copy()


    # --- Normalize text safely ---
    df = df.map(lambda v: normalize_text(v) if pd.notna(v) else "")

    # --- Rename dynamically ---
    rename_map = {}
    for col in df.columns:
        cl = str(col).lower()
        if "trans" in cl and "time" in cl:
            rename_map[col] = "date"
        elif "value" in cl and "date" in cl:
            rename_map[col] = "value_date"
        elif "desc" in cl:
            rename_map[col] = "description"
        elif "debit" in cl and "credit" in cl or "debit/credit" in cl:
            rename_map[col] = "debit_credit"
        elif "balance" in cl:
            rename_map[col] = "balance"
        elif "channel" in cl:
            rename_map[col] = "channel"
        elif "ref" in cl:
            rename_map[col] = "transaction_reference"
    df.rename(columns=rename_map, inplace=True)

    # --- Prepare required columns with defaults ---
    expected = ["date", "value_date", "description", "debit_credit", "balance", "channel", "transaction_reference"]
    for c in expected:
        if c not in df.columns:
            df[c] = ""

    # --- Convert and parse dates safely ---
    parsed_dates = []
    for i in range(len(df)):
        raw_val = str(df.iloc[i][df.columns.get_loc("date")]) if "date" in df.columns else ""
        parsed_dt = parse_date_str(raw_val)

        if parsed_dt:
            parsed_dates.append(parsed_dt.strftime("%b. %d, %Y"))  # e.g., "May. 14, 2025"
        else:
            parsed_dates.append("❌ Unparsed")

    df["Parsed Date"] = parsed_dates
    # Handle duplicate 'date' columns safely
    if "date" in df.columns:
        # if duplicate columns exist, take the first
        if isinstance(df["date"], pd.DataFrame):
            df["Raw Date"] = df["date"].iloc[:, 0].astype(str)
        else:
            df["Raw Date"] = df["date"].astype(str)
    else:
        df["Raw Date"] = ""


    # --- Apply cleaners for numeric fields ---
    df["balance"] = df["balance"].apply(clean_amount)
    dc = df.get("debit_credit", pd.Series([""] * len(df))).astype(str)
    df["debit"] = dc.apply(lambda x: clean_amount(x) if "-" in x else 0.0)
    df["credit"] = dc.apply(lambda x: clean_amount(x) if "+" in x else 0.0)

    # --- Channel & description cleanup ---
    df["channel"] = df.get("channel", pd.Series([""] * len(df))).apply(extract_channel)
    df["description"] = df.get("description", "")
    df["transaction_reference"] = df.get("transaction_reference", "")

    # --- Detect per-row issues ---
    def detect_issues(row):
        issues = []
        if row["Parsed Date"] == "❌ Unparsed":
            issues.append("invalid_date")
        if isinstance(row["balance"], str) and "INVALID_AMOUNT" in row["balance"]:
            issues.append("invalid_balance")
        if row["channel"] == "EMPTY":
            issues.append("missing_channel")
        return ", ".join(issues)

    df["row_issue"] = df.apply(detect_issues, axis=1)

    # --- Column order for UI ---
    cols = [
        "Parsed Date",
        "value_date",
        "Raw Date",
        "description",
        "debit",
        "credit",
        "balance",
        "channel",
        "transaction_reference",
        "row_issue",
    ]
    df = df[[c for c in cols if c in df.columns]]

    # --- Save snapshot for inspection ---
    DEBUG_DIR = os.path.join(getattr(settings, "BASE_DIR", "."), "debug_exports")
    os.makedirs(DEBUG_DIR, exist_ok=True)
    stamp = int(datetime.utcnow().timestamp())
    snapshot_path = os.path.join(DEBUG_DIR, f"cleaned_table_snapshot_{stamp}.csv")
    try:
        df.to_csv(snapshot_path, index=False)
        print(f"✅ Saved cleaned snapshot to {snapshot_path}")
    except Exception as e:
        print(f"⚠️ Failed to save cleaned snapshot: {e}")

    return df
