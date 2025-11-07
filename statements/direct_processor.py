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
def process_tables_directly(tables, stmt_pk=None):
    """
    Main entry — process a list of table dicts (each table contains 'df', 'table_id', 'page', ...).
    This function is defensive: it will capture exceptions, write debug artifacts, and return either
    a cleaned DataFrame or an empty DataFrame.
    """
    debug_run_id = f"stmt_{stmt_pk or 'unknown'}_{int(datetime.utcnow().timestamp())}"
    safe_save({"tables_count": len(tables)}, f"directproc_{debug_run_id}_meta.json")

    # Lazy imports from cleaning_utils to avoid import-time failure if cleaning_utils is broken.
    try:
        from .cleaning_utils import (
            normalize_text,
            parse_date_str,
            clean_amount,
            extract_channel,
        )
    except Exception as e:
        tb = traceback.format_exc()
        err = {"error": "Failed to import cleaning utilities", "exception": str(e), "trace": tb}
        print("DEBUG: Direct processor import error:", err)
        safe_save(err, f"directproc_{debug_run_id}_import_error.json")
        # Return empty DataFrame so upstream falls back
        return pd.DataFrame()

    # -----------------------
    # Bank detection & knowledge load (new)
    # -----------------------
    detected_bank = "UNKNOWN"
    try:
        # Collect a small sample of text from the tables to run bank detection on
        sample_texts = []
        for t in tables[:6]:  # limit to first few tables for speed
            df_sample = t.get("df")
            if df_sample is None:
                continue
            # take first 3 rows and first 6 columns as text sample
            try:
                sample_rows = df_sample.head(3).astype(str).fillna("").values.tolist()
                for r in sample_rows:
                    sample_texts.append(" ".join([str(c) for c in r[:6]]))
            except Exception:
                continue
        joined_sample = " ".join(sample_texts)[:20000]  # cap length
        if joined_sample.strip():
            try:
                # local import to avoid circular issues
                from .bank_detection import detect_bank_from_text
                detected_bank = detect_bank_from_text(joined_sample)
            except Exception as e:
                print("DEBUG: bank_detection failed:", e)
                detected_bank = "UNKNOWN"
    except Exception as e:
        print("DEBUG: Unexpected error during bank detection sample collection:", e)
        detected_bank = "UNKNOWN"

    # Try to load base knowledge and bank-specific rules (lazy import)
    try:
        from banklytik_core.knowledge_loader import reload_knowledge, load_bank_rules
        try:
            reload_knowledge()
        except Exception as e:
            print("DEBUG: reload_knowledge() failed:", e)
        # Attempt to load bank-specific rules (no-op if none found)
        try:
            loaded_bank_rules = False
            if detected_bank and detected_bank != "UNKNOWN":
                loaded_bank_rules = load_bank_rules(detected_bank)
            safe_save({"detected_bank": detected_bank, "loaded_bank_rules": bool(loaded_bank_rules)},
                      f"directproc_{debug_run_id}_bankinfo.json")
        except Exception as e:
            print("DEBUG: load_bank_rules failed:", e)
    except Exception as e:
        print("DEBUG: Could not import knowledge_loader to load bank rules:", e)

    all_transactions = []
    table_reports = []

    try:
        for table in tables:
            try:
                table_id = table.get("table_id", "unknown")
                page = table.get("page", "unknown")
                df = table.get("df")
                report = {"table_id": table_id, "page": page, "original_shape": None, "detected_bank": detected_bank}
                if df is None:
                    report["skipped_reason"] = "no_df"
                    table_reports.append(report)
                    continue

                report["original_shape"] = list(df.shape)

                # quick cleanup: ensure every cell normalized string for header detection
                df_preview = df.head(5).astype(str).replace("nan", "", regex=False)
                report["preview_rows"] = df_preview.values.tolist()

                # detect structure
                structure = detect_table_structure(df)
                report["structure"] = structure

                # skip too small or obviously not transaction (but keep sample)
                if df.empty or len(df) < 1:
                    report["skipped_reason"] = "empty_or_too_small"
                    table_reports.append(report)
                    continue

                # If the first row looks like headers, take them
                if structure == "with_headers":
                    headers = df.iloc[0].tolist()
                    report["detected_headers"] = headers

                    # --- NEW: AI-assisted header normalization ---
                    try:
                        from .header_detector import detect_headers_ai
                        mapping = detect_headers_ai(headers)
                        normalized_headers = [mapping.get(h, h) for h in headers]
                        report["ai_header_mapping"] = mapping
                        df_table = df.iloc[1:].copy().reset_index(drop=True)
                        df_table.columns = normalized_headers
                    except Exception as e:
                        print("DEBUG: header_detector failed:", e)
                        df_table = df.iloc[1:].copy().reset_index(drop=True)
                        df_table.columns = [normalize_text(h) for h in headers]
                else:
                    # data_only -> assign known column positions
                    report["detected_headers"] = None
                    known_headers = [
                        "Trans. Time",
                        "Value Date",
                        "Description",
                        "Debit/Credit(W)",
                        "Balance(N)",
                        "Channel",
                        "Transaction Reference",
                    ]
                    df_table = df.copy().reset_index(drop=True)
                    # If there are fewer columns, trim known_headers
                    ncols = len(df_table.columns)
                    assigned = known_headers[:ncols]
                    df_table.columns = assigned
                    report["assigned_headers"] = assigned

                # Save a CSV snapshot for debugging
                csv_name = f"table_{table_id}_page_{page}_snapshot_{debug_run_id}.csv"
                try:
                    df_table.to_csv(os.path.join(DEBUG_DIR, csv_name), index=False)
                except Exception:
                    safe_save({"error": "failed_to_save_csv"}, f"table_{table_id}_page_{page}_snapshot_{debug_run_id}.json")

                # Clean table using robust in-function steps (not relying on external code to avoid raising)
                cleaned = _clean_table_dataframe(df_table, normalize_text, parse_date_str, clean_amount, extract_channel)

                # Add metadata and append if not empty
                report["before_rows"] = len(df_table)
                report["after_rows"] = len(cleaned)
                table_reports.append(report)

                if not cleaned.empty:
                    # annotate with detected bank at row level for traceability
                    cleaned["detected_bank"] = detected_bank
                    all_transactions.append(cleaned)
                    print(f"DEBUG: Table {table_id} contributed {len(cleaned)} transactions (bank={detected_bank})")
                else:
                    print(f"DEBUG: Table {table_id} yielded 0 transactions after cleaning")

            except Exception as e_table:
                # capture any per-table exception and continue
                tb = traceback.format_exc()
                print(f"DEBUG: Exception processing table {table.get('table_id')}: {e_table}")
                safe_save({"table": table.get("table_id"), "error": str(e_table), "trace": tb},
                          f"directproc_{debug_run_id}_table_{table.get('table_id')}_error.json")
                table_reports.append({"table_id": table.get("table_id"), "error": str(e_table)})
                continue

        # merge all
        if all_transactions:
            final_df = pd.concat(all_transactions, ignore_index=True)
        else:
            final_df = pd.DataFrame()

        # final validations: check duplicates and obvious invalid rows
        if not final_df.empty:
            # normalize columns expected
            expected = ["date", "value_date", "description", "debit", "credit", "balance", "channel", "transaction_reference"]
            for c in expected:
                if c not in final_df.columns:
                    final_df[c] = pd.NA

            # create a small report
            final_report = {
                "tables_processed": len(table_reports),
                "rows_extracted": int(len(final_df)),
                "table_reports": table_reports,
                "detected_bank": detected_bank,
            }
            safe_save(final_report, f"directproc_{debug_run_id}_final_report.json")
        else:
            final_report = {"tables_processed": len(table_reports), "rows_extracted": 0, "table_reports": table_reports, "detected_bank": detected_bank}
            safe_save(final_report, f"directproc_{debug_run_id}_final_report.json")

        return final_df

    except Exception as e:
        tb = traceback.format_exc()
        print("DEBUG: Unexpected error in process_tables_directly:", e)
        safe_save({"error": str(e), "trace": tb}, f"directproc_{debug_run_id}_fatal_error.json")
        return pd.DataFrame()


# ---------- Internal cleaning helper ----------
def _clean_table_dataframe(df_table, normalize_text, parse_date_str, clean_amount, extract_channel):
    """
    Clean one table DataFrame safely.
    Handles Series-to-scalar issues and logs invalid dates.
    """
    import pandas as pd
    from datetime import datetime
    import os, json

    df = df_table.copy()

    # normalize all cells
    df = df.apply(lambda col: col.map(lambda v: normalize_text(v) if pd.notna(v) else ""))


    # --- Dynamic column rename mapping ---
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
        elif "debit" in cl:
            rename_map[col] = "debit"
        elif "credit" in cl:
            rename_map[col] = "credit"
        elif "balance" in cl:
            rename_map[col] = "balance"
        elif "channel" in cl:
            rename_map[col] = "channel"
        elif "ref" in cl:
            rename_map[col] = "transaction_reference"
    df.rename(columns=rename_map, inplace=True)

    # Ensure required columns exist
    for c in ["date","value_date","description","debit","credit","balance","channel","transaction_reference"]:
        if c not in df.columns:
            df[c] = ""

    # --- Parse and clean columns safely ---
    df["date"] = df["date"].apply(lambda x: parse_date_str(x) if isinstance(x,str) or not pd.isna(x) else None)
    df["value_date"] = df["value_date"].apply(lambda x: parse_date_str(x) if isinstance(x,str) or not pd.isna(x) else None)

    df["balance"] = df["balance"].apply(lambda x: clean_amount(x) if isinstance(x,(str,int,float)) else 0.0)

    if "debit_credit" in df.columns:
        dc = df["debit_credit"].astype(str)
        df["debit"] = dc.apply(lambda x: clean_amount(x) if "-" in x else 0.0)
        df["credit"] = dc.apply(lambda x: clean_amount(x) if "+" in x else 0.0)
    else:
        df["debit"] = df["debit"].apply(lambda x: clean_amount(x) if isinstance(x,(str,int,float)) else 0.0)
        df["credit"] = df["credit"].apply(lambda x: clean_amount(x) if isinstance(x,(str,int,float)) else 0.0)

    df["channel"] = df["channel"].apply(lambda x: extract_channel(str(x)) if pd.notna(x) else "EMPTY")

    # --- Detect issues ---
    def detect_issues(row):
        issues = []
        date_val = row.get("date", None)
        bal_val = row.get("balance", None)
        chan_val = row.get("channel", None)

        # force to scalar strings
        if isinstance(date_val, (pd.Series, list)):
            date_val = date_val.iloc[0] if isinstance(date_val, pd.Series) else (date_val[0] if date_val else None)
        if isinstance(bal_val, (pd.Series, list)):
            bal_val = bal_val.iloc[0] if isinstance(bal_val, pd.Series) else (bal_val[0] if bal_val else None)
        if isinstance(chan_val, (pd.Series, list)):
            chan_val = chan_val.iloc[0] if isinstance(chan_val, pd.Series) else (chan_val[0] if chan_val else None)

        if date_val is None or (isinstance(date_val, str) and "INVALID_DATE" in date_val):
            issues.append("invalid_date")
        if isinstance(bal_val, str) and "INVALID_AMOUNT" in bal_val:
            issues.append("invalid_balance")
        if str(chan_val).strip().upper() == "EMPTY":
            issues.append("missing_channel")

        return ", ".join(issues)


    df["row_issue"] = df.apply(detect_issues, axis=1)

    # --- Date debug log ---
    try:
        debug_dir = os.path.join(os.getcwd(), "debug_exports")
        os.makedirs(debug_dir, exist_ok=True)
        timestamp = int(datetime.utcnow().timestamp())
        log_path = os.path.join(debug_dir, f"date_debug_table_{timestamp}.json")
        date_logs = []
        for i, row in df.iterrows():
            date_logs.append({
                "row_index": int(i),
                "raw_date": str(df_table.iloc[i,0]) if len(df_table.columns)>0 else "",
                "parsed_date": str(row.get("date")),
                "description": str(row.get("description"))
            })
        with open(log_path,"w",encoding="utf-8") as f:
            json.dump(date_logs,f,indent=2,default=str)
        print(f"✅ Saved date debug log to {log_path}")
    except Exception as e:
        print(f"⚠️ Failed to save date debug log: {e}")

    # enforce consistent column order
    cols = ["date","value_date","description","debit","credit","balance","channel","transaction_reference","row_issue"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df[cols]
