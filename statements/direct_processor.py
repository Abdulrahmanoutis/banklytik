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
                # Ensure df_table is a valid DataFrame before cleaning
                if not isinstance(df_table, pd.DataFrame):
                    print(f"⚠️ Table {table_id} has non-DataFrame df_table ({type(df_table)}); skipping.")
                    report["skipped_reason"] = f"invalid_type_{type(df_table)}"
                    table_reports.append(report)
                    continue

                # Defensive: sometimes df_table becomes Series for single-row tables
                if isinstance(df_table, pd.Series):
                    df_table = df_table.to_frame().T.reset_index(drop=True)

                
                try:
                    # 🔍 Diagnostic: show what type/shape the incoming df_table is
                    print(f"DEBUG: Cleaning table {table_id} (page={page}) type={type(df_table)}")
                    if isinstance(df_table, pd.DataFrame):
                        print(f"DEBUG: df_table shape={df_table.shape}, columns={list(df_table.columns)}")
                        print(f"DEBUG: First 3 rows preview:\n{df_table.head(3)}")
                    else:
                        print(f"DEBUG: df_table is NOT a DataFrame (type={type(df_table)})")

                    cleaned = _clean_table_dataframe(
                        df_table,
                        normalize_text,
                        parse_date_str,
                        clean_amount,
                        extract_channel,
                        table_id=table_id,
                        page=page,
                        debug_run_id=debug_run_id,
                    )
                except Exception as e_clean:
                    tb = traceback.format_exc()
                    print(f"DEBUG: Cleaning failed for table {table_id}: {e_clean}")
                    safe_save({
                        "table_id": table_id,
                        "error": str(e_clean),
                        "trace": tb,
                        "df_type": str(type(df_table)),
                        "df_columns": list(df_table.columns) if hasattr(df_table, "columns") else None,
                        "df_shape": df_table.shape if hasattr(df_table, "shape") else None,
                        "df_sample": df_table.head(5).astype(str).values.tolist() if isinstance(df_table, pd.DataFrame) else str(df_table)
                    }, f"directproc_{debug_run_id}_table_{table_id}_clean_error.json")
                    report["error"] = str(e_clean)
                    table_reports.append(report)
                    continue


           


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
def _clean_table_dataframe(df_table, normalize_text, parse_date_str, clean_amount, extract_channel,
                           table_id=None, page=None, debug_run_id=None):
    """
    Clean one table DataFrame safely and robustly.

    - Coerces any Series -> scalar where needed to avoid ambiguous truth-value errors.
    - Uses per-column normalization with `.apply(... .map(...))` to avoid deprecated applymap issues.
    - Returns a DataFrame with stable columns:
       ["date","value_date","description","debit","credit","balance","channel","transaction_reference","row_issue"]
    - Writes a per-table date debug log to debug_exports/date_debug_table_<debug_run_id>_<table_id>.json
    """
    import pandas as pd
    import os, json
    from datetime import datetime

    # Defensive conversion: if a single-row DataFrame became a Series, convert back
    if isinstance(df_table, pd.Series):
        df = df_table.to_frame().T.reset_index(drop=True)
    else:
        df = df_table.copy()
        
    # --- Fix duplicate and empty column names ---
    df.columns = [str(c).strip() if str(c).strip() != "" else f"col_{i}" for i, c in enumerate(df.columns)]

    # Drop exact duplicate column names by keeping first occurrence
    deduped_cols = []
    for c in df.columns:
        if c not in deduped_cols:
            deduped_cols.append(c)
    df = df.loc[:, deduped_cols]

    # If any column label repeats, rename remaining duplicates with suffixes
    df = df.loc[:, ~df.columns.duplicated()].copy()

    print(f"DEBUG: Normalizing columns → {list(df.columns)}")
    # Normalize every cell safely (column-wise)
    try:
        df = df.apply(lambda col: col.map(lambda v: normalize_text(v) if pd.notna(v) else ""))
    except Exception:
        # fallback: coerce all to str
        df = df.applymap(lambda v: str(v) if pd.notna(v) else "")

    # --- Rename columns dynamically (tolerant matching) ---
    rename_map = {}
    for col in df.columns:
        cl = str(col).lower()
        if "trans" in cl and "time" in cl:
            rename_map[col] = "date"
        elif "value" in cl and "date" in cl:
            rename_map[col] = "value_date"
        elif "desc" in cl or "narr" in cl or "detail" in cl:
            rename_map[col] = "description"
        elif "debit" in cl and "credit" in cl or "debit/credit" in cl:
            rename_map[col] = "debit_credit"
        elif "debit" in cl and "credit" not in cl:
            rename_map[col] = "debit"
        elif "credit" in cl and "debit" not in cl:
            rename_map[col] = "credit"
        elif "balance" in cl:
            rename_map[col] = "balance"
        elif "channel" in cl or "mode" in cl or "type" in cl or "category" in cl:
            rename_map[col] = "channel"
        elif "ref" in cl or "txn" in cl or "id" in cl:
            rename_map[col] = "transaction_reference"
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    # Ensure expected columns exist as scalar series
    expected_cols = ["date", "value_date", "description", "debit", "credit", "balance", "channel", "transaction_reference"]
    for c in expected_cols:
        if c not in df.columns:
            df[c] = pd.Series([""] * len(df), dtype=object)

    # --- Safe parsing and conversions ---
    # Use apply on series (safe scalar per-cell)
    try:
        df["date"] = df["date"].apply(lambda x: parse_date_str(x) if (not (pd.isna(x) or str(x).strip() == "")) else None)
    except Exception:
        # fallback: ensure no crash; mark as None
        df["date"] = pd.Series([None] * len(df))

    try:
        df["value_date"] = df["value_date"].apply(lambda x: parse_date_str(x) if (not (pd.isna(x) or str(x).strip() == "")) else None)
    except Exception:
        df["value_date"] = pd.Series([None] * len(df))

    # Balance / amount cleaning
    def safe_clean_amount(v):
        try:
            return clean_amount(v)
        except Exception:
            try:
                # ensure numeric fallback
                if isinstance(v, (int, float)):
                    return float(v)
                s = str(v).strip()
                s = s.replace(",", "")
                return float(s) if s not in ["", "NaN", "nan"] else 0.0
            except Exception:
                return 0.0

    df["balance"] = df["balance"].apply(safe_clean_amount)

    # Handle combined debit/credit or separate columns
    if "debit_credit" in df.columns:
        dc = df["debit_credit"].astype(str)
        df["debit"] = dc.apply(lambda x: safe_clean_amount(x) if "-" in x else 0.0)
        df["credit"] = dc.apply(lambda x: safe_clean_amount(x) if "+" in x else 0.0)
    else:
        df["debit"] = df["debit"].apply(lambda x: safe_clean_amount(x))
        df["credit"] = df["credit"].apply(lambda x: safe_clean_amount(x))

    # Channel and description as cleaned strings
    df["channel"] = df["channel"].apply(lambda x: extract_channel(str(x)) if (not pd.isna(x) and str(x).strip() != "") else "EMPTY")
    df["description"] = df["description"].apply(lambda x: str(x).strip() if not pd.isna(x) else "")
    df["transaction_reference"] = df["transaction_reference"].apply(lambda x: str(x).strip() if not pd.isna(x) else "")

    # --- Detect issues per row (no ambiguous boolean checks) ---
    def detect_issues(row):
        issues = []
        # extract scalars defensively
        date_val = row.get("date") if hasattr(row, "get") else (row["date"] if "date" in row else None)
        balance_val = row.get("balance") if hasattr(row, "get") else (row["balance"] if "balance" in row else 0.0)
        channel_val = row.get("channel") if hasattr(row, "get") else (row["channel"] if "channel" in row else "EMPTY")

        # coerce types
        try:
            dv = date_val
            if isinstance(dv, pd.Series):
                dv = dv.iloc[0] if len(dv) > 0 else None
        except Exception:
            dv = date_val

        try:
            bv = balance_val
            if isinstance(bv, pd.Series):
                bv = bv.iloc[0] if len(bv) > 0 else "INVALID_AMOUNT"
        except Exception:
            bv = balance_val

        try:
            cv = channel_val
            if isinstance(cv, pd.Series):
                cv = cv.iloc[0] if len(cv) > 0 else "EMPTY"
        except Exception:
            cv = channel_val

        if dv is None or (isinstance(dv, str) and "INVALID_DATE" in dv):
            issues.append("invalid_date")
        if isinstance(bv, str) and "INVALID_AMOUNT" in bv:
            issues.append("invalid_balance")
        if str(cv).strip().upper() == "EMPTY":
            issues.append("missing_channel")

        return ", ".join(issues)

    df["row_issue"] = df.apply(detect_issues, axis=1)

    # --- Column order and ensure presence ---
    cols = ["date", "value_date", "description", "debit", "credit", "balance", "channel", "transaction_reference", "row_issue"]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.Series([pd.NA] * len(df))

    df = df[cols]

    # --- Date debug logging (raw -> parsed) ---
    try:
        debug_dir = DEBUG_DIR if 'DEBUG_DIR' in globals() else os.path.join(os.getcwd(), "debug_exports")
        os.makedirs(debug_dir, exist_ok=True)
        timestamp = debug_run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        tid = table_id or "unknown"
        p = page or "unknown"
        log_path = os.path.join(debug_dir, f"date_debug_table_{tid}_{timestamp}.json")
        date_logs = []
        # Attempt to get raw date column from original df_table if present; fallback to df["date"] string
        for i in range(len(df)):
            raw_val = ""
            try:
                if isinstance(df_table, pd.DataFrame) and "date" in df_table.columns:
                    raw_val = str(df_table.iloc[i][df_table.columns.get_loc("date")]) if "date" in df_table.columns else ""
                else:
                    # fallback: try first column from original table
                    if isinstance(df_table, pd.DataFrame) and len(df_table.columns) > 0:
                        raw_val = str(df_table.iloc[i, 0])
            except Exception:
                raw_val = ""
            parsed_val = df["date"].iloc[i]
            desc = df["description"].iloc[i] if "description" in df.columns else ""
            date_logs.append({
                "table_id": tid,
                "page": p,
                "row_index": int(i),
                "raw_date": raw_val,
                "parsed": str(parsed_val),
                "description": str(desc),
            })
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(date_logs, f, indent=2, default=str)
        print(f"✅ Saved date debug log to {log_path}")
    except Exception as e:
        print(f"⚠️ Failed to save date debug log: {e}")

    # Save a CSV snapshot for inspection (non-fatal)
    try:
        stamp = int(datetime.utcnow().timestamp())
        df.to_csv(os.path.join(debug_dir, f"cleaned_table_snapshot_{table_id or 't'}_{stamp}.csv"), index=False)
    except Exception:
        pass

    return df
