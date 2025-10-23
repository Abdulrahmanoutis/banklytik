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

    all_transactions = []
    table_reports = []

    try:
        for table in tables:
            try:
                table_id = table.get("table_id", "unknown")
                page = table.get("page", "unknown")
                df = table.get("df")
                report = {"table_id": table_id, "page": page, "original_shape": None}
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
                    # data rows start at next row
                    df_table = df.iloc[1:].copy().reset_index(drop=True)
                    # set columns to detected headers (normalize text)
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
                    all_transactions.append(cleaned)
                    print(f"DEBUG: Table {table_id} contributed {len(cleaned)} transactions")
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
            }
            safe_save(final_report, f"directproc_{debug_run_id}_final_report.json")
        else:
            final_report = {"tables_processed": len(table_reports), "rows_extracted": 0, "table_reports": table_reports}
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
    Clean one table DataFrame in lossless debug mode.
    Keeps invalid rows and marks errors in 'row_issue'.
    """
    df = df_table.copy()

    # Normalize every cell safely
    df = df.map(lambda v: normalize_text(v) if pd.notna(v) else "")

    # --- Rename columns dynamically ---
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

    # --- Safe conversions ---
    df["date"] = df.get("date", pd.Series([""] * len(df))).apply(parse_date_str)
    df["value_date"] = df.get("value_date", pd.Series([""] * len(df))).apply(parse_date_str)
    df["balance"] = df.get("balance", pd.Series([""] * len(df))).apply(clean_amount)

    dc = df.get("debit_credit", pd.Series(["0"] * len(df))).astype(str)
    df["debit"] = dc.apply(lambda x: clean_amount(x) if "-" in x else 0.0)
    df["credit"] = dc.apply(lambda x: clean_amount(x) if "+" in x else 0.0)
    df["channel"] = df.get("channel", pd.Series([""] * len(df))).apply(extract_channel)
    df["description"] = df.get("description", "")
    df["transaction_reference"] = df.get("transaction_reference", "")

    # --- Detect issues per row ---
    def detect_issues(row):
        issues = []
        if isinstance(row["date"], str) and "INVALID_DATE" in row["date"]:
            issues.append("invalid_date")
        if isinstance(row["value_date"], str) and "INVALID_DATE" in row["value_date"]:
            issues.append("invalid_value_date")
        if isinstance(row["balance"], str) and "INVALID_AMOUNT" in row["balance"]:
            issues.append("invalid_balance")
        if row["channel"] == "EMPTY":
            issues.append("missing_channel")
        return ", ".join(issues)

    df["row_issue"] = df.apply(detect_issues, axis=1)

    # --- Column order ---
    cols = ["date", "value_date", "description", "debit", "credit", "balance", "channel", "transaction_reference", "row_issue"]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[cols]

    # --- Extra debugging: flag invalid date rows ---
    invalid_dates = df[df["date"].astype(str).str.startswith("INVALID_DATE")]
    if not invalid_dates.empty:
        print(f"DEBUG: Found {len(invalid_dates)} rows with invalid date formats.")
        print(invalid_dates[["date", "description", "row_issue"]].head(5))

    # Save CSV snapshot for inspection
    stamp = int(datetime.utcnow().timestamp())
    df.to_csv(os.path.join(DEBUG_DIR, f"cleaned_table_snapshot_{stamp}.csv"), index=False)
    return df
