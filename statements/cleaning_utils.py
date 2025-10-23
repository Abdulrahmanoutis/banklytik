
# statements/cleaning_utils.py
import pandas as pd
import re
from datetime import datetime


# ---------------------------------------------------------------------
# TEXT NORMALIZATION
# ---------------------------------------------------------------------
def normalize_text(value):
    """Normalize OCR text by stripping spaces, newlines, and hidden characters."""
    if pd.isna(value):
        return ""
    s = str(value).strip()
    s = s.replace("\n", " ").replace("\r", " ").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\x20-\x7E\u00A0-\uFFFF]", "", s)  # remove control chars
    return s.strip()


# ---------------------------------------------------------------------
# DATE PARSING
# ---------------------------------------------------------------------
def parse_date_str(date_str):
    """
    Parse various statement date formats.
    Returns:
        datetime if parsed, else string marker: "INVALID_DATE: <original>"
    """
    if pd.isna(date_str):
        return "INVALID_DATE: NaN"

    s = str(date_str).strip()
    if s in ["", "nan", "none", "null", "0.0"]:
        return "INVALID_DATE: empty"

    s = s.replace(".", " ").replace(",", "")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"(\d{2})([A-Za-z]{3,})", r"\1 \2", s)

    formats = [
        "%Y %b %d %H:%M %S", "%Y %b %d %H:%M:%S",
        "%Y %b %d %H:%M", "%d %b %Y %H:%M %S",
        "%d %b %Y %H:%M:%S", "%d %b %Y %H:%M",
        "%Y %b %d", "%d %b %Y", "%b %Y",
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(s, fmt)
            if fmt == "%b %Y":
                parsed = parsed.replace(day=1)
            return parsed
        except Exception:
            continue

    try:
        parsed = pd.to_datetime(s, errors="raise", dayfirst=True)
        return parsed
    except Exception:
        return f"INVALID_DATE: {s}"


# ---------------------------------------------------------------------
# AMOUNT CLEANING
# ---------------------------------------------------------------------
def clean_amount(value):
    """
    Convert ₦ amounts to float.
    If invalid, return "INVALID_AMOUNT:<original>" string instead of dropping.
    """
    if pd.isna(value):
        return "INVALID_AMOUNT: NaN"

    s = str(value).replace("₦", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        s_clean = re.sub(r"[^0-9.\-]", "", s)
        try:
            return float(s_clean)
        except Exception:
            return f"INVALID_AMOUNT: {s}"


# ---------------------------------------------------------------------
# CHANNEL EXTRACTION
# ---------------------------------------------------------------------
def extract_channel(desc):
    """Guess transaction channel based on description text."""
    if pd.isna(desc) or not str(desc).strip():
        return "EMPTY"
    d = str(desc).upper()
    if "AIRTIME" in d:
        return "AIRTIME"
    if "TRANSFER" in d:
        return "TRANSFER"
    if "POS" in d:
        return "POS"
    if "ATM" in d:
        return "ATM"
    if "CHARGE" in d or "FEE" in d or "USSD" in d:
        return "CHARGES"
    if "REVERSAL" in d:
        return "REVERSAL"
    return "OTHER"


# ---------------------------------------------------------------------
# ROBUST CLEANING (Legacy fallback)
# ---------------------------------------------------------------------
def robust_clean_dataframe(df_raw):
    """
    Legacy fallback cleaner — kept for compatibility.
    Updated to not drop rows or raise parsing errors.
    """
    print("DEBUG: robust_clean_dataframe input shape:", df_raw.shape)

    df = df_raw.copy()
    df = df.applymap(lambda v: normalize_text(v) if pd.notna(v) else "")

    # Simple heuristic headers
    headers = [
        "Trans. Time", "Value Date", "Description",
        "Debit/Credit(W)", "Balance(N)", "Channel", "Transaction Reference"
    ]
    df.columns = headers[:len(df.columns)]

    # Apply safe cleaners
    df["date"] = df["Trans. Time"].apply(parse_date_str)
    df["value_date"] = df["Value Date"].apply(parse_date_str)
    df["description"] = df["Description"]
    df["balance"] = df["Balance(N)"].apply(clean_amount)

    # Split Debit/Credit
    dc = df["Debit/Credit(W)"].astype(str)
    df["debit"] = dc.apply(lambda x: clean_amount(x) if "-" in x else 0.0)
    df["credit"] = dc.apply(lambda x: clean_amount(x) if "+" in x else 0.0)

    # Channel inference
    df["channel"] = df["Channel"].apply(extract_channel)
    df["transaction_reference"] = df["Transaction Reference"]

    # Build row_issue column
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
        return ", ".join(issues) if issues else ""

    df["row_issue"] = df.apply(detect_issues, axis=1)

    print("DEBUG: Cleaned shape (lossless mode):", df.shape)
    return df[
        ["date", "value_date", "description", "debit", "credit", "balance", "channel", "transaction_reference", "row_issue"]
    ]