# statements/cleaning_utils.py

import re
import dateparser
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

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
# DATE PARSING (Robust with OCR Fix)
# ---------------------------------------------------------------------
def fix_missing_space_date(date_str):
    """
    Fix dates with missing space between day and time or small OCR glitches like:
    "2025 Feb 2310:00 48" -> "2025 Feb 23 10:00 48"
    "2025 Feb 23 20:11: 58" -> "2025 Feb 23 20:11:58"
    """
    if not isinstance(date_str, str):
        return date_str

    # Pattern 1: Year Month DayTime Seconds
    pattern1 = r'(\d{4}\s+[A-Za-z]{3,}\s+)(\d{2})(\d{2}:\d{2}\s+\d{2})'
    fixed1 = re.sub(pattern1, r'\1\2 \3', date_str)
    if fixed1 != date_str:
        print(f"DEBUG: Fixed missing space pattern1: '{date_str}' -> '{fixed1}'")
        return fixed1

    # Pattern 2: Year Month DayTime (no seconds)
    pattern2 = r'(\d{4}\s+[A-Za-z]{3,}\s+)(\d{2})(\d{2}:\d{2})'
    fixed2 = re.sub(pattern2, r'\1\2 \3', date_str)
    if fixed2 != date_str:
        print(f"DEBUG: Fixed missing space pattern2: '{date_str}' -> '{fixed2}'")
        return fixed2

    # Pattern 3: 23Feb2025 10:00 -> 23 Feb 2025 10:00
    pattern3 = r'(\d{2})([A-Za-z]{3,})(\d{4}\s+\d{2}:\d{2})'
    fixed3 = re.sub(pattern3, r'\1 \2 \3', date_str)
    if fixed3 != date_str:
        print(f"DEBUG: Fixed missing space pattern3: '{date_str}' -> '{fixed3}'")
        return fixed3

    # Pattern 4: Generic fallback - e.g. "2310:00" -> "23 10:00"
    pattern4 = r'(\d{2})(\d{2}:\d{2})'
    def repl(m): return f"{m.group(1)} {m.group(2)}"
    fixed4 = re.sub(pattern4, repl, date_str)
    if fixed4 != date_str:
        print(f"DEBUG: Fixed missing space pattern4: '{date_str}' -> '{fixed4}'")
        return fixed4

    # 🩹 Pattern 5: Fix "extra space before seconds" issue, e.g. "20:11: 58"
    pattern5 = r'(\d{2}:\d{2}):\s+(\d{2})'
    fixed5 = re.sub(pattern5, r'\1:\2', date_str)
    if fixed5 != date_str:
        print(f"DEBUG: Fixed colon-space pattern5: '{date_str}' -> '{fixed5}'")
        return fixed5

    return date_str



def parse_date_str(date_str):
    """
    Parse bank statement dates with robust fallback mechanisms.
    Handles OCR errors like missing spaces between day and time.
    """
    if pd.isna(date_str):
        return None

    s = str(date_str).strip()
    if s.lower() in ["", "nan", "none", "null", "0.0"]:
        return None

    print(f"DEBUG: Attempting to parse date: '{s}'")

    # Step 1: Fix malformed OCR spacing
    s_fixed = fix_missing_space_date(s)
    if s_fixed != s:
        print(f"DEBUG: Applied space fix: '{s}' -> '{s_fixed}'")
        s = s_fixed

    # Step 2: Try dateparser
    try:
        parsed = dateparser.parse(
            s,
            settings={
                'DATE_ORDER': 'DMY',
                'PREFER_DAY_OF_MONTH': 'first',
                'PREFER_DATES_FROM': 'current_period',
                'RETURN_AS_TIMEZONE_AWARE': False
            }
        )
        if parsed:
            print(f"DEBUG: dateparser successfully parsed '{s}' -> {parsed}")
            return parsed
    except Exception as e:
        print(f"DEBUG: dateparser failed for '{s}': {e}")

    # Step 3: Try manual datetime formats
    s_clean = re.sub(r"[.,-]", " ", s)
    s_clean = re.sub(r"\s+", " ", s_clean).strip()

    formats = [
        "%Y %b %d %H:%M %S",
        "%Y %b %d %H:%M:%S",
        "%Y %b %d %H:%M",
        "%d %b %Y %H:%M %S",
        "%d %b %Y %H:%M:%S",
        "%d %b %Y %H:%M",
        "%Y %b %d",
        "%d %b %Y",
        "%b %Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%m/%d/%Y",
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(s_clean, fmt)
            if fmt == "%b %Y":
                parsed = parsed.replace(day=1)
            print(f"DEBUG: strptime successfully parsed '{s}' -> {parsed} using format '{fmt}'")
            return parsed
        except ValueError:
            continue

    # Step 4: Try pandas fallback
    try:
        parsed = pd.to_datetime(s_clean, errors="coerce", dayfirst=True)
        if not pd.isna(parsed):
            print(f"DEBUG: pandas successfully parsed '{s}' -> {parsed}")
            return parsed.to_pydatetime()
    except Exception as e:
        print(f"DEBUG: pandas failed for '{s}': {e}")

    print(f"DEBUG: All parsing methods failed for: '{s}'")
    return None

# ---------------------------------------------------------------------
# AMOUNT CLEANING
# ---------------------------------------------------------------------
def clean_amount(value):
    """Convert ₦ amounts to float safely."""
    if pd.isna(value):
        return 0.0

    s = str(value).replace("₦", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        s_clean = re.sub(r"[^0-9.\-]", "", s)
        try:
            return float(s_clean)
        except Exception:
            return 0.0

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
# ROBUST CLEANING PIPELINE
# ---------------------------------------------------------------------
def robust_clean_dataframe(df_raw):
    """
    Clean extracted statement tables safely and robustly.
    Handles OCR noise, malformed dates, and numeric issues.
    """
    print("DEBUG: robust_clean_dataframe input shape:", df_raw.shape)

    df = df_raw.copy()
    df = df.applymap(lambda v: normalize_text(v) if pd.notna(v) else "")

    # Basic column normalization
    headers = [
        "Trans. Time", "Value Date", "Description",
        "Debit/Credit(W)", "Balance(N)", "Channel", "Transaction Reference"
    ]
    df.columns = headers[:len(df.columns)]

    # Debug: print first few date strings
    print("DEBUG: First 5 date strings in 'Trans. Time':")
    for i, date_str in enumerate(df["Trans. Time"].head(5)):
        print(f"  {i}: '{date_str}'")

    # Preserve raw date
    df["raw_date"] = df["Trans. Time"]

    # Apply cleaning functions
    df["date"] = df["Trans. Time"].apply(parse_date_str)
    df["value_date"] = df["Value Date"].apply(parse_date_str)
    df["description"] = df["Description"]
    df["balance"] = df["Balance(N)"].apply(clean_amount)

    # Split Debit/Credit column
    dc = df["Debit/Credit(W)"].astype(str)
    df["debit"] = dc.apply(lambda x: clean_amount(x) if "-" in x else 0.0)
    df["credit"] = dc.apply(lambda x: clean_amount(x) if "+" in x else 0.0)

    # Infer channel and reference
    df["channel"] = df["Channel"].apply(extract_channel)
    df["transaction_reference"] = df["Transaction Reference"]

    # Detect row-level issues
    def detect_issues(row):
        issues = []
        if row["date"] is None:
            issues.append("invalid_date")
        if row["value_date"] is None:
            issues.append("invalid_value_date")
        if isinstance(row["balance"], str) and "INVALID_AMOUNT" in row["balance"]:
            issues.append("invalid_balance")
        if row["channel"] == "EMPTY":
            issues.append("missing_channel")
        return ", ".join(issues) if issues else ""

    df["row_issue"] = df.apply(detect_issues, axis=1)

    print("DEBUG: Cleaned shape:", df.shape)
    print("DEBUG: Date parsing summary:")
    print(f"  - Valid dates: {df['date'].notna().sum()}")
    print(f"  - Invalid dates: {df['date'].isna().sum()}")

    return df[
        ["date", "raw_date", "value_date", "description", "debit", "credit",
         "balance", "channel", "transaction_reference", "row_issue"]
    ]
