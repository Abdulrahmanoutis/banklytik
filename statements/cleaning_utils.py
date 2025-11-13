import re
import os
import json
import pandas as pd
from datetime import datetime
from django.conf import settings

from banklytik_core.knowledge_loader import get_rules
from banklytik_core.deepseek_adapter import get_deepseek_patterns


# ---------------------------------------------------------------------
# TEXT NORMALIZATION
# ---------------------------------------------------------------------
def normalize_text(value):
    """Normalize OCR text by stripping spaces, newlines, and hidden characters."""
    if pd.isna(value):
        return ""
    s = str(value).replace("\n", " ").replace("\r", " ").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\x20-\x7E\u00A0-\uFFFF]", "", s)
    return s.strip()


# ---------------------------------------------------------------------
# DATE PARSING (Improved for Kuda format)
# ---------------------------------------------------------------------
# Put this function in statements/cleaning_utils.py (replace existing parse_date_str)

def parse_date_str(s_raw):
    """
    Parse a wide range of bank-statement date formats safely.
    Handles:
      - Kuda format: 'DD/MM/YY HH:MM:SS' or 'DD/MM/YY'
      - DD/MM/YYYY etc.
    Returns a naive datetime (caller may localize) or None.
    """
    if s_raw is None:
        return None

    s = str(s_raw).strip()
    if s == "" or s.lower() in ("nan", "none", "nat"):
        return None

    # Normalize separators
    s_norm = re.sub(r"[-\.]", "/", s)

    # 1) Kuda style full datetime: DD/MM/YY HH:MM:SS (or single-digit day/month)
    kuda_dt_pattern = r'^\s*(\d{1,2})/(\d{1,2})/(\d{2})\s+(\d{1,2}):(\d{2}):(\d{2})\s*$'
    m = re.match(kuda_dt_pattern, s_norm)
    if m:
        try:
            day, month, year2, hour, minute, second = m.groups()
            year = int(year2)
            # convert 2-digit year to 4-digit (assume 2000-2099)
            year_full = 2000 + year if year < 100 else year
            dt = datetime(int(year_full), int(month), int(day), int(hour), int(minute), int(second))
            print(f"✅ Parsed date: {s} → {dt}")
            return dt
        except Exception as e:
            print(f"⚠️ Kuda full-datetime parsing failed for '{s}': {e}")

    # 2) Kuda date only: DD/MM/YY or DD/MM/YYYY
    kuda_date_pattern = r'^\s*(\d{1,2})/(\d{1,2})/(\d{2,4})\s*$'
    m2 = re.match(kuda_date_pattern, s_norm)
    if m2:
        try:
            day, month, year_token = m2.groups()
            year = int(year_token)
            if year < 100:
                year = 2000 + year
            dt = datetime(year, int(month), int(day))
            print(f"✅ Parsed date: {s} → {dt}")
            return dt
        except Exception as e:
            print(f"⚠️ Kuda date-only parsing failed for '{s}': {e}")

    # 3) If it's a time-only string (e.g., '23:08:23'), try to attach today's date as fallback
    time_only = re.match(r'^\s*(\d{1,2}):(\d{2}):(\d{2})\s*$', s)
    if time_only:
        try:
            h, m_, s_ = time_only.groups()
            now = datetime.utcnow()
            dt = datetime(now.year, now.month, now.day, int(h), int(m_), int(s_))
            print(f"✅ Parsed time-only string: {s} → {dt} (attached today)")
            return dt
        except Exception:
            pass

    # 4) Pandas fallback with dayfirst
    try:
        parsed_pd = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.notna(parsed_pd):
            dt = parsed_pd.to_pydatetime()
            print(f"✅ Pandas parsed: {s} → {dt}")
            return dt
    except Exception as e:
        print(f"⚠️ Pandas parse attempt failed for '{s}': {e}")

    # 5) Manual format list fallback
    known_formats = [
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%y %H:%M",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%b %d, %Y",
        "%d %b %Y",
    ]
    for fmt in known_formats:
        try:
            dt = datetime.strptime(s, fmt)
            print(f"✅ Manual format parsed: {s} → {dt} using {fmt}")
            return dt
        except Exception:
            continue

    print(f"❌ All date parsing methods failed for: '{s}'")
    return None


# ---------------------------------------------------------------------
# AMOUNT CLEANING
# ---------------------------------------------------------------------
def clean_amount(value):
    """Convert ₦ amounts to float safely."""
    if pd.isna(value):
        return 0.0
    s = str(value).replace("₦", "").replace(",", "").strip()
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s)
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
    Clean and normalize extracted bank statement tables.
    Filters junk rows and ensures consistent canonical format.
    """
    import pandas as pd
    from datetime import datetime

    print("DEBUG: robust_clean_dataframe input shape:", getattr(df_raw, "shape", None))
    df = df_raw.copy() if df_raw is not None else pd.DataFrame()

    if df is None or df.empty:
        cols = [
            "date", "raw_date", "value_date", "description",
            "debit", "credit", "balance", "channel",
            "transaction_reference", "row_issue",
        ]
        return pd.DataFrame(columns=cols)

    # Normalize text
    df = df.map(lambda v: normalize_text(v) if pd.notna(v) else "")

    # Assign fallback headers if needed
    if not any("date" in str(c).lower() for c in df.columns):
        df.columns = [
            "Trans. Time", "Value Date", "Description",
            "Debit/Credit(W)", "Balance(N)", "Channel", "Transaction Reference"
        ][: len(df.columns)]

    # Fill required columns
    required = {
        "Trans. Time": "",
        "Value Date": "",
        "Description": "",
        "Debit/Credit(W)": "",
        "Balance(N)": "0",
        "Channel": "",
        "Transaction Reference": "",
    }
    for c, default in required.items():
        if c not in df.columns:
            df[c] = default

    # Parse and clean
    df["raw_date"] = df["Trans. Time"].astype(str)
    df["date"] = df["raw_date"].apply(lambda v: parse_date_str(v) if str(v).strip() else None)
    df["value_date"] = df["Value Date"].apply(lambda v: parse_date_str(v) if str(v).strip() else None)
    df["description"] = df["Description"].astype(str)
    df["balance"] = df["Balance(N)"].apply(clean_amount)

    dc = df["Debit/Credit(W)"].astype(str)
    df["debit"] = dc.apply(lambda x: clean_amount(x) if "-" in x else 0.0)
    df["credit"] = dc.apply(lambda x: clean_amount(x) if "+" in x else 0.0)
    df["channel"] = df["Channel"].apply(extract_channel)
    df["transaction_reference"] = df["Transaction Reference"].astype(str)

    # --- Filter invalid / junk rows ---
    before = len(df)
    df = df[
        df["date"].notna() |
        df["debit"].astype(float).ne(0) |
        df["credit"].astype(float).ne(0) |
        df["balance"].astype(float).ne(0)
    ]
    df = df[df["description"].str.strip() != ""]
    after = len(df)
    print(f"🧹 Filtered junk rows: {before - after} removed, {after} kept")

    # Detect row issues
    def _detect_issues(r):
        issues = []
        if r["date"] is None:
            issues.append("invalid_date")
        if r.get("value_date") is None:
            issues.append("invalid_value_date")
        if r.get("channel") == "EMPTY":
            issues.append("missing_channel")
        return ", ".join(issues) if issues else ""

    df["row_issue"] = df.apply(_detect_issues, axis=1)

    # Canonical order
    cols = [
        "date", "raw_date", "value_date", "description",
        "debit", "credit", "balance", "channel",
        "transaction_reference", "row_issue",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    final_df = df[cols]

    # Save debug snapshot
    try:
        DEBUG_DIR = os.path.join(getattr(settings, "BASE_DIR", "."), "debug_exports")
        os.makedirs(DEBUG_DIR, exist_ok=True)
        stamp = int(datetime.utcnow().timestamp())
        path = os.path.join(DEBUG_DIR, f"robust_clean_snapshot_{stamp}.csv")
        final_df.to_csv(path, index=False)
        print(f"💾 Saved cleaned snapshot → {path}")
    except Exception as e:
        print(f"⚠️ Snapshot save failed: {e}")

    return final_df
