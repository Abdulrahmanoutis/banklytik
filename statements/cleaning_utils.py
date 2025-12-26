import re
import os
import json
import pandas as pd
from datetime import datetime
from django.conf import settings

from banklytik_core.knowledge_loader import get_rules
from banklytik_core.deepseek_adapter import get_deepseek_patterns
from .date_validator import validate_and_flag_dates


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


def fix_missing_space_date(date_str):
    """
    Fix OCR spacing and colon issues in date strings.
    This helps DeepSeek pattern parsing and fallback regex cleaning.
    """
    if not isinstance(date_str, str):
        return date_str

    s = date_str.strip()

    # Fix common missing spaces between day/time
    s = re.sub(r"(\d{2})(?=\d{2}:\d{2})", r"\1 ", s)
    s = re.sub(r"(\d{4})(?=[A-Za-z]{3,})", r"\1 ", s)
    s = re.sub(r"(\d{2}:\d{2})(?=\d{2})", r"\1 ", s)
    s = re.sub(r"\s{2,}", " ", s)

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
      - OPay format: 'YYYY MMM DD HH:MM:SS' or 'DD MMM YYYY'
      - DD/MM/YYYY etc.
    Returns a naive datetime (caller may localize) or None.
    """
    if s_raw is None:
        return None

    s = str(s_raw).strip()
    print(f"🔍 DEBUG parse_date_str: Input = '{s}' (type: {type(s).__name__})")
    
    if s == "" or s.lower() in ("nan", "none", "nat"):
        print(f"   → Empty/null value, returning None")
        return None

    # Normalize separators
    s_norm = re.sub(r"[-\.]", "/", s)

    # 1) OPay Trans. Time format: YYYY MMM DD HH:MM:SS (e.g., "2025 Feb 24 07:36:01")
    opay_dt_pattern = r'^\s*(\d{4})\s+([A-Za-z]{3})\s+(\d{1,2})\s+(\d{1,2}):(\d{2}):(\d{2})\s*$'
    m = re.match(opay_dt_pattern, s)
    if m:
        try:
            year, month_str, day, hour, minute, second = m.groups()
            month = datetime.strptime(month_str, "%b").month
            dt = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
            print(f"✅ Parsed OPay datetime: {s} → {dt}")
            return dt
        except Exception as e:
            print(f"⚠️ OPay datetime parsing failed for '{s}': {e}")

    # 2) OPay Value Date format: DD MMM YYYY (e.g., "23 Feb 2025")
    opay_date_pattern = r'^\s*(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\s*$'
    m = re.match(opay_date_pattern, s)
    if m:
        try:
            day, month_str, year = m.groups()
            month = datetime.strptime(month_str, "%b").month
            dt = datetime(int(year), int(month), int(day))
            print(f"✅ Parsed OPay date: {s} → {dt}")
            return dt
        except Exception as e:
            print(f"⚠️ OPay date parsing failed for '{s}': {e}")

    # 3) Kuda style full datetime: DD/MM/YY HH:MM:SS (or single-digit day/month)
    kuda_dt_pattern = r'^\s*(\d{1,2})/(\d{1,2})/(\d{2})\s+(\d{1,2}):(\d{2}):(\d{2})\s*$'
    m = re.match(kuda_dt_pattern, s_norm)
    if m:
        try:
            day, month, year2, hour, minute, second = m.groups()
            year = int(year2)
            # convert 2-digit year to 4-digit (assume 2000-2099)
            year_full = 2000 + year if year < 100 else year
            dt = datetime(int(year_full), int(month), int(day), int(hour), int(minute), int(second))
            print(f"✅ Parsed Kuda datetime: {s} → {dt}")
            return dt
        except Exception as e:
            print(f"⚠️ Kuda datetime parsing failed for '{s}': {e}")

    # 4) Kuda date only: DD/MM/YY or DD/MM/YYYY
    kuda_date_pattern = r'^\s*(\d{1,2})/(\d{1,2})/(\d{2,4})\s*$'
    m2 = re.match(kuda_date_pattern, s_norm)
    if m2:
        try:
            day, month, year_token = m2.groups()
            year = int(year_token)
            if year < 100:
                year = 2000 + year
            dt = datetime(year, int(month), int(day))
            print(f"✅ Parsed Kuda date: {s} → {dt}")
            return dt
        except Exception as e:
            print(f"⚠️ Kuda date parsing failed for '{s}': {e}")

    # 5) If it's a time-only string (e.g., '23:08:23'), try to attach today's date as fallback
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

    # 6) Pandas fallback with dayfirst (but NOT for YYYY-MM-DD format)
    try:
        # Check for month-year only patterns (e.g., "Feb 2025") - these are incomplete dates
        month_year_pattern = r'^([A-Za-z]{3,})\s+(\d{4})$'
        if re.match(month_year_pattern, s):
            print(f"⚠️ Incomplete date pattern detected: '{s}' - missing day component")
            return None  # Let the validation system handle this
        
        # Don't use dayfirst=True if it's already in YYYY-MM-DD format
        use_dayfirst = not bool(re.match(r'^\d{4}-\d{2}-\d{2}', s))
        parsed_pd = pd.to_datetime(s, errors="coerce", dayfirst=use_dayfirst)
        if pd.notna(parsed_pd):
            dt = parsed_pd.to_pydatetime()
            print(f"✅ Pandas parsed: {s} → {dt} (dayfirst={use_dayfirst})")
            return dt
    except Exception as e:
        print(f"⚠️ Pandas parse attempt failed for '{s}': {e}")

    # 7) Manual format list fallback
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
        "%d %b %Y %H:%M:%S",  # Additional format for OPay-style
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
    Handles both:
    1. Old format from direct_processor (Trans. Time, Description, etc.)
    2. New format from column_mapper (date, description, debit, credit, etc.)
    
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

    # PRESERVE raw date BEFORE any normalization (important for validation)
    if 'date' in df.columns:
        df['_original_date'] = df['date'].astype(str)
    
    # Normalize text (but NOT raw date values which we need for validation)
    df = df.map(lambda v: normalize_text(v) if pd.notna(v) else "")
    
    print(f"DEBUG: Incoming columns: {list(df.columns)}")
    print(f"DEBUG: Data types: {df.dtypes.to_dict()}")
    print(f"DEBUG: First row sample:\n{df.head(1).to_string()}")

    # Detect if this is the NEW format from column_mapper
    has_standardized_cols = all(col in df.columns for col in ['date', 'description', 'debit', 'credit'])
    print(f"DEBUG: Checking for standardized cols: date={('date' in df.columns)}, description={('description' in df.columns)}, debit={('debit' in df.columns)}, credit={('credit' in df.columns)}")
    
    if has_standardized_cols:
        print("✅ Detected standardized format from column_mapper")
        # Already in standardized format - just ensure all required columns exist
        
        # PRESERVE raw_date BEFORE any parsing (for validation) - use original before normalization
        if '_original_date' in df.columns:
            df['raw_date'] = df['_original_date']
        elif 'raw_date' not in df.columns:
            df['raw_date'] = df['date'].astype(str) if 'date' in df.columns else ""
        else:
            # Make sure raw_date is preserved as-is before parsing
            df['raw_date'] = df['raw_date'].astype(str)
        
        # Clean up temporary column
        df = df.drop(columns=['_original_date'], errors='ignore')
            
        if 'value_date' not in df.columns:
            df['value_date'] = None
        if 'channel' not in df.columns:
            df['channel'] = df['description'].apply(extract_channel) if 'description' in df.columns else "OTHER"
        if 'transaction_reference' not in df.columns:
            df['transaction_reference'] = ""
        
        # Parse dates if they're strings
        if df['date'].dtype == 'object':
            df['date'] = df['date'].apply(lambda v: parse_date_str(v) if isinstance(v, str) and v.strip() else v)
        
        # Handle OPay format: single "amount" column with +/- values
        # If debit/credit are empty but amount column exists, split it
        if 'amount' in df.columns:
            print("🔍 Detected 'amount' column - checking if we need to split it to debit/credit")
            
            # Check if debit and credit are mostly empty
            debit_empty = ('debit' not in df.columns) or (df['debit'].astype(str).str.strip() == '').all()
            credit_empty = ('credit' not in df.columns) or (df['credit'].astype(str).str.strip() == '').all()
            
            if debit_empty and credit_empty:
                print("✅ Splitting 'amount' column into debit/credit based on sign")
                # Convert amount to float first
                df['_amount_float'] = df['amount'].apply(clean_amount)
                # Split into debit (negative) and credit (positive)
                df['debit'] = df['_amount_float'].apply(lambda x: abs(x) if x < 0 else 0.0)
                df['credit'] = df['_amount_float'].apply(lambda x: abs(x) if x > 0 else 0.0)
                df = df.drop(columns=['_amount_float'])
        
        # Convert amounts to float
        for col in ['debit', 'credit', 'balance']:
            if col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(clean_amount)
                else:
                    df[col] = df[col].fillna(0.0).astype(float)
            else:
                # Create empty column if it doesn't exist
                df[col] = 0.0
        
    else:
        print("⚠️ Detected old format - remapping columns")
        # OLD FORMAT: Assign fallback headers if needed
        if not any("date" in str(c).lower() for c in df.columns):
            df.columns = [
                "Trans. Time", "Value Date", "Description",
                "Debit/Credit(W)", "Balance(N)", "Channel", "Transaction Reference"
            ][: len(df.columns)]

        # Fill required columns for old format
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

        # Parse and clean for old format
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

    # --- Filter rows: keep if has valid date OR has transaction amount ---
    before = len(df)
    
    # More lenient filtering: accept rows with valid dates OR non-zero amounts OR valid balance
    df['_has_date'] = df["date"].notna()
    df['_has_debit'] = df["debit"].astype(float) != 0
    df['_has_credit'] = df["credit"].astype(float) != 0
    df['_has_balance'] = df["balance"].astype(float) != 0
    df['_has_desc'] = df["description"].astype(str).str.strip() != ""
    
    # Keep rows that have: (date AND description) OR (transaction amounts) OR (balance)
    df = df[
        (df['_has_date'] & df['_has_desc']) |  # Valid date + description
        (df['_has_debit'] | df['_has_credit']) |  # Transaction amounts
        df['_has_balance']  # Valid balance
    ]
    
    # Remove temporary columns
    df = df.drop(columns=['_has_date', '_has_debit', '_has_credit', '_has_balance', '_has_desc'], errors='ignore')
    
    after = len(df)
    print(f"🧹 Filtered junk rows: {before - after} removed, {after} kept")

    # Detect row issues
    def _detect_issues(r):
        issues = []
        
        # Check for invalid/missing date
        if r["date"] is None:
            issues.append("❌ INVALID_DATE")
        else:
            # Check if date appears incomplete (defaulted to midnight with no time component)
            # This typically happens when only date (no time) is provided
            parsed_date = r["date"]
            if isinstance(parsed_date, datetime):
                # If time is 00:00:00 and raw_date doesn't contain time indicators
                if parsed_date.hour == 0 and parsed_date.minute == 0 and parsed_date.second == 0:
                    raw = str(r.get("raw_date", "")).strip()
                    # Check if raw_date has any time indicators
                    if not re.search(r'\d{1,2}:\d{2}', raw) and raw:
                        issues.append("⚠️ INCOMPLETE_DATE")
        
        if r.get("value_date") is None and str(r.get("raw_date", "")).strip():
            issues.append("⚠️ MISSING_VALUE_DATE")
        if r.get("channel") == "EMPTY":
            issues.append("⚠️ MISSING_CHANNEL")
        
        # Check for zero transaction amounts with valid description
        debit_val = float(r.get("debit", 0) or 0)
        credit_val = float(r.get("credit", 0) or 0)
        desc = str(r.get("description", "")).strip()
        if debit_val == 0 and credit_val == 0 and desc and r["date"] is not None:
            issues.append("⚠️ ZERO_AMOUNT")
        
        return " | ".join(issues) if issues else ""

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

    # Apply date validation using the new DateValidator
    print("\n🔍 Applying OCR error detection and date validation...")
    final_df = validate_and_flag_dates(final_df, verbose=True)

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
