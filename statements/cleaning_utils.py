# statements/cleaning_utils.py

import re
import dateparser
import pandas as pd
from datetime import datetime
import logging

from banklytik_core.knowledge_loader import get_rules
from banklytik_core.deepseek_adapter import get_deepseek_patterns


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# HELPER: Apply one regex rule safely (used by dynamic rule system)
# ---------------------------------------------------------------------
def apply_rule_once(text, pattern, replacement):
    """
    Apply a single regex-based rule to text safely.
    Returns (new_text, changed: bool)
    """
    try:
        new_text = re.sub(pattern, replacement, text)
        changed = new_text != text
        if changed:
            logger.debug(f"🧩 Rule applied: '{pattern}' -> '{replacement}'")
            logger.debug(f"Before: '{text}'")
            logger.debug(f"After : '{new_text}'")
        return new_text, changed
    except re.error as e:
        logger.warning(f"⚠️ Invalid regex in rule: {pattern} — {e}")
        return text, False


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
# DATE PARSING (Robust with OCR Fix + Dynamic Rules)
# ---------------------------------------------------------------------
def fix_missing_space_date(date_str):
    """
    Fix OCR spacing and colon issues in date strings.
    Dynamically applies regex rules from both:
    1. DeepSeek knowledge (deepseek_knowledge.json)
    2. Knowledge base (dates_rules.md)
    Then falls back to internal static patterns.
    Always returns a valid string (never None).
    """
    if not isinstance(date_str, str):
        return "" if date_str is None else str(date_str)

    changed_any = False

    # --- Step 1: Load DeepSeek rules ---
    deepseek_rules = []
    try:
        from banklytik_core.deepseek_adapter import get_deepseek_patterns
        deepseek_rules = get_deepseek_patterns() or []
    except Exception as e:
        print(f"⚠️ DeepSeek load failed: {e}")

    if deepseek_rules:
        print("✅ Loaded DeepSeek rules:", len(deepseek_rules))
        for rule_text in deepseek_rules:
            pattern_match = re.search(r"Regex:\s*(.+?)\s+Replace:", rule_text)
            replace_match = re.search(r"Replace:\s*(.+?)(?:\s+Notes:|$)", rule_text)
            if pattern_match and replace_match:
                pattern = pattern_match.group(1).strip()
                replacement = replace_match.group(1).strip()
                try:
                    new_str = re.sub(pattern, replacement, date_str)
                    if new_str != date_str:
                        print(f"DEBUG: DeepSeek applied pattern '{pattern}'")
                        print(f"       '{date_str}' -> '{new_str}'")
                        date_str = new_str
                        changed_any = True
                except re.error as e:
                    print(f"⚠️ Regex error in DeepSeek rule '{pattern}': {e}")

    # --- Step 2: Load static KB rules ---
    kb_rules = []
    try:
        from banklytik_core.knowledge_loader import get_rules
        kb_rules = get_rules("dates") or []
    except Exception as e:
        print(f"⚠️ Knowledge base load failed: {e}")

    if kb_rules:
        for rule_text in kb_rules:
            pattern_match = re.search(r"Regex:\s*(.+)", rule_text)
            replace_match = re.search(r"Replace:\s*(.+)", rule_text)
            if pattern_match and replace_match:
                pattern = pattern_match.group(1).strip()
                replacement = replace_match.group(1).strip()
                try:
                    new_str = re.sub(pattern, replacement, date_str)
                    if new_str != date_str:
                        print(f"DEBUG: KB rule applied: '{pattern}'")
                        date_str = new_str
                        changed_any = True
                except re.error as e:
                    print(f"⚠️ Regex error in KB rule '{pattern}': {e}")

    # --- Step 3: Apply fallback internal patterns ---
    internal_patterns = [
        # Pattern 1: Missing space between day and time (with seconds)
        (r'(\d{4}\s+[A-Za-z]{3,}\s+)(\d{2})(\d{2}:\d{2}\s+\d{2})', r'\1\2 \3', "pattern1"),
        # Pattern 2: Missing space between day and time (no seconds)
        (r'(\d{4}\s+[A-Za-z]{3,}\s+)(\d{2})(\d{2}:\d{2})', r'\1\2 \3', "pattern2"),
        # Pattern 3: 23Feb2025 10:00 → 23 Feb 2025 10:00
        (r'(\d{2})([A-Za-z]{3,})(\d{4}\s+\d{2}:\d{2})', r'\1 \2 \3', "pattern3"),
        # Pattern 4: Generic fallback “2310:00” → “23 10:00”
        (r'(\d{2})(\d{2}:\d{2})', r'\1 \2', "pattern4"),
        # Pattern 5: Colon-space issue “20:11: 58” → “20:11 58”
        (r'(\d{2}:\d{2}):\s+(\d{2})', r'\1 \2', "pattern5"),
    ]

    for pattern, replacement, label in internal_patterns:
        try:
            new_str = re.sub(pattern, replacement, date_str)
            if new_str != date_str:
                print(f"DEBUG: Fixed {label}: '{date_str}' -> '{new_str}'")
                date_str = new_str
                changed_any = True
        except re.error as e:
            print(f"⚠️ Regex error in internal rule {label}: {e}")

    # --- Step 4: Final safety and return ---
    if changed_any:
        print(f"DEBUG: Final fixed date string: '{date_str}'")

    # Always return a valid string
    if not isinstance(date_str, str) or date_str is None:
        return ""
    return str(date_str).strip()





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

    # 🧩 Step 1: Normalize OCR spacing before parsing
    corrected = fix_missing_space_date(s)
    if corrected != s:
        print(f"DEBUG: Applied fix_missing_space_date: '{s}' -> '{corrected}'")
        s = corrected

    # 🧩 Step 2: Try dateparser first
    try:
        parsed = dateparser.parse(
            s,
            settings={
                "DATE_ORDER": "DMY",
                "PREFER_DAY_OF_MONTH": "first",
                "PREFER_DATES_FROM": "current_period",
                "RETURN_AS_TIMEZONE_AWARE": False,
            },
        )
        if parsed:
            print(f"DEBUG: dateparser successfully parsed '{s}' -> {parsed}")
            return parsed
    except Exception as e:
        print(f"DEBUG: dateparser failed for '{s}': {e}")

    # 🧩 Step 3: Manual fallback formats
    s_clean = re.sub(r"[.,-]", " ", s)
    s_clean = re.sub(r"\s+", " ", s_clean).strip()
    for fmt in [
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
    ]:
        try:
            parsed = datetime.strptime(s_clean, fmt)
            if fmt == "%b %Y":
                parsed = parsed.replace(day=1)
            print(f"DEBUG: strptime successfully parsed '{s}' -> {parsed} using '{fmt}'")
            return parsed
        except ValueError:
            continue

    # 🧩 Step 4: Pandas fallback
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

    headers = [
        "Trans. Time", "Value Date", "Description",
        "Debit/Credit(W)", "Balance(N)", "Channel", "Transaction Reference"
    ]
    df.columns = headers[:len(df.columns)]

    print("DEBUG: First 5 date strings in 'Trans. Time':")
    for i, date_str in enumerate(df["Trans. Time"].head(5)):
        print(f"  {i}: '{date_str}'")

    df["raw_date"] = df["Trans. Time"]
    df["date"] = df["Trans. Time"].apply(parse_date_str)
    df["value_date"] = df["Value Date"].apply(parse_date_str)
    df["description"] = df["Description"]
    df["balance"] = df["Balance(N)"].apply(clean_amount)

    dc = df["Debit/Credit(W)"].astype(str)
    df["debit"] = dc.apply(lambda x: clean_amount(x) if "-" in x else 0.0)
    df["credit"] = dc.apply(lambda x: clean_amount(x) if "+" in x else 0.0)

    df["channel"] = df["Channel"].apply(extract_channel)
    df["transaction_reference"] = df["Transaction Reference"]

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
