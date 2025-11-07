# statements/cleaning_utils.py

from __future__ import annotations
import re
import json
import os
import dateparser
import pandas as pd
from datetime import datetime
import logging

from banklytik_core.knowledge_loader import get_rules
from banklytik_core.deepseek_adapter import get_deepseek_patterns

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# DeepSeek Learning Log System
# ---------------------------------------------------------------------
LEARNING_LOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "banklytik_knowledge",
    "deepseek_learning_log.json"
)

def log_failed_date(date_str, reason, context=None):
    """Append unparsed or failed date strings to DeepSeek learning log."""
    try:
        # Create file if missing
        if not os.path.exists(LEARNING_LOG_PATH):
            with open(LEARNING_LOG_PATH, "w") as f:
                json.dump({"unparsed_dates": []}, f, indent=2)

        # Load existing data
        with open(LEARNING_LOG_PATH, "r") as f:
            data = json.load(f)

        # New entry
        entry = {
            "date_str": str(date_str),
            "reason": reason,
            "context": context or {},
            "timestamp": datetime.utcnow().isoformat()
        }

        data["unparsed_dates"].append(entry)

        with open(LEARNING_LOG_PATH, "w") as f:
            json.dump(data, f, indent=2)

        print(f"🧠 Logged failed date for DeepSeek learning: {date_str} ({reason})")

    except Exception as e:
        print(f"⚠️ Failed to write DeepSeek learning log: {e}")


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
    s = re.sub(r"[^\x20-\x7E\u00A0-\uFFFF]", "", s)
    return s.strip()


# ---------------------------------------------------------------------
# DATE PARSING (DeepSeek + Robust Fallback)
# ---------------------------------------------------------------------
def fix_missing_space_date(date_str):
    """
    Fix OCR spacing and colon issues in date strings.
    Dynamically applies regex rules from both:
    1. DeepSeek knowledge (JSON)
    2. Knowledge base (dates_rules.md)
    Then falls back to internal static patterns.
    """
    if not isinstance(date_str, str):
        return date_str

    changed_any = False

    # --- Step 1: Apply DeepSeek patterns (from exported JSON) ---
    deepseek_rules = get_deepseek_patterns() or []
    if deepseek_rules:
        print(f"✅ Loaded DeepSeek rules: {len(deepseek_rules)}")
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

    # --- Step 2: Apply Knowledge Base rules (Markdown) ---
    kb_rules = get_rules("dates") or []
    for rule_text in kb_rules:
        pattern_match = re.search(r"Regex:\s*(.+)", rule_text)
        replace_match = re.search(r"Replace:\s*(.+)", rule_text)
        if pattern_match and replace_match:
            pattern = pattern_match.group(1).strip()
            replacement = replace_match.group(1).strip()
            new_str = re.sub(pattern, replacement, date_str)
            if new_str != date_str:
                print(f"DEBUG: KB rule applied: '{pattern}'")
                date_str = new_str
                changed_any = True

    # --- Step 3: Internal legacy fallbacks ---
    internal_patterns = [
        (r'(\d{4}\s+[A-Za-z]{3,}\s+)(\d{2})(\d{2}:\d{2}\s+\d{2})', r'\1\2 \3'),
        (r'(\d{4}\s+[A-Za-z]{3,}\s+)(\d{2})(\d{2}:\d{2})', r'\1\2 \3'),
        (r'(\d{2})([A-Za-z]{3,})(\d{4}\s+\d{2}:\d{2})', r'\1 \2 \3'),
        (r'(\d{2})(\d{2}:\d{2})', r'\1 \2'),
        (r'(\d{2}:\d{2}):\s+(\d{2})', r'\1 \2'),
    ]
    for pattern, replacement in internal_patterns:
        new_str = re.sub(pattern, replacement, date_str)
        if new_str != date_str:
            print(f"DEBUG: Fixed fallback '{pattern}' -> '{new_str}'")
            date_str = new_str
            changed_any = True

    if changed_any:
        print(f"DEBUG: Final fixed date string: '{date_str}'")

    return date_str


def parse_date_str(date_str):
    """Robust multi-strategy date parser with enhanced Kuda/Access format support."""
    import re
    import pandas as pd
    from datetime import datetime
    import dateparser

    if pd.isna(date_str):
        return None

    s = str(date_str).strip()
    if s.lower() in ["", "nan", "none", "null", "0.0"]:
        return None

    # Normalize spaces and separators
    s = re.sub(r"(\d{1,2})([A-Za-z]{3,})(\d{4})", r"\1 \2 \3", s)
    s = re.sub(r"(\d{4})([A-Za-z]{3,})(\d{1,2})", r"\1 \2 \3", s)
    s = re.sub(r"(\d{2})([A-Za-z]{3,})(\d{2})", r"\1 \2 \3", s)
    s = re.sub(r"[\t\r\n]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    s = s.replace("  ", " ")

    # --- 1. Try dateparser first ---
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
            return parsed
    except Exception:
        pass

    # --- 2. Manual known formats ---
    common_formats = [
        "%d %b %Y %I:%M %p",  # 15 Oct 2025 07:32 PM
        "%b %d, %Y %H:%M:%S",
        "%b %d, %Y %I:%M %p",
        "%d %b, %Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M",
        "%d %b %Y",
        "%b %d %Y",
    ]
    for fmt in common_formats:
        try:
            parsed = datetime.strptime(s, fmt)
            return parsed
        except Exception:
            continue

    # --- 3. Fallback using pandas ---
    try:
        parsed = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if not pd.isna(parsed):
            return parsed.to_pydatetime()
    except Exception:
        pass

    # --- 4. Log unparsed date for DeepSeek learning ---
    try:
        from banklytik_core.deepseek_rule_generator import log_failed_date
        log_failed_date(s, "unparsed_kuda_like_date")
    except Exception:
        pass

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
    """Clean extracted statement tables safely and robustly."""
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
