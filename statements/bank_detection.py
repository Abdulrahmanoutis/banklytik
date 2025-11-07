# statements/bank_detection.py
"""
Bank detection helpers.
Identify bank from raw OCR text or Textract blocks.
"""

from typing import List, Dict


COMMON_BANK_KEYWORDS = {
    "KUDA": ["KUDA", "KUDA BANK", "KUDA MICROFINANCE", "KUDA BANK LIMITED"],
    "GTBANK": ["GTBANK", "GUARANTY", "GUARANTY TRUST"],
    "ZENITH": ["ZENITH", "ZENITH BANK"],
    "ACCESS": ["ACCESS", "ACCESS BANK"],
    "UBA": ["UBA", "UNITED BANK FOR AFRICA"],
    "FCMB": ["FCMB", "FIRST CITY MONUMENT BANK"],
    "UNKNOWN": []
}


def detect_bank_from_text(raw_text: str) -> str:
    """
    Very lightweight bank detector based on presence of known keywords.
    Returns canonical bank key (e.g. 'KUDA', 'GTBANK', or 'UNKNOWN').
    """
    if not raw_text:
        return "UNKNOWN"
    s = raw_text.upper()
    for bank_key, keys in COMMON_BANK_KEYWORDS.items():
        for k in keys:
            if k.upper() in s:
                return bank_key
    return "UNKNOWN"


def detect_bank_from_textract_blocks(blocks: List[Dict]) -> str:
    """
    Extracts concatenated words from Textract 'Blocks' (TYPE == 'LINE' or 'WORD')
    and runs keyword detection on the joined string.
    """
    if not blocks:
        return "UNKNOWN"
    texts = []
    for b in blocks:
        t = b.get("Text") or b.get("text") or ""
        if t:
            texts.append(t)
    joined = " ".join(texts[:600])  # limit length for speed
    return detect_bank_from_text(joined)
