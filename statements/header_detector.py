# statements/header_detector.py
"""
AI-assisted header detector for Banklytik.
Uses DeepSeek knowledge + heuristic matching to infer column mappings.
"""

import re
import os
import json
from difflib import SequenceMatcher
from typing import List, Dict
from datetime import datetime

# 🔒 Safe import guard for DeepSeek dependencies
try:
    from banklytik_core.knowledge_loader import get_examples, get_rules
except Exception as e:
    print(f"⚠️ Warning: knowledge_loader import failed in header_detector: {e}")
    get_examples = lambda *a, **k: []
    get_rules = lambda *a, **k: []


# canonical header labels we want to end up with
TARGET_HEADERS = [
    "date", "value_date", "description", "debit", "credit", "balance",
    "channel", "transaction_reference"
]

# keywords that hint each field
HEADER_HINTS = {
    "date": ["date", "trans", "time", "posted"],
    "value_date": ["value date", "val date"],
    "description": ["desc", "details", "narration", "remark", "to / from", "beneficiary"],
    "debit": ["debit", "withdraw", "dr", "paid", "money out", "spent"],
    "credit": ["credit", "deposit", "cr", "received", "money in", "income"],
    "balance": ["balance", "bal"],
    "channel": ["channel", "mode", "type", "category"],
    "transaction_reference": ["ref", "reference", "id", "txn"],
}


def normalize_header_name(header: str) -> str:
    """Simplify header text for matching."""
    return re.sub(r"[^a-z]", " ", str(header).lower()).strip()


def score_similarity(a: str, b: str) -> float:
    """Compute fuzzy similarity between two strings (0–1)."""
    return SequenceMatcher(None, a, b).ratio()


def detect_headers_ai(columns: List[str]) -> Dict[str, str]:
    """
    Try to map raw OCR column names to canonical headers.
    Returns dict mapping raw->canonical.
    """
    mapping = {}
    normalized_cols = [normalize_header_name(c) for c in columns]

    # Try to use DeepSeek examples as additional hints (optional)
    examples = get_examples("dates") or []
    extra_hints = [normalize_header_name(x) for x in examples if isinstance(x, str)]
    if extra_hints:
        HEADER_HINTS["date"] += [h for h in extra_hints if h not in HEADER_HINTS["date"]]

    for raw, norm in zip(columns, normalized_cols):
        best_label = None
        best_score = 0.0

        # direct keyword match
        for target, hints in HEADER_HINTS.items():
            for hint in hints:
                if hint in norm:
                    mapping[raw] = target
                    best_label = target
                    best_score = 1.0
                    break
            if best_label:
                break

        # fuzzy fallback
        if not best_label:
            for target in TARGET_HEADERS:
                for hint in HEADER_HINTS[target]:
                    score = score_similarity(norm, hint)
                    if score > best_score:
                        best_score = score
                        best_label = target
            if best_label and best_score >= 0.5:
                mapping[raw] = best_label
            else:
                mapping[raw] = raw  # keep as-is if uncertain

    # --- Save every mapping for DeepSeek training ---
    try:
        debug_dir = os.path.join(os.getcwd(), "debug_exports")
        os.makedirs(debug_dir, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(debug_dir, f"header_mapping_log_{timestamp}.json")
        payload = {
            "timestamp": timestamp,
            "raw_headers": columns,
            "ai_mapping": mapping,
        }
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"✅ Header mapping saved to {log_path}")
    except Exception as e:
        print(f"⚠️ Failed to save header mapping log: {e}")

    return mapping
