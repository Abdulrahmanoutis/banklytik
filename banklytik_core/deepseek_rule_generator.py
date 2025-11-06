# banklytik_core/deepseek_rule_generator.py

import os
import re
import json
from collections import Counter

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
LEARNING_LOG_PATH = os.path.join(BASE_DIR, "banklytik_knowledge", "deepseek_learning_log.json")
SUGGESTIONS_PATH = os.path.join(BASE_DIR, "banklytik_knowledge", "deepseek_suggestions.json")

def analyze_learning_log():
    """Analyze logged unparsed dates and suggest potential regex fixes."""
    if not os.path.exists(LEARNING_LOG_PATH):
        print("⚠️ No learning log found.")
        return []

    with open(LEARNING_LOG_PATH, "r") as f:
        data = json.load(f)

    entries = data.get("unparsed_dates", [])
    if not entries:
        print("✅ No unparsed dates to analyze — everything parsed cleanly!")
        return []

    print(f"📊 Analyzing {len(entries)} failed date entries...")

    raw_dates = [e["date_str"] for e in entries]

    # Basic frequency analysis
    counter = Counter(raw_dates)
    common = counter.most_common(5)
    print(f"🧩 Top problematic date strings: {common}")

    suggestions = []

    for date_str, count in common:
        # Detect missing space issues
        if re.search(r"\d{4}\s+[A-Za-z]{3,}\s*\d{2}\d{2}:\d{2}", date_str):
            suggestions.append({
                "pattern": r"(\d{4}\s+[A-Za-z]{3,}\s+)(\d{2})(\d{2}:\d{2})",
                "replace": r"\1\2 \3",
                "notes": "Fix missing space between day and time."
            })
        # Detect colon-space issues
        elif re.search(r":\s+\d{2}", date_str):
            suggestions.append({
                "pattern": r"(\d{2}:\d{2}):\s+(\d{2})",
                "replace": r"\1 \2",
                "notes": "Fix colon-space issue (e.g., '20:11: 58' → '20:11 58')."
            })
        # Detect merged month/day issues (e.g. 23Feb2025)
        elif re.search(r"\d{2}[A-Za-z]{3,}\d{4}", date_str):
            suggestions.append({
                "pattern": r"(\d{2})([A-Za-z]{3,})(\d{4})",
                "replace": r"\1 \2 \3",
                "notes": "Fix merged day-month-year without space."
            })
        else:
            suggestions.append({
                "pattern": None,
                "replace": None,
                "notes": f"Unrecognized pattern (needs manual review): {date_str}"
            })

    # Save to file
    os.makedirs(os.path.dirname(SUGGESTIONS_PATH), exist_ok=True)
    with open(SUGGESTIONS_PATH, "w") as f:
        json.dump(suggestions, f, indent=2)

    print(f"✅ Suggestions saved to {SUGGESTIONS_PATH}")
    return suggestions


if __name__ == "__main__":
    analyze_learning_log()
