# banklytik_core/deepseek_interface.py
"""
DeepSeek Interface Layer
------------------------
Bridges the internal knowledge base with DeepSeek or other AI models.
It reads exported knowledge (deepseek_knowledge.json), identifies failures,
and saves AI-ready prompts or suggestions for further improvement.
"""

import json
import re
import os
from datetime import datetime

KNOWLEDGE_DIR = os.path.join("banklytik_knowledge")
DEEPSEEK_FILE = os.path.join(KNOWLEDGE_DIR, "deepseek_knowledge.json")
SUGGESTIONS_FILE = os.path.join(KNOWLEDGE_DIR, "deepseek_suggestions.json")

# ---------------------------------------------------------------------
# 🧩 Utility functions
# ---------------------------------------------------------------------
def load_json(path):
    """Safely load a JSON file."""
    if not os.path.exists(path):
        print(f"⚠️ Missing file: {path}")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Error reading {path}: {e}")
        return {}

def save_json(path, data):
    """Safely write JSON to disk."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ Saved suggestions → {path}")

# ---------------------------------------------------------------------
# 🧩 Core interface logic
# ---------------------------------------------------------------------
def prepare_failed_dates_log(log_text):
    """
    Extract unparsed date strings from debug logs and return a clean list.
    Looks for 'All parsing methods failed for:' lines.
    """
    pattern = r"All parsing methods failed for:\s*'([^']+)'"
    failed = re.findall(pattern, log_text)
    return sorted(set(failed))

def build_ai_query(failed_dates):
    """
    Create a query payload that DeepSeek can later use to suggest regex fixes.
    """
    if not failed_dates:
        print("✅ No failed dates found to process.")
        return None

    return {
        "timestamp": datetime.now().isoformat(),
        "category": "dates",
        "failed_samples": failed_dates,
        "context": "These date strings could not be parsed by the current robust fallback parser. Suggest regex fixes or format patterns.",
    }

def collect_and_store_suggestions(log_path="debug.log"):
    """
    Step 1: Parse log file for failed dates.
    Step 2: Prepare query payload.
    Step 3: Store suggestions file.
    """
    if not os.path.exists(log_path):
        print(f"⚠️ Log file not found: {log_path}")
        return

    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        log_text = f.read()

    failed_dates = prepare_failed_dates_log(log_text)
    payload = build_ai_query(failed_dates)

    if payload:
        save_json(SUGGESTIONS_FILE, payload)
        print(f"✅ Collected {len(failed_dates)} failed samples for DeepSeek analysis.")
    else:
        print("✅ No new data to collect.")

# ---------------------------------------------------------------------
# 🧩 Simple inspection functions
# ---------------------------------------------------------------------
def view_exported_knowledge():
    """Preview currently exported DeepSeek knowledge base."""
    kb = load_json(DEEPSEEK_FILE)
    print(json.dumps(kb, indent=2))

def view_suggestions():
    """View saved DeepSeek suggestions."""
    data = load_json(SUGGESTIONS_FILE)
    print(json.dumps(data, indent=2))

# ---------------------------------------------------------------------
# 🧩 Example usage (manual in shell)
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Example: Collect new suggestions from the latest logs
    collect_and_store_suggestions("debug.log")
