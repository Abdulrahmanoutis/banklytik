# banklytik_core/deepseek_adapter.py

import json
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
KNOWLEDGE_PATH = BASE_DIR / "banklytik_knowledge" / "deepseek_knowledge.json"

# ---------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------

def load_deepseek_knowledge():
    """Safely load DeepSeek knowledge JSON."""
    if not KNOWLEDGE_PATH.exists():
        print(f"⚠️ DeepSeek knowledge file not found at {KNOWLEDGE_PATH}")
        return {}
    try:
        with open(KNOWLEDGE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"✅ Loaded DeepSeek knowledge from {KNOWLEDGE_PATH}")
        return data
    except Exception as e:
        print(f"⚠️ Failed to load DeepSeek knowledge: {e}")
        return {}

def save_deepseek_knowledge(data):
    """Safely write DeepSeek knowledge JSON."""
    try:
        with open(KNOWLEDGE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved DeepSeek knowledge to {KNOWLEDGE_PATH}")
    except Exception as e:
        print(f"⚠️ Failed to save DeepSeek knowledge: {e}")

def get_deepseek_patterns():
    """Return list of regex rules currently known to DeepSeek."""
    data = load_deepseek_knowledge()
    return data.get("regex_rules", [])

def add_new_pattern(rule_text):
    """
    Add a new regex rule or correction example to the DeepSeek knowledge file.
    Avoids duplicates.
    """
    data = load_deepseek_knowledge()
    rules = data.get("regex_rules", [])

    if rule_text not in rules:
        rules.append(rule_text)
        data["regex_rules"] = rules
        save_deepseek_knowledge(data)
        print(f"✅ Added new pattern to DeepSeek knowledge: {rule_text[:60]}...")
    else:
        print("ℹ️ Pattern already exists in DeepSeek knowledge.")

def get_examples(section="dates"):
    """Get DeepSeek examples for a given section (e.g. 'dates', 'amounts')."""
    data = load_deepseek_knowledge()
    examples = data.get("examples", {})
    return examples.get(section, [])

def add_example(section, example_data):
    """Add a learning example to DeepSeek knowledge (for adaptive training)."""
    data = load_deepseek_knowledge()
    examples = data.get("examples", {})
    section_examples = examples.get(section, [])

    if example_data not in section_examples:
        section_examples.append(example_data)
        examples[section] = section_examples
        data["examples"] = examples
        save_deepseek_knowledge(data)
        print(f"✅ Added new {section} example to DeepSeek knowledge.")
    else:
        print(f"ℹ️ Example already exists in {section}.")
