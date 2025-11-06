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



def merge_suggestions_into_knowledge():
    """
    Merge DeepSeek suggestions (deepseek_suggestions.json)
    into the main DeepSeek knowledge file (deepseek_knowledge.json).
    Automatically fixes structure issues (dict vs list),
    avoids duplicates, and reloads knowledge after merge.
    """
    base_dir = os.path.dirname(os.path.dirname(__file__))
    knowledge_path = os.path.join(base_dir, "banklytik_knowledge", "deepseek_knowledge.json")
    suggestions_path = os.path.join(base_dir, "banklytik_knowledge", "deepseek_suggestions.json")

    if not os.path.exists(suggestions_path):
        print("⚠️ No deepseek_suggestions.json found.")
        return False

    # Load suggestions safely
    with open(suggestions_path, "r") as f:
        try:
            suggestions = json.load(f)
        except json.JSONDecodeError:
            print("⚠️ deepseek_suggestions.json is invalid JSON.")
            return False

    if not isinstance(suggestions, list):
        print("⚠️ Suggestions file must contain a list. Auto-wrapping in list.")
        suggestions = [suggestions]

    # Load or create main knowledge base
    if os.path.exists(knowledge_path):
        with open(knowledge_path, "r") as f:
            try:
                knowledge_data = json.load(f)
            except json.JSONDecodeError:
                print("⚠️ Knowledge file was empty or invalid. Resetting.")
                knowledge_data = []
    else:
        print("⚠️ No DeepSeek knowledge file found. Creating new one.")
        knowledge_data = []

    # Fix wrong structure: sometimes a dict is stored instead of a list
    if isinstance(knowledge_data, dict):
        print("⚠️ DeepSeek knowledge file is a dict. Converting to list...")
        knowledge_data = [knowledge_data]

    # Deduplicate based on JSON string comparison
    existing_rules_str = {json.dumps(rule, sort_keys=True) for rule in knowledge_data if isinstance(rule, dict)}
    new_rules = []

    for s in suggestions:
        if not isinstance(s, dict):
            print("⚠️ Skipping non-dict rule:", s)
            continue
        rule_str = json.dumps(s, sort_keys=True)
        if rule_str not in existing_rules_str:
            knowledge_data.append(s)
            existing_rules_str.add(rule_str)
            new_rules.append(s)

    if not new_rules:
        print("ℹ️ No new rules to merge — everything is already up-to-date.")
        return False

    # Save updated knowledge base
    with open(knowledge_path, "w") as f:
        json.dump(knowledge_data, f, indent=2)

    print(f"✅ Merged {len(new_rules)} new DeepSeek suggestions into live knowledge base.")

    # Reload DeepSeek knowledge dynamically
    try:
        from banklytik_core.knowledge_loader import reload_knowledge
        reload_knowledge()
        print("🔁 DeepSeek knowledge reloaded successfully.")
    except Exception as e:
        print(f"⚠️ Could not reload knowledge automatically: {e}")

    return True


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
