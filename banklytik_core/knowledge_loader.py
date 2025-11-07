import os
import json

# Global cache for loaded knowledge
_knowledge_data = {
    "rules": {},
    "examples": {}
}

def _load_text_file(path):
    """Load a text (Markdown or plain text) file safely."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception as e:
        print(f"⚠️ Error loading text file {path}: {e}")
        return None

def _load_json_file(path):
    """Load a JSON file safely."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Error loading JSON file {path}: {e}")
        return None

def reload_knowledge(base_dir=None):
    """
    Reload all knowledge files into memory.
    Example folder layout:
      banklytik_knowledge/
        dates/date_fix_rules.md
        examples/dates.json
    """
    global _knowledge_data

    if base_dir is None:
        base_dir = os.path.join(os.getcwd(), "banklytik_knowledge")

    _knowledge_data = {"rules": {}, "examples": {}}

    # Walk through all folders under banklytik_knowledge/
    for root, _, files in os.walk(base_dir):
        for fname in files:
            file_path = os.path.join(root, fname)
            rel_path = os.path.relpath(file_path, base_dir)
            section = rel_path.split(os.sep)[0]  # e.g. "dates" or "examples"

            if fname.endswith(".md"):
                content = _load_text_file(file_path)
                if content:
                    _knowledge_data["rules"].setdefault(section, []).append(content)

            elif fname.endswith(".json"):
                content = _load_json_file(file_path)
                if content:
                    _knowledge_data["examples"].setdefault(section, []).extend(content)

    print("✅ Knowledge base reloaded successfully.")
    print(f"📘 Rules loaded for sections: {list(_knowledge_data['rules'].keys())}")
    print(f"📗 Examples loaded for sections: {list(_knowledge_data['examples'].keys())}")

def get_rules(section):
    """Get all markdown rule content for a given section."""
    return _knowledge_data["rules"].get(section, [])

def get_examples(section):
    """Get example JSON data for a given section."""
    return _knowledge_data["examples"].get(section, [])

# --- Auto-export to DeepSeek knowledge file on reload ---
try:
    from banklytik_core.deepseek_bridge import export_to_deepseek
    export_to_deepseek()
    print("🤖 Auto-exported updated knowledge to DeepSeek JSON.")
except Exception as e:
    print(f"⚠️ DeepSeek auto-export skipped: {e}")



def load_bank_rules(bank: str, base_dir=None):
    """
    Load additional rules/examples for a specific bank (if present).
    This appends rules into the global _knowledge_data.
    Directory layout expected:
      banklytik_knowledge/rules/<bank_lower>/*.md or *.json
    """
    global _knowledge_data

    if base_dir is None:
        base_dir = os.path.join(os.getcwd(), "banklytik_knowledge")

    if not bank or bank.upper() == "UNKNOWN":
        return False

    bank_dir = os.path.join(base_dir, "rules", bank.lower())
    if not os.path.isdir(bank_dir):
        # no bank-specific rules found (not an error)
        return False

    # Walk bank_dir and load files similarly to reload_knowledge()
    for root, _, files in os.walk(bank_dir):
        for fname in files:
            file_path = os.path.join(root, fname)
            rel_path = os.path.relpath(file_path, base_dir)
            # section for bank rules can be 'dates', 'amounts', etc. or 'rules'
            # we simply append to 'rules' under a bank namespace
            section = "rules"
            try:
                if fname.endswith(".md"):
                    content = _load_text_file(file_path)
                    if content:
                        # store under rules -> <bank>: [ ... ]
                        _knowledge_data["rules"].setdefault(f"{bank.lower()}", []).append(content)
                elif fname.endswith(".json"):
                    content = _load_json_file(file_path)
                    if content:
                        _knowledge_data["examples"].setdefault(f"{bank.lower()}", []).extend(
                            content if isinstance(content, list) else [content]
                        )
            except Exception as e:
                print(f"⚠️ Failed to load bank-specific file {file_path}: {e}")

    print(f"✅ Bank-specific rules loaded for: {bank}")
    return True
