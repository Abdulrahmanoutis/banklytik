# banklytik_core/knowledge_registry.py
"""
Knowledge Registry for banklytik_core
-------------------------------------
This module acts as a bridge between internal robust functions
and AI/LLM models like DeepSeek. It allows exporting concise
summaries of parsing logic, regex rules, and examples.
"""

import inspect
import json
from pathlib import Path
from banklytik_core.knowledge_loader import get_rules, get_examples

# -------------------------------------------------------------------
# Core registry
# -------------------------------------------------------------------
REGISTRY = {
    "functions": {},
    "rules": {},
    "examples": {}
}

def register_function(name, func, description=""):
    """Register a function and store its code + docstring."""
    REGISTRY["functions"][name] = {
        "description": description or (func.__doc__ or "").strip(),
        "source": inspect.getsource(func)
    }

def register_rules(section):
    """Register a section of rules from the knowledge base."""
    REGISTRY["rules"][section] = get_rules(section)

def register_examples(section):
    """Register a section of examples from the knowledge base."""
    REGISTRY["examples"][section] = get_examples(section)

# -------------------------------------------------------------------
# Export / Import utilities
# -------------------------------------------------------------------
def export_knowledge(section=None):
    """Export the entire registry or a single section as JSON text."""
    if section:
        data = REGISTRY.get(section, {})
    else:
        data = REGISTRY
    return json.dumps(data, indent=2, ensure_ascii=False)

def save_knowledge_to_file(filepath="banklytik_knowledge/exported_knowledge.json"):
    """Save the current knowledge registry to disk."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(REGISTRY, f, indent=2, ensure_ascii=False)
    print(f"✅ Knowledge exported to {path}")

# -------------------------------------------------------------------
# Bootstrapping (with lazy imports to avoid circular imports)
# -------------------------------------------------------------------
def initialize_registry():
    """
    Load and register key functions and rules.
    Lazily imports cleaning utilities to avoid circular import issues.
    """
    try:
        # Import only when function runs (not at module import time)
        from statements import cleaning_utils

        register_function(
            "fix_missing_space_date",
            cleaning_utils.fix_missing_space_date,
            "Fix OCR and spacing issues in date strings before parsing."
        )
        register_function(
            "parse_date_str",
            cleaning_utils.parse_date_str,
            "Robust multi-strategy date parser for bank statements."
        )
        register_function(
            "robust_clean_dataframe",
            cleaning_utils.robust_clean_dataframe,
            "Top-level cleaning pipeline for statement DataFrames."
        )

    except Exception as e:
        print(f"⚠️ Failed to import cleaning_utils functions: {e}")

    # Load known rule groups
    for section in ["dates", "amounts", "text_normalization"]:
        register_rules(section)
        register_examples(section)

    print("✅ Knowledge registry initialized successfully.")
