# banklytik_core/knowledge_api.py
"""
Lightweight local API to expose BankLytik's knowledge registry.
Used by AI/LLM engines (like DeepSeek) to access robust functions,
rules, and examples safely without touching live logic.
"""

from banklytik_core.knowledge_registry import (
    initialize_registry,
    export_knowledge,
    save_knowledge_to_file,
)

_initialized = False


def get_knowledge(section=None, as_dict=False):
    """
    Initialize registry (if not done) and return current knowledge.
    - section: 'functions', 'rules', or 'examples' (optional)
    - as_dict: if True, return a Python dict instead of JSON text
    """
    global _initialized
    if not _initialized:
        initialize_registry()
        _initialized = True

    json_data = export_knowledge(section=section)
    if as_dict:
        import json
        return json.loads(json_data)
    return json_data


def refresh_knowledge():
    """Force reload and save current knowledge snapshot."""
    initialize_registry()
    save_knowledge_to_file()
    print("✅ Knowledge refreshed and saved to exported_knowledge.json")
