# banklytik_core/deepseek_bridge.py
"""
Bridge between BankLytik's internal knowledge base and DeepSeek (or other AI engines).
Provides safe read-only access to rules, examples, and function sources.
"""

from banklytik_core.knowledge_api import get_knowledge, refresh_knowledge
import json

def fetch_ai_knowledge(section="functions"):
    """
    Return a dictionary of knowledge for AI usage.
    Supported sections: 'functions', 'rules', 'examples'
    """
    data = get_knowledge(section=section, as_dict=True)
    return data

def export_to_deepseek(file_path="banklytik_knowledge/deepseek_knowledge.json"):
    """
    Export all knowledge sections into a single JSON file
    that DeepSeek can later index and train on.
    """
    all_sections = {}
    for sec in ["functions", "rules", "examples"]:
        all_sections[sec] = get_knowledge(section=sec, as_dict=True)

    with open(file_path, "w") as f:
        json.dump(all_sections, f, indent=2)
    print(f"✅ DeepSeek knowledge exported → {file_path}")
