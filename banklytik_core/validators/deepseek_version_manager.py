# banklytik_core/validators/deepseek_version_manager.py
"""
DeepSeek Knowledge Validator & Rollback System
------------------------------------------------
Ensures that newly learned DeepSeek rules and patterns
are validated before merging into production knowledge.
"""

import os
import json
import shutil
from datetime import datetime
from pathlib import Path

from banklytik_core.knowledge_loader import reload_knowledge


# --- PATH CONFIGS ---
BASE_DIR = Path(os.getcwd())
KNOWLEDGE_DIR = BASE_DIR / "banklytik_knowledge"
ACTIVE_FILE = KNOWLEDGE_DIR / "deepseek_knowledge.json"
BACKUP_DIR = KNOWLEDGE_DIR / "versions"
AUDIT_FILE = KNOWLEDGE_DIR / "deepseek_audit.json"

os.makedirs(BACKUP_DIR, exist_ok=True)


def list_versions():
    """List all saved DeepSeek knowledge versions."""
    versions = sorted(BACKUP_DIR.glob("deepseek_knowledge_v*.json"))
    return [v.name for v in versions]


def _next_version_number():
    existing = list_versions()
    if not existing:
        return 1
    last = max(int(v.split("_v")[-1].split(".")[0]) for v in existing)
    return last + 1


def backup_current_version():
    """Save current active DeepSeek knowledge as a numbered version."""
    if not ACTIVE_FILE.exists():
        print("⚠️  No active DeepSeek knowledge file found to backup.")
        return None

    version_num = _next_version_number()
    backup_path = BACKUP_DIR / f"deepseek_knowledge_v{version_num}.json"
    shutil.copy2(ACTIVE_FILE, backup_path)
    print(f"✅ Backed up current DeepSeek knowledge → {backup_path.name}")
    return backup_path


def validate_new_rules(test_file="banklytik_knowledge/deepseek_suggestions.json"):
    """
    Validate a new rules file before merging.
    Checks for:
      - JSON validity
      - non-empty 'pattern' and 'replace' keys
      - duplicates against current knowledge
    """
    if not os.path.exists(test_file):
        print("⚠️  Suggestions file not found.")
        return False

    try:
        with open(test_file, "r", encoding="utf-8") as f:
            new_rules = json.load(f)
    except Exception as e:
        print(f"❌ Invalid JSON: {e}")
        return False

    if not isinstance(new_rules, list):
        print("❌ Suggestions must be a list of rules.")
        return False

    # Load current knowledge
    try:
        with open(ACTIVE_FILE, "r", encoding="utf-8") as f:
            current = json.load(f)
    except Exception:
        current = []

    current_rules = set()
    if isinstance(current, list):
        for item in current:
            if isinstance(item, dict):
                current_rules.add(json.dumps(item, sort_keys=True))

    valid_rules = []
    for rule in new_rules:
        if not isinstance(rule, dict):
            continue
        if not rule.get("pattern") or not rule.get("replace"):
            print(f"⚠️  Skipping incomplete rule: {rule}")
            continue
        rule_str = json.dumps(rule, sort_keys=True)
        if rule_str not in current_rules:
            valid_rules.append(rule)

    print(f"✅ Validation complete: {len(valid_rules)} new valid rules found.")
    return valid_rules


def merge_validated_rules(valid_rules):
    """Merge validated rules into the live knowledge file."""
    if not valid_rules:
        print("ℹ️  No new validated rules to merge.")
        return False

    try:
        with open(ACTIVE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = []

    if isinstance(data, list):
        data.extend(valid_rules)
    else:
        data = [data] + valid_rules

    with open(ACTIVE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    reload_knowledge()
    print(f"✅ Merged {len(valid_rules)} validated rules into live knowledge.")
    return True


def rollback_to_version(version_number: int):
    """Restore a previous version of DeepSeek knowledge."""
    target_file = BACKUP_DIR / f"deepseek_knowledge_v{version_number}.json"
    if not target_file.exists():
        print(f"❌ Version {version_number} not found in backups.")
        return False

    shutil.copy2(target_file, ACTIVE_FILE)
    reload_knowledge()
    print(f"♻️  Rolled back DeepSeek knowledge to version v{version_number}")
    return True


def record_audit_entry(action: str, status: str, details=None):
    """Record validation/rollback actions for audit tracking."""
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "action": action,
        "status": status,
        "details": details or {}
    }

    audit_data = []
    if AUDIT_FILE.exists():
        try:
            audit_data = json.loads(AUDIT_FILE.read_text())
        except Exception:
            pass

    audit_data.append(entry)
    AUDIT_FILE.write_text(json.dumps(audit_data, indent=2))
    print(f"📝 Audit entry recorded: {action} ({status})")
