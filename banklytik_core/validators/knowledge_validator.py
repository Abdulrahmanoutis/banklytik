import json
from pathlib import Path

def validate_deepseek_file(path="banklytik_knowledge/deepseek_knowledge.json"):
    file = Path(path)
    if not file.exists():
        print("⚠️ DeepSeek file not found.")
        return False

    try:
        data = json.loads(file.read_text())
        if not isinstance(data, dict):
            raise ValueError("DeepSeek file must be a dict at root level.")
        print(f"✅ DeepSeek file validated with {len(data)} sections.")
        return True
    except Exception as e:
        print(f"❌ DeepSeek validation failed: {e}")
        return False
