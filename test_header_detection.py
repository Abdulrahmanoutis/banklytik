import json
from statements.header_detector import detect_headers_ai
from statements.textract_utils import extract_combined_table

import os
import sys
import django

# make sure project root is in sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "banklytik.settings")
django.setup()


# Path to your saved Textract JSON
file_path = "debug_exports/analyzeDocResponse_19_20251106_213028.json"

# Load the JSON
with open(file_path, "r", encoding="utf-8") as f:
    textract_data = json.load(f)

blocks = textract_data.get("Blocks", [])
df = extract_combined_table(blocks)

# Extract header row
headers = df.iloc[0].tolist()
print("\n🧾 Raw Headers Detected:")
print(headers)

# Run the AI-assisted mapping
mapping = detect_headers_ai(headers)

print("\n🤖 AI Header Mapping Result:")
for raw, mapped in mapping.items():
    print(f"  {raw:30} →  {mapped}")
