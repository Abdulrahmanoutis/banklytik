# banklytik/statements/deepseek_utils.py

import json
import re
import requests
from collections import defaultdict
from django.conf import settings

# --- Helpers to convert Textract blocks into a compact JSON payload --- #
def blocks_to_compact_tables(blocks, max_rows_per_table=200):
    """
    Convert Textract TABLE blocks into a compact representation:
    [{'page': 1, 'rows': [['col1', 'col2'], ...]}, ...]
    Limits rows per table to max_rows_per_table for size control.
    """
    block_map = {b["Id"]: b for b in blocks}
    table_blocks = [b for b in blocks if b.get("BlockType") == "TABLE"]
    table_blocks.sort(key=lambda b: b.get("Page", 0))

    compact_tables = []
    for table in table_blocks:
        page = table.get("Page", 0)
        cells = []
        for rel in table.get("Relationships", []) or []:
            if rel.get("Type") == "CHILD":
                for cid in rel.get("Ids", []):
                    cell = block_map.get(cid)
                    if not cell or cell.get("BlockType") != "CELL":
                        continue
                    row = cell.get("RowIndex")
                    col = cell.get("ColumnIndex")
                    text = ""
                    for crel in cell.get("Relationships", []) or []:
                        if crel.get("Type") == "CHILD":
                            for wid in crel.get("Ids", []):
                                w = block_map.get(wid)
                                if not w:
                                    continue
                                if w.get("BlockType") == "WORD":
                                    text += (w.get("Text") or "") + " "
                                elif w.get("BlockType") == "SELECTION_ELEMENT":
                                    text += (w.get("SelectionStatus") or "") + " "
                    cells.append({"row": row, "col": col, "text": text.strip()})

        if not cells:
            continue

        max_row = max(c["row"] for c in cells)
        max_col = max(c["col"] for c in cells)
        table_map = defaultdict(dict)
        for c in cells:
            table_map[c["row"]][c["col"]] = c["text"]

        rows = []
        for r in range(1, max_row + 1):
            row_vals = [table_map[r].get(c, "") for c in range(1, max_col + 1)]
            rows.append(row_vals)
            if len(rows) >= max_rows_per_table:
                break

        compact_tables.append({"page": page, "rows": rows})

    return compact_tables


def lines_from_blocks(blocks, pages=None, max_lines=500):
    """
    Extract LINE blocks (text lines). Optionally filter by pages list.
    Returns list of {'page':n, 'text': '...'}
    """
    lines = []
    for b in blocks:
        if b.get("BlockType") != "LINE":
            continue
        page = b.get("Page", None)
        if pages and page not in pages:
            continue
        lines.append({"page": page, "text": b.get("Text", "")})
        if len(lines) >= max_lines:
            break
    return lines


def build_sample_json(blocks, sampled_pages):
    """
    Builds the compact JSON to send to DeepSeek:
    { "tables": [ { "page":int, "rows": [[...],[...]] } ... ],
      "lines": [ { "page":int, "text": "..." }, ... ] }
    """
    tables = blocks_to_compact_tables([b for b in blocks if b.get("Page") in sampled_pages])
    lines = lines_from_blocks(blocks, pages=sampled_pages)
    payload = {"tables": tables, "lines": lines, "sampled_pages": sampled_pages}
    return payload


# --- Prompt building & DeepSeek API call --- #
SYSTEM_INSTRUCTIONS = (
    "You are an expert Python data engineer. You will receive a compact JSON "
    "object containing representative tables extracted from a bank statement (Textract). "
    "Your task: inspect the sample tables and generate robust Pandas code that will "
    "transform a raw concatenated DataFrame `df` (constructed from all extracted tables across the document) "
    "into a cleaned transactions DataFrame and assign it to a variable named `result`.\n\n"

    "IMPORTANT CONTEXT:\n"
    "- The raw DataFrame `df` may contain metadata rows such as 'Statement Print Date', 'Start Date', 'End Date', "
    "'Branch Name', 'Account Type', 'Currency'. These must be discarded.\n"
    "- The true header row typically contains these columns (though exact spelling/spacing may vary):\n"
    "  POST DATE | TRANSACCTNAMION DESC | DOC NO. | VALUE DATE | DR | CR | BALANCE\n"
    "- Only the rows after this header row are real transactions.\n\n"

    "REQUIREMENTS:\n"
    "- Detect and align the real header row dynamically (do not hardcode row numbers).\n"
    "- Rename final columns to: date, description, debit, credit, balance, channel, transaction_reference.\n"
    "- Parse and coerce dates into datetime, amounts into floats (remove commas/extra symbols).\n"
    "- Handle missing debit/credit gracefully by filling with 0.\n"
    "- Extract channel from description if possible (e.g., 'MOBILE/UNION', 'NXG', 'ATM').\n"
    "- Output ONLY valid Python code (no explanations, no comments).\n"
    "- Always import all libraries you use (e.g., pandas, numpy, re).\n"
    "- The final DataFrame must be stored in a variable called `result`.\n"
)


def build_deepseek_prompt(sample_json: dict, question: str = None):
    """
    Build the user prompt containing the compact sample JSON and explicit instructions.
    """
    sample_str = json.dumps(sample_json, ensure_ascii=False)
    max_chars = 150_000
    truncated_note = ""
    if len(sample_str) > max_chars:
        sample_str = sample_str[:max_chars]
        truncated_note = "\n\nNOTE: the sample JSON was truncated for size.\n"

    user_instructions = (
        "Here is a compact JSON sample of tables and lines (Textract -> compact):\n\n"
        f"{sample_str}\n\n"
        f"{truncated_note}"
        "Now, produce the Python (pandas) code that, given a DataFrame named `df` "
        "(which is the concatenation of all extracted tables across pages), transforms `df` into a "
        "clean transactions DataFrame. Assign the final DataFrame to the variable `result`.\n"
        "Remember: ONLY return Python code. No explanations."
    )
    if question:
        user_instructions += f"\nUser note: {question}\n"

    prompt = {"system": SYSTEM_INSTRUCTIONS, "user": user_instructions}
    return prompt


def extract_python_code_from_text(text: str) -> str:
    """
    Extract code from markdown fenced blocks if present.
    """
    if not text:
        return ""
    match = re.search(r"```python(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text.strip()


def call_deepseek(prompt: dict, timeout: int = 60) -> str:
    """
    Call DeepSeek chat-completions endpoint with built prompt.
    Returns the python code string (no markdown fences).
    Raises RuntimeError on failure.
    """
    api_url = getattr(settings, "DEEPSEEK_API_URL", "https://api.deepseek.com/chat/completions")
    api_key = getattr(settings, "DEEPSEEK_API_KEY", None)
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY not configured in settings.")

    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": prompt["system"]},
            {"role": "user", "content": prompt["user"]},
        ],
    }

    resp = requests.post(
        api_url,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=timeout,
    )

    if resp.status_code != 200:
        raise RuntimeError(f"DeepSeek API error {resp.status_code}: {resp.text}")

    data = resp.json()
    text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    code = extract_python_code_from_text(text)
    return code
