
import json
import os
import traceback
from django.conf import settings
from .deepseek_utils import call_deepseek
from .textract_sampling import process_textract_blocks

# Max characters of payload to include in the prompt (keeps requests reasonable)
MAX_PAYLOAD_CHARS = 120_000

SYSTEM_INSTRUCTIONS = (
    "You are an expert in analyzing tabular data extracted from bank statements. "
    "You will receive a compact JSON 'payload' containing a small set of representative pages "
    "with simplified tables and a few text lines. Your task: identify which table(s) contain "
    "transaction rows, and produce a mapping from the original column names (exact spelling as "
    "they appear) to semantic roles from this canonical set: "
    "['date', 'value_date', 'description', 'debit', 'credit', 'debit_credit', 'balance', "
    "'channel', 'transaction_reference', 'other'].\n\n"
    "REQUIREMENTS:\n"
    "- Return EXACTLY one valid JSON object (no markdown, no explanation) whose top-level schema is:\n"
    "{\n"
    "  \"transaction_table_pages\": [<page numbers>],\n"
    "  \"tables\": [\n"
    "      {\n"
    "          \"page\": <page_number>,\n"
    "          \"original_header\": [\"col1\", \"col2\", ...],\n"
    "          \"column_mapping\": { \"Original Col Name\": \"role\", ... }\n"
    "      }\n"
    "  ],\n"
    "  \"reasoning_summary\": \"one-sentence summary\"\n"
    "}\n\n"
    "- Use the original header text exactly as it appears (preserve case/spacing/typos).\n"
    "- If multiple tables are clearly transaction tables, include them all.\n"
    "- Provide a short reasoning_summary explaining the choice.\n"
)

def _truncate_payload_str(payload_str: str, max_chars: int = MAX_PAYLOAD_CHARS) -> (str, bool):
    if len(payload_str) <= max_chars:
        return payload_str, False
    truncated = payload_str[:max_chars]
    note = "\n\nNOTE: payload truncated for size. Some rows/columns omitted.\n"
    return truncated + note, True

def _extract_json_object_from_text(text: str):
    if not isinstance(text, str):
        raise ValueError("Response text is not a string.")
    try:
        return json.loads(text)
    except Exception:
        pass

    first = text.find("{")
    if first == -1:
        raise ValueError("No JSON object found in response text.")

    depth = 0
    for i in range(first, len(text)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[first:i+1]
                try:
                    return json.loads(candidate)
                except Exception:
                    pass
    raise ValueError("Failed to extract a valid JSON object from response text.")

def build_column_detection_prompt_dict(payload: dict) -> dict:
    payload_str = json.dumps(payload, ensure_ascii=False, indent=2)
    payload_str, truncated = _truncate_payload_str(payload_str, MAX_PAYLOAD_CHARS)
    user_instructions = (
        "Below is a compact JSON payload summarizing representative pages from a bank statement.\n\n"
        "Payload:\n\n"
        f"{payload_str}\n\n"
        "Please identify which tables/pages contain transaction rows and map the ORIGINAL column names "
        "to the canonical roles requested. Output only a single JSON object following the schema described in the system instructions."
    )
    return {"system": SYSTEM_INSTRUCTIONS, "user": user_instructions}


def run_column_detection_with_deepseek(blocks, stmt_pk=None, timeout: int = 60):
    """
    Stage 1: Build payload from Textract blocks, then call DeepSeek to detect 
    transaction table(s) and column mapping.
    """
    try:
        # 1️⃣ Build payload (using textract_sampling)
        payload = process_textract_blocks(blocks)

        # 2️⃣ Build prompt
        prompt = build_column_detection_prompt_dict(payload)

        # 3️⃣ Save prompt for debug
        debug_dir = os.path.join(settings.BASE_DIR, "debug_exports")
        os.makedirs(debug_dir, exist_ok=True)
        
        if stmt_pk:
            with open(
                os.path.join(debug_dir, f"deepseek_stage1_prompt_{stmt_pk}.txt"),
                "w", encoding="utf-8"
            ) as f:
                f.write(json.dumps(prompt, indent=2))

        # 4️⃣ Call DeepSeek
        response_text = call_deepseek(prompt, timeout=timeout)

        # 5️⃣ Save raw response for debug
        if stmt_pk:
            with open(
                os.path.join(debug_dir, f"deepseek_stage1_response_{stmt_pk}.txt"),
                "w", encoding="utf-8"
            ) as f:
                f.write(response_text)

        # 6️⃣ Parse JSON result
        try:
            parsed = _extract_json_object_from_text(response_text)
        except Exception as e:
            error_msg = f"Failed to parse DeepSeek JSON: {e}"
            print(f"DEBUG: {error_msg}")
            return {"error": error_msg, "raw_response": response_text}

        # 7️⃣ Basic validation
        if not isinstance(parsed, dict):
            error_msg = "DeepSeek returned JSON that is not a dict."
            print(f"DEBUG: {error_msg}")
            return {"error": error_msg, "raw_response": response_text}

        # Check for required fields
        if "tables" not in parsed:
            parsed["_validation_warning"] = "DeepSeek JSON missing 'tables' field."

        print(f"DEBUG: DeepSeek Stage 1 successful. Found {len(parsed.get('tables', []))} tables")
        return parsed

    except Exception as e:
        error_msg = f"Exception during DeepSeek Stage 1: {e}"
        print(f"DEBUG: {error_msg}")
        
        # Save exception for debug
        debug_dir = os.path.join(settings.BASE_DIR, "debug_exports")
        os.makedirs(debug_dir, exist_ok=True)
        
        if stmt_pk:
            with open(
                os.path.join(debug_dir, f"deepseek_stage1_exception_{stmt_pk}.txt"),
                "w", encoding="utf-8"
            ) as f:
                f.write(traceback.format_exc())
                
        return {"error": error_msg, "trace": traceback.format_exc()}