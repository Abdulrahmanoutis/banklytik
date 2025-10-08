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
    """
    Truncate payload_str to max_chars if needed. Returns (payload_str, truncated_flag).
    """
    if len(payload_str) <= max_chars:
        return payload_str, False
    truncated = payload_str[:max_chars]
    note = "\n\nNOTE: payload truncated for size. Some rows/columns omitted.\n"
    return truncated + note, True

def _extract_json_object_from_text(text: str):
    """
    Try to extract the first balanced JSON object from text.
    Returns parsed JSON or raises ValueError.
    """
    if not isinstance(text, str):
        raise ValueError("Response text is not a string.")

    # Quick attempt: try direct load
    try:
        return json.loads(text)
    except Exception:
        pass

    # Find first '{' and match braces
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
                    # continue searching if this candidate fails parsing
                    pass
    raise ValueError("Failed to extract a valid JSON object from response text.")

def build_column_detection_prompt_dict(payload: dict) -> dict:
    """
    Returns a prompt dict with 'system' and 'user' keys suitable for call_deepseek().
    """
    payload_str = json.dumps(payload, ensure_ascii=False, indent=2)
    payload_str, truncated = _truncate_payload_str(payload_str, MAX_PAYLOAD_CHARS)

    user_instructions = (
        "Below is a compact JSON payload summarizing representative pages from a bank statement.\n\n"
        "Payload:\n\n"
        f"{payload_str}\n\n"
        "Please identify which tables/pages contain transaction rows and map the ORIGINAL column names "
        "to the canonical roles requested. Output only a single JSON object following the schema described in the system instructions."
    )

    prompt = {"system": SYSTEM_INSTRUCTIONS, "user": user_instructions}
    return prompt

def run_column_detection_with_deepseek(blocks, stmt_pk=None, timeout: int = 60):
    """
    Full DeepSeek pipeline:
    Stage 1 → column detection
    Stage 2 → structured data cleaning
    Falls back gracefully on failure.
    """

    from .textract_sampling import process_textract_blocks
    from .deepseek_stage2_cleaning import run_deepseek_stage2_cleaning
    from .cleaning_utils import robust_clean_dataframe
    import pandas as pd

    try:
        # -------------------------------
        # 1) Build compact sampling payload
        # -------------------------------
        payload = process_textract_blocks(blocks)
        prompt = build_column_detection_prompt_dict(payload)
        prompt_text_for_debug = prompt["user"]

        # -------------------------------
        # 2) Save Stage 1 prompt (debug)
        # -------------------------------
        try:
            debug_dir = os.path.join(settings.BASE_DIR, "debug_exports")
            os.makedirs(debug_dir, exist_ok=True)
            if stmt_pk:
                with open(
                    os.path.join(debug_dir, f"deepseek_stage1_prompt_{stmt_pk}.txt"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(prompt_text_for_debug)
        except Exception:
            print("DEBUG: failed to save stage1 prompt:", traceback.format_exc())

        # -------------------------------
        # 3) Call DeepSeek API (Stage 1)
        # -------------------------------
        response_text = call_deepseek(prompt, timeout=timeout)

        # -------------------------------
        # 4) Save Stage 1 raw response
        # -------------------------------
        try:
            debug_dir = os.path.join(settings.BASE_DIR, "debug_exports")
            os.makedirs(debug_dir, exist_ok=True)
            if stmt_pk:
                with open(
                    os.path.join(debug_dir, f"deepseek_stage1_response_{stmt_pk}.txt"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response_text)
        except Exception:
            print("DEBUG: failed to save stage1 response:", traceback.format_exc())

        # -------------------------------
        # 5) Parse JSON safely
        # -------------------------------
        try:
            stage1_result = _extract_json_object_from_text(response_text)
        except Exception as e:
            return {"error": f"Failed to parse DeepSeek JSON: {str(e)}", "raw_response": response_text}

        if not isinstance(stage1_result, dict):
            return {"error": "DeepSeek returned non-JSON object.", "raw_response": response_text}

        # -------------------------------
        # 6) Build raw DataFrame from blocks
        # -------------------------------
        import pandas as pd

        # In your existing pipeline, df_raw is extracted earlier;
        # if not, build a basic placeholder here:
        rows = []
        for blk in blocks:
            if blk.get("BlockType") == "TABLE" and "Rows" in blk:
                rows.extend(blk["Rows"])
        df_raw = pd.DataFrame(rows) if rows else pd.DataFrame()

        # -------------------------------
        # 7) Run DeepSeek Stage 2 cleaning
        # -------------------------------
        try:
            df_clean = run_deepseek_stage2_cleaning(df_raw, stage1_result)
            if df_clean is not None and not df_clean.empty:
                logger.debug("DeepSeek Stage 2 succeeded.")
                return df_clean
            else:
                logger.warning("DeepSeek Stage 2 returned empty DF → fallback cleaner triggered.")
        except Exception as stage2_err:
            logger.warning(f"Stage 2 cleaning failed: {stage2_err}")

        # -------------------------------
        # 8) Fallback cleaner
        # -------------------------------
        logger.debug("Falling back to heuristic cleaner.")
        df_fallback = robust_clean_dataframe(df_raw)
        return df_fallback

    except Exception as e:
        # -------------------------------
        # 9) Capture and persist crash info
        # -------------------------------
        err = {"error": f"DeepSeek pipeline failed: {str(e)}", "trace": traceback.format_exc()}
        try:
            debug_dir = os.path.join(settings.BASE_DIR, "debug_exports")
            os.makedirs(debug_dir, exist_ok=True)
            if stmt_pk:
                with open(
                    os.path.join(debug_dir, f"deepseek_stage1_exception_{stmt_pk}.txt"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(err["trace"])
        except Exception:
            pass
        return err
