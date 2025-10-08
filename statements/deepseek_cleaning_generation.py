import os
import json
import traceback
import pandas as pd
from django.conf import settings
from .deepseek_utils import call_deepseek
from .sandbox_utils import run_user_code_in_sandbox  # you already have this utility
from .cleaning_utils import robust_clean_dataframe


SYSTEM_INSTRUCTIONS_STAGE2 = (
    "You are an expert Python data engineer. "
    "You will receive: (1) a JSON object describing detected column mappings and "
    "(2) a sample raw table from OCR extraction. "
    "Your task is to generate Python code that takes a pandas DataFrame `df_raw` "
    "and returns a cleaned DataFrame `df_clean` ready for analysis.\n\n"
    "Requirements:\n"
    "- Fix split or broken transaction rows (e.g., dates or descriptions split across multiple rows).\n"
    "- Use the provided column_mapping to rename columns correctly.\n"
    "- Detect debit/credit columns, handle negatives, and compute a single 'debit' and 'credit' if necessary.\n"
    "- Preserve numeric formatting, handle commas, and cast balances as float.\n"
    "- Parse dates robustly using `pd.to_datetime` with `errors='coerce'` and `dayfirst=True`.\n"
    "- Ensure the function returns a valid pandas DataFrame with these columns:\n"
    "  ['date', 'value_date', 'description', 'debit', 'credit', 'balance', 'channel', 'transaction_reference']\n"
    "- Output ONLY the Python code (no markdown or explanations)."
)


def build_stage2_prompt(column_detection_result: dict, df_raw: pd.DataFrame):
    """Construct DeepSeek Stage 2 prompt."""
    sample_rows = df_raw.head(20).to_dict(orient="records")
    payload = {
        "column_detection_result": column_detection_result,
        "sample_rows": sample_rows,
    }
    user_prompt = (
        "Below is the detected column mapping and sample raw data. "
        "Generate Python code that cleans the table as instructed.\n\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )
    return {"system": SYSTEM_INSTRUCTIONS_STAGE2, "user": user_prompt}


def run_stage2_cleaning_with_deepseek(df_raw: pd.DataFrame, deepseek_stage1_result: dict, stmt_pk=None):
    """
    Stage 2: Ask DeepSeek to generate Python cleaning code, execute it safely,
    and return cleaned DataFrame. Falls back to robust_clean_dataframe if it fails.
    """
    try:
        # 1. Build prompt
        prompt = build_stage2_prompt(deepseek_stage1_result, df_raw)

        # 2. Call DeepSeek
        response_text = call_deepseek(prompt, timeout=90)

        # 3. Save raw response for debugging
        debug_dir = os.path.join(settings.BASE_DIR, "debug_exports")
        os.makedirs(debug_dir, exist_ok=True)
        if stmt_pk:
            with open(os.path.join(debug_dir, f"deepseek_stage2_response_{stmt_pk}.txt"), "w", encoding="utf-8") as f:
                f.write(response_text)

        # 4. Extract Python code (strip markdown, if any)
        code = response_text
        if "```" in code:
            code = code.split("```")[1]
            code = code.replace("python", "").strip()

        # 5. Save generated code for reference
        if stmt_pk:
            with open(os.path.join(debug_dir, f"deepseek_stage2_code_{stmt_pk}.py"), "w", encoding="utf-8") as f:
                f.write(code)

        # 6. Run code in sandbox
        df_clean = run_user_code_in_sandbox(code, df_raw)

        # 7. Validate output
        if not isinstance(df_clean, pd.DataFrame):
            raise ValueError("DeepSeek code did not return a DataFrame")

        print("DEBUG: Stage 2 DeepSeek cleaning succeeded with shape:", df_clean.shape)
        return df_clean

    except Exception as e:
        print("WARNING: DeepSeek Stage 2 cleaning failed:", str(e))
        print(traceback.format_exc())
        print("DEBUG: Falling back to robust_clean_dataframe")
        return robust_clean_dataframe(df_raw)
