"""
Stage 2 DeepSeek Cleaning:
Takes DeepSeek Stage 1 result + Textract blocks and produces
a clean, normalized pandas DataFrame ready for the UI.
"""

import pandas as pd
import logging
import re

logger = logging.getLogger(__name__)


def clean_amount(value):
    """Convert currency strings like '₦1,200.50' or '-100.00' to float."""
    if pd.isna(value):
        return 0.0
    s = str(value)
    s = s.replace("₦", "").replace(",", "").replace(" ", "")
    try:
        return float(s)
    except Exception:
        # handle weird OCR garbage
        s = re.sub(r"[^0-9.\-]", "", s)
        try:
            return float(s)
        except Exception:
            return 0.0


def normalize_description(desc):
    """Trim and fix OCR noise in description text."""
    if not isinstance(desc, str):
        return ""
    desc = re.sub(r"\s+", " ", desc.strip())
    return desc


def extract_channel(description):
    """Heuristic channel extraction."""
    d = description.upper()
    if "ATM" in d:
        return "ATM"
    if "POS" in d:
        return "POS"
    if "TRANSFER" in d:
        return "TRANSFER"
    if "AIRTIME" in d:
        return "AIRTIME"
    if "CHARGE" in d:
        return "CHARGES"
    return "OTHER"


def run_deepseek_stage2_cleaning(textract_df, deepseek_stage1_result):
    """
    Given raw Textract DataFrame + DeepSeek stage 1 JSON,
    return a clean, normalized DataFrame.
    """

    logger.debug("Starting DeepSeek Stage 2 cleaning")
    if not isinstance(deepseek_stage1_result, dict) or "tables" not in deepseek_stage1_result:
        logger.warning("DeepSeek Stage 1 result missing or invalid → fallback triggered")
        return None

    tables = deepseek_stage1_result.get("tables", [])
    if not tables:
        logger.warning("No tables found in DeepSeek Stage 1 result")
        return None

    # Build unified DataFrame based on DeepSeek column mapping
    all_frames = []
    for table_meta in tables:
        mapping = table_meta.get("column_mapping", {})
        if not mapping:
            continue

        logger.debug(f"Applying mapping: {mapping}")

        # map raw columns if header names exist in raw DataFrame
        df_copy = textract_df.copy()
        df_copy.columns = [c.strip() for c in df_copy.columns]

        # re-assign columns based on DeepSeek mapping
        renamed = {}
        for raw_col, normalized in mapping.items():
            for actual_col in df_copy.columns:
                if raw_col.lower() in str(actual_col).lower():
                    renamed[actual_col] = normalized
        df_copy = df_copy.rename(columns=renamed)

        # keep only recognized columns
        keep = [c for c in [
            "date", "value_date", "description", "debit_credit",
            "balance", "channel", "transaction_reference"
        ] if c in df_copy.columns]

        df_copy = df_copy[keep].copy()
        all_frames.append(df_copy)

    if not all_frames:
        logger.warning("No valid tables mapped in Stage 2")
        return None

    df = pd.concat(all_frames, ignore_index=True)
    logger.debug(f"Concatenated Stage 2 DF shape: {df.shape}")

    # --- Clean individual columns ---
    if "description" in df.columns:
        df["description"] = df["description"].apply(normalize_description)
        df["channel"] = df["description"].apply(extract_channel)

    if "debit_credit" in df.columns:
        # Split combined debit/credit column into two
        dc_series = df["debit_credit"].astype(str)
        df["debit"] = dc_series.apply(
            lambda x: clean_amount(x) if "-" in x or "dr" in x.lower() else 0.0
        )
        df["credit"] = dc_series.apply(
            lambda x: clean_amount(x) if "+" in x or "cr" in x.lower() else 0.0
        )

    if "balance" in df.columns:
        df["balance"] = df["balance"].apply(clean_amount)

    # --- Parse dates ---
    for col in ["date", "value_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    # --- Drop empty rows ---
    df = df.dropna(subset=["date", "description"], how="all").reset_index(drop=True)

    # --- Final column order ---
    preferred_cols = [
        "date", "value_date", "description",
        "debit", "credit", "balance",
        "channel", "transaction_reference"
    ]
    for c in preferred_cols:
        if c not in df.columns:
            df[c] = None

    df = df[preferred_cols]

    logger.debug(f"DeepSeek Stage 2 cleaned DF shape: {df.shape}")
    logger.debug(f"DeepSeek Stage 2 sample:\n{df.head(10)}")

    return df
