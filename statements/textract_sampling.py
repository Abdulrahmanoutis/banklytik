import json
import re
import random
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


def normalize_header_text(text: str) -> str:
    """
    Normalize header strings to improve matching accuracy for OCR noise.
    Handles variants like 'Debit/Credit(W)', 'Balance(N)', 'Dr/Cr', etc.
    """
    if not isinstance(text, str):
        return ""

    # Basic cleanup
    text = text.strip().lower()

    # Common OCR misreads and symbol variants
    substitutions = {
        "\\/": "/",           # OCR escaping
        "(w)": "naira",
        "(n)": "naira",
        "(₦)": "naira",
        "₦": "naira",
        "n)": "naira",
        "dr/cr": "debit_credit",
        "cr/dr": "debit_credit",
        "debit/credit": "debit_credit",
        "debit credit": "debit_credit",
        "credit/debit": "debit_credit",
    }

    for k, v in substitutions.items():
        text = text.replace(k, v)

    # Remove unwanted punctuation except underscores and spaces
    text = re.sub(r"[^a-z0-9_ ]", "", text)

    # Handle fuzzy variants
    if "debit" in text and "credit" in text:
        text = "debit_credit"
    elif "balance" in text:
        text = "balance"
    elif "date" in text or "trans" in text or "value" in text:
        text = "date"
    elif "amount" in text or "naira" in text:
        text = "amount"
    elif "desc" in text:
        text = "description"
    elif "ref" in text:
        text = "transaction_reference"
    elif "channel" in text:
        text = "channel"

    return text


def identify_transaction_pages(pages_data):
    """
    Identify transaction-like pages using improved header matching.
    """
    transaction_pages = []
    for page_num, page_content in pages_data.items():
        if "tables" not in page_content:
            continue

        for table in page_content["tables"]:
            header_texts = [normalize_header_text(h) for h in table.get("headers", [])]
            header_row = " ".join(header_texts)

            # Look for all three key indicators together
            has_date = any(k in header_row for k in ["date", "trans", "value_date"])
            has_balance = "balance" in header_row
            has_amount = any(k in header_row for k in ["debit", "credit", "debit_credit", "naira", "amount"])

            # Strong heuristic: must contain at least these three
            if has_date and has_balance and has_amount:
                transaction_pages.append(page_num)
                break

    return transaction_pages


def sample_representative_pages(textract_json):
    """
    Organize textract output into pages and detect transaction tables.
    """
    logger.debug(f"Type of blocks before DeepSeek: {type(textract_json)}")

    pages_data = defaultdict(lambda: {"tables": [], "lines": []})

    for block in textract_json:
        if block.get("BlockType") == "TABLE":
            page = block.get("Page", 1)
            table_data = {"headers": [], "cells": []}

            # Collect headers if present
            relationships = block.get("Relationships", [])
            for rel in relationships:
                if rel.get("Type") == "CHILD":
                    for child_id in rel.get("Ids", []):
                        pass  # In your setup, DeepSeek handles cell mapping

            pages_data[page]["tables"].append(table_data)

        elif block.get("BlockType") == "LINE":
            page = block.get("Page", 1)
            pages_data[page]["lines"].append(block.get("Text", ""))

    logger.debug(f"Organized {len(textract_json)} blocks into {len(pages_data)} pages")

    # Step 1: detect transaction-like pages
    transaction_like_pages = identify_transaction_pages(pages_data)
    logger.debug(f"Transaction-like pages identified: {transaction_like_pages}")

    # Step 2: sample pages (fallback if none found)
    all_pages = list(pages_data.keys())
    if not transaction_like_pages:
        sample_pages = random.sample(all_pages, min(len(all_pages), 2))
    else:
        sample_pages = transaction_like_pages[:2]

    logger.debug(f"sample_representative_pages → {sample_pages}")
    return sample_pages, pages_data


def build_deepseek_sampling_payload(pages_data, sampled_pages):
    """
    Build payload for DeepSeek analysis.
    """
    payload = {"pages": []}
    for p in sampled_pages:
        page_lines = pages_data[p]["lines"]
        payload["pages"].append({
            "page_number": p,
            "content": "\n".join(page_lines)
        })
    logger.debug(f"Payload built successfully with {len(payload['pages'])} pages")
    return payload


def process_textract_blocks(textract_blocks):
    """
    Main entry point called from your view.
    """
    sampled_pages, pages_data = sample_representative_pages(textract_blocks)
    deepseek_payload = build_deepseek_sampling_payload(pages_data, sampled_pages)
    return deepseek_payload
