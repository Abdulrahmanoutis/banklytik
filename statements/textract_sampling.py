# banklytik/statements/textract_sampling.py
import json
import re
import random
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

def normalize_header_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.strip().lower()
    text = text.replace("\\/", "/")
    text = text.replace("(w)", "naira").replace("(n)", "naira").replace("₦", "naira")
    text = text.replace("dr/cr", "debit_credit").replace("debit/credit", "debit_credit")
    text = re.sub(r"[^a-z0-9_ ]", "", text)
    if "debit" in text and "credit" in text:
        return "debit_credit"
    if "balance" in text:
        return "balance"
    if "date" in text or "trans" in text or "value" in text:
        return "date"
    if "amount" in text or "naira" in text:
        return "amount"
    if "desc" in text:
        return "description"
    if "ref" in text:
        return "transaction_reference"
    if "channel" in text:
        return "channel"
    return text

def organize_blocks_by_page(blocks):
    pages = defaultdict(lambda: {"tables": [], "lines": []})
    for b in blocks:
        page = b.get("Page", 1)
        if b.get("BlockType") == "TABLE":
            pages[page]["tables"].append(b)
        elif b.get("BlockType") == "LINE":
            pages[page]["lines"].append(b.get("Text", ""))
    return pages

def detect_transaction_pages_from_sample(pages_data):
    tx_pages = []
    for p, content in pages_data.items():
        for table in content.get("tables", []):
            # try to collect header lines inside the table block if present
            headers = []
            if "Children" in table:
                for cell in table.get("Children", []):
                    if isinstance(cell, dict):
                        text = cell.get("Text", "")
                        headers.append(normalize_header_text(text))
            # fallback: also check nearby lines for header-like text
            header_row = " ".join(headers + [normalize_header_text(l) for l in content.get("lines", [])[:5]])
            has_date = any(k in header_row for k in ["date", "trans", "value_date"])
            has_balance = "balance" in header_row
            has_amount = any(k in header_row for k in ["debit", "credit", "debit_credit", "amount", "naira"])
            if has_date and has_balance and has_amount:
                tx_pages.append(p)
                break
    return tx_pages

def sample_representative_pages(blocks, max_pages=3):
    # returns sampled_pages, and pages_data (detailed)
    pages_data = organize_blocks_by_page(blocks)
    pages = sorted(list(pages_data.keys()))
    if not pages:
        return [], pages_data

    tx_pages = detect_transaction_pages_from_sample(pages_data)
    if tx_pages:
        sampled = tx_pages[:max_pages]
    else:
        # pick first, mid, last as fallback
        if len(pages) <= max_pages:
            sampled = pages
        else:
            mid = pages[len(pages)//2]
            sampled = [pages[0], mid, pages[-1]][:max_pages]
    return sampled, pages_data

def build_deepseek_sampling_payload(pages_data, sampled_pages):
    payload = {"pages": []}
    for p in sampled_pages:
        payload["pages"].append({
            "page_number": p,
            "lines": pages_data[p]["lines"][:200],  # limit
            # tables may be heavy; we only send lines and small table summaries
            "num_tables": len(pages_data[p]["tables"]),
        })
    payload["metadata"] = {"num_pages": len(pages_data), "sampled_pages": sampled_pages}
    logger.debug("Payload built successfully with %d pages", len(payload["pages"]))
    return payload

def process_textract_blocks(textract_blocks):
    """
    Main unified entry used across the pipeline.
    Returns a payload dict ready to be inserted into DeepSeek prompts.
    """
    sampled_pages, pages_data = sample_representative_pages(textract_blocks)
    payload = build_deepseek_sampling_payload(pages_data, sampled_pages)
    payload["pages_data"] = pages_data  # optional extra context
    return payload