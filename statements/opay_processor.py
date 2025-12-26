# statements/opay_processor.py
import pandas as pd
import re
from datetime import datetime
from typing import List, Dict, Any


def process_opay_statement(blocks: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Specialized processor for OPay bank statements.
    Focuses on extracting OPay-specific transaction format.
    """
    # Extract all text lines first
    lines = []
    for block in blocks:
        if block.get("BlockType") == "LINE":
            text = block.get("Text", "").strip()
            if text and _is_opay_transaction_line(text):
                lines.append(text)
    
    print(f"DEBUG: Found {len(lines)} OPAY transaction lines")
    
    # Parse OPAY transactions
    transactions = []
    
    for line in lines:
        transaction = _parse_opay_line(line)
        if transaction and _is_valid_opay_transaction(transaction):
            transactions.append(transaction)
            print(f"DEBUG: Found OPAY transaction: {transaction}")
    
    print(f"DEBUG: Extracted {len(transactions)} OPAY transactions")
    
    # Convert to DataFrame
    if not transactions:
        return pd.DataFrame()
    
    df = pd.DataFrame(transactions)
    
    # Ensure all required columns exist
    required_columns = ['date', 'description', 'debit', 'credit', 'balance', 'channel', 'transaction_reference']
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''
    
    return df[required_columns]


def _is_opay_transaction_line(text: str) -> bool:
    """Check if a line contains OPAY transaction data."""
    # OPAY format: YYYY MMM DD HH:MM:SS + description + amount
    opay_pattern = r'\d{4}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{1,2}:\d{2}:\d{2}'
    
    return bool(re.search(opay_pattern, text))


def _parse_opay_line(line: str) -> Dict:
    """Parse a single OPAY transaction line."""
    # Extract date and time: "2025 Feb 24 07:36:01"
    datetime_match = re.search(r'(\d{4})\s+([A-Za-z]{3})\s+(\d{1,2})\s+(\d{1,2}):(\d{2}):(\d{2})', line)
    
    if not datetime_match:
        return None
    
    # CRITICAL: Keep date as raw text - don't parse to datetime yet!
    # Let robust_clean_dataframe() handle parsing and validation
    raw_date_str = datetime_match.group(0)  # e.g., "2025 Feb 24 07:36:01"
    
    print(f"DEBUG: OPAY extracted raw date: '{raw_date_str}'")
    
    # Extract amounts - look for numbers with currency symbols
    amounts = re.findall(r'[₦$]?\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)', line)
    amounts = [float(amt.replace(',', '')) for amt in amounts if amt.replace(',', '').replace('.', '').isdigit()]
    
    # Extract description
    description = _clean_opay_description(line, amounts)
    
    # Determine debit/credit
    debit = 0.0
    credit = 0.0
    
    if amounts:
        # OPAY typically shows credit as positive, debit as negative
        if len(amounts) >= 2:
            # Assume second amount is balance, first is transaction
            transaction_amount = amounts[0]
            if transaction_amount > 0:
                credit = transaction_amount
            else:
                debit = abs(transaction_amount)
        elif len(amounts) == 1:
            transaction_amount = amounts[0]
            if transaction_amount > 0:
                credit = transaction_amount
            else:
                debit = abs(transaction_amount)
    
    # Extract channel
    channel = _extract_opay_channel(line)
    
    return {
        'date': raw_date_str,  # Raw text, not datetime!
        'description': description,
        'debit': debit,
        'credit': credit,
        'balance': amounts[1] if len(amounts) > 1 else 0.0,
        'channel': channel,
        'transaction_reference': _extract_opay_reference(line)
    }


def _clean_opay_description(line: str, amounts: List[float]) -> str:
    """Clean description by removing dates, times, and amounts."""
    # Remove date/time pattern
    cleaned = re.sub(r'\d{4}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{1,2}:\d{2}:\d{2}', '', line)
    
    # Remove amounts
    for amount in amounts:
        cleaned = cleaned.replace(str(amount), '')
    
    # Remove currency symbols
    cleaned = re.sub(r'[₦$]', '', cleaned)
    
    # Clean up extra spaces
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned


def _extract_opay_channel(line: str) -> str:
    """Extract transaction channel from OPAY line."""
    line_lower = line.lower()
    
    if 'airtime' in line_lower:
        return 'AIRTIME'
    elif 'transfer' in line_lower:
        return 'TRANSFER'
    elif 'bill' in line_lower:
        return 'BILLS'
    elif 'pos' in line_lower:
        return 'POS'
    elif 'atm' in line_lower:
        return 'ATM'
    elif 'reversal' in line_lower:
        return 'REVERSAL'
    else:
        return 'OTHER'


def _extract_opay_reference(line: str) -> str:
    """Extract transaction reference from OPAY line."""
    # Look for transaction ID patterns
    ref_match = re.search(r'[A-Z]{2}\d{8,12}', line)  # OPAY reference format
    if ref_match:
        return ref_match.group(0)
    
    # Look for phone numbers
    phone_match = re.search(r'\d{10,13}', line)
    if phone_match:
        return phone_match.group(0)
    
    return ''


def _is_valid_opay_transaction(transaction: Dict) -> bool:
    """Validate that this looks like a real OPAY transaction."""
    # Must have non-empty description
    if not transaction.get('description', '').strip():
        return False
    
    # Must have either debit or credit
    if transaction.get('debit', 0) == 0 and transaction.get('credit', 0) == 0:
        return False
    
    # Description must not contain footer text
    footer_terms = ['opay', 'rights reserved', 'deposit insurance', 'page', 'account number']
    description_lower = transaction.get('description', '').lower()
    
    return not any(term in description_lower for term in footer_terms)
