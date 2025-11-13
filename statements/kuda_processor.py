# Create a new file: statements/kuda_processor.py

import pandas as pd
import re
from datetime import datetime
from typing import List, Dict, Any

def process_kuda_statement(blocks: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Specialized processor for Kuda bank statements.
    Focuses on extracting only actual transaction data.
    """
    # Extract all text lines first
    lines = _extract_lines(blocks)
    
    # Find transaction sections
    transactions = _extract_kuda_transactions(lines)
    
    # Convert to DataFrame
    df = _create_transactions_dataframe(transactions)
    
    return df

def _extract_lines(blocks: List[Dict[str, Any]]) -> List[str]:
    """Extract and clean text lines from blocks."""
    lines = []
    for block in blocks:
        if block.get("BlockType") == "LINE":
            text = block.get("Text", "").strip()
            if text and _is_transaction_line(text):
                lines.append(text)
    return lines

def _is_transaction_line(text: str) -> bool:
    """Check if a line contains transaction data."""
    # Skip header, footer, and summary lines
    skip_patterns = [
        r'kuda', r'summary', r'account', r'opening balance', 
        r'closing balance', r'page \d+ of \d+', r'all rights reserved',
        r'deposits are insured', r'licensed by', r'trademarks',
        r'account number', r'street', r'kano', r'lagos', r'london'
    ]
    
    text_lower = text.lower()
    if any(re.search(pattern, text_lower) for pattern in skip_patterns):
        return False
    
    # Look for transaction patterns: date + amount
    date_pattern = r'\d{1,2}/\d{1,2}/\d{2,4}'
    amount_pattern = r'[¥₦$]\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?'
    
    has_date = bool(re.search(date_pattern, text))
    has_amount = bool(re.search(amount_pattern, text))
    
    return has_date and has_amount

def _extract_kuda_transactions(lines: List[str]) -> List[Dict]:
    """Extract transaction data from Kuda statement lines."""
    transactions = []
    current_transaction = None
    
    for line in lines:
        # Check if this line starts a new transaction
        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})\s+(\d{1,2}:\d{2}:\d{2})', line)
        if date_match:
            # Save previous transaction
            if current_transaction and _is_valid_transaction(current_transaction):
                transactions.append(current_transaction)
            
            # Start new transaction
            date_str = date_match.group(1)
            time_str = date_match.group(2)
            current_transaction = {
                'date': f"{date_str} {time_str}",
                'raw_text': line,
                'description': '',
                'debit': 0.0,
                'credit': 0.0,
                'balance': 0.0
            }
        elif current_transaction:
            # Continue building current transaction
            current_transaction['raw_text'] += ' ' + line
    
    # Add the last transaction
    if current_transaction and _is_valid_transaction(current_transaction):
        transactions.append(current_transaction)
    
    return transactions

def _is_valid_transaction(transaction: Dict) -> bool:
    """Validate that this looks like a real transaction."""
    raw_text = transaction['raw_text'].lower()
    
    # Must have actual transaction keywords
    transaction_indicators = [
        'airtime', 'transfer', 'purchase', 'bill', 'kedco', 
        'inward', 'reversal', 'withdrawal', 'payment'
    ]
    
    return any(indicator in raw_text for indicator in transaction_indicators)

def _create_transactions_dataframe(transactions: List[Dict]) -> pd.DataFrame:
    """Convert transaction list to clean DataFrame."""
    if not transactions:
        return pd.DataFrame()
    
    parsed_transactions = []
    for tx in transactions:
        parsed = _parse_kuda_transaction(tx)
        if parsed:
            parsed_transactions.append(parsed)
    
    # Create DataFrame with proper columns
    df = pd.DataFrame(parsed_transactions)
    
    # Ensure all required columns exist
    required_columns = ['date', 'description', 'debit', 'credit', 'balance', 'channel', 'transaction_reference']
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''
    
    return df[required_columns]

def _parse_kuda_transaction(transaction: Dict) -> Dict:
    """Parse individual Kuda transaction."""
    text = transaction['raw_text']
    
    # Extract amounts
    amount_pattern = r'[¥₦$]?\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)'
    amounts = re.findall(amount_pattern, text)
    amounts = [float(amt.replace(',', '')) for amt in amounts if amt.replace(',', '').replace('.', '').isdigit()]
    
    # Determine debit/credit
    debit = 0.0
    credit = 0.0
    balance = 0.0
    
    if amounts:
        if 'inward transfer' in text.lower() or 'reversal' in text.lower():
            credit = amounts[0] if len(amounts) > 0 else 0.0
            balance = amounts[1] if len(amounts) > 1 else 0.0
        else:
            debit = amounts[0] if len(amounts) > 0 else 0.0
            balance = amounts[1] if len(amounts) > 1 else 0.0
    
    # Extract description
    description = _extract_description(text)
    
    # Extract channel
    channel = _extract_channel(text)
    
    return {
        'date': transaction['date'],
        'description': description,
        'debit': debit,
        'credit': credit,
        'balance': balance,
        'channel': channel,
        'transaction_reference': _extract_reference(text)
    }

def _extract_description(text: str) -> str:
    """Extract clean description from transaction text."""
    # Remove date, time, and amounts
    cleaned = re.sub(r'\d{1,2}/\d{1,2}/\d{2,4}', '', text)
    cleaned = re.sub(r'\d{1,2}:\d{2}:\d{2}', '', cleaned)
    cleaned = re.sub(r'[¥₦$]\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?', '', cleaned)
    
    # Clean up extra spaces
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned

def _extract_channel(text: str) -> str:
    """Extract transaction channel."""
    text_lower = text.lower()
    if 'airtime' in text_lower:
        return 'AIRTIME'
    elif 'transfer' in text_lower:
        return 'TRANSFER'
    elif 'bill' in text_lower or 'kedco' in text_lower:
        return 'BILLS'
    elif 'reversal' in text_lower:
        return 'REVERSAL'
    else:
        return 'OTHER'

def _extract_reference(text: str) -> str:
    """Extract transaction reference if available."""
    # Look for phone numbers or reference numbers
    phone_match = re.search(r'\d{10,13}', text)
    if phone_match:
        return phone_match.group(0)
    return ''