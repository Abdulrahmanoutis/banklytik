# Bank Processor Architecture

## Overview

This document describes the scalable architecture for processing bank statements from multiple banks while maintaining consistent validation and allowing bank-specific extraction rules.

## Design Pattern: Hybrid Architecture

### Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    LAYER 1: EXTRACTION                      │
│  Bank-Specific Processors (opay_processor, kuda_processor) │
│  • Extract raw text dates AS-IS (no parsing)                │
│  • Maintain bank-specific extraction rules                  │
│  • Return DataFrame with 'raw_date' column (text only)      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              LAYER 2: VALIDATION & CLEANING                 │
│         Central Pipeline (robust_clean_dataframe)           │
│  • Receives raw dates from ANY bank processor               │
│  • Applies universal validation rules                       │
│  • Applies bank-specific rules from knowledge base          │
│  • Flags quality issues (incomplete dates, OCR errors)      │
│  • Parses dates to datetime objects                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   LAYER 3: PRESENTATION                     │
│                         UI Layer                            │
│  • Displays validation warnings/errors                      │
│  • Shows both raw_date and parsed_date                      │
│  • Provides interactive editing for corrections             │
└─────────────────────────────────────────────────────────────┘
```

## Key Principles

### 1. Separation of Concerns

**Bank Processors** (Layer 1):
- Focus: Extract transaction data from raw text/tables
- Responsibility: Bank-specific format recognition
- Output: DataFrame with text-based dates

**Central Pipeline** (Layer 2):
- Focus: Validate and normalize data
- Responsibility: Consistent quality across all banks
- Output: Clean DataFrame with validated dates

**UI Layer** (Layer 3):
- Focus: Display and user interaction
- Responsibility: Present validation results
- Output: User-friendly transaction view

### 2. Scalability

Adding a new bank is straightforward:

```python
# 1. Create new processor
def process_newbank_statement(blocks):
    transactions = []
    for block in blocks:
        # Extract using bank-specific logic
        transaction = {
            'raw_date': 'DD MMM YYYY',  # Keep as text
            'description': '...',
            'amount': '...'
        }
        transactions.append(transaction)
    return pd.DataFrame(transactions)

# 2. Pipeline automatically validates
# No changes needed to validation logic!
```

### 3. Maintainability

- **Fix validation once** → Applies to all banks
- **Bank-specific quirks** → Handled in knowledge base rules
- **No code duplication** → DRY principle

## Implementation Details

### Bank Processor Contract

Every bank processor MUST return a DataFrame with these columns:

```python
{
    'raw_date': str,          # Original date text (REQUIRED)
    'description': str,        # Transaction description
    'debit': float,           # Debit amount (0 if credit)
    'credit': float,          # Credit amount (0 if debit)
    'balance': float,         # Account balance
    'channel': str,           # Transaction channel
    'transaction_reference': str  # Reference/ID
}
```

**Critical:** `raw_date` must be kept as **text** - do NOT parse to datetime

### Example: OPay Processor

```python
def process_opay_statement(blocks):
    transactions = []
    
    for line in extract_lines(blocks):
        # Extract date text AS-IS
        date_match = re.search(r'(\d{4}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{1,2}:\d{2}:\d{2})', line)
        
        transaction = {
            'raw_date': date_match.group(1) if date_match else '',  # Text only!
            'description': extract_description(line),
            # ... other fields
        }
        transactions.append(transaction)
    
    return pd.DataFrame(transactions)
```

### Example: Kuda Processor

```python
def process_kuda_statement(blocks):
    transactions = []
    
    for row in extract_table(blocks):
        transaction = {
            'raw_date': row['Trans. Time'],  # Keep DD/MM/YY HH:MM:SS as text
            'description': row['Description'],
            # ... other fields
        }
        transactions.append(transaction)
    
    return pd.DataFrame(transactions)
```

### Central Validation Pipeline

The `robust_clean_dataframe()` function processes all bank outputs:

```python
def robust_clean_dataframe(df_raw):
    # 1. Preserve raw dates
    df['raw_date'] = df_raw['date'].astype(str)
    
    # 2. Parse dates with validation
    df['date'] = df['raw_date'].apply(parse_date_str)
    
    # 3. Apply validation rules
    df = validate_and_flag_dates(df)
    
    # 4. Return clean data with validation flags
    return df
```

### Validation Results

The pipeline adds these columns:

```python
{
    'date_validation_warning': 'INFO' | 'WARNING' | 'ERROR',
    'date_validation_issue': 'OK' | 'UNPARSEABLE' | 'OCR_ERROR_PATTERN' | etc,
    'date_correction_applied': 'None' | 'SMART_INFERENCE' | rule_name,
    'date_inference_confidence': 'LOW' | 'MEDIUM' | 'HIGH',
    'date_action_required': 'NONE' | 'MANUAL_REVIEW' | 'IMMEDIATE_REVIEW'
}
```

## Benefits

### For Development
- ✅ Add new banks quickly (no validation code needed)
- ✅ Test banks independently
- ✅ Fix validation bugs once for all banks
- ✅ Clear separation of concerns

### For Maintenance
- ✅ Bank-specific changes isolated to processors
- ✅ Validation changes centralized
- ✅ Easy to debug (clear data flow)
- ✅ Knowledge base provides flexibility

### For Users
- ✅ Consistent experience across banks
- ✅ Clear validation warnings
- ✅ Can see both raw and parsed dates
- ✅ Interactive correction interface

## Date Format Support

The pipeline automatically handles these common formats:

### OPay Formats
- `2025 Feb 24 07:36:01` → Datetime
- `23 Feb 2025` → Datetime  
- `Feb 2025` → ⚠️ WARNING (incomplete)

### Kuda Formats
- `24/02/25 23:08:23` → Datetime
- `24/02/2025` → Datetime
- `24/02/25` → Datetime

### GTB Formats
- `24 FEB 2025` → Datetime
- `24-FEB-2025` → Datetime
- `FEB 2025` → ⚠️ WARNING (incomplete)

### Universal Formats
- `2025-02-24` → Datetime
- `24/02/2025` → Datetime
- `24-02-2025` → Datetime

## Incomplete Date Detection

The system detects incomplete dates (missing day component):

```
Input: "Feb 2025"
Output: None + WARNING flag

Input: "February 2025"  
Output: None + WARNING flag

Input: "23 Feb 2025"
Output: datetime(2025, 2, 23) + INFO flag
```

This ensures data quality and alerts users to fix incomplete dates.

## Adding a New Bank

### Step 1: Create Processor

```python
# statements/newbank_processor.py

def process_newbank_statement(blocks):
    """
    Extract transactions from NewBank statements.
    Returns DataFrame with raw_date as text.
    """
    transactions = []
    
    # Bank-specific extraction logic
    for block in blocks:
        transaction = {
            'raw_date': extract_date_text(block),  # Text only!
            'description': extract_description(block),
            'debit': extract_debit(block),
            'credit': extract_credit(block),
            'balance': extract_balance(block),
            'channel': determine_channel(block),
            'transaction_reference': extract_reference(block)
        }
        transactions.append(transaction)
    
    return pd.DataFrame(transactions)
```

### Step 2: Register Processor

```python
# statements/processing_router.py

BANK_PROCESSORS = {
    'opay': process_opay_statement,
    'kuda': process_kuda_statement,
    'newbank': process_newbank_statement,  # Add here
}
```

### Step 3: Add Knowledge Base Rules (Optional)

```json
// banklytik_knowledge/rules/dates/newbank_rules.json
{
  "bank": "newbank",
  "rules": [
    {
      "title": "NewBank Date Format",
      "regex": "...",
      "category": "AUTO_CORRECT"
    }
  ]
}
```

### Step 4: Test

```python
# Test that validation works automatically
df = process_newbank_statement(blocks)
cleaned_df = robust_clean_dataframe(df)

assert 'date_validation_warning' in cleaned_df.columns
assert cleaned_df['date_validation_warning'].isin(['INFO', 'WARNING', 'ERROR']).all()
```

Done! The validation pipeline automatically handles your new bank.

## Debugging

### Enable Debug Logging

All key functions include debug logging:

```python
# In parse_date_str()
print(f"🔍 DEBUG parse_date_str: Input = '{date_str}' (type: {type(date_str)})")

# In robust_clean_dataframe()
print(f"DEBUG: robust_clean_dataframe input shape: {df.shape}")

# In validate_and_flag_dates()
print(f"📊 Enhanced Date Validation Summary:")
```

### Check Data Flow

```python
# 1. Check processor output
df_raw = process_opay_statement(blocks)
print("Processor output:", df_raw[['raw_date']].head())

# 2. Check after cleaning
df_clean = robust_clean_dataframe(df_raw)
print("After cleaning:", df_clean[['raw_date', 'date', 'date_validation_warning']].head())

# 3. Check in database
from statements.models import Transaction
txn = Transaction.objects.first()
print(f"DB: raw_date={txn.raw_date}, date={txn.date}")
```

## Known Issues and Solutions

### Issue: "Feb 2025" showing as valid

**Cause:** Bank processor parsing dates directly to datetime  
**Solution:** Return raw_date as text, let pipeline validate  
**Status:** ✅ Fixed in all processors

### Issue: Validation warnings not showing in UI

**Cause:** Template not receiving validation columns  
**Solution:** Ensure view passes validation flags to template  
**Status:** ✅ Fixed in detail.html and preview_data.html

### Issue: Different banks need different date formats

**Cause:** Using single format list for all banks  
**Solution:** Bank processors handle extraction, pipeline validates  
**Status:** ✅ Addressed by hybrid architecture

## Testing

### Unit Tests

```python
# Test processor
def test_opay_processor():
    df = process_opay_statement(test_blocks)
    assert 'raw_date' in df.columns
    assert df['raw_date'].dtype == 'object'  # Text, not datetime

# Test validation
def test_incomplete_date_detection():
    df = pd.DataFrame({'raw_date': ['Feb 2025']})
    df_clean = robust_clean_dataframe(df)
    assert df_clean['date_validation_warning'].iloc[0] in ['WARNING', 'ERROR']
```

### Integration Tests

```bash
# Run simple test
python simple_date_test.py

# Run comprehensive test (requires Django)
python test_complete_feb_2025_fix.py
```

## Summary

This architecture provides:

- ✅ **Scalability**: Easy to add new banks
- ✅ **Maintainability**: Fix validation once for all banks
- ✅ **Flexibility**: Bank-specific rules via knowledge base
- ✅ **Quality**: Consistent validation across all banks
- ✅ **Debuggability**: Clear data flow with logging

Each bank processor focuses on extraction, the central pipeline ensures quality, and the UI presents results clearly to users.
