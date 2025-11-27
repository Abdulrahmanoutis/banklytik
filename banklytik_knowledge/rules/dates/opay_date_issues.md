# OPay Date Parsing Issues & Solutions

## Issue Analysis

Based on real OPay statement data, we've identified the following problematic date formats:

### 🔴 CRITICAL ISSUES

#### 1. Missing Space Between Day and Month
```
'24Feb 2025'  ❌ INVALID - Will fail parsing
'25Feb 2025'  ❌ INVALID - No space between day and month
```
**Solution**: Add regex to detect and fix by inserting space
```
'24Feb 2025' → '24 Feb 2025'
```

#### 2. Missing Day Component
```
'Feb 2025'    ❌ INVALID - No day specified
```
**Status**: ⚠️ CANNOT PARSE - Multiple transactions have this
**Impact**: System defaults to day 01 (INCORRECT)
**Solution**: 
- Mark these as problematic (highlight in red)
- Flag for user review
- Suggest using trans_time if available
- Or ask user to manually specify day

#### 3. Malformed Time Component in Trans Time
```
'2025 Feb 24 07:45 34'  ❌ MALFORMED - seconds field is wrong
```
**Issue**: Seconds show as '34' instead of '07:45:34'
**Solution**: Extract date portion, ignore malformed time

### 🟡 EDGE CASES

#### 4. Inconsistent Spacing
```
'23 Feb 2025'    ✅ CORRECT
'24Feb 2025'     ❌ MISSING SPACE
'24 Feb 2025'    ✅ CORRECT (after fixing)
```

## Parsing Rules for OPay

### Priority Order
1. **Use value_date** (primary) - "23 Feb 2025" or "Feb. 23, 2025"
2. **Ignore trans_time** (has formatting issues)
3. **Handle missing days** - Flag and ask user

### Regex Patterns

```python
# Pattern 1: Normal format with space
PATTERN_NORMAL = r'^(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})$'
# Matches: "23 Feb 2025", "24 February 2025"

# Pattern 2: Missing space (needs fixing)
PATTERN_NO_SPACE = r'^(\d{1,2})([A-Za-z]{3,9})\s+(\d{4})$'
# Matches: "24Feb 2025" → Fix to "24 Feb 2025"

# Pattern 3: Month and year only (PROBLEMATIC)
PATTERN_NO_DAY = r'^([A-Za-z]{3,9})\s+(\d{4})$'
# Matches: "Feb 2025" → MARK AS INVALID ⚠️

# Pattern 4: Full month name with day
PATTERN_FULL_MONTH = r'^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$'
# Matches: "23 February 2025"

# Pattern 5: Trans time format (ignore)
PATTERN_TRANSTIME = r'^(\d{4})\s+([A-Za-z]{3,9})\s+(\d{1,2})\s+(\d{2}):(\d{2})\s*(?:\d{1,2})?'
# Matches: "2025 Feb 24 07:45 34"
```

## Sample OPay Dates from Real Statements

### Valid Dates (Can Parse)
```
✅ '23 Feb 2025'     → 2025-02-23
✅ '25 Feb 2025'     → 2025-02-25
✅ '28 Feb 2025'     → 2025-02-28
```

### Problematic Dates (Need Fixing)
```
⚠️ '24Feb 2025'      → Fix space → '24 Feb 2025' → 2025-02-24
⚠️ '24 Feb 2025'     → OK but inconsistent with others
```

### INVALID Dates (Cannot Parse - Flag These)
```
❌ 'Feb 2025'        → NO DAY - Default to 01? WRONG!
                       Action: MARK WITH RED FLAG 🚩
                       Show user: "Cannot determine exact day for Feb 2025"
```

## Implementation Strategy

### Step 1: Detect Format
```python
def detect_date_format(date_string):
    if re.match(PATTERN_NO_DAY, date_string):
        return 'MISSING_DAY'  # 🚩 Flag as problematic
    elif re.match(PATTERN_NO_SPACE, date_string):
        return 'NEEDS_SPACE_FIX'
    elif re.match(PATTERN_NORMAL, date_string):
        return 'VALID'
    # etc.
```

### Step 2: Parse or Flag
```python
def parse_opay_date(date_string):
    format_type = detect_date_format(date_string)
    
    if format_type == 'MISSING_DAY':
        return {
            'status': 'PROBLEMATIC',
            'value': None,
            'reason': 'Missing day - cannot determine exact date',
            'raw': date_string,
            'suggestion': 'Use trans_time or manually specify day'
        }
    
    elif format_type == 'NEEDS_SPACE_FIX':
        fixed = fix_space(date_string)  # '24Feb 2025' → '24 Feb 2025'
        return parse_normal_date(fixed)
    
    # ... handle other formats
```

### Step 3: User Feedback
When saving transactions with problematic dates:

```json
{
  "transaction_id": 123,
  "date_status": "PROBLEMATIC",
  "value_date_original": "Feb 2025",
  "issue": "Cannot determine exact day",
  "displayed_as": "2025-02-01 ⚠️",
  "user_action_needed": true,
  "message": "Date is incomplete. Please verify the exact day this transaction occurred."
}
```

## Visual Feedback for Users

### In Transaction Table
```
Date Column:
"Feb. 01, 2025 🚩"     ← Red flag indicates problematic date
"Feb. 24, 2025 ✅"     ← Green checkmark for verified date

In Status Bar:
"3 problematic dates found in this statement"
"Click to review and correct these dates"
```

## Rules Summary

| Date Format | Status | Action |
|---|---|---|
| '23 Feb 2025' | ✅ Valid | Parse as 2025-02-23 |
| '24Feb 2025' | ⚠️ Fixable | Add space, then parse |
| 'Feb 2025' | ❌ Invalid | Flag 🚩 - ask user |
| '2025 Feb 24 07:45 34' | ❌ Malformed | Extract date part only |
| '26 Feb 2025' | ✅ Valid | Parse as 2025-02-26 |
| '27 Feb 2025' | ✅ Valid | Parse as 2025-02-27 |

## Next Steps

1. Update OPayProcessor to use these patterns
2. Add status indicators in UI for problematic dates
3. Create interface for users to review and correct problematic dates
4. Log all problematic dates for quality monitoring
5. Update opay_rules.md with these findings
