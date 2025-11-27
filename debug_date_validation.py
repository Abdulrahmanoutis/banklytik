#!/usr/bin/env python
import os
import django
import sys
from datetime import datetime

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'banklytik.settings')
django.setup()

from statements.cleaning_utils import parse_date_str
import re

# Test cases - simulate incomplete dates
test_cases = [
    "2025-02-01",          # Just date, no time
    "Feb 01, 2025",        # Text format, no time
    "2025 Feb 01",         # Year month day, no time
    "01/02/2025",          # DD/MM/YYYY, no time
    "2025-02-01 00:00:00", # Full datetime but midnight
    "2025-02-01 14:30:00", # Full datetime with time
]

print("=" * 80)
print("TESTING DATE PARSING AND VALIDATION")
print("=" * 80)

for test_date in test_cases:
    print(f"\nInput: '{test_date}'")
    parsed = parse_date_str(test_date)
    
    if parsed is None:
        print(f"  → Parsed as: None (FAILED)")
    else:
        print(f"  → Parsed as: {parsed}")
        print(f"     Hour: {parsed.hour}, Minute: {parsed.minute}, Second: {parsed.second}")
        
        # Check validation logic
        has_time_indicators = bool(re.search(r'\d{1,2}:\d{2}', test_date))
        is_midnight = parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0
        
        print(f"     Has time indicators in input: {has_time_indicators}")
        print(f"     Is midnight (00:00:00): {is_midnight}")
        
        if is_midnight and not has_time_indicators:
            print(f"  ⚠️ INCOMPLETE_DATE DETECTED!")
        elif is_midnight and has_time_indicators:
            print(f"  ✅ OK (midnight but has time indicators)")
        else:
            print(f"  ✅ OK (has time component)")

print("\n" + "=" * 80)
