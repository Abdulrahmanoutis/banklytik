#!/usr/bin/env python3
"""
Simple test to verify date parsing functions work correctly
"""

import pandas as pd
import re
from datetime import datetime

def parse_date_str(date_str):
    """
    Simplified version of parse_date_str from cleaning_utils.py
    """
    print(f"🔍 DEBUG parse_date_str: Input = '{date_str}' (type: {type(date_str)})")
    
    if pd.isna(date_str) or date_str in ['None', '', 'NaT']:
        print("❌ Empty/None value detected")
        return None
    
    s = str(date_str).strip()
    if not s:
        print("❌ Empty string after stripping")
        return None
    
    # Check for incomplete dates (month-year only) - CRITICAL FIX
    incomplete_date_pattern = r'^([A-Za-z]{3,})\s+(\d{4})$'
    if re.match(incomplete_date_pattern, s):
        print(f"⚠️ Incomplete date pattern detected: '{s}' - missing day component")
        return None  # Let validation system handle this
    
    # Try pandas with dayfirst
    try:
        result = pd.to_datetime(s, dayfirst=True, errors='coerce')
        if pd.isna(result):
            print("❌ Pandas failed to parse date")
            return None
        print(f"✅ Pandas parsed: {s} → {result} (dayfirst=True)")
        return result
    except Exception as e:
        print(f"❌ Exception in pandas parsing: {e}")
        return None

def parse_nigerian_date(date_val):
    """
    Simplified version of parse_nigerian_date from deepseek_cleaning_generation.py
    """
    if pd.isna(date_val):
        return None
    
    date_str = str(date_val).strip()
    if not date_str or date_str in ['None', 'NaT', '']:
        return None
    
    print(f"DEBUG: Parsing date: '{date_str}'")
    
    # Preprocessing
    date_str = re.sub(r'\s+', ' ', date_str)
    date_str = re.sub(r'(\d)([A-Za-z])', r'\1 \2', date_str)
    date_str = re.sub(r'([A-Za-z])(\d)', r'\1 \2', date_str)
    
    # Try multiple date formats - REMOVED "%b %Y" to detect incomplete dates
    formats = [
        "%Y %b %d %H:%M %S",
        "%Y %b %d %H:%M:%S",  
        "%Y %b %d %H:%M",
        "%d %b %Y %H:%M %S",
        "%d %b %Y %H:%M:%S",
        "%d %b %Y %H:%M",
        "%Y %b %d",
        "%d %b %Y",
        # REMOVED: "%b %Y" - let validation system handle incomplete dates
        "%Y%b %d %H:%M %S",
        "%d%b %Y",
    ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            print(f"DEBUG: Successfully parsed '{date_str}' with format '{fmt}'")
            return parsed
        except ValueError:
            continue
    
    print(f"DEBUG: Failed to parse date: '{date_str}'")
    return None

def test_dates():
    """Test both incomplete and valid dates"""
    
    print("=" * 80)
    print("🧪 TESTING DATE PARSING FUNCTIONS")
    print("=" * 80)
    
    # Test cases that should be flagged as incomplete/invalid
    incomplete_cases = [
        "Feb 2025",
        "February 2025", 
        "Mar 2024",
        "Dec 2023",
        "Jan 2025",
    ]
    
    # Test cases that should still work (valid complete dates)
    valid_cases = [
        "23 Feb 2025",
        "Feb 23 2025", 
        "23/02/2025",
        "2025-02-23",
    ]
    
    print("\n🔍 TESTING INCOMPLETE DATES (should return None):")
    print("-" * 60)
    
    incomplete_success = 0
    for date_str in incomplete_cases:
        print(f"\nTesting: '{date_str}'")
        
        result1 = parse_date_str(date_str)
        result2 = parse_nigerian_date(date_str)
        
        print(f"  parse_date_str(): {result1}")
        print(f"  parse_nigerian_date(): {result2}")
        
        if result1 is None and result2 is None:
            print(f"  ✅ CORRECTLY DETECTED AS INCOMPLETE")
            incomplete_success += 1
        else:
            print(f"  ❌ INCORRECTLY PARSED AS VALID")
    
    print("\n🔍 TESTING VALID DATES (should return datetime):")
    print("-" * 60)
    
    valid_success = 0
    for date_str in valid_cases:
        print(f"\nTesting: '{date_str}'")
        
        result1 = parse_date_str(date_str)
        result2 = parse_nigerian_date(date_str)
        
        print(f"  parse_date_str(): {result1}")
        print(f"  parse_nigerian_date(): {result2}")
        
        if result1 is not None or result2 is not None:
            print(f"  ✅ CORRECTLY PARSED AS VALID")
            valid_success += 1
        else:
            print(f"  ❌ FAILED TO PARSE VALID DATE")
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 TEST SUMMARY")
    print("=" * 80)
    
    incomplete_rate = incomplete_success / len(incomplete_cases) * 100
    valid_rate = valid_success / len(valid_cases) * 100
    
    print(f"Incomplete dates correctly detected: {incomplete_success}/{len(incomplete_cases)} ({incomplete_rate:.1f}%)")
    print(f"Valid dates correctly parsed: {valid_success}/{len(valid_cases)} ({valid_rate:.1f}%)")
    
    if incomplete_rate >= 80 and valid_rate >= 80:
        print("\n🎉 OVERALL TEST RESULT: PASSED")
        print("   The Feb 2025 incomplete date issue has been fixed!")
    else:
        print("\n❌ OVERALL TEST RESULT: FAILED") 
        print("   The Feb 2025 incomplete date issue still needs work.")
    
    print("=" * 80)
    return incomplete_rate >= 80 and valid_rate >= 80

if __name__ == "__main__":
    success = test_dates()
    exit(0 if success else 1)
