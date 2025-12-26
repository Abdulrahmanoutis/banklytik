#!/usr/bin/env python3
"""
Complete end-to-end test for Feb 2025 incomplete date fix
Tests the entire data pipeline from raw input to UI validation
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the functions we need to test
from statements.cleaning_utils import parse_date_str, robust_clean_dataframe
from statements.date_validator import parse_date_flexible, validate_and_flag_dates
from statements.deepseek_cleaning_generation import parse_nigerian_date

def test_parsing_functions():
    """Test individual parsing functions"""
    print("=" * 80)
    print("🧪 TEST 1: Individual Parsing Functions")
    print("=" * 80)
    
    test_cases = {
        'incomplete': ['Feb 2025', 'February 2025', 'Mar 2024', 'Jan 2025'],
        'valid': ['23 Feb 2025', 'Feb 23 2025', '23/02/2025', '2025-02-23']
    }
    
    incomplete_pass = 0
    valid_pass = 0
    
    print("\n📍 Testing Incomplete Dates (should return None):")
    for date_str in test_cases['incomplete']:
        result1 = parse_date_str(date_str)
        result2 = parse_date_flexible(date_str)
        result3 = parse_nigerian_date(date_str)
        
        if result1 is None and result2 is None and result3 is None:
            print(f"  ✅ '{date_str}' → All parsers correctly returned None")
            incomplete_pass += 1
        else:
            print(f"  ❌ '{date_str}' → FAIL: parse_date_str={result1}, parse_date_flexible={result2}, parse_nigerian_date={result3}")
    
    print("\n📍 Testing Valid Dates (should return datetime):")
    for date_str in test_cases['valid']:
        result1 = parse_date_str(date_str)
        result2 = parse_date_flexible(date_str)
        result3 = parse_nigerian_date(date_str)
        
        if result1 is not None or result2 is not None or result3 is not None:
            print(f"  ✅ '{date_str}' → At least one parser succeeded")
            valid_pass += 1
        else:
            print(f"  ❌ '{date_str}' → FAIL: All parsers returned None")
    
    print(f"\n📊 Results: Incomplete={incomplete_pass}/{len(test_cases['incomplete'])}, Valid={valid_pass}/{len(test_cases['valid'])}")
    return incomplete_pass == len(test_cases['incomplete']) and valid_pass == len(test_cases['valid'])


def test_data_pipeline():
    """Test complete data pipeline with DataFrame"""
    print("\n" + "=" * 80)
    print("🧪 TEST 2: Complete Data Pipeline")
    print("=" * 80)
    
    # Create test DataFrame simulating raw bank statement data
    test_data = pd.DataFrame({
        'date': ['Feb 2025', '23 Feb 2025', 'Mar 2024', '24 Mar 2024', '15 Feb 2025'],
        'description': ['Transfer', 'ATM Withdrawal', 'POS', 'Online Transfer', 'Airtime'],
        'debit': [0, 5000, 2000, 0, 500],
        'credit': [10000, 0, 0, 15000, 0],
        'balance': [10000, 5000, 3000, 18000, 17500],
    })
    
    print("\n📍 Input Data:")
    print(test_data[['date', 'description', 'debit', 'credit']].to_string())
    
    # Process through robust_clean_dataframe
    print("\n📍 Processing through robust_clean_dataframe...")
    cleaned_df = robust_clean_dataframe(test_data)
    
    print("\n📍 After Cleaning:")
    if 'date_validation_warning' in cleaned_df.columns:
        display_cols = ['raw_date', 'date', 'description', 'date_validation_warning', 'date_validation_issue']
        available_cols = [col for col in display_cols if col in cleaned_df.columns]
        print(cleaned_df[available_cols].to_string())
    else:
        print("⚠️  date_validation_warning column not found")
        print(cleaned_df[['raw_date', 'date', 'description']].to_string())
    
    # Check validation results
    print("\n📍 Validation Analysis:")
    incomplete_flagged = 0
    valid_passed = 0
    
    for idx, row in cleaned_df.iterrows():
        raw = str(row.get('raw_date', ''))
        parsed = row.get('date')
        warning = row.get('date_validation_warning', 'UNKNOWN')
        issue = row.get('date_validation_issue', 'UNKNOWN')
        
        # Check if incomplete dates are flagged
        if raw in ['Feb 2025', 'Mar 2024']:
            if warning in ['WARNING', 'ERROR'] or parsed is None:
                print(f"  ✅ '{raw}' correctly flagged: warning={warning}, parsed={parsed}")
                incomplete_flagged += 1
            else:
                print(f"  ❌ '{raw}' NOT flagged: warning={warning}, parsed={parsed}, issue={issue}")
        
        # Check if valid dates pass
        elif raw in ['23 Feb 2025', '24 Mar 2024', '15 Feb 2025']:
            if parsed is not None:
                print(f"  ✅ '{raw}' correctly parsed: {parsed}")
                valid_passed += 1
            else:
                print(f"  ❌ '{raw}' failed to parse: warning={warning}, issue={issue}")
    
    print(f"\n📊 Results: Incomplete Flagged={incomplete_flagged}/2, Valid Passed={valid_passed}/3")
    return incomplete_flagged == 2 and valid_passed == 3


def test_validation_function():
    """Test the validation function directly"""
    print("\n" + "=" * 80)
    print("🧪 TEST 3: Direct Validation Function Test")
    print("=" * 80)
    
    # Create test DataFrame
    test_data = pd.DataFrame({
        'raw_date': ['Feb 2025', '23 Feb 2025', 'Mar 2024', '24 Mar 2024'],
        'date': [None, datetime(2025, 2, 23), None, datetime(2024, 3, 24)],
        'description': ['Transfer', 'ATM', 'POS', 'Online']
    })
    
    print("\n📍 Input Data:")
    print(test_data.to_string())
    
    # Apply validation
    print("\n📍 Applying validate_and_flag_dates...")
    validated_df = validate_and_flag_dates(test_data, verbose=True)
    
    print("\n📍 After Validation:")
    display_cols = ['raw_date', 'date_validation_warning', 'date_validation_issue']
    available_cols = [col for col in display_cols if col in validated_df.columns]
    print(validated_df[available_cols].to_string())
    
    # Check results
    incomplete_flagged = 0
    valid_passed = 0
    
    for idx, row in validated_df.iterrows():
        raw = row['raw_date']
        warning = row.get('date_validation_warning', 'UNKNOWN')
        
        if raw in ['Feb 2025', 'Mar 2024']:
            if warning in ['WARNING', 'ERROR']:
                incomplete_flagged += 1
        elif raw in ['23 Feb 2025', '24 Mar 2024']:
            if warning == 'INFO':
                valid_passed += 1
    
    print(f"\n📊 Results: Incomplete Flagged={incomplete_flagged}/2, Valid Passed={valid_passed}/2")
    return incomplete_flagged == 2 and valid_passed == 2


def main():
    """Run all tests"""
    print("🚀 Starting Complete End-to-End Test for Feb 2025 Fix")
    print("=" * 80)
    
    results = {
        'parsing': test_parsing_functions(),
        'pipeline': test_data_pipeline(),
        'validation': test_validation_function()
    }
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 FINAL TEST SUMMARY")
    print("=" * 80)
    
    all_passed = all(results.values())
    
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {test_name.title()} Tests: {status}")
    
    print("\n" + "=" * 80)
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
        print("✅ The Feb 2025 incomplete date issue has been completely fixed!")
        print("✅ Incomplete dates are now properly flagged across the entire pipeline")
    else:
        print("❌ SOME TESTS FAILED")
        print("⚠️  The Feb 2025 fix needs additional work")
    print("=" * 80)
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())
