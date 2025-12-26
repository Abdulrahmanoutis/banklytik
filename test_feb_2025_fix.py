#!/usr/bin/env python3
"""
Test script to verify that "Feb 2025" incomplete dates are now properly detected
and flagged as WARNING/ERROR instead of being incorrectly parsed as valid dates.
"""

import os
import sys
import django
import pandas as pd

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'banklytik.settings')
django.setup()

from statements.cleaning_utils import parse_date_str
from statements.deepseek_cleaning_generation import parse_nigerian_date
from statements.date_validator import validate_and_flag_dates

def test_feb_2025_parsing():
    """Test that Feb 2025 is properly detected as incomplete date"""
    
    print("=" * 80)
    print("🧪 TESTING FEB 2025 INCOMPLETE DATE DETECTION")
    print("=" * 80)
    
    # Test cases that should be flagged as incomplete/invalid
    test_cases = [
        "Feb 2025",
        "February 2025", 
        "Mar 2024",
        "Dec 2023",
        "Jan 2025",
        "  Feb 2025  ",  # with extra spaces
        "FEB 2025",      # uppercase
        "feb 2025",      # lowercase
    ]
    
    # Test cases that should still work (valid complete dates)
    valid_cases = [
        "23 Feb 2025",
        "Feb 23 2025", 
        "23/02/2025",
        "2025-02-23",
        "23 Feb 2025 10:30:00",
    ]
    
    print("\n🔍 TESTING INCOMPLETE DATES (should return None):")
    print("-" * 60)
    
    incomplete_results = []
    for date_str in test_cases:
        print(f"\nTesting: '{date_str}'")
        
        # Test parse_date_str from cleaning_utils
        result1 = parse_date_str(date_str)
        print(f"  parse_date_str(): {result1}")
        
        # Test parse_nigerian_date from deepseek_cleaning_generation  
        result2 = parse_nigerian_date(date_str)
        print(f"  parse_nigerian_date(): {result2}")
        
        # Both should return None for incomplete dates
        if result1 is None and result2 is None:
            print(f"  ✅ CORRECTLY DETECTED AS INCOMPLETE")
            incomplete_results.append(True)
        else:
            print(f"  ❌ INCORRECTLY PARSED AS VALID")
            incomplete_results.append(False)
    
    print("\n🔍 TESTING VALID DATES (should return datetime):")
    print("-" * 60)
    
    valid_results = []
    for date_str in valid_cases:
        print(f"\nTesting: '{date_str}'")
        
        # Test parse_date_str from cleaning_utils
        result1 = parse_date_str(date_str)
        print(f"  parse_date_str(): {result1}")
        
        # Test parse_nigerian_date from deepseek_cleaning_generation
        result2 = parse_nigerian_date(date_str)
        print(f"  parse_nigerian_date(): {result2}")
        
        # At least one should work for valid dates
        if result1 is not None or result2 is not None:
            print(f"  ✅ CORRECTLY PARSED AS VALID")
            valid_results.append(True)
        else:
            print(f"  ❌ FAILED TO PARSE VALID DATE")
            valid_results.append(False)
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 TEST SUMMARY")
    print("=" * 80)
    
    incomplete_success_rate = sum(incomplete_results) / len(incomplete_results) * 100
    valid_success_rate = sum(valid_results) / len(valid_results) * 100
    
    print(f"Incomplete dates correctly detected: {sum(incomplete_results)}/{len(incomplete_results)} ({incomplete_success_rate:.1f}%)")
    print(f"Valid dates correctly parsed: {sum(valid_results)}/{len(valid_results)} ({valid_success_rate:.1f}%)")
    
    # Test with DataFrame validation (end-to-end)
    print("\n🔬 TESTING END-TO-END VALIDATION SYSTEM:")
    print("-" * 60)
    
    # Create a test DataFrame with mixed dates
    test_df = pd.DataFrame([
        {"raw_date": "Feb 2025", "description": "Test incomplete", "debit": 100.0, "credit": 0.0, "balance": 1000.0},
        {"raw_date": "23 Feb 2025", "description": "Test valid", "debit": 0.0, "credit": 200.0, "balance": 1200.0},
        {"raw_date": "Mar 2024", "description": "Another incomplete", "debit": 50.0, "credit": 0.0, "balance": 1150.0},
        {"raw_date": "24 Feb 2025", "description": "Another valid", "debit": 0.0, "credit": 150.0, "balance": 1300.0},
    ])
    
    # Apply date validation
    validated_df = validate_and_flag_dates(test_df, verbose=True)
    
    print("\nValidation results:")
    for idx, row in validated_df.iterrows():
        raw_date = row['raw_date']
        parsed_date = row['date'] 
        validation_warning = row.get('date_validation_warning', 'N/A')
        
        print(f"  Row {idx + 1}: '{raw_date}' -> {parsed_date} [{validation_warning}]")
    
    # Check if incomplete dates are properly flagged
    incomplete_flags = validated_df[
        validated_df['raw_date'].isin(['Feb 2025', 'Mar 2024'])
    ]['date_validation_warning'].tolist()
    
    expected_flags = ['ERROR', 'WARNING']  # Incomplete dates should be flagged
    
    print(f"\nIncomplete date flags: {incomplete_flags}")
    print(f"Expected flags contain: {expected_flags}")
    
    if any(flag in expected_flags for flag in incomplete_flags):
        print("✅ Incomplete dates are properly flagged by validation system")
    else:
        print("❌ Incomplete dates are NOT properly flagged by validation system")
    
    # Overall test result
    print("\n" + "=" * 80)
    if incomplete_success_rate >= 80 and valid_success_rate >= 80:
        print("🎉 OVERALL TEST RESULT: PASSED")
        print("   The Feb 2025 incomplete date issue has been fixed!")
    else:
        print("❌ OVERALL TEST RESULT: FAILED") 
        print("   The Feb 2025 incomplete date issue still needs work.")
    print("=" * 80)
    
    return incomplete_success_rate >= 80 and valid_success_rate >= 80

if __name__ == "__main__":
    success = test_feb_2025_parsing()
    sys.exit(0 if success else 1)
