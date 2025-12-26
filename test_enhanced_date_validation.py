#!/usr/bin/env python3
"""
Test script for enhanced date validation system.
Demonstrates the new tiered classification and smart inference capabilities.
"""

import pandas as pd
from datetime import datetime
import sys
import os

# Add the project directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from statements.date_validator import enhanced_date_validation

def create_test_data():
    """Create test DataFrame with the sample transaction data."""
    sample_data = [
        "'24Feb 2025'",    # Missing space between day and month
        "'Feb 2025'",      # Missing day parameter
        "'24 Feb 2025'",   # Correct format
        "'Feb 2025'",      # Missing day parameter
        "'24 Feb 2025'",   # Correct format
        "'Feb 2025'",      # Missing day parameter
        "'25 Feb 2025'",   # Correct format
        "'25 Feb 2025'",   # Correct format
        "'25 Feb 2025'",   # Correct format
        "'25 Feb 2025'",   # Correct format
        "'25 Feb 2025'",   # Correct format
        "'25 Feb 2025'",   # Correct format
        "'25 Feb 2025'",   # Correct format
        "'25 Feb 2025'",   # Correct format
        "'25 Feb 2025'",   # Correct format
        "'26 Feb 2025'",   # Correct format
        "'27 Feb 2025'",   # Correct format
        "'28 Feb 2025'",   # Correct format
        "'28 Feb 2025'",   # Correct format
        "'28 Feb 2025'",   # Correct format
        "'28 Feb 2025'",   # Correct format
    ]
    
    transaction_descriptions = [
        'Airtime', 'Transfer to POS Transfer', 'Transfer to ABUBAKAR SABITU',
        'Transfer to MOHAMMED ALHASSAN', 'Transfer to SAI GODIYA FURA DA NONO',
        'Airtime', 'Airtime', 'Transfer to POS Transfer', 'Transfer to AUWALU SHUAIBU',
        'Airtime', 'Transfer from AMEENA ILAH KAMILU', 'Transfer to SKY STAR',
        'Transfer from AMEENA ILAH KAMILU', 'Airtime', 'Transfer to Al Ayyub Bread',
        'Airtime', 'Transfer from EaseMoni', 'Airtime', 'Transfer from ILAH ISMAIL KAMILU',
        'Electronic Money Transfer Levy', 'Transfer from ILAH ISMAIL KAMILU',
        'Electronic Money Transfer Levy', 'Transfer to PEGASUS NIG LTD'
    ]
    
    amounts = [
        -100.00, -620.00, -2500.00, -1000.00, -1400.00, -300.00, -900.00,
        -250.00, -2800.00, -200.00, 2000.00, -4000.00, 1000.00, -217.00,
        -1000.00, -70.00, 8000.00, -500.00, 20000.00, -50.00, 100000.00,
        -50.00, -100000.00
    ]
    
    # Trim to match length of sample_data
    transaction_descriptions = transaction_descriptions[:len(sample_data)]
    amounts = amounts[:len(sample_data)]
    
    # Create DataFrame with sample dates
    df = pd.DataFrame({
        'raw_date': sample_data,
        'transaction_description': transaction_descriptions,
        'amount': amounts
    })
    
    return df

def test_enhanced_validation():
    """Test the enhanced date validation system."""
    print("🔬 Testing Enhanced Date Validation System")
    print("=" * 50)
    
    # Create test data
    df = create_test_data()
    
    print(f"📊 Created test DataFrame with {len(df)} transactions")
    print("\nRaw date samples:")
    print(df['raw_date'].head(10).tolist())
    
    # Apply enhanced validation
    print("\n🔧 Applying Enhanced Date Validation...")
    validated_df = enhanced_date_validation(df, verbose=True)
    
    # Display results
    print("\n📋 Validation Results:")
    print("=" * 50)
    
    # Show corrections and classifications
    results_df = validated_df[[
        'raw_date', 
        'date_validation_issue', 
        'date_correction_applied',
        'date_inference_confidence', 
        'date_action_required'
    ]].copy()
    
    # Show original vs corrected for interesting cases
    print("\n🔍 Detailed Analysis of Date Corrections:")
    for idx, row in validated_df.iterrows():
        if row['date_action_required'] != 'NONE':
            validation_info = row['_date_validation']
            original = validation_info.get('original_raw', 'N/A')
            current = row['raw_date']
            correction = row['date_correction_applied']
            confidence = row['date_inference_confidence']
            action = row['date_action_required']
            
            print(f"\nRow {idx}:")
            print(f"  Original: {original}")
            print(f"  Corrected: {current}")
            print(f"  Correction Applied: {correction}")
            print(f"  Confidence: {confidence}")
            print(f"  Action Required: {action}")
            print(f"  Issues: {row['date_validation_issue']}")
    
    # Summary statistics
    print("\n📈 Summary Statistics:")
    action_counts = validated_df['date_action_required'].value_counts()
    for action, count in action_counts.items():
        print(f"  {action}: {count}")
    
    correction_counts = validated_df['date_correction_applied'].value_counts()
    print(f"\nCorrections Applied:")
    for correction, count in correction_counts.items():
        if correction != 'None':
            print(f"  {correction}: {count}")
    
    return validated_df

def test_edge_cases():
    """Test edge cases and problematic dates."""
    print("\n🧪 Testing Edge Cases")
    print("=" * 30)
    
    edge_cases = pd.DataFrame({
        'raw_date': [
            '',                    # Empty
            'None',                # None string
            'Feb 2025',            # Missing day
            '24 2025',             # Missing month
            '2025 Feb 2310:00 48', # Compact time
            '32 Feb 2025',         # Invalid day
            'Feb 32 2025',         # Invalid day (different format)
            '13:45:67',            # Invalid time
            '2025-02-30',          # Invalid date (Feb 30)
        ],
        'transaction_description': [f'Test transaction {i}' for i in range(9)],
        'amount': [-100.00] * 9
    })
    
    print("Edge case data:")
    print(edge_cases['raw_date'].tolist())
    
    validated_edge = enhanced_date_validation(edge_cases, verbose=True)
    
    print("\nEdge case results:")
    for idx, row in validated_edge.iterrows():
        print(f"\n{row['raw_date']} -> {row['date_validation_issue']}")
        print(f"  Action: {row['date_action_required']}")
        print(f"  Correction: {row['date_correction_applied']}")

if __name__ == "__main__":
    try:
        # Run main test
        result_df = test_enhanced_validation()
        
        # Test edge cases
        test_edge_cases()
        
        print("\n✅ Enhanced Date Validation Test Complete!")
        print("\nKey Improvements Demonstrated:")
        print("  🔧 Auto-correction of missing spaces (e.g., '24Feb 2025')")
        print("  🧠 Smart inference for missing components (e.g., 'Feb 2025')")
        print("  📊 Tiered classification (AUTO_CORRECT, FLAG_REVIEW, FLAG_CRITICAL)")
        print("  🎯 Context-aware validation using neighboring transactions")
        print("  📈 Detailed reporting with confidence levels")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
