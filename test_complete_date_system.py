#!/usr/bin/env python3
"""
Comprehensive test of the enhanced date validation system.
Integrates validation, review workflow, and continuous learning.
"""

import pandas as pd
import sys
import os

# Add the project directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from statements.date_validator import enhanced_date_validation
from statements.date_review_workflow import DateReviewWorkflow, ReviewAction
from statements.date_learning_engine import DateLearningEngine


def create_comprehensive_test_data():
    """Create comprehensive test data with various date issues."""
    test_data = pd.DataFrame({
        'raw_date': [
            "'24Feb 2025'",      # Missing space - should auto-correct
            "'Feb 2025'",         # Missing day - needs review
            "'24 2025'",          # Missing month - needs review
            "",                   # Empty - critical
            "'32 Feb 2025'",      # Invalid day - critical
            "'2025 Feb 2310:00 48'", # Compact time - auto-correct
            "'13:45:67'",        # Invalid time - critical
            "'25 Feb 2025'",      # Valid - no action needed
            "'24Feb 2025'",      # Repeat pattern for learning
            "'Mar2025'",          # Missing space and day
        ],
        'transaction_description': [
            'Airtime Purchase',
            'Transfer to POS Transfer- SIUTO YAQUBA ADAMU',
            'Transfer to ABUBAKAR SABITU',
            'Transfer to MOHAMMED ALHASSAN',
            'Transfer to SAI GODIYA FURA DA NONO',
            'Airtime Recharge',
            'Electronic Money Transfer Levy',
            'Transfer from AMEENA ILAH KAMILU',
            'Airtime Top-up',
            'Transfer to SKY STAR'
        ],
        'amount': [
            -100.00, -620.00, -2500.00, -1000.00, -1400.00,
            -300.00, -50.00, 2000.00, -200.00, -4000.00
        ]
    })
    
    return test_data


def test_enhanced_validation_system():
    """Test the complete enhanced date validation system."""
    print("🚀 Testing Complete Enhanced Date Validation System")
    print("=" * 60)
    
    # Step 1: Create test data
    print("📊 Step 1: Creating Test Data")
    df = create_comprehensive_test_data()
    print(f"Created DataFrame with {len(df)} test transactions")
    
    # Step 2: Apply enhanced validation
    print("\n🔍 Step 2: Applying Enhanced Date Validation")
    validated_df = enhanced_date_validation(df, verbose=True)
    
    # Step 3: Analyze validation results
    print("\n📈 Step 3: Analyzing Validation Results")
    action_counts = validated_df['date_action_required'].value_counts()
    print("Action Required Breakdown:")
    for action, count in action_counts.items():
        print(f"  {action}: {count}")
    
    # Show detailed results for problematic dates
    problematic = validated_df[validated_df['date_action_required'] != 'NONE']
    print(f"\n🚨 Problematic Dates Found: {len(problematic)}")
    
    for idx, row in problematic.iterrows():
        validation_info = row['_date_validation']
        print(f"\n  Row {idx}:")
        print(f"    Original: {validation_info.get('original_raw', 'N/A')}")
        print(f"    Current: {row['raw_date']}")
        print(f"    Issues: {row['date_validation_issue']}")
        print(f"    Action: {row['date_action_required']}")
        print(f"    Correction: {row['date_correction_applied']}")
        print(f"    Confidence: {row['date_inference_confidence']}")
    
    return validated_df


def test_review_workflow(validated_df):
    """Test the review workflow component."""
    print("\n🔄 Step 4: Testing Review Workflow")
    print("=" * 40)
    
    # Create review workflow
    workflow = DateReviewWorkflow()
    
    # Extract candidates for review
    candidates = workflow.extract_review_candidates(validated_df)
    print(f"📋 Found {len(candidates)} candidates for manual review")
    
    # Create review session
    session = workflow.create_review_session(validated_df)
    print(f"🆔 Created review session: {session['session_id']}")
    
    # Simulate manual review decisions
    print("\n👤 Simulating Manual Review Decisions:")
    
    decisions = [
        (0, ReviewAction.APPROVE, "Auto-correction looks correct"),
        (1, ReviewAction.MODIFY, "1 Feb 2025", "Inferred first day of month"),
        (2, ReviewAction.REJECT, "Cannot reliably infer month"),
        (3, ReviewAction.REJECT, "Empty date - requires manual entry"),
        (4, ReviewAction.MODIFY, "28 Feb 2025", "Adjusted to last day of February"),
    ]
    
    for i, decision_data in enumerate(decisions):
        if len(decision_data) == 3:
            row_idx, action, notes = decision_data
            corrected_date = None
        else:
            row_idx, action, corrected_date, notes = decision_data
        
        # Get the actual row index from session
        if i < len(session['candidates']):
            actual_idx = session['candidates'][i]['row_index']
            workflow.apply_review_decision(
                session, actual_idx, action, corrected_date, notes
            )
            print(f"  ✓ Applied {action.value} to candidate {i}: {notes}")
    
    # Generate review summary
    summary = workflow.generate_review_summary(session)
    print(f"\n📊 Review Session Summary:")
    print(f"  Status: {summary['status']}")
    print(f"  Completion: {summary['completion_rate']:.1f}%")
    print(f"  Action Breakdown: {summary['action_breakdown']}")
    
    # Apply approved corrections
    corrected_df = workflow.apply_approved_corrections(validated_df, session)
    print(f"\n✅ Applied corrections to DataFrame")
    
    return workflow, session, corrected_df


def test_learning_engine(workflow, session):
    """Test the continuous learning engine."""
    print("\n🧠 Step 5: Testing Continuous Learning Engine")
    print("=" * 50)
    
    # Create learning engine
    learning_engine = DateLearningEngine()
    
    # Import review session data
    learning_engine.import_review_session_data(session)
    print("📥 Imported review session data into learning engine")
    
    # Test suggestion system
    print("\n🔮 Testing Learning-Based Suggestions:")
    test_dates = [
        "'24Feb 2025'",      # Should suggest based on learning
        "'Mar2025'",          # Might suggest corrections
        "'Apr 2025'",         # Should suggest day inference
    ]
    
    for test_date in test_dates:
        suggestions = learning_engine.suggest_corrections(test_date, ['MISSING_SPACE'])
        print(f"\n  Date: {test_date}")
        if suggestions:
            for suggestion in suggestions:
                print(f"    💡 Suggestion: {suggestion['corrected_date']}")
                print(f"       Confidence: {suggestion['confidence']:.2f}")
                print(f"       Source: {suggestion['source']}")
        else:
            print("    ❌ No suggestions available")
    
    # Export learning summary
    summary = learning_engine.export_learning_summary()
    print(f"\n📊 Learning Engine Summary:")
    print(f"  Total Corrections: {summary['total_corrections']}")
    print(f"  Patterns Learned: {summary['total_patterns_learned']}")
    print(f"  Successful Patterns: {summary['successful_patterns']}")
    print(f"  Rules Generated: {summary['total_rules_generated']}")
    
    # Show top patterns
    if summary['top_patterns']:
        print(f"\n🏆 Top Performing Patterns:")
        for i, pattern in enumerate(summary['top_patterns'][:3]):
            print(f"  {i+1}. Success Rate: {pattern['success_rate']:.2f}")
            print(f"     Attempts: {pattern['attempts']}")
            print(f"     Successful: {pattern['successful']}")
    
    return learning_engine


def demonstrate_complete_workflow():
    """Demonstrate the complete workflow from validation to learning."""
    print("🎯 Complete Enhanced Date Validation Workflow Demo")
    print("=" * 60)
    
    # Step 1: Enhanced validation
    validated_df = test_enhanced_validation_system()
    
    # Step 2: Review workflow
    workflow, session, corrected_df = test_review_workflow(validated_df)
    
    # Step 3: Continuous learning
    learning_engine = test_learning_engine(workflow, session)
    
    # Step 4: Show final results
    print("\n🏁 Final Results Summary")
    print("=" * 30)
    
    original_issues = validated_df[validated_df['date_action_required'] != 'NONE'].shape[0]
    final_issues = corrected_df[corrected_df['date_action_required'].isin(['MANUAL_REVIEW', 'IMMEDIATE_REVIEW'])].shape[0]
    resolved_issues = original_issues - final_issues
    
    print(f"📊 Processing Summary:")
    print(f"  Original Issues: {original_issues}")
    print(f"  Resolved Issues: {resolved_issues}")
    print(f"  Remaining Issues: {final_issues}")
    print(f"  Resolution Rate: {resolved_issues/original_issues*100:.1f}%")
    
    print(f"\n🎯 System Capabilities Demonstrated:")
    print(f"  ✅ Automatic pattern recognition and correction")
    print(f"  🧠 Smart inference for missing date components")
    print(f"  📊 Tiered classification of validation issues")
    print(f"  👥 Structured manual review workflow")
    print(f"  🎓 Continuous learning from user corrections")
    print(f"  📈 Performance tracking and improvement")
    
    return validated_df, corrected_df, learning_engine


if __name__ == "__main__":
    try:
        # Run complete workflow demonstration
        validated_df, corrected_df, learning_engine = demonstrate_complete_workflow()
        
        print("\n🎉 Enhanced Date Validation System Test Complete!")
        print("\nKey Improvements Over Original System:")
        print("  🔧 Auto-correction of common OCR errors (missing spaces)")
        print("  🧠 Intelligent inference for missing date components")
        print("  📊 Tiered risk assessment (AUTO_CORRECT, FLAG_REVIEW, FLAG_CRITICAL)")
        print("  🔄 Structured review workflow with audit trail")
        print("  📈 Continuous learning that improves over time")
        print("  🎯 Confidence-based decision making")
        print("  📝 Detailed reporting and analytics")
        
    except Exception as e:
        print(f"❌ Error during comprehensive testing: {e}")
        import traceback
        traceback.print_exc()
