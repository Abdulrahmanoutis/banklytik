"""
Enhanced date processing integration for the main statement processing pipeline.
Integrates validation, review workflow, and continuous learning seamlessly.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

from date_validator import enhanced_date_validation
from date_review_workflow import DateReviewWorkflow, ReviewAction
from date_learning_engine import DateLearningEngine

logger = logging.getLogger(__name__)


class EnhancedDateProcessor:
    """
    Main processor that integrates all enhanced date validation components.
    Provides a simple interface for the main processing pipeline.
    """

    def __init__(self, enable_learning: bool = True, 
                 auto_approve_threshold: float = 0.8):
        """
        Initialize the enhanced date processor.
        
        Args:
            enable_learning: Whether to enable continuous learning
            auto_approve_threshold: Confidence threshold for auto-approval
        """
        self.enable_learning = enable_learning
        self.auto_approve_threshold = auto_approve_threshold
        self.learning_engine = DateLearningEngine() if enable_learning else None
        self.review_workflow = DateReviewWorkflow()
        
        self.processing_stats = {
            'total_processed': 0,
            'auto_corrected': 0,
            'flagged_review': 0,
            'flagged_critical': 0,
            'manually_reviewed': 0,
            'learning_updates': 0
        }

    def process_statement_dates(self, df: pd.DataFrame, 
                           date_column: str = 'raw_date',
                           context_column: str = 'date',
                           auto_process: bool = True) -> Tuple[pd.DataFrame, Dict]:
        """
        Process dates in a bank statement DataFrame.
        
        Args:
            df: DataFrame containing transaction data
            date_column: Column name for raw date strings
            context_column: Column name for parsed dates (context)
            auto_process: Whether to auto-process or create review session
            
        Returns:
            Tuple of (processed_dataframe, processing_metadata)
        """
        logger.info(f"🔧 Processing {len(df)} transaction dates")
        
        # Step 1: Apply enhanced validation
        validated_df = enhanced_date_validation(
            df, 
            date_column=date_column,
            context_column=context_column,
            verbose=False
        )
        
        # Update statistics
        self.processing_stats['total_processed'] += len(df)
        
        action_counts = validated_df['date_action_required'].value_counts()
        self.processing_stats['auto_corrected'] += action_counts.get('AUTO_CORRECT', 0)
        self.processing_stats['flagged_review'] += action_counts.get('MANUAL_REVIEW', 0)
        self.processing_stats['flagged_critical'] += action_counts.get('IMMEDIATE_REVIEW', 0)
        
        # Step 2: Handle based on processing mode
        if auto_process:
            processed_df, review_session = self._auto_process(validated_df)
        else:
            processed_df, review_session = self._create_review_session(validated_df)
        
        # Step 3: Update learning engine if enabled
        if self.learning_engine and review_session:
            self.learning_engine.import_review_session_data(review_session)
            self.processing_stats['learning_updates'] += 1
        
        # Create processing metadata
        metadata = {
            'processing_timestamp': datetime.now().isoformat(),
            'total_transactions': len(df),
            'validation_summary': self._get_validation_summary(validated_df),
            'processing_stats': self.processing_stats.copy(),
            'review_session_id': review_session.get('session_id') if review_session else None,
            'auto_processed': auto_process,
            'learning_enabled': self.enable_learning
        }
        
        logger.info(f"✅ Date processing complete. Auto-corrected: {action_counts.get('AUTO_CORRECT', 0)}, Flagged: {action_counts.get('MANUAL_REVIEW', 0) + action_counts.get('IMMEDIATE_REVIEW', 0)}")
        
        return processed_df, metadata

    def _auto_process(self, validated_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Automatically process validations using confidence thresholds.
        """
        processed_df = validated_df.copy()
        review_session = None
        
        # Auto-approve high-confidence corrections
        high_confidence_mask = (
            (processed_df['date_action_required'] == 'AUTO_CORRECT') &
            (processed_df['date_inference_confidence'].isin(['HIGH', 'MEDIUM']))
        )
        
        auto_approved_count = high_confidence_mask.sum()
        processed_df.loc[high_confidence_mask, 'date_action_required'] = 'AUTO_APPROVED'
        processed_df.loc[high_confidence_mask, 'date_validation_issue'] = 'AUTO_APPROVED'
        
        # Create review session for remaining issues
        remaining_issues = processed_df[
            processed_df['date_action_required'].isin(['MANUAL_REVIEW', 'IMMEDIATE_REVIEW'])
        ]
        
        if len(remaining_issues) > 0:
            review_session = self.review_workflow.create_review_session(processed_df)
            self.processing_stats['manually_reviewed'] += len(review_session['candidates'])
        
        return processed_df, review_session

    def _create_review_session(self, validated_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Create a review session for all problematic dates.
        """
        review_session = self.review_workflow.create_review_session(validated_df)
        
        # Mark all problematic dates as pending review
        problematic_mask = validated_df['date_action_required'] != 'NONE'
        validated_df = validated_df.copy()
        validated_df.loc[problematic_mask, 'date_action_required'] = 'PENDING_REVIEW'
        
        return validated_df, review_session

    def apply_review_decisions(self, df: pd.DataFrame, 
                           review_session: Dict,
                           decisions: List[Dict]) -> pd.DataFrame:
        """
        Apply manual review decisions to the DataFrame.
        
        Args:
            df: Original DataFrame
            review_session: Review session data
            decisions: List of decisions with format:
                      [{'row_index': int, 'action': str, 'corrected_date': str, 'notes': str}]
        
        Returns:
            DataFrame with decisions applied
        """
        # Apply decisions to review session
        for decision in decisions:
            row_idx = decision['row_index']
            action = ReviewAction(decision['action'])
            corrected_date = decision.get('corrected_date')
            notes = decision.get('notes', '')
            
            self.review_workflow.apply_review_decision(
                review_session, row_idx, action, corrected_date, notes
            )
        
        # Apply approved corrections to DataFrame
        corrected_df = self.review_workflow.apply_approved_corrections(df, review_session)
        
        # Update learning engine
        if self.learning_engine:
            self.learning_engine.import_review_session_data(review_session)
        
        return corrected_df

    def get_processing_summary(self) -> Dict:
        """Get a summary of all processing statistics."""
        return {
            'total_processed': self.processing_stats['total_processed'],
            'auto_corrected': self.processing_stats['auto_corrected'],
            'flagged_review': self.processing_stats['flagged_review'],
            'flagged_critical': self.processing_stats['flagged_critical'],
            'manually_reviewed': self.processing_stats['manually_reviewed'],
            'learning_updates': self.processing_stats['learning_updates'],
            'auto_correction_rate': (self.processing_stats['auto_corrected'] / 
                                  max(self.processing_stats['total_processed'], 1)) * 100,
            'review_rate': ((self.processing_stats['flagged_review'] + 
                           self.processing_stats['flagged_critical']) / 
                          max(self.processing_stats['total_processed'], 1)) * 100
        }

    def _get_validation_summary(self, df: pd.DataFrame) -> Dict:
        """Extract validation summary from DataFrame."""
        action_counts = df['date_action_required'].value_counts()
        
        return {
            'valid': action_counts.get('NONE', 0),
            'auto_corrected': action_counts.get('AUTO_CORRECT', 0),
            'flagged_review': action_counts.get('MANUAL_REVIEW', 0),
            'flagged_critical': action_counts.get('IMMEDIATE_REVIEW', 0),
            'total_issues': (action_counts.get('MANUAL_REVIEW', 0) + 
                           action_counts.get('IMMEDIATE_REVIEW', 0))
        }

    def suggest_corrections(self, date_str: str, issues: List[str]) -> List[Dict]:
        """
        Get corrections suggestions based on learned patterns.
        
        Args:
            date_str: Date string to correct
            issues: List of detected issues
            
        Returns:
            List of correction suggestions
        """
        if not self.learning_engine:
            return []
        
        return self.learning_engine.suggest_corrections(date_str, issues)

    def export_learning_data(self, filepath: str):
        """Export learning data for backup or analysis."""
        if self.learning_engine:
            self.learning_engine.save_learning_data()
            logger.info(f"📚 Learning data exported to {filepath}")


def process_statement_enhanced(df: pd.DataFrame, 
                              date_column: str = 'raw_date',
                              auto_process: bool = True) -> Tuple[pd.DataFrame, Dict]:
    """
    Convenience function for enhanced date processing.
    
    Args:
        df: DataFrame containing transaction data
        date_column: Column name for raw date strings  
        auto_process: Whether to auto-process or create review session
        
    Returns:
        Tuple of (processed_dataframe, processing_metadata)
    """
    processor = EnhancedDateProcessor()
    return processor.process_statement_dates(
        df, 
        date_column=date_column,
        auto_process=auto_process
    )


# Example usage for integration
if __name__ == "__main__":
    # Create sample data
    sample_df = pd.DataFrame({
        'raw_date': ["'24Feb 2025'", "'Feb 2025'", "'25 Feb 2025'"],
        'description': ['Test 1', 'Test 2', 'Test 3'],
        'amount': [-100, -200, -300]
    })
    
    # Process with enhanced validation
    processed_df, metadata = process_statement_enhanced(sample_df)
    
    print("🎯 Enhanced Date Processing Integration Test")
    print("=" * 50)
    print(f"Processed {len(sample_df)} transactions")
    print(f"Processing metadata: {metadata}")
    
    # Show results
    print("\n📊 Processing Results:")
    for idx, row in processed_df.iterrows():
        print(f"  Row {idx}: {row['raw_date']} -> {row['date_action_required']}")
