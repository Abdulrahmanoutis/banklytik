"""
Manual review workflow for date validation issues.
Provides interface for reviewing and approving/rejecting automatic corrections.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json
from enum import Enum


class ReviewAction(Enum):
    """Actions that can be taken during manual review."""
    APPROVE = "approve"
    REJECT = "reject"
    MODIFY = "modify"
    SKIP = "skip"


class DateReviewWorkflow:
    """
    Workflow for managing manual review of date validation issues.
    Tracks review history and provides interface for corrections.
    """

    def __init__(self):
        self.review_history = []
        self.pending_reviews = []
        self.correction_learning = {}

    def extract_review_candidates(self, df: pd.DataFrame, 
                             action_filter: List[str] = None) -> pd.DataFrame:
        """
        Extract transactions that require manual review.
        
        Args:
            df: DataFrame with validation results
            action_filter: List of action types to extract (default: all review types)
        
        Returns:
            DataFrame of transactions needing review
        """
        if action_filter is None:
            action_filter = ['MANUAL_REVIEW', 'IMMEDIATE_REVIEW']
        
        review_mask = df['date_action_required'].isin(action_filter)
        return df[review_mask].copy()

    def create_review_session(self, df: pd.DataFrame) -> Dict:
        """
        Create a review session for problematic dates.
        
        Returns:
            Dict with review session data and metadata
        """
        candidates = self.extract_review_candidates(df)
        
        session = {
            'session_id': f"review_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'created_at': datetime.now(),
            'total_candidates': len(candidates),
            'candidates': [],
            'status': 'pending'
        }
        
        for idx, row in candidates.iterrows():
            validation_info = row['_date_validation']
            
            candidate = {
                'row_index': idx,
                'original_date': validation_info.get('original_raw', ''),
                'current_date': row['raw_date'],
                'transaction_desc': row.get('transaction_description', ''),
                'amount': row.get('amount', 0),
                'issues': row['date_validation_issue'],
                'action_required': row['date_action_required'],
                'confidence': row['date_inference_confidence'],
                'corrections_applied': validation_info.get('corrections_applied', []),
                'review_action': None,
                'review_notes': '',
                'review_timestamp': None
            }
            
            session['candidates'].append(candidate)
        
        return session

    def apply_review_decision(self, session: Dict, row_index: int, 
                           action: ReviewAction, corrected_date: str = None,
                           notes: str = '') -> Dict:
        """
        Apply a review decision to a specific transaction.
        
        Args:
            session: Review session data
            row_index: Index of the transaction being reviewed
            action: ReviewAction to apply
            corrected_date: Manually corrected date (if action is MODIFY)
            notes: Review notes
        
        Returns:
            Updated session data
        """
        # Find the candidate
        candidate = None
        for i, cand in enumerate(session['candidates']):
            if cand['row_index'] == row_index:
                candidate = session['candidates'][i]
                break
        
        if not candidate:
            raise ValueError(f"Row index {row_index} not found in session candidates")
        
        # Apply the decision
        candidate['review_action'] = action.value
        candidate['review_notes'] = notes
        candidate['review_timestamp'] = datetime.now()
        
        if action == ReviewAction.MODIFY and corrected_date:
            candidate['manual_correction'] = corrected_date
        
        # Track for learning
        self._track_correction_for_learning(candidate, action, corrected_date)
        
        return session

    def _track_correction_for_learning(self, candidate: Dict, action: ReviewAction, 
                                   corrected_date: str = None):
        """Track corrections for continuous learning."""
        original = candidate['original_date']
        issues = candidate['issues']
        
        learning_key = f"{original}:{issues}"
        
        if learning_key not in self.correction_learning:
            self.correction_learning[learning_key] = {
                'original_pattern': original,
                'issues': issues,
                'corrections_applied': [],
                'success_count': 0,
                'rejection_count': 0
            }
        
        correction_record = {
            'action': action.value,
            'manual_correction': corrected_date,
            'timestamp': datetime.now(),
            'confidence': candidate['confidence']
        }
        
        self.correction_learning[learning_key]['corrections_applied'].append(correction_record)
        
        if action == ReviewAction.APPROVE:
            self.correction_learning[learning_key]['success_count'] += 1
        elif action == ReviewAction.REJECT:
            self.correction_learning[learning_key]['rejection_count'] += 1

    def generate_review_summary(self, session: Dict) -> Dict:
        """Generate a summary of the review session."""
        total = len(session['candidates'])
        reviewed = sum(1 for c in session['candidates'] if c['review_action'])
        pending = total - reviewed
        
        action_counts = {}
        for candidate in session['candidates']:
            action = candidate['review_action']
            if action:
                action_counts[action] = action_counts.get(action, 0) + 1
        
        return {
            'session_id': session['session_id'],
            'total_candidates': total,
            'reviewed': reviewed,
            'pending': pending,
            'completion_rate': (reviewed / total * 100) if total > 0 else 0,
            'action_breakdown': action_counts,
            'status': 'completed' if pending == 0 else 'in_progress'
        }

    def export_corrections_for_learning(self, filepath: str):
        """Export correction history for machine learning."""
        learning_data = []
        
        for key, data in self.correction_learning.items():
            success_rate = data['success_count'] / (data['success_count'] + data['rejection_count']) if (data['success_count'] + data['rejection_count']) > 0 else 0
            
            learning_entry = {
                'pattern': data['original_pattern'],
                'issues': data['issues'],
                'total_corrections': len(data['corrections_applied']),
                'success_count': data['success_count'],
                'rejection_count': data['rejection_count'],
                'success_rate': success_rate,
                'recommended_action': self._get_recommended_action(data)
            }
            
            learning_data.append(learning_entry)
        
        with open(filepath, 'w') as f:
            json.dump(learning_data, f, indent=2, default=str)

    def _get_recommended_action(self, data: Dict) -> str:
        """Get recommended action based on correction history."""
        if data['success_count'] > data['rejection_count'] * 2:
            return 'AUTO_CORRECT'
        elif data['rejection_count'] > data['success_count'] * 2:
            return 'FLAG_CRITICAL'
        else:
            return 'FLAG_REVIEW'

    def apply_approved_corrections(self, df: pd.DataFrame, 
                             session: Dict) -> pd.DataFrame:
        """
        Apply approved corrections from a review session to the DataFrame.
        
        Args:
            df: Original DataFrame
            session: Completed review session
        
        Returns:
            DataFrame with corrections applied
        """
        df_corrected = df.copy()
        
        for candidate in session['candidates']:
            if candidate['review_action'] == ReviewAction.APPROVE.value:
                idx = candidate['row_index']
                
                # Apply the corrected date
                if 'manual_correction' in candidate:
                    df_corrected.at[idx, 'raw_date'] = candidate['manual_correction']
                else:
                    # Use the auto-corrected date
                    df_corrected.at[idx, 'raw_date'] = candidate['current_date']
                
                # Update validation status
                df_corrected.at[idx, 'date_action_required'] = 'APPROVED'
                df_corrected.at[idx, 'date_validation_issue'] = 'MANUALLY_APPROVED'
                
            elif candidate['review_action'] == ReviewAction.REJECT.value:
                idx = candidate['row_index']
                df_corrected.at[idx, 'date_action_required'] = 'REJECTED'
                df_corrected.at[idx, 'date_validation_issue'] = 'MANUALLY_REJECTED'
        
        return df_corrected

    def create_review_interface_data(self, df: pd.DataFrame) -> Dict:
        """
        Create data structure for review interface (e.g., web UI).
        
        Returns:
            Dict formatted for frontend consumption
        """
        session = self.create_review_session(df)
        
        interface_data = {
            'session_info': {
                'id': session['session_id'],
                'created_at': session['created_at'].isoformat(),
                'total_items': session['total_candidates']
            },
            'summary': self.generate_review_summary(session),
            'items': []
        }
        
        for candidate in session['candidates']:
            item = {
                'id': candidate['row_index'],
                'transaction': {
                    'description': candidate['transaction_desc'],
                    'amount': candidate['amount'],
                    'original_date': candidate['original_date'],
                    'current_date': candidate['current_date']
                },
                'validation': {
                    'issues': candidate['issues'],
                    'action_required': candidate['action_required'],
                    'confidence': candidate['confidence'],
                    'corrections_applied': candidate['corrections_applied']
                },
                'review': {
                    'action': candidate['review_action'],
                    'notes': candidate['review_notes'],
                    'timestamp': candidate['review_timestamp']
                }
            }
            
            interface_data['items'].append(item)
        
        return interface_data


def create_sample_review_workflow():
    """Create a sample review workflow for demonstration."""
    print("🔧 Creating Sample Date Review Workflow")
    print("=" * 50)
    
    # Sample problematic data
    sample_data = pd.DataFrame({
        'raw_date': ['Feb 2025', '24 2025', '', '32 Feb 2025'],
        'transaction_description': ['Test 1', 'Test 2', 'Test 3', 'Test 4'],
        'amount': [-100, -200, -300, -400]
    })
    
    # Apply validation
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from date_validator import enhanced_date_validation
    validated_df = enhanced_date_validation(sample_data, verbose=False)
    
    # Create review workflow
    workflow = DateReviewWorkflow()
    session = workflow.create_review_session(validated_df)
    
    print(f"📊 Created review session: {session['session_id']}")
    print(f"📋 Total candidates for review: {session['total_candidates']}")
    
    # Display candidates
    for i, candidate in enumerate(session['candidates'][:3]):  # Show first 3
        print(f"\n🔍 Candidate {i+1}:")
        print(f"  Original: {candidate['original_date']}")
        print(f"  Current: {candidate['current_date']}")
        print(f"  Issues: {candidate['issues']}")
        print(f"  Action Required: {candidate['action_required']}")
    
    # Simulate some review decisions
    if session['candidates']:
        first_candidate = session['candidates'][0]
        workflow.apply_review_decision(
            session, 
            first_candidate['row_index'],
            ReviewAction.APPROVE,
            notes="Auto-correction looks correct"
        )
    
    # Generate summary
    summary = workflow.generate_review_summary(session)
    print(f"\n📈 Review Summary:")
    print(f"  Status: {summary['status']}")
    print(f"  Completion: {summary['completion_rate']:.1f}%")
    
    return workflow, session


if __name__ == "__main__":
    create_sample_review_workflow()
