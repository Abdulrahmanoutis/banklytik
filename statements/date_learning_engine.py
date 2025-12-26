"""
Continuous learning engine for date validation improvements.
Learns from manual corrections and improves automatic detection over time.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import re
import pandas as pd


class DateLearningEngine:
    """
    Engine for continuous learning from date validation corrections.
    Improves pattern recognition and correction rules based on user feedback.
    """

    def __init__(self, learning_data_path: str = None):
        self.learning_data_path = learning_data_path or os.path.join(
            os.path.dirname(__file__), 
            '..', 'banklytik_knowledge', 'learning', 'date_corrections.json'
        )
        self.correction_history = []
        self.pattern_success_rates = {}
        self.learned_rules = []
        self.load_learning_data()

    def load_learning_data(self):
        """Load existing learning data from file."""
        try:
            if os.path.exists(self.learning_data_path):
                with open(self.learning_data_path, 'r') as f:
                    data = json.load(f)
                    self.correction_history = data.get('correction_history', [])
                    self.pattern_success_rates = data.get('pattern_success_rates', {})
                    self.learned_rules = data.get('learned_rules', [])
        except Exception as e:
            print(f"⚠️ Could not load learning data: {e}")
            self.correction_history = []
            self.pattern_success_rates = {}
            self.learned_rules = []

    def save_learning_data(self):
        """Save learning data to file."""
        try:
            os.makedirs(os.path.dirname(self.learning_data_path), exist_ok=True)
            
            data = {
                'last_updated': datetime.now().isoformat(),
                'correction_history': self.correction_history,
                'pattern_success_rates': self.pattern_success_rates,
                'learned_rules': self.learned_rules,
                'total_corrections': len(self.correction_history)
            }
            
            with open(self.learning_data_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
                
        except Exception as e:
            print(f"⚠️ Could not save learning data: {e}")

    def record_correction(self, original_date: str, corrected_date: str, 
                        issues: List[str], action: str, confidence: float,
                        user_feedback: str = 'approved'):
        """
        Record a correction for learning purposes.
        
        Args:
            original_date: Original problematic date
            corrected_date: Final corrected date
            issues: List of issues detected
            action: Action taken (AUTO_CORRECT, MANUAL_REVIEW, etc.)
            confidence: Confidence level of the correction
            user_feedback: User feedback (approved, rejected, modified)
        """
        correction_record = {
            'timestamp': datetime.now().isoformat(),
            'original_date': original_date,
            'corrected_date': corrected_date,
            'issues': issues,
            'action': action,
            'confidence': confidence,
            'user_feedback': user_feedback,
            'pattern_signature': self._extract_pattern_signature(original_date, issues)
        }
        
        self.correction_history.append(correction_record)
        self._update_pattern_success_rates(correction_record)
        self._generate_new_rules_if_needed()

    def _extract_pattern_signature(self, date_str: str, issues: List[str]) -> str:
        """Extract a normalized pattern signature for grouping similar corrections."""
        # Normalize the date string to focus on structural patterns
        normalized = re.sub(r'\d+', 'NUM', date_str)
        normalized = re.sub(r'[A-Za-z]{3,}', 'MON', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        # Create signature from pattern + issues
        issues_str = '|'.join(sorted(issues))
        signature = f"{normalized}:{issues_str}"
        
        return signature

    def _update_pattern_success_rates(self, correction_record: Dict):
        """Update success rates for pattern signatures."""
        signature = correction_record['pattern_signature']
        feedback = correction_record['user_feedback']
        
        if signature not in self.pattern_success_rates:
            self.pattern_success_rates[signature] = {
                'total_attempts': 0,
                'successful_corrections': 0,
                'failed_corrections': 0,
                'corrections': []
            }
        
        pattern_data = self.pattern_success_rates[signature]
        pattern_data['total_attempts'] += 1
        
        if feedback == 'approved':
            pattern_data['successful_corrections'] += 1
        elif feedback == 'rejected':
            pattern_data['failed_corrections'] += 1
            
        pattern_data['corrections'].append({
            'timestamp': correction_record['timestamp'],
            'original': correction_record['original_date'],
            'corrected': correction_record['corrected_date'],
            'feedback': feedback
        })

    def _generate_new_rules_if_needed(self):
        """Generate new correction rules based on learning patterns."""
        new_rules = []
        
        for signature, data in self.pattern_success_rates.items():
            success_rate = data['successful_corrections'] / data['total_attempts'] if data['total_attempts'] > 0 else 0
            
            # Generate rule for high-success patterns
            if success_rate >= 0.8 and data['total_attempts'] >= 3:
                rule = self._create_rule_from_pattern(signature, data)
                if rule and not self._rule_exists(rule):
                    new_rules.append(rule)
        
        # Add new learned rules
        self.learned_rules.extend(new_rules)

    def _create_rule_from_pattern(self, signature: str, pattern_data: Dict) -> Optional[Dict]:
        """Create a correction rule from a successful pattern."""
        # Find the most common successful correction
        successful_corrections = [c for c in pattern_data['corrections'] if c['feedback'] == 'approved']
        
        if not successful_corrections:
            return None
        
        # Get the most frequent correction
        correction_counts = {}
        for correction in successful_corrections:
            key = (correction['original'], correction['corrected'])
            correction_counts[key] = correction_counts.get(key, 0) + 1
        
        most_common = max(correction_counts.items(), key=lambda x: x[1])
        original, corrected = most_common[0]
        
        # Generate regex pattern
        regex_pattern = self._generate_regex_from_example(original)
        if not regex_pattern:
            return None
        
        rule = {
            'title': f"Learned Rule: {signature}",
            'description': f"Auto-generated rule with {pattern_data['successful_corrections']}/{pattern_data['total_attempts']} success rate",
            'regex': regex_pattern,
            'replace': corrected,
            'category': 'AUTO_CORRECT',
            'confidence': pattern_data['successful_corrections'] / pattern_data['total_attempts'],
            'learned_at': datetime.now().isoformat(),
            'signature': signature
        }
        
        return rule

    def _generate_regex_from_example(self, example: str) -> Optional[str]:
        """Generate a regex pattern from an example date string."""
        try:
            # Escape special characters first
            escaped = re.escape(example)
            
            # Replace escaped numbers with number patterns
            escaped = re.sub(r'\\\d+', r'\\d+', escaped)
            
            # Replace escaped letters with letter patterns
            escaped = re.sub(r'\\\[]', '[a-zA-Z]', escaped)
            
            # Replace escaped month names with month pattern
            month_pattern = r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*'
            escaped = re.sub(r'[A-Za-z]{3,}', month_pattern, escaped)
            
            return escaped
            
        except Exception:
            return None

    def _rule_exists(self, new_rule: Dict) -> bool:
        """Check if a rule already exists in learned rules."""
        for existing_rule in self.learned_rules:
            if existing_rule.get('signature') == new_rule.get('signature'):
                return True
        return False

    def get_learned_rules(self) -> List[Dict]:
        """Get all learned correction rules."""
        return self.learned_rules.copy()

    def get_pattern_success_rate(self, pattern_signature: str) -> float:
        """Get success rate for a specific pattern."""
        if pattern_signature not in self.pattern_success_rates:
            return 0.0
        
        data = self.pattern_success_rates[pattern_signature]
        return data['successful_corrections'] / data['total_attempts'] if data['total_attempts'] > 0 else 0.0

    def suggest_corrections(self, date_str: str, issues: List[str]) -> List[Dict]:
        """
        Suggest corrections based on learned patterns.
        
        Args:
            date_str: Date string to correct
            issues: Issues detected in the date string
            
        Returns:
            List of suggested corrections with confidence scores
        """
        suggestions = []
        signature = self._extract_pattern_signature(date_str, issues)
        
        # Check if we have learned patterns for this signature
        if signature in self.pattern_success_rates:
            pattern_data = self.pattern_success_rates[signature]
            success_rate = self.get_pattern_success_rate(signature)
            
            if success_rate >= 0.6:  # Only suggest if reasonably confident
                # Get most successful corrections
                successful_corrections = [c for c in pattern_data['corrections'] if c['feedback'] == 'approved']
                
                for correction in successful_corrections:
                    if correction['original'] == date_str:
                        suggestions.append({
                            'corrected_date': correction['corrected'],
                            'confidence': success_rate,
                            'based_on': f"{pattern_data['successful_corrections']}/{pattern_data['total_attempts']} successful corrections",
                            'source': 'learned_pattern'
                        })
        
        # Also check learned rules
        for rule in self.learned_rules:
            if rule.get('regex') and re.search(rule['regex'], date_str):
                suggestions.append({
                    'corrected_date': rule['replace'],
                    'confidence': rule.get('confidence', 0.5),
                    'based_on': rule.get('description', ''),
                    'source': 'learned_rule'
                })
        
        return suggestions

    def export_learning_summary(self) -> Dict:
        """Export a summary of learning progress."""
        total_patterns = len(self.pattern_success_rates)
        successful_patterns = sum(1 for data in self.pattern_success_rates.values() 
                                if data['successful_corrections'] > data['failed_corrections'])
        
        return {
            'total_corrections': len(self.correction_history),
            'total_patterns_learned': total_patterns,
            'successful_patterns': successful_patterns,
            'total_rules_generated': len(self.learned_rules),
            'last_updated': datetime.now().isoformat(),
            'top_patterns': self._get_top_patterns()
        }

    def _get_top_patterns(self, top_n: int = 5) -> List[Dict]:
        """Get top performing patterns."""
        patterns = []
        
        for signature, data in self.pattern_success_rates.items():
            if data['total_attempts'] >= 2:  # At least 2 attempts
                success_rate = data['successful_corrections'] / data['total_attempts']
                patterns.append({
                    'signature': signature,
                    'success_rate': success_rate,
                    'attempts': data['total_attempts'],
                    'successful': data['successful_corrections']
                })
        
        # Sort by success rate and return top N
        patterns.sort(key=lambda x: x['success_rate'], reverse=True)
        return patterns[:top_n]

    def import_review_session_data(self, session_data: Dict):
        """Import learning data from a review session."""
        for candidate in session_data.get('candidates', []):
            if candidate.get('review_action'):
                original = candidate.get('original_date', '')
                current = candidate.get('current_date', '')
                issues = candidate.get('issues', '').split('|') if candidate.get('issues') else []
                action = candidate.get('action_required', '')
                confidence = candidate.get('confidence', 'low')
                
                # Convert confidence to float
                if confidence == 'HIGH':
                    confidence_val = 0.8
                elif confidence == 'MEDIUM':
                    confidence_val = 0.6
                else:
                    confidence_val = 0.3
                
                user_feedback = 'approved' if candidate.get('review_action') == 'approve' else 'rejected'
                
                self.record_correction(
                    original_date=original,
                    corrected_date=current,
                    issues=issues,
                    action=action,
                    confidence=confidence_val,
                    user_feedback=user_feedback
                )
        
        self.save_learning_data()


def create_sample_learning_engine():
    """Create and test a sample learning engine."""
    print("🧠 Creating Sample Date Learning Engine")
    print("=" * 50)
    
    engine = DateLearningEngine()
    
    # Simulate some learning data
    sample_corrections = [
        ('24Feb 2025', '24 Feb 2025', ['MISSING_SPACE'], 'AUTO_CORRECT', 0.9, 'approved'),
        ('Feb 2025', '1 Feb 2025', ['MISSING_DAY'], 'MANUAL_REVIEW', 0.5, 'approved'),
        ('24Feb 2025', '24 Feb 2025', ['MISSING_SPACE'], 'AUTO_CORRECT', 0.9, 'approved'),
        ('Mar2025', '1 Mar 2025', ['MISSING_SPACE', 'MISSING_DAY'], 'MANUAL_REVIEW', 0.4, 'rejected'),
    ]
    
    for original, corrected, issues, action, confidence, feedback in sample_corrections:
        engine.record_correction(original, corrected, issues, action, confidence, feedback)
    
    # Test suggestion system
    print("🔍 Testing suggestion system:")
    test_dates = ['24Feb 2025', 'Feb 2025', 'Mar2025']
    
    for test_date in test_dates:
        suggestions = engine.suggest_corrections(test_date, ['MISSING_SPACE'])
        print(f"\n  Date: {test_date}")
        if suggestions:
            for suggestion in suggestions:
                print(f"    Suggestion: {suggestion['corrected_date']} (confidence: {suggestion['confidence']:.2f})")
        else:
            print("    No suggestions available")
    
    # Show learning summary
    summary = engine.export_learning_summary()
    print(f"\n📊 Learning Summary:")
    print(f"  Total Corrections: {summary['total_corrections']}")
    print(f"  Patterns Learned: {summary['total_patterns_learned']}")
    print(f"  Successful Patterns: {summary['successful_patterns']}")
    print(f"  Rules Generated: {summary['total_rules_generated']}")
    
    return engine


if __name__ == "__main__":
    create_sample_learning_engine()
