"""
Date validation utilities for OCR error detection in bank statements.
Detects OCR-specific error patterns and validates date sanity.
Enhanced with tiered classification and smart inference.
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd
from calendar import monthrange


def parse_date_flexible(date_str: str) -> Optional[datetime]:
    """
    Flexible date parsing that handles various date formats.
    Falls back through multiple parsing strategies.
    """
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Remove quotes if present
    date_str = date_str.strip("'\"")
    
    # List of date formats to try
    formats = [
        '%d %b %Y',      # '24 Feb 2025'
        '%d %B %Y',       # '24 February 2025'
        '%b %d %Y',       # 'Feb 24 2025'
        '%B %d %Y',       # 'February 24 2025'
        '%d %m %Y',       # '24 02 2025'
        '%Y %m %d',       # '2025 02 24'
        '%Y-%m-%d',       # '2025-02-24'
        '%d/%m/%Y',       # '24/02/2025'
        '%m/%d/%Y',       # '02/24/2025'
        # REMOVED: '%b %Y' - incomplete date format (missing day)
        '%Y %b %d',       # '2025 Feb 24'
        '%Y %b %d %H:%M', # '2025 Feb 24 10:30'
        '%Y %b %d %H:%M:%S', # '2025 Feb 24 10:30:45'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # Special handling for month-year only (Feb 2025) - FLAG as incomplete
    month_year_match = re.match(r'^([A-Za-z]{3,})\s+(\d{4})$', date_str)
    if month_year_match:
        # Don't auto-parse incomplete dates - return None to trigger manual review
        return None
    
    return None


class DateValidator:
    """
    Validates dates and detects OCR-specific errors in bank statement dates.
    Enhanced with tiered classification and smart inference capabilities.
    """

    # Classification categories
    CATEGORIES = {
        'AUTO_CORRECT': 'Automatically correctable patterns',
        'FLAG_REVIEW': 'Requires manual review but potentially recoverable',
        'FLAG_CRITICAL': 'Critical issues requiring immediate attention'
    }

    # Known OCR error patterns from learning log
    OCR_ERROR_PATTERNS = [
        r'^\d{4}\s+[A-Za-z]{3}\s+\d{1,2}:\d{2}\s+\d{1,2}$',  # "2025 Feb 20:42 59"
        r'^\d{1,2}:\d{2}\s+\d{1,2}$',  # Time mixed with date
        r'[A-Za-z]{3}\s+\d{2}:\d{2}',  # Month with time
        r'^\d{4}\s+[A-Za-z]{3,9}\s+\d{1,2}:\d{2}\s+\d{2}$',  # "2025 February 20:42 59"
        r'^[A-Za-z]{3}\s+\d{4}$',  # "Feb 2025" - incomplete date (missing day)
        # Note: Removed pattern for "24 Feb 2025" as this is a valid format
    ]

    # Config for date sanity checks
    MAX_FUTURE_DAYS = 365  # Don't accept dates more than 1 year in future
    MAX_PAST_YEARS = 50  # Don't accept dates older than 50 years

    def __init__(self):
        self.errors_found = []
        self.validation_summary = {
            'valid': 0,
            'suspicious': 0,
            'invalid': 0,
            'patterns_matched': {},
            'auto_corrected': 0,
            'flagged_review': 0,
            'flagged_critical': 0
        }
        self.correction_rules = self._load_correction_rules()

    def _load_correction_rules(self) -> List[Dict]:
        """Load date correction rules from knowledge base."""
        try:
            import json
            import os
            rules_path = os.path.join(
                os.path.dirname(__file__),
                '..', 'banklytik_knowledge', 'rules', 'dates', 'dates.json'
            )
            with open(rules_path, 'r') as f:
                return json.load(f)
        except Exception:
            return []

    def apply_correction_rules(self, date_str: str) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Apply correction rules to date string.
        Returns: (corrected_date, rule_applied, category)
        """
        if not date_str:
            return date_str, None, None

        for rule in self.correction_rules:
            regex = rule.get('regex')
            replace = rule.get('replace')
            category = rule.get('category')

            if regex and re.search(regex, str(date_str)):
                if replace is not None:
                    corrected = re.sub(regex, replace, str(date_str))
                    if corrected != str(date_str):
                        return corrected, rule.get('title'), category
                else:
                    # Detection-only rule
                    return date_str, rule.get('title'), category

        return date_str, None, None

    def infer_missing_components(self, date_str: str, context_dates: List[datetime] = None) -> Tuple[str, str]:
        """
        Attempt to infer missing date components using context.
        Returns: (inferred_date, confidence_level)
        """
        if not date_str:
            return date_str, 'LOW'

        # Pattern for "Feb 2025" - missing day
        month_year_pattern = r'^([A-Za-z]{3,})\s+(\d{4})$'
        match = re.match(month_year_pattern, str(date_str))
        
        if match:
            month_name = match.group(1)
            year = int(match.group(2))
            
            # Try to infer day from context with enhanced logic
            if context_dates:
                # Filter dates from same year and month
                same_year_dates = [d for d in context_dates if d.year == year]
                if same_year_dates:
                    # Try to find dates from same month
                    same_month_dates = [d for d in same_year_dates if d.strftime('%b') == month_name[:3]]
                    
                    if same_month_dates:
                        # Use median day from same month transactions
                        days = sorted([d.day for d in same_month_dates])
                        median_day = days[len(days)//2] if len(days) > 1 else days[0]
                        last_day = monthrange(year, datetime.strptime(month_name[:3], '%b').month)[1]
                        inferred_day = min(median_day, last_day)
                        inferred_date = f"{inferred_day} {date_str}"
                        return inferred_date, 'HIGH'
                    else:
                        # Use most recent date from same year
                        recent_dates = sorted(same_year_dates)
                        last_day = monthrange(year, datetime.strptime(month_name[:3], '%b').month)[1]
                        inferred_day = min(recent_dates[-1].day, last_day)
                        inferred_date = f"{inferred_day} {date_str}"
                        return inferred_date, 'MEDIUM'
            
            # Default to middle of month (15th) if no context - more neutral than 1st
            inferred_date = f"15 {date_str}"
            return inferred_date, 'LOW'

        # Pattern for "24 2025" - missing month
        day_year_pattern = r'^(\d{1,2})\s+(\d{4})$'
        match = re.match(day_year_pattern, str(date_str))
        
        if match:
            day = int(match.group(1))
            year = int(match.group(2))
            
            # Try to infer month from context
            if context_dates:
                recent_dates = [d for d in context_dates if d.year == year]
                if recent_dates:
                    inferred_month = recent_dates[-1].strftime('%b')
                    inferred_date = f"{day} {inferred_month} {year}"
                    return inferred_date, 'MEDIUM'
            
            # Default to current month if no context
            current_month = datetime.now().strftime('%b')
            inferred_date = f"{day} {current_month} {year}"
            return inferred_date, 'LOW'

        return date_str, 'LOW'

    def classify_date_issue(self, category: str, confidence: str = 'LOW') -> Dict[str, any]:
        """Classify date issue into tiered categories."""
        classification = {
            'category': category,
            'action_required': 'NONE',
            'confidence': confidence,
            'auto_correctable': False,
            'review_needed': False,
            'critical': False
        }

        if category == 'AUTO_CORRECT':
            classification.update({
                'action_required': 'AUTO_CORRECT',
                'auto_correctable': True,
                'confidence': 'HIGH'
            })
        elif category == 'FLAG_REVIEW':
            classification.update({
                'action_required': 'MANUAL_REVIEW',
                'review_needed': True,
                'confidence': confidence
            })
        elif category == 'FLAG_CRITICAL':
            classification.update({
                'action_required': 'IMMEDIATE_REVIEW',
                'review_needed': True,
                'critical': True,
                'confidence': 'HIGH'
            })

        return classification

    def is_ocr_error_pattern(self, date_str: str) -> Tuple[bool, Optional[str]]:
        """Check if date string matches known OCR error patterns."""
        if not date_str or str(date_str).lower() in ('nat', 'none', ''):
            return False, None

        for pattern in self.OCR_ERROR_PATTERNS:
            if re.search(pattern, str(date_str)):
                pattern_name = f"OCR_PATTERN_{len(self.validation_summary['patterns_matched'])}"
                self.validation_summary['patterns_matched'][pattern_name] = date_str
                return True, pattern_name

        return False, None

    def has_impossible_time(self, date_str: str) -> Tuple[bool, Optional[str]]:
        """Detect impossible time values in date strings (e.g., 25:61:90)."""
        if not date_str:
            return False, None

        # Match time-like patterns
        time_pattern = r'(\d{1,2}):(\d{2})(?::(\d{2}))?'
        matches = re.findall(time_pattern, str(date_str))

        for match in matches:
            hour = int(match[0])
            minute = int(match[1])
            second = int(match[2]) if match[2] else 0

            if hour > 23 or minute > 59 or second > 59:
                return True, f"Impossible time: {hour}:{minute}:{second}"

        return False, None

    def is_date_too_far_in_future(self, parsed_date: datetime) -> bool:
        """Check if date is unreasonably far in the future."""
        if not parsed_date:
            return False

        now = datetime.now()
        max_future = now + timedelta(days=self.MAX_FUTURE_DAYS)

        return parsed_date > max_future

    def is_date_too_far_in_past(self, parsed_date: datetime) -> bool:
        """Check if date is unreasonably far in the past."""
        if not parsed_date:
            return False

        now = datetime.now()
        min_past = now - timedelta(days=365 * self.MAX_PAST_YEARS)

        return parsed_date < min_past

    def validate_date(self, raw_date_str: str, parsed_date: Optional[datetime]) -> Dict[str, any]:
        """
        Comprehensive date validation.
        Returns dict with validation results and specific issue flags.
        """
        result = {
            'raw_date': raw_date_str,
            'parsed_date': parsed_date,
            'is_valid': True,
            'is_suspicious': False,
            'issues': [],
            'warning_level': 'INFO'  # INFO, WARNING, ERROR
        }

        if raw_date_str is None or str(raw_date_str).lower() in ('nat', 'none', ''):
            result['is_valid'] = False
            result['issues'].append('NULL_DATE')
            result['warning_level'] = 'ERROR'
            return result

        # Check for OCR error patterns
        is_ocr_error, pattern_name = self.is_ocr_error_pattern(raw_date_str)
        if is_ocr_error:
            result['is_suspicious'] = True
            result['issues'].append(f'OCR_ERROR_PATTERN:{pattern_name}')
            result['warning_level'] = 'ERROR'

        # Check for impossible times in date string
        has_impossible, impossible_msg = self.has_impossible_time(raw_date_str)
        if has_impossible:
            result['is_suspicious'] = True
            result['issues'].append(f'IMPOSSIBLE_TIME:{impossible_msg}')
            result['warning_level'] = 'ERROR'

        # If parsed_date is None but we have a raw date string, it's suspicious
        if parsed_date is None and result['is_valid']:
            result['is_valid'] = False
            result['issues'].append('UNPARSEABLE')
            result['warning_level'] = 'WARNING'

        # Validate parsed date sanity
        if parsed_date is not None:
            if self.is_date_too_far_in_future(parsed_date):
                result['is_suspicious'] = True
                result['issues'].append('DATE_TOO_FAR_FUTURE')
                result['warning_level'] = 'WARNING'

            if self.is_date_too_far_in_past(parsed_date):
                result['is_suspicious'] = True
                result['issues'].append('DATE_TOO_FAR_PAST')
                result['warning_level'] = 'WARNING'

        # Mark as suspicious if issues were found
        if result['issues']:
            result['is_suspicious'] = True

        return result

    def get_validation_summary(self) -> Dict:
        """Return validation summary statistics."""
        return {
            'total_valid': self.validation_summary['valid'],
            'total_suspicious': self.validation_summary['suspicious'],
            'total_invalid': self.validation_summary['invalid'],
            'ocr_patterns_found': len(self.validation_summary['patterns_matched']),
            'pattern_examples': self.validation_summary['patterns_matched']
        }


def enhanced_date_validation(df: pd.DataFrame, date_column: str = 'raw_date', 
                           context_column: str = 'date', verbose: bool = True) -> pd.DataFrame:
    """
    Enhanced date validation with tiered classification and smart inference.
    
    Args:
        df: DataFrame containing date columns
        date_column: Column containing raw date strings
        context_column: Column containing parsed dates for context
        verbose: Whether to print validation summary
        
    Returns DataFrame with added columns:
    - _date_validation: Dict with comprehensive validation results
    - date_validation_issue: Human-readable issue string
    - date_validation_warning: WARNING/ERROR flag
    - date_correction_applied: Correction rule that was applied
    - date_inference_confidence: Confidence level for inferred values
    - date_action_required: Action needed (AUTO_CORRECT, MANUAL_REVIEW, IMMEDIATE_REVIEW)
    """
    validator = DateValidator()

    if date_column not in df.columns:
        if verbose:
            print(f"⚠️ DataFrame missing '{date_column}' column for validation")
        return df

    def enhanced_validate_row(row, idx, context_dates):
        """Enhanced validation for a single row's date."""
        raw_date = str(row.get(date_column, ''))
        parsed_date = row.get(context_column, None) if context_column in df.columns else None
        
        result = {
            'raw_date': raw_date,
            'parsed_date': parsed_date,
            'original_raw': raw_date,
            'corrections_applied': [],
            'inference_applied': False,
            'validation_issues': [],
            'classification': None,
            'action_required': 'NONE',
            'confidence': 'LOW'
        }

        # Step 1: Apply correction rules
        corrected_date, rule_applied, category = validator.apply_correction_rules(raw_date)
        if rule_applied:
            result['corrections_applied'].append({
                'rule': rule_applied,
                'category': category,
                'original': raw_date,
                'corrected': corrected_date
            })
            result['raw_date'] = corrected_date
            
            # Update summary
            if category == 'AUTO_CORRECT':
                validator.validation_summary['auto_corrected'] += 1
            elif category == 'FLAG_REVIEW':
                validator.validation_summary['flagged_review'] += 1
            elif category == 'FLAG_CRITICAL':
                validator.validation_summary['flagged_critical'] += 1

        # Step 2: Apply smart inference if still problematic
        if category in ['FLAG_REVIEW', 'FLAG_CRITICAL'] and context_dates:
            inferred_date, confidence = validator.infer_missing_components(
                corrected_date, context_dates
            )
            if inferred_date != corrected_date:
                result['inference_applied'] = True
                result['raw_date'] = inferred_date
                result['confidence'] = confidence
                result['corrections_applied'].append({
                    'rule': 'SMART_INFERENCE',
                    'category': 'INFERRED',
                    'original': corrected_date,
                    'corrected': inferred_date,
                    'confidence': confidence
                })

        # Step 3: Standard validation
        try:
            # Try to parse the potentially corrected date using standard Python datetime
            if result['raw_date']:
                test_parsed = parse_date_flexible(result['raw_date'])
                validation_result = validator.validate_date(result['raw_date'], test_parsed)
            else:
                validation_result = validator.validate_date(result['raw_date'], None)
        except Exception:
            validation_result = validator.validate_date(result['raw_date'], None)

        # Merge validation results
        result.update(validation_result)
        
        # Step 4: Classification
        if result['corrections_applied']:
            last_correction = result['corrections_applied'][-1]
            result['classification'] = validator.classify_date_issue(
                last_correction['category'], 
                last_correction.get('confidence', result['confidence'])
            )
            result['action_required'] = result['classification']['action_required']
        
        # Update validation summary
        if result['is_valid'] and not result['is_suspicious']:
            validator.validation_summary['valid'] += 1
        elif result['is_suspicious']:
            validator.validation_summary['suspicious'] += 1
        else:
            validator.validation_summary['invalid'] += 1

        return result

    # Build context from existing valid dates
    context_dates = []
    if context_column in df.columns:
        context_dates = [
            row[context_column] for _, row in df.iterrows() 
            if pd.notna(row[context_column]) and isinstance(row[context_column], datetime)
        ]

    # Apply enhanced validation to each row
    df = df.copy()
    df['_date_validation'] = df.apply(
        lambda row: enhanced_validate_row(row, row.name, context_dates), axis=1
    )

    # Extract human-readable information
    df.loc[:, 'date_validation_issue'] = df['_date_validation'].apply(
        lambda v: ' | '.join(v.get('issues', [])) if v.get('issues') else 'OK'
    )

    df.loc[:, 'date_validation_warning'] = df['_date_validation'].apply(
        lambda v: v.get('warning_level', 'INFO')
    )

    df.loc[:, 'date_correction_applied'] = df['_date_validation'].apply(
        lambda v: v.get('corrections_applied', [{}])[-1].get('rule', 'None') if v.get('corrections_applied') else 'None'
    )

    df.loc[:, 'date_inference_confidence'] = df['_date_validation'].apply(
        lambda v: v.get('confidence', 'LOW')
    )

    df.loc[:, 'date_action_required'] = df['_date_validation'].apply(
        lambda v: v.get('action_required', 'NONE')
    )

    if verbose:
        summary = validator.get_validation_summary()
        print(f"\n📊 Enhanced Date Validation Summary:")
        print(f"  ✅ Valid: {summary['total_valid']}")
        print(f"  ⚠️  Suspicious: {summary['total_suspicious']}")
        print(f"  ❌ Invalid: {summary['total_invalid']}")
        print(f"  🔧 Auto-Corrected: {summary.get('auto_corrected', 0)}")
        print(f"  👁️  Flagged for Review: {summary.get('flagged_review', 0)}")
        print(f"  🚨 Flagged Critical: {summary.get('flagged_critical', 0)}")
        print(f"  🔍 OCR Patterns Found: {summary['ocr_patterns_found']}")
        if summary['pattern_examples']:
            print(f"  Examples: {list(summary['pattern_examples'].values())[:3]}")

    return df


def validate_and_flag_dates(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Legacy function for backward compatibility.
    Uses enhanced validation with default parameters.
    """
    return enhanced_date_validation(df, verbose=verbose)


def flag_suspicious_dates_in_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag suspicious dates and add visual indicators.
    This is an alias for validate_and_flag_dates for backward compatibility.
    """
    return validate_and_flag_dates(df, verbose=True)
