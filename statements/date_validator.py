"""
Date validation utilities for OCR error detection in bank statements.
Detects OCR-specific error patterns and validates date sanity.
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd


class DateValidator:
    """
    Validates dates and detects OCR-specific errors in bank statement dates.
    """

    # Known OCR error patterns from learning log
    OCR_ERROR_PATTERNS = [
        r'^\d{4}\s+[A-Za-z]{3}\s+\d{1,2}:\d{2}\s+\d{1,2}$',  # "2025 Feb 20:42 59"
        r'^\d{1,2}:\d{2}\s+\d{1,2}$',  # Time mixed with date
        r'[A-Za-z]{3}\s+\d{2}:\d{2}',  # Month with time
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
            'patterns_matched': {}
        }

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


def validate_and_flag_dates(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Validate dates in a DataFrame and add validation columns.
    
    Returns DataFrame with added columns:
    - _date_validation: Dict with validation results
    - date_validation_issue: Human-readable issue string
    - date_validation_warning: WARNING/ERROR flag
    """
    validator = DateValidator()

    if 'date' not in df.columns or 'raw_date' not in df.columns:
        if verbose:
            print("⚠️ DataFrame missing 'date' or 'raw_date' columns for validation")
        return df

    def validate_row(row):
        """Validate a single row's date."""
        raw = row.get('raw_date', '')
        parsed = row.get('date', None)

        validation = validator.validate_date(raw, parsed)

        # Update summary
        if validation['is_valid'] and not validation['is_suspicious']:
            validator.validation_summary['valid'] += 1
        elif validation['is_suspicious']:
            validator.validation_summary['suspicious'] += 1
        else:
            validator.validation_summary['invalid'] += 1

        return validation

    # Apply validation to each row
    df['_date_validation'] = df.apply(validate_row, axis=1)

    # Extract human-readable issue strings
    df['date_validation_issue'] = df['_date_validation'].apply(
        lambda v: ' | '.join(v['issues']) if v['issues'] else 'OK'
    )

    # Extract warning levels
    df['date_validation_warning'] = df['_date_validation'].apply(
        lambda v: v['warning_level']
    )

    if verbose:
        summary = validator.get_validation_summary()
        print(f"\n📊 Date Validation Summary:")
        print(f"  ✅ Valid: {summary['total_valid']}")
        print(f"  ⚠️  Suspicious: {summary['total_suspicious']}")
        print(f"  ❌ Invalid: {summary['total_invalid']}")
        print(f"  🚨 OCR Patterns Found: {summary['ocr_patterns_found']}")
        if summary['pattern_examples']:
            print(f"  Examples: {list(summary['pattern_examples'].values())[:3]}")

    return df


def flag_suspicious_dates_in_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag suspicious dates and add visual indicators.
    This is an alias for validate_and_flag_dates for backward compatibility.
    """
    return validate_and_flag_dates(df, verbose=True)
