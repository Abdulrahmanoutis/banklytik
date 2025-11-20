"""
Column mapping system for Banklytik manual table selection.

This module helps users map extracted columns to standard transaction fields.
"""

import pandas as pd
import re
from typing import Dict, List, Any, Optional
from datetime import datetime


class ColumnMapper:
    """
    Analyzes and maps columns to standard transaction fields.
    """
    
    # Standard field types and their patterns
    FIELD_PATTERNS = {
        'date': {
            'patterns': [
                r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
                r'\d{1,2}\s+[A-Za-z]{3}\s+\d{4}',
                r'\d{4}-\d{2}-\d{2}',
                r'\d{1,2}:\d{2}'
            ],
            'keywords': ['date', 'time', 'datetime', 'trans date']
        },
        'description': {
            'patterns': [],
            'keywords': ['description', 'desc', 'particulars', 'details', 'narration']
        },
        'debit': {
            'patterns': [
                r'₦\s*[\d,]+\.?\d*',
                r'[\d,]+\.?\d*\s*₦',
                r'[\d,]+\.?\d*'
            ],
            'keywords': ['debit', 'withdrawal', 'dr', 'debit amount']
        },
        'credit': {
            'patterns': [
                r'₦\s*[\d,]+\.?\d*',
                r'[\d,]+\.?\d*\s*₦',
                r'[\d,]+\.?\d*'
            ],
            'keywords': ['credit', 'deposit', 'cr', 'credit amount']
        },
        'amount': {
            'patterns': [
                r'₦\s*[\d,]+\.?\d*',
                r'[\d,]+\.?\d*\s*₦',
                r'[\d,]+\.?\d*'
            ],
            'keywords': ['amount', 'transaction amount', 'value']
        },
        'balance': {
            'patterns': [
                r'₦\s*[\d,]+\.?\d*',
                r'[\d,]+\.?\d*\s*₦',
                r'[\d,]+\.?\d*'
            ],
            'keywords': ['balance', 'running balance', 'available balance']
        },
        'reference': {
            'patterns': [],
            'keywords': ['reference', 'ref', 'transaction ref', 'trn ref']
        }
    }
    
    def analyze_columns(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Analyze all columns and suggest field types.
        
        Returns:
            Dict mapping column names to analysis results
        """
        if df.empty:
            return {}
        
        analysis = {}
        
        for column in df.columns:
            analysis[column] = self._analyze_single_column(df[column], column)
        
        return analysis
    
    def _analyze_single_column(self, series: pd.Series, column_name: str) -> Dict[str, Any]:
        """
        Analyze a single column for field type detection.
        """
        if series.empty:
            return {
                'suggested_type': 'unknown',
                'confidence': 0,
                'reasons': ['Empty column'],
                'sample_data': []
            }
        
        # Get sample data
        sample_data = series.head(10).astype(str).tolist()
        
        # Calculate scores for each field type
        scores = {}
        for field_type, patterns in self.FIELD_PATTERNS.items():
            score = self._calculate_field_score(series, column_name, field_type, patterns)
            scores[field_type] = score
        
        # Find best match
        best_type = max(scores.items(), key=lambda x: x[1])
        
        # Determine confidence level
        confidence = 'low'
        if best_type[1] >= 80:
            confidence = 'high'
        elif best_type[1] >= 50:
            confidence = 'medium'
        
        # Generate reasons
        reasons = self._generate_reasons(series, column_name, best_type[0])
        
        return {
            'suggested_type': best_type[0],
            'confidence': confidence,
            'score': best_type[1],
            'reasons': reasons,
            'sample_data': sample_data,
            'all_scores': scores
        }
    
    def _calculate_field_score(self, series: pd.Series, column_name: str, 
                             field_type: str, patterns: Dict) -> float:
        """
        Calculate how well a column matches a field type.
        """
        score = 0
        
        # Column name matching (30% weight)
        col_name_lower = str(column_name).lower()
        name_keywords = patterns.get('keywords', [])
        name_matches = sum(1 for keyword in name_keywords if keyword in col_name_lower)
        name_score = (name_matches / len(name_keywords)) * 30 if name_keywords else 0
        score += name_score
        
        # Content pattern matching (50% weight)
        content_patterns = patterns.get('patterns', [])
        if content_patterns:
            pattern_matches = 0
            sample_size = min(len(series), 20)
            
            for i in range(sample_size):
                value = str(series.iloc[i]) if i < len(series) else ''
                for pattern in content_patterns:
                    if re.search(pattern, value, re.IGNORECASE):
                        pattern_matches += 1
                        break
            
            pattern_score = (pattern_matches / sample_size) * 50
            score += pattern_score
        
        # Data type consistency (20% weight)
        type_score = self._calculate_type_consistency(series, field_type)
        score += type_score * 20
        
        return min(score, 100)
    
    def _calculate_type_consistency(self, series: pd.Series, field_type: str) -> float:
        """
        Calculate consistency of data with expected field type.
        """
        if series.empty:
            return 0
        
        sample = series.head(20).astype(str)
        
        if field_type in ['debit', 'credit', 'amount', 'balance']:
            # Check for numeric/currency patterns
            currency_pattern = r'[₦$]?\s*[\d,]+\.?\d*\s*[₦$]?'
            numeric_count = sum(1 for val in sample if re.search(currency_pattern, val))
            return numeric_count / len(sample)
        
        elif field_type == 'date':
            # Check for date patterns
            date_patterns = [
                r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
                r'\d{1,2}\s+[A-Za-z]{3}\s+\d{4}',
                r'\d{4}-\d{2}-\d{2}'
            ]
            date_count = sum(1 for val in sample if any(re.search(pattern, val) for pattern in date_patterns))
            return date_count / len(sample)
        
        elif field_type == 'description':
            # Check for text content (longer strings)
            avg_length = sample.str.len().mean()
            return min(avg_length / 50, 1.0)  # Normalize to 0-1
        
        elif field_type == 'reference':
            # Check for reference-like patterns (mix of letters and numbers)
            ref_pattern = r'^[A-Za-z0-9]{6,20}$'
            ref_count = sum(1 for val in sample if re.match(ref_pattern, val))
            return ref_count / len(sample)
        
        return 0
    
    def _generate_reasons(self, series: pd.Series, column_name: str, field_type: str) -> List[str]:
        """
        Generate human-readable reasons for the field type suggestion.
        """
        reasons = []
        
        if field_type in ['debit', 'credit', 'amount', 'balance']:
            # Check for currency symbols
            sample = series.head(5).astype(str)
            has_currency = any('₦' in val or '$' in val or 'NGN' in val.upper() for val in sample)
            if has_currency:
                reasons.append('Contains currency symbols')
            
            # Check for numeric patterns
            numeric_pattern = r'[\d,]+\.?\d*'
            numeric_count = sum(1 for val in sample if re.search(numeric_pattern, val))
            if numeric_count > 0:
                reasons.append(f'{numeric_count}/5 sample values contain numbers')
        
        elif field_type == 'date':
            sample = series.head(5).astype(str)
            date_patterns = [
                r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
                r'\d{1,2}\s+[A-Za-z]{3}\s+\d{4}',
                r'\d{4}-\d{2}-\d{2}'
            ]
            date_count = sum(1 for val in sample if any(re.search(pattern, val) for pattern in date_patterns))
            if date_count > 0:
                reasons.append(f'{date_count}/5 sample values match date patterns')
        
        elif field_type == 'description':
            sample = series.head(5).astype(str)
            avg_length = sample.str.len().mean()
            if avg_length > 10:
                reasons.append('Contains descriptive text (average length: {:.1f} chars)'.format(avg_length))
        
        elif field_type == 'reference':
            sample = series.head(5).astype(str)
            ref_pattern = r'^[A-Za-z0-9]{6,20}$'
            ref_count = sum(1 for val in sample if re.match(ref_pattern, val))
            if ref_count > 0:
                reasons.append(f'{ref_count}/5 sample values look like transaction references')
        
        # Add column name reason
        col_name_lower = str(column_name).lower()
        field_keywords = self.FIELD_PATTERNS[field_type]['keywords']
        matching_keywords = [kw for kw in field_keywords if kw in col_name_lower]
        if matching_keywords:
            reasons.append(f'Column name contains: {", ".join(matching_keywords)}')
        
        if not reasons:
            reasons.append('Limited matching patterns detected')
        
        return reasons
    
    def apply_column_mapping(self, df: pd.DataFrame, mappings: Dict[str, str]) -> pd.DataFrame:
        """
        Apply user column mappings to create standardized dataframe.
        
        Args:
            df: Original dataframe
            mappings: Dict mapping original column names to standard field types
            
        Returns:
            Standardized dataframe
        """
        if df.empty:
            return df
        
        result_data = {}
        
        # Standard fields to create
        standard_fields = [
            'date', 'description', 'debit', 'credit', 
            'amount', 'balance', 'reference'
        ]
        
        for field in standard_fields:
            # Find which original column maps to this field
            source_columns = [col for col, field_type in mappings.items() if field_type == field]
            
            if source_columns:
                # Use the first matching column
                result_data[field] = df[source_columns[0]]
            else:
                # Create empty column
                result_data[field] = pd.Series([None] * len(df), dtype=object)
        
        # Create the standardized dataframe
        standardized_df = pd.DataFrame(result_data)
        
        return standardized_df


def analyze_merged_table(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Convenience function to analyze a merged table for column mapping.
    
    Returns:
        Analysis results for UI display
    """
    mapper = ColumnMapper()
    column_analysis = mapper.analyze_columns(df)
    
    # Create preview rows as list of lists (for easier template iteration)
    preview_df = df.head(10).fillna('')
    preview_rows = [
        [str(val) for val in row] 
        for _, row in preview_df.iterrows()
    ]
    
    return {
        'column_analysis': column_analysis,
        'table_preview': df.head(10).to_dict(orient='records'),
        'table_shape': df.shape,
        'column_names': list(df.columns),
        'preview_rows': preview_rows  # Add this for template display
    }
