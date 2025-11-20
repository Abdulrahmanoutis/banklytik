"""
Table scoring system for Banklytik manual table selection.

This module analyzes extracted tables and scores them based on their likelihood
of containing transaction data.
"""

import pandas as pd
import re
from typing import Dict, List, Any
from datetime import datetime


class TableScorer:
    """
    Scores tables based on transaction likelihood patterns.
    """
    
    # Transaction-related keywords
    TRANSACTION_KEYWORDS = [
        'transfer', 'pos', 'atm', 'airtime', 'payment', 'deposit',
        'withdrawal', 'charge', 'fee', 'bill', 'purchase', 'debit',
        'credit', 'balance', 'transaction', 'amount', 'date', 'description'
    ]
    
    # Currency patterns
    CURRENCY_PATTERNS = [
        r'₦\s*[\d,]+\.?\d*',  # Naira symbol
        r'[\d,]+\.?\d*\s*₦',  # Naira after amount
        r'NGN\s*[\d,]+\.?\d*',  # NGN currency code
        r'[\d,]+\.?\d*\s*NGN',  # NGN after amount
        r'\$[\d,]+\.?\d*',  # Dollar symbol
        r'[\d,]+\.?\d*',    # Plain numbers (potential amounts)
    ]
    
    # Date patterns
    DATE_PATTERNS = [
        r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # DD/MM/YYYY, MM/DD/YYYY
        r'\d{1,2}\s+[A-Za-z]{3}\s+\d{4}',  # 01 Jan 2024
        r'[A-Za-z]{3}\s+\d{1,2}\s+\d{4}',  # Jan 01 2024
        r'\d{4}-\d{2}-\d{2}',              # YYYY-MM-DD
        r'\d{1,2}:\d{2}',                  # Time patterns
    ]
    
    def __init__(self):
        self.patterns_compiled = {
            'currency': [re.compile(pattern, re.IGNORECASE) for pattern in self.CURRENCY_PATTERNS],
            'date': [re.compile(pattern, re.IGNORECASE) for pattern in self.DATE_PATTERNS]
        }
    
    def score_table(self, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Score a table for transaction likelihood.
        
        Returns:
            Dict with score (0-100) and detailed breakdown
        """
        df = table_data.get('df')
        if df is None or df.empty:
            return {
                'score': 0,
                'confidence': 'low',
                'reasons': ['Empty table'],
                'row_count': 0,
                'column_count': 0
            }
        
        score_components = {}
        
        # Basic table characteristics
        row_count = len(df)
        col_count = len(df.columns)
        score_components['row_count'] = min(row_count / 50 * 10, 10)  # Max 10 points for reasonable row count
        score_components['column_count'] = min(col_count / 10 * 10, 10)  # Max 10 points for reasonable column count
        
        # Content analysis
        all_text = ' '.join(df.astype(str).values.flatten()).lower()
        
        # Keyword matching
        keyword_score = 0
        found_keywords = []
        for keyword in self.TRANSACTION_KEYWORDS:
            if keyword in all_text:
                keyword_score += 2  # 2 points per keyword
                found_keywords.append(keyword)
        score_components['keywords'] = min(keyword_score, 20)  # Max 20 points
        
        # Currency pattern detection
        currency_score = 0
        currency_matches = []
        for pattern in self.patterns_compiled['currency']:
            matches = pattern.findall(all_text)
            if matches:
                currency_score += len(matches) * 1  # 1 point per currency match
                currency_matches.extend(matches[:3])  # Keep first 3 examples
        score_components['currency'] = min(currency_score, 20)  # Max 20 points
        
        # Date pattern detection
        date_score = 0
        date_matches = []
        for pattern in self.patterns_compiled['date']:
            matches = pattern.findall(all_text)
            if matches:
                date_score += len(matches) * 1.5  # 1.5 points per date match
                date_matches.extend(matches[:3])  # Keep first 3 examples
        score_components['dates'] = min(date_score, 15)  # Max 15 points
        
        # Data consistency (check if rows have similar structure)
        consistency_score = self._calculate_consistency_score(df)
        score_components['consistency'] = consistency_score
        
        # Header detection
        header_score = self._detect_header_row(df)
        score_components['headers'] = header_score
        
        # Calculate total score (0-100)
        total_score = sum(score_components.values())
        
        # Determine confidence level
        if total_score >= 70:
            confidence = 'high'
        elif total_score >= 40:
            confidence = 'medium'
        else:
            confidence = 'low'
        
        # Prepare reasons for scoring
        reasons = []
        if found_keywords:
            reasons.append(f"Contains keywords: {', '.join(set(found_keywords))}")
        if currency_matches:
            reasons.append(f"Currency patterns found: {', '.join(set(currency_matches[:3]))}")
        if date_matches:
            reasons.append(f"Date patterns found: {', '.join(set(date_matches[:3]))}")
        if consistency_score > 5:
            reasons.append("Consistent row structure")
        if header_score > 5:
            reasons.append("Header row detected")
        
        if not reasons:
            reasons = ["Limited transaction indicators found"]
        
        return {
            'score': min(total_score, 100),
            'confidence': confidence,
            'reasons': reasons,
            'row_count': row_count,
            'column_count': col_count,
            'score_breakdown': score_components,
            'preview_data': self._get_table_preview(df)
        }
    
    def _calculate_consistency_score(self, df: pd.DataFrame) -> float:
        """Calculate how consistent the table structure is."""
        if len(df) < 2:
            return 0
        
        # Check if rows have similar number of non-empty cells
        non_empty_counts = []
        for _, row in df.iterrows():
            non_empty = sum(1 for cell in row if str(cell).strip() != '')
            non_empty_counts.append(non_empty)
        
        if len(set(non_empty_counts)) == 1:
            return 10  # Perfect consistency
        elif max(non_empty_counts) - min(non_empty_counts) <= 2:
            return 7   # Good consistency
        elif max(non_empty_counts) - min(non_empty_counts) <= 4:
            return 4   # Fair consistency
        else:
            return 1   # Poor consistency
    
    def _detect_header_row(self, df: pd.DataFrame) -> float:
        """Detect if the table has a header row."""
        if len(df) == 0:
            return 0
        
        first_row = df.iloc[0].astype(str).str.lower().tolist()
        first_row_text = ' '.join(first_row)
        
        header_indicators = [
            'date', 'time', 'description', 'desc', 'particulars',
            'debit', 'credit', 'amount', 'balance', 'transaction',
            'reference', 'ref', 'channel', 'type'
        ]
        
        matches = sum(1 for indicator in header_indicators if indicator in first_row_text)
        
        if matches >= 3:
            return 15  # Strong header
        elif matches >= 2:
            return 10  # Good header
        elif matches >= 1:
            return 5   # Weak header
        else:
            return 0   # No header
    
    def _get_table_preview(self, df: pd.DataFrame, max_rows: int = 5) -> List[List[str]]:
        """Get preview data for the table."""
        preview = []
        for i in range(min(len(df), max_rows)):
            row = []
            for cell in df.iloc[i]:
                cell_str = str(cell).strip()
                # Truncate very long cells
                if len(cell_str) > 50:
                    cell_str = cell_str[:47] + '...'
                row.append(cell_str)
            preview.append(row)
        return preview


def score_all_tables(tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Score all extracted tables and return sorted by transaction likelihood.
    
    Args:
        tables: List of table data from extract_all_tables
        
    Returns:
        List of tables with scoring information, sorted by score (descending)
    """
    scorer = TableScorer()
    scored_tables = []
    
    for table in tables:
        score_result = scorer.score_table(table)
        scored_table = {**table, **score_result}
        scored_tables.append(scored_table)
    
    # Sort by score descending
    scored_tables.sort(key=lambda x: x['score'], reverse=True)
    
    return scored_tables
