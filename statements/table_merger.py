"""
Table merging system for Banklytik manual table selection.

This module handles merging multiple selected tables into a unified dataset.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import re


class TableMerger:
    """
    Merges multiple tables into a unified dataset.
    """
    
    def __init__(self):
        self.column_aliases = {
            'date': ['date', 'time', 'datetime', 'transaction date', 'trans date'],
            'description': ['description', 'desc', 'particulars', 'details', 'narration'],
            'debit': ['debit', 'withdrawal', 'dr', 'debit amount'],
            'credit': ['credit', 'deposit', 'cr', 'credit amount'],
            'amount': ['amount', 'transaction amount', 'value'],
            'balance': ['balance', 'running balance', 'available balance'],
            'reference': ['reference', 'ref', 'transaction ref', 'trn ref']
        }
    
    def merge_tables(self, tables: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
        """
        Merge multiple tables into a single DataFrame.
        
        Args:
            tables: List of selected table data
            
        Returns:
            Merged DataFrame or None if merging fails
        """
        if not tables:
            return None
        
        print("\n" + "="*80)
        print("🔀 TABLE MERGE DEBUG LOG")
        print("="*80)
        print(f"📊 Merging {len(tables)} table(s)...")
        
        # If only one table, return it directly
        if len(tables) == 1:
            df = tables[0]['df'].copy()
            print(f"✅ Single table: {df.shape[0]} rows × {df.shape[1]} columns")
            print(f"   Columns: {list(df.columns)}")
            return df
        
        # Try to align columns across tables
        aligned_tables = []
        for idx, table in enumerate(tables):
            df = table['df'].copy()
            print(f"\n📄 Table {idx + 1} (before processing):")
            print(f"   Shape: {df.shape[0]} rows × {df.shape[1]} columns")
            print(f"   Columns: {list(df.columns)}")
            print(f"   First 3 rows:\n{df.head(3)}")
            
            # Standardize column names
            df = self._standardize_column_names(df)
            print(f"\n   After standardizing columns:")
            print(f"   Columns: {list(df.columns)}")
            
            # Remove empty rows and columns
            before_clean = len(df)
            df = self._clean_dataframe(df)
            after_clean = len(df)
            
            print(f"   After cleaning: {df.shape[0]} rows (removed {before_clean - after_clean} rows)")
            if not df.empty:
                print(f"   Sample data:\n{df.head(3)}")
                aligned_tables.append(df)
            else:
                print(f"   ❌ Table is empty after cleaning!")
        
        if not aligned_tables:
            print("\n❌ No valid tables after processing!")
            return None
        
        print(f"\n✅ Valid tables for merging: {len(aligned_tables)}")
        
        # Try different merging strategies
        merged_df = self._try_vertical_merge(aligned_tables)
        if merged_df is not None and not merged_df.empty:
            print(f"\n✅ Merge successful!")
            print(f"   Final shape: {merged_df.shape[0]} rows × {merged_df.shape[1]} columns")
            print(f"   All merged data:\n{merged_df}")
            print("="*80 + "\n")
            return merged_df
        
        # Fallback: return the largest table
        largest_table = max(aligned_tables, key=lambda x: len(x))
        print(f"\n⚠️ Smart merge failed, returning largest table: {largest_table.shape[0]} rows")
        print("="*80 + "\n")
        return largest_table
    
    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names across tables."""
        if df.empty:
            return df
        
        # Create a mapping from current column names to standardized names
        column_mapping = {}
        for i, col_name in enumerate(df.columns):
            col_lower = str(col_name).lower().strip()
            
            # Try to match with known column types
            matched_type = None
            for col_type, aliases in self.column_aliases.items():
                if any(alias in col_lower for alias in aliases):
                    matched_type = col_type
                    break
            
            if matched_type:
                column_mapping[col_name] = matched_type
            else:
                # Use generic column name
                column_mapping[col_name] = f'col_{i}'
        
        # Apply mapping
        df = df.rename(columns=column_mapping)
        return df
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove empty rows and columns."""
        if df.empty:
            return df
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Remove completely empty columns
        df = df.dropna(axis=1, how='all')
        
        # Remove rows where all values are empty strings
        mask = df.astype(str).apply(lambda x: x.str.strip() == '').all(axis=1)
        df = df[~mask]
        
        return df
    
    def _try_vertical_merge(self, tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Try to merge tables vertically (stacking rows)."""
        if not tables:
            return None
        
        # Find common columns across all tables
        all_columns = []
        for table in tables:
            all_columns.extend(table.columns.tolist())
        
        common_columns = []
        for col in set(all_columns):
            if all(col in table.columns for table in tables):
                common_columns.append(col)
        
        if not common_columns:
            # No common columns, try to find partial matches
            return self._try_smart_vertical_merge(tables)
        
        # Merge using common columns
        merged_df = pd.concat(
            [table[common_columns] for table in tables],
            ignore_index=True
        )
        
        # Remove duplicate header rows
        merged_df = self._remove_header_rows(merged_df)
        
        return merged_df
    
    def _try_smart_vertical_merge(self, tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Smart vertical merge when tables don't have exact column matches."""
        if len(tables) < 2:
            return tables[0] if tables else None
        
        # Try to align columns by content similarity
        base_table = tables[0].copy()
        
        for i in range(1, len(tables)):
            current_table = tables[i].copy()
            
            # Find best column matches
            column_matches = self._find_column_matches(base_table, current_table)
            
            # Align current table to base table structure
            aligned_table = self._align_table_structure(current_table, base_table.columns, column_matches)
            
            if aligned_table is not None:
                base_table = pd.concat([base_table, aligned_table], ignore_index=True)
        
        return base_table
    
    def _find_column_matches(self, table1: pd.DataFrame, table2: pd.DataFrame) -> Dict[str, str]:
        """Find matching columns between two tables."""
        matches = {}
        
        for col1 in table1.columns:
            col1_type = self._infer_column_type(table1[col1])
            
            best_match = None
            best_score = 0
            
            for col2 in table2.columns:
                col2_type = self._infer_column_type(table2[col2])
                
                # Type compatibility score
                type_score = 1.0 if col1_type == col2_type else 0.0
                
                # Name similarity score
                name_score = self._calculate_name_similarity(col1, col2)
                
                # Content pattern score
                content_score = self._calculate_content_similarity(table1[col1], table2[col2])
                
                total_score = (type_score * 0.4) + (name_score * 0.3) + (content_score * 0.3)
                
                if total_score > best_score and total_score > 0.5:
                    best_score = total_score
                    best_match = col2
            
            if best_match:
                matches[col1] = best_match
        
        return matches
    
    def _infer_column_type(self, series: pd.Series) -> str:
        """Infer the type of data in a column."""
        if series.empty:
            return 'unknown'
        
        sample = series.head(20).astype(str)
        
        # Check for dates
        date_patterns = [
            r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
            r'\d{1,2}\s+[A-Za-z]{3}\s+\d{4}',
            r'\d{4}-\d{2}-\d{2}'
        ]
        
        date_count = sum(1 for val in sample if any(re.match(pattern, str(val)) for pattern in date_patterns))
        if date_count > len(sample) * 0.3:
            return 'date'
        
        # Check for amounts/currency
        currency_patterns = [
            r'₦\s*[\d,]+\.?\d*',
            r'[\d,]+\.?\d*\s*₦',
            r'[\d,]+\.?\d*'
        ]
        
        currency_count = sum(1 for val in sample if any(re.search(pattern, str(val)) for pattern in currency_patterns))
        if currency_count > len(sample) * 0.3:
            return 'amount'
        
        # Check for text/descriptions
        avg_length = sample.str.len().mean()
        if avg_length > 10:
            return 'text'
        
        return 'unknown'
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two column names."""
        name1 = str(name1).lower()
        name2 = str(name2).lower()
        
        if name1 == name2:
            return 1.0
        
        # Check if one contains the other
        if name1 in name2 or name2 in name1:
            return 0.8
        
        # Check for common words
        words1 = set(name1.split())
        words2 = set(name2.split())
        
        if words1 & words2:
            return 0.6
        
        return 0.0
    
    def _calculate_content_similarity(self, series1: pd.Series, series2: pd.Series) -> float:
        """Calculate similarity between column content patterns."""
        if series1.empty or series2.empty:
            return 0.0
        
        sample1 = series1.head(10).astype(str)
        sample2 = series2.head(10).astype(str)
        
        # Compare data types and patterns
        type1 = self._infer_column_type(series1)
        type2 = self._infer_column_type(series2)
        
        if type1 != type2:
            return 0.0
        
        # For same types, return moderate similarity
        return 0.7
    
    def _align_table_structure(self, table: pd.DataFrame, target_columns: List[str], 
                             column_matches: Dict[str, str]) -> Optional[pd.DataFrame]:
        """Align a table to match target column structure."""
        aligned_data = {}
        
        for target_col in target_columns:
            if target_col in column_matches:
                # Use matched column
                source_col = column_matches[target_col]
                aligned_data[target_col] = table[source_col]
            else:
                # Add empty column
                aligned_data[target_col] = pd.Series([None] * len(table), dtype=object)
        
        try:
            return pd.DataFrame(aligned_data)
        except Exception:
            return None
    
    def _remove_header_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate header rows from merged data."""
        if df.empty:
            return df
        
        # List of common header keywords
        header_keywords = [
            'date', 'time', 'datetime', 'trans',
            'description', 'particulars', 'details', 'narration',
            'debit', 'credit', 'amount', 'money',
            'balance', 'reference', 'channel',
            'category', 'to / from', 'from/to'
        ]
        
        # Mark rows that look like headers
        rows_to_keep = []
        for idx, row in df.iterrows():
            row_str = ' '.join(str(val).lower().strip() for val in row if val)
            
            # Count how many header keywords appear in this row
            keyword_count = sum(1 for keyword in header_keywords if keyword in row_str)
            
            # If more than 30% of the keywords are found, it's likely a header
            if keyword_count < len(header_keywords) * 0.3:
                rows_to_keep.append(idx)
        
        if rows_to_keep:
            df = df.iloc[rows_to_keep].reset_index(drop=True)
        
        return df


def merge_selected_tables(tables: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    """
    Convenience function to merge selected tables.
    
    Args:
        tables: List of selected table data
        
    Returns:
        Merged DataFrame or None if merging fails
    """
    merger = TableMerger()
    return merger.merge_tables(tables)
