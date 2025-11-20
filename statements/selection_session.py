"""
Session management for manual table selection workflow.

This module handles the state management for the multi-step table selection process.
"""

import json
import uuid
from typing import Dict, List, Any, Optional
from django.core.cache import cache
from django.conf import settings


class TableSelectionSession:
    """
    Manages the state of a table selection session.
    """
    
    def __init__(self, statement_id: int, user_id: int):
        self.session_id = f"table_selection_{statement_id}_{user_id}"
        self.statement_id = statement_id
        self.user_id = user_id
        self.state = {
            'step': 'table_selection',  # table_selection, column_mapping, preview, complete
            'extracted_tables': [],
            'selected_table_ids': [],
            'column_mappings': {},
            'merged_data': None,
            'final_dataframe': None,
            'processing_complete': False,
            'error_message': None
        }
    
    def save(self):
        """Save session state to cache."""
        cache.set(self.session_id, json.dumps(self.state), timeout=3600)  # 1 hour timeout
    
    def load(self) -> bool:
        """Load session state from cache."""
        cached = cache.get(self.session_id)
        if cached:
            self.state = json.loads(cached)
            return True
        return False
    
    def clear(self):
        """Clear session state."""
        cache.delete(self.session_id)
    
    def set_extracted_tables(self, tables: List[Dict[str, Any]]):
        """Store extracted tables in session (without DataFrames)."""
        # Store only metadata, not the actual DataFrames
        serializable_tables = []
        for table in tables:
            serializable_table = {
                'table_id': table.get('table_id'),
                'page': table.get('page'),
                'score': table.get('score', 0),
                'confidence': table.get('confidence', 'low'),
                'reasons': table.get('reasons', []),
                'row_count': table.get('row_count', 0),
                'column_count': table.get('column_count', 0),
                'score_breakdown': table.get('score_breakdown', {}),
                'preview_data': table.get('preview_data', [])
            }
            serializable_tables.append(serializable_table)
        
        self.state['extracted_tables'] = serializable_tables
        self.save()
    
    def get_extracted_tables(self) -> List[Dict[str, Any]]:
        """Get extracted tables from session."""
        return self.state.get('extracted_tables', [])
    
    def set_selected_tables(self, table_ids: List[int]):
        """Store user-selected table IDs."""
        self.state['selected_table_ids'] = table_ids
        self.state['step'] = 'column_mapping'
        self.save()
    
    def get_selected_tables(self) -> List[Dict[str, Any]]:
        """Get the actual table data for selected tables."""
        # Note: This method needs to be called in a context where the actual table data
        # (with DataFrames) is available, not from the serialized session
        selected_ids = self.state.get('selected_table_ids', [])
        all_tables = self.state.get('extracted_tables', [])
        return [t for t in all_tables if t.get('table_id') in selected_ids]
    
    def set_column_mappings(self, mappings: Dict[str, str]):
        """Store column mapping configuration."""
        self.state['column_mappings'] = mappings
        self.state['step'] = 'preview'
        self.save()
    
    def get_column_mappings(self) -> Dict[str, str]:
        """Get column mapping configuration."""
        return self.state.get('column_mappings', {})
    
    def set_merged_data(self, merged_df: Any):
        """Store merged dataframe."""
        # Convert DataFrame to JSON-serializable format
        if merged_df is not None:
            self.state['merged_data'] = merged_df.to_dict(orient='records')
        else:
            self.state['merged_data'] = None
        self.save()
    
    def get_merged_data(self) -> Any:
        """Get merged dataframe."""
        import pandas as pd
        merged_data = self.state.get('merged_data')
        if merged_data:
            return pd.DataFrame(merged_data)
        return None
    
    def set_final_dataframe(self, final_df: Any):
        """Store final processed dataframe."""
        if final_df is not None:
            self.state['final_dataframe'] = final_df.to_dict(orient='records')
        else:
            self.state['final_dataframe'] = None
        self.state['step'] = 'complete'
        self.save()
    
    def get_final_dataframe(self) -> Any:
        """Get final processed dataframe."""
        import pandas as pd
        final_data = self.state.get('final_dataframe')
        if final_data:
            return pd.DataFrame(final_data)
        return None
    
    def set_error(self, error_message: str):
        """Store error message."""
        self.state['error_message'] = error_message
        self.save()
    
    def get_error(self) -> Optional[str]:
        """Get error message."""
        return self.state.get('error_message')
    
    def get_current_step(self) -> str:
        """Get current step in the workflow."""
        return self.state.get('step', 'table_selection')
    
    def is_complete(self) -> bool:
        """Check if processing is complete."""
        return self.state.get('processing_complete', False)


def get_session(statement_id: int, user_id: int) -> TableSelectionSession:
    """
    Get or create a table selection session.
    
    Args:
        statement_id: BankStatement ID
        user_id: User ID
        
    Returns:
        TableSelectionSession instance
    """
    session = TableSelectionSession(statement_id, user_id)
    session.load()  # Try to load existing session
    return session


def clear_session(statement_id: int, user_id: int):
    """Clear a table selection session."""
    session = TableSelectionSession(statement_id, user_id)
    session.clear()
