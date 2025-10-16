import pandas as pd
import json
import os
from django.conf import settings

def save_complete_tables_for_deepseek(tables, stmt_pk):
    """
    Save complete extracted tables as JSON for DeepSeek analysis
    """
    debug_dir = os.path.join(settings.BASE_DIR, "debug_exports")
    os.makedirs(debug_dir, exist_ok=True)
    
    # Create a comprehensive payload with ALL table data
    complete_payload = {"tables": []}
    
    for table in tables:
        df = table['df']
        
        # Convert entire DataFrame to list format
        table_data = {
            "table_id": table['table_id'],
            "page": table['page'],
            "headers": df.iloc[0].tolist() if not df.empty else [],
            "all_rows": df.fillna('').values.tolist(),  # ALL rows, not just samples
            "total_rows": len(df),
            "shape": f"{len(df)}x{len(df.columns)}",
            "columns": df.columns.tolist() if not df.empty else []
        }
        
        complete_payload["tables"].append(table_data)
    
    # Save the complete payload
    file_path = os.path.join(debug_dir, f"complete_tables_{stmt_pk}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(complete_payload, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"DEBUG: Saved complete tables to {file_path}")
    print(f"DEBUG: Total tables: {len(complete_payload['tables'])}")
    
    return complete_payload

def tables_to_deepseek_payload(tables, max_sample_rows=5):
    """
    Convert extracted tables to a compact payload for DeepSeek analysis
    """
    payload = {"tables": []}
    
    for table in tables:
        df = table['df']
        
        # Skip empty tables
        if df.empty or len(df) < 2:
            continue
            
        # Get headers (first row)
        headers = df.iloc[0].tolist()
        
        # Skip tables that don't look like transaction tables
        header_str = ' '.join(str(h) for h in headers).lower()
        if not any(keyword in header_str for keyword in ['trans', 'date', 'description', 'debit', 'credit', 'balance']):
            print(f"DEBUG: Skipping non-transaction table with headers: {headers}")
            continue
            
        # Get sample rows (skip header row)
        sample_data = []
        start_row = 1
        
        for i in range(start_row, min(start_row + max_sample_rows, len(df))):
            sample_data.append(df.iloc[i].fillna('').tolist())
        
        table_payload = {
            "table_id": table['table_id'],
            "page": table['page'],
            "headers": headers,
            "sample_rows": sample_data,
            "total_rows": len(df) - start_row,
            "shape": f"{len(df)}x{len(df.columns)}"
        }
        
        payload["tables"].append(table_payload)
        print(f"DEBUG: Added table {table['table_id']} to payload")
    
    print(f"DEBUG: Created payload with {len(payload['tables'])} transaction tables")
    return payload

def save_payload_for_debug(payload, stmt_pk, debug_dir):
    """Save payload for debugging"""
    os.makedirs(debug_dir, exist_ok=True)
    
    with open(os.path.join(debug_dir, f"table_payload_{stmt_pk}.json"), "w") as f:
        json.dump(payload, f, indent=2, default=str)