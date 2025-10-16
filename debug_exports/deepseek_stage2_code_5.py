import pandas as pd
import numpy as np

def clean_transaction_data(df_raw):
    # Create a copy to avoid modifying the original
    df = df_raw.copy()
    
    # Define the expected columns for the final output
    expected_columns = ['date', 'value_date', 'description', 'debit', 'credit', 'balance', 'channel', 'transaction_reference']
    
    # Identify header rows and data rows
    header_mask = df.astype(str).apply(lambda row: 'Trans. Time' in row.values or 'Value Date' in row.values, axis=1)
    data_start_idx = header_mask.idxmax() if header_mask.any() else 0
    
    # Skip header and metadata rows, keep only transaction data
    df_clean = df.iloc[data_start_idx + 1:].reset_index(drop=True)
    
    # If we found a header, use it to set column names
    if header_mask.any():
        header_row = df.iloc[data_start_idx]
        df_clean.columns = [header_row[i] if i < len(header_row) else f'Unnamed_{i}' for i in range(len(df_clean.columns))]
    
    # Apply column mapping
    column_mapping = {
        'Trans. Time': 'date',
        'Value Date': 'value_date', 
        'Description': 'description',
        'Debit/Credit(W)': 'debit_credit',
        'Balance(N)': 'balance',
        'Channel': 'channel',
        'Transaction Reference': 'transaction_reference'
    }
    
    df_clean = df_clean.rename(columns=column_mapping)
    
    # Keep only mapped columns that exist in the data
    available_columns = [col for col in column_mapping.values() if col in df_clean.columns]
    df_clean = df_clean[available_columns]
    
    # Remove rows where all values are null or empty
    df_clean = df_clean.dropna(how='all')
    
    # Handle date parsing with various formats
    if 'date' in df_clean.columns:
        # Clean date column - fix common OCR issues
        df_clean['date'] = df_clean['date'].astype(str).str.replace(r'(\d{4})\s+(\w{3})\s+(\d{1,2})(\d{2}:\d{2}:\d{2})', r'\1 \2 \3 \4', regex=True)
        df_clean['date'] = df_clean['date'].str.replace(r'(\d{4})\s+(\w{3})\s+(\d{2})(\d{2}:\d{2}\s+\d{2})', r'\1 \2 \3 \4', regex=True)
        df_clean['date'] = df_clean['date'].str.replace(r'(\d{4})\s+(\w{3})\s+(\d{2})(\d{2}:\d{2}:\s+\d{2})', r'\1 \2 \3 \4', regex=True)
        df_clean['date'] = df_clean['date'].str.replace(r'\s+', ' ', regex=True)
        
        # Parse dates
        df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce', dayfirst=True)
    
    if 'value_date' in df_clean.columns:
        # Clean value_date column
        df_clean['value_date'] = df_clean['value_date'].astype(str).str.replace(r'(\d{1,2})([A-Za-z]{3})', r'\1 \2', regex=True)
        df_clean['value_date'] = df_clean['value_date'].str.replace(r'([A-Za-z]{3})(\d{4})', r'\1 \2', regex=True)
        df_clean['value_date'] = df_clean['value_date'].str.replace(r'\s+', ' ', regex=True)
        
        # Parse value dates
        df_clean['value_date'] = pd.to_datetime(df_clean['value_date'], errors='coerce', dayfirst=True)
    
    # Handle debit/credit column
    if 'debit_credit' in df_clean.columns:
        # Clean the debit_credit column
        df_clean['debit_credit'] = df_clean['debit_credit'].astype(str).str.replace('₦', '').str.replace(',', '')
        
        # Extract numeric values
        debit_credit_numeric = pd.to_numeric(df_clean['debit_credit'], errors='coerce')
        
        # Create separate debit and credit columns
        df_clean['debit'] = np.where(debit_credit_numeric < 0, abs(debit_credit_numeric), 0)
        df_clean['credit'] = np.where(debit_credit_numeric > 0, debit_credit_numeric, 0)
        
        # Drop the original debit_credit column
        df_clean = df_clean.drop('debit_credit', axis=1)
    
    # Handle balance column
    if 'balance' in df_clean.columns:
        df_clean['balance'] = df_clean['balance'].astype(str).str.replace('₦', '').str.replace(',', '')
        df_clean['balance'] = pd.to_numeric(df_clean['balance'], errors='coerce')
    
    # Ensure all expected columns exist
    for col in expected_columns:
        if col not in df_clean.columns:
            df_clean[col] = np.nan
    
    # Reorder columns to match expected output
    df_clean = df_clean[expected_columns]
    
    # Remove any rows where date is null (non-transaction rows)
    df_clean = df_clean.dropna(subset=['date'])
    
    # Reset index
    df_clean = df_clean.reset_index(drop=True)
    
    return df_clean