import pandas as pd
import re
from datetime import datetime

def parse_nigerian_date(date_val):
    """Parse Nigerian date formats"""
    # Handle Series/DataFrame input properly
    if hasattr(date_val, 'empty'):
        if date_val.empty:
            return None
        # If it's a Series, apply function to each element
        return date_val.apply(parse_nigerian_date)
    
    if pd.isna(date_val) or not date_val:
        return None
    
    date_str = str(date_val).strip()
    if not date_str or date_str in ['None', 'NaT', '']:
        return None
    
    # Fix common OCR issues
    date_str = re.sub(r'\.', ' ', date_str)  # Replace dots with spaces
    date_str = re.sub(r':\s*:', ':', date_str)  # Fix double colons
    date_str = re.sub(r'(\d{2}):\s+(\d{2})', r'\1:\2', date_str)  # Remove spaces in time
    date_str = re.sub(r'(\d{4}\s+[A-Za-z]+\s+)(\d{2})(\d{2}:\d{2})', r'\1\2 \3', date_str)
    date_str = re.sub(r':$', '', date_str)  # Remove trailing colons
    date_str = re.sub(r'\s+', ' ', date_str).strip()
    
    # Try multiple date formats
    formats = [
        "%Y %b %d %H:%M:%S",  # "2025 Feb 23 08:05:38"
        "%Y %b %d %H:%M %S",  # "2025 Feb 23 08:05 38"  
        "%Y %b %d %H:%M",     # "2025 Feb 23 08:05"
        "%d %b %Y %H:%M %S",  # "23 Feb 2025 09:05 38"
        "%d %b %Y %H:%M:%S",  # "23 Feb 2025 11:11:40"
        "%d %b %Y %H:%M",     # "23 Feb 2025 09:05"
        "%Y %b %d",           # "2025 Feb 23"
        "%d %b %Y",           # "23 Feb 2025"
        "%b %Y",              # "Feb 2025" - set day to 1
    ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            # If the format doesn't include day, set to 1st of month
            if fmt in ["%b %Y"]:
                parsed = parsed.replace(day=1)
            return parsed
        except ValueError:
            continue
    
    return None

def clean_amount(value):
    """Clean amount values"""
    if pd.isna(value): 
        return 0.0
    s = str(value).replace("₦", "").replace(",", "").replace(" ", "")
    s = re.sub(r"[^0-9.\-]", "", s)
    try: 
        return float(s)
    except Exception: 
        return 0.0

def extract_channel(description):
    """Extract channel from description"""
    if pd.isna(description) or not description:
        return "OTHER"
        
    d = str(description).upper()
    if "ATM" in d: return "ATM"
    if "POS" in d: return "POS"
    if "TRANSFER" in d: return "TRANSFER"
    if "AIRTIME" in d: return "AIR TIME"
    if "CHARGE" in d or "USSD" in d: return "CHARGES"
    return "OTHER"

def is_summary_table(headers):
    """Check if this is a summary table (not transactions)"""
    header_str = ' '.join(str(h).lower() for h in headers)
    summary_indicators = ['opening balance', 'closing balance', 'date printed', 'start date', 'end date']
    return any(indicator in header_str for indicator in summary_indicators)

def is_transaction_table(headers):
    """Check if this is a transaction table"""
    header_str = ' '.join(str(h).lower() for h in headers)
    transaction_indicators = ['trans. time', 'value date', 'description', 'debit/credit', 'balance', 'channel', 'transaction reference']
    return any(indicator in header_str for indicator in transaction_indicators)

def process_tables_directly(tables):
    """
    Process tables directly using known structure - should extract all 33 transactions
    """
    all_transactions = []
    
    for table in tables:
        df = table['df']
        
        # Skip tables that are too small
        if df.empty or len(df) < 2:
            continue
            
        # Get headers (first row)
        headers = df.iloc[0].tolist()
        
        # Skip summary tables (like Table 0)
        if is_summary_table(headers):
            print(f"DEBUG: Skipping summary table {table['table_id']}")
            continue
            
        # Check if this is a proper transaction table
        if is_transaction_table(headers):
            print(f"DEBUG: Processing transaction table {table['table_id']} with {len(df)-1} rows")
            
            # Use first row as header, rest as data
            df_table = df.iloc[1:].copy()
            df_table.columns = headers
            
            # Clean the data
            cleaned_df = clean_transaction_dataframe(df_table)
            if not cleaned_df.empty:
                all_transactions.append(cleaned_df)
                print(f"DEBUG: Table {table['table_id']} contributed {len(cleaned_df)} transactions")
        else:
            # Check if this might be a transaction table without headers (like Table 2)
            # Look at the data pattern to determine if it's transactions
            if len(df.columns) >= 5:  # Transaction tables usually have multiple columns
                # Check if first data row looks like a transaction
                first_data_row = df.iloc[0].tolist() if len(df) > 0 else []
                first_row_str = ' '.join(str(cell) for cell in first_data_row).lower()
                
                # If it has date patterns and amounts, it's likely transactions
                has_date = any(keyword in first_row_str for keyword in ['2025', 'feb', 'jan', 'mar', 'apr'])
                has_amount = any('₦' in str(cell) or '.' in str(cell) for cell in first_data_row)
                
                if has_date and has_amount:
                    print(f"DEBUG: Table {table['table_id']} appears to be transaction data without headers")
                    print(f"DEBUG: Using known column structure for transactions")
                    
                    # Use known column structure for transaction tables
                    known_headers = ['Trans. Time', 'Value Date', 'Description', 'Debit/Credit(W)', 'Balance(N)', 'Channel', 'Transaction Reference']
                    df_table = df.copy()
                    
                    # If we have fewer columns, adjust
                    if len(df_table.columns) < len(known_headers):
                        known_headers = known_headers[:len(df_table.columns)]
                    
                    df_table.columns = known_headers[:len(df_table.columns)]
                    cleaned_df = clean_transaction_dataframe(df_table)
                    if not cleaned_df.empty:
                        all_transactions.append(cleaned_df)
                        print(f"DEBUG: Table {table['table_id']} contributed {len(cleaned_df)} transactions")
    
    if all_transactions:
        final_df = pd.concat(all_transactions, ignore_index=True)
        print(f"DEBUG: Combined {len(all_transactions)} tables into {len(final_df)} transactions")
        return final_df
    else:
        print("DEBUG: No transaction tables found")
        return pd.DataFrame()

def clean_transaction_dataframe(df):
    """Clean transaction DataFrame using the structure we know from CSV"""
    if df.empty:
        return df
        
    # Create a copy to avoid SettingWithCopyWarning
    df_clean = df.copy()
    
    # Debug: show what we're working with
    print(f"DEBUG: Cleaning DataFrame with columns: {list(df_clean.columns)}")
    print(f"DEBUG: First few rows:")
    print(df_clean.head(2))
    
    try:
        # Rename columns to standard names
        column_mapping = {}
        for col in df_clean.columns:
            col_str = str(col)
            col_lower = col_str.lower()
            if 'trans. time' in col_lower or ('date' in col_lower and 'value' not in col_lower):
                column_mapping[col] = 'date'
            elif 'value date' in col_lower:
                column_mapping[col] = 'value_date'
            elif 'description' in col_lower:
                column_mapping[col] = 'description'
            elif 'debit/credit' in col_lower:
                column_mapping[col] = 'debit_credit'
            elif 'balance' in col_lower:
                column_mapping[col] = 'balance'
            elif 'channel' in col_lower:
                column_mapping[col] = 'channel'
            elif 'transaction reference' in col_lower:
                column_mapping[col] = 'transaction_reference'
        
        df_clean = df_clean.rename(columns=column_mapping)
        print(f"DEBUG: After renaming - Columns: {list(df_clean.columns)}")
        
        # Parse dates - handle Series properly
        if 'date' in df_clean.columns:
            df_clean['date'] = df_clean['date'].apply(lambda x: parse_nigerian_date(x) if not pd.isna(x) else None)
        
        if 'value_date' in df_clean.columns:
            df_clean['value_date'] = df_clean['value_date'].apply(lambda x: parse_nigerian_date(x) if not pd.isna(x) else None)
        
        # Handle debit/credit column
        if 'debit_credit' in df_clean.columns:
            df_clean['debit'] = 0.0
            df_clean['credit'] = 0.0
            
            for idx, value in df_clean['debit_credit'].items():
                if pd.isna(value):
                    continue
                str_val = str(value)
                amount = clean_amount(str_val)
                
                if '-' in str_val:
                    df_clean.at[idx, 'debit'] = amount
                elif '+' in str_val:
                    df_clean.at[idx, 'credit'] = amount
        
        # Clean balance
        if 'balance' in df_clean.columns:
            df_clean['balance'] = df_clean['balance'].apply(clean_amount)
        
        # Extract channel if not present
        if 'channel' not in df_clean.columns and 'description' in df_clean.columns:
            df_clean['channel'] = df_clean['description'].apply(extract_channel)
        
        # Ensure we have all required columns
        required_columns = ['date', 'description', 'debit', 'credit', 'balance', 'channel', 'transaction_reference']
        for col in required_columns:
            if col not in df_clean.columns:
                df_clean[col] = "" if col == 'description' else 0.0
        
        # Remove rows with invalid dates
        if 'date' in df_clean.columns:
            before_removal = len(df_clean)
            df_clean = df_clean[df_clean['date'].notna()]
            after_removal = len(df_clean)
            removed_count = before_removal - after_removal
            if removed_count > 0:
                print(f"DEBUG: Removed {removed_count} rows with invalid dates")
        
        # Select final columns
        final_columns = [col for col in required_columns if col in df_clean.columns]
        
        print(f"DEBUG: Cleaned DataFrame shape: {df_clean[final_columns].shape}")
        return df_clean[final_columns]
        
    except Exception as e:
        print(f"DEBUG: Error in clean_transaction_dataframe: {e}")
        import traceback
        print(f"DEBUG: Traceback: {traceback.format_exc()}")
        return pd.DataFrame()