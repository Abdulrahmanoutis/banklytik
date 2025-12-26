import pandas as pd
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

def clean_amount(value):
    if pd.isna(value): return 0.0
    s = str(value).replace("₦", "").replace(",", "").replace(" ", "")
    s = re.sub(r"[^0-9.\-]", "", s)
    try: return float(s)
    except Exception: return 0.0

def normalize_description(desc):
    if not isinstance(desc, str): return ""
    return re.sub(r"\s+", " ", desc.strip())

def extract_channel(description):
    d = str(description).upper()
    if "ATM" in d: return "ATM"
    if "POS" in d: return "POS"
    if "TRANSFER" in d: return "TRANSFER"
    if "AIRTIME" in d: return "AIR TIME"
    if "CHARGE" in d: return "CHARGES"
    return "OTHER"

def parse_nigerian_date(date_val):
    """
    Custom parser for Nigerian date formats that handles various spacings and formats
    """
    if pd.isna(date_val):
        return None
    
    date_str = str(date_val).strip()
    if not date_str or date_str in ['None', 'NaT', '']:
        return None
    
    # Debug: print what we're trying to parse
    print(f"DEBUG: Parsing date: '{date_str}'")
    
    # Common patterns in your data:
    # "2025 Feb 23 09:05 38"
    # "2025 Feb 09:06 04" 
    # "2025 Feb 2310:00 48"
    # "2025 Feb 23 11:11:40"
    # "23 Feb 2025" (value_date)
    # "24Feb 2025" (value_date - missing space)
    
    # Preprocessing: Replace multiple spaces with one
    date_str = re.sub(r'\s+', ' ', date_str)
    
    # Preprocessing: Ensure space between digit and letter (both ways)
    date_str = re.sub(r'(\d)([A-Za-z])', r'\1 \2', date_str)
    date_str = re.sub(r'([A-Za-z])(\d)', r'\1 \2', date_str)
    
    # Try multiple date formats
    formats = [
        # DateTime formats with seconds
        "%Y %b %d %H:%M %S",  # "2025 Feb 23 09:05 38"
        "%Y %b %d %H:%M:%S",  # "2025 Feb 23 11:11:40"  
        "%Y %b %d %H:%M",     # "2025 Feb 23 09:05"
        "%d %b %Y %H:%M %S",  # "23 Feb 2025 09:05 38"
        "%d %b %Y %H:%M:%S",  # "23 Feb 2025 11:11:40"
        "%d %b %Y %H:%M",     # "23 Feb 2025 09:05"
        
        # Date-only formats (for value_date)
        "%Y %b %d",           # "2025 Feb 23"
        "%d %b %Y",           # "23 Feb 2025"
        # REMOVED: "%b %Y" - let validation system handle incomplete dates
        
        # Handle formats with missing spaces
        "%Y%b %d %H:%M %S",   # "2025Feb 23 09:05 38"
        "%d%b %Y",            # "24Feb 2025"
    ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            # If the format doesn't include day, set to 1st of month
            if fmt in ["%b %Y"]:
                parsed = parsed.replace(day=1)
            print(f"DEBUG: Successfully parsed '{date_str}' with format '{fmt}'")
            return parsed
        except ValueError:
            continue
    
    print(f"DEBUG: Failed to parse date: '{date_str}'")
    return None

def run_deepseek_stage2_cleaning(df_raw, deepseek_stage1_result, stmt_pk=None):
    """
    Apply the column mapping from Stage 1 to clean and normalize the data
    """
    logger.info("Starting Stage 2 cleaning with column mapping")
    
    try:
        # If Stage 1 failed, use fallback
        if not deepseek_stage1_result or "tables" not in deepseek_stage1_result:
            logger.warning("Invalid Stage 1 result; using fallback cleaner")
            from .cleaning_utils import robust_clean_dataframe
            return robust_clean_dataframe(df_raw)
        
        # Start with a copy of the raw data
        df_clean = df_raw.copy()
        print(f"DEBUG: Starting Stage 2 with shape: {df_clean.shape}")
        print(f"DEBUG: Columns: {list(df_clean.columns)}")
        
        # Apply column mapping from the first table
        table_mapping = deepseek_stage1_result["tables"][0]["column_mapping"]
        original_header = deepseek_stage1_result["tables"][0]["original_header"]
        print(f"DEBUG: Applying column mapping: {table_mapping}")
        print(f"DEBUG: Original header: {original_header}")
        
        # FIX: Since we have integer columns, we need to map based on position
        # The original_header tells us what each column should be
        column_renames = {}
        
        # Map integer columns to semantic names based on the original header order
        for i, original_col_name in enumerate(original_header):
            if i < len(df_clean.columns):  # Make sure we don't exceed DataFrame columns
                if original_col_name in table_mapping:
                    new_col_name = table_mapping[original_col_name]
                    column_renames[i] = new_col_name
                    print(f"DEBUG: Mapping column {i} '{original_col_name}' -> '{new_col_name}'")
        
        df_clean = df_clean.rename(columns=column_renames)
        print(f"DEBUG: After renaming - Columns: {list(df_clean.columns)}")
        
        # Remove header rows and keep only transaction data
        # Find and remove the header row (the one that contains all the original header values)
        header_mask = pd.Series([False] * len(df_clean))
        
        for i, row in df_clean.iterrows():
            # Check if this row looks like a header row (contains all the original header terms)
            row_contains_headers = 0
            for header_term in original_header:
                if any(str(cell).lower() == header_term.lower() for cell in row if pd.notna(cell)):
                    row_contains_headers += 1
            
            # If most header terms are found in this row, it's likely the header row
            if row_contains_headers >= len(original_header) * 0.7:  # 70% match
                header_mask[i] = True
                print(f"DEBUG: Found header row at index {i}")
        
        # Also remove summary rows like "Opening Balance"
        summary_mask = df_clean.iloc[:, 0].astype(str).str.contains(
            'Opening Balance|Closing Balance', 
            case=False, na=False
        )
        
        # Combine masks - we want to KEEP rows that are NOT headers and NOT summaries
        keep_mask = ~(header_mask | summary_mask)
        df_clean = df_clean[keep_mask].reset_index(drop=True)
        print(f"DEBUG: After removing headers/summaries - Shape: {df_clean.shape}")
        
        # Clean and normalize the data
        if "description" in df_clean.columns:
            df_clean["description"] = df_clean["description"].apply(normalize_description)
            df_clean["channel"] = df_clean["description"].apply(extract_channel)
        
        # Handle debit/credit column
        if "debit_credit" in df_clean.columns:
            df_clean["debit"] = 0.0
            df_clean["credit"] = 0.0
            
            for idx, value in df_clean["debit_credit"].items():
                str_val = str(value)
                amount = clean_amount(str_val)
                
                if "-" in str_val or "dr" in str_val.lower():
                    df_clean.at[idx, "debit"] = amount
                elif "+" in str_val or "cr" in str_val.lower():
                    df_clean.at[idx, "credit"] = amount
                else:
                    df_clean.at[idx, "debit"] = amount if amount > 0 else 0.0
        
        # Clean balance column
        if "balance" in df_clean.columns:
            df_clean["balance"] = df_clean["balance"].apply(clean_amount)
        
        # Handle dates with custom parser for Nigerian formats
        date_parse_success = 0
        date_parse_failed = 0
        
        for date_col in ["date", "value_date"]:
            if date_col in df_clean.columns:
                print(f"DEBUG: Processing date column: {date_col}")
                parsed_dates = []
                for date_val in df_clean[date_col]:
                    parsed = parse_nigerian_date(date_val)
                    parsed_dates.append(parsed)
                    if parsed is not None:
                        date_parse_success += 1
                    else:
                        date_parse_failed += 1
                        print(f"DEBUG: Failed to parse date: '{date_val}'")
                
                df_clean[date_col] = parsed_dates
        
        print(f"DEBUG: Date parsing - Success: {date_parse_success}, Failed: {date_parse_failed}")
        
        # Remove rows where date failed to parse (likely invalid transactions)
        if "date" in df_clean.columns:
            valid_date_mask = ~df_clean["date"].isna()
            before_removal = len(df_clean)
            df_clean = df_clean[valid_date_mask].reset_index(drop=True)
            after_removal = len(df_clean)
            print(f"DEBUG: Removed {before_removal - after_removal} rows with invalid dates")
        
        # Ensure we have all required columns
        required_columns = ["date", "description", "debit", "credit", "balance", "channel", "transaction_reference"]
        for col in required_columns:
            if col not in df_clean.columns:
                df_clean[col] = "" if col == "description" else 0.0
        
        # Select final columns in the right order
        final_columns = [col for col in required_columns if col in df_clean.columns]
        
        print(f"DEBUG: Stage 2 cleaning completed. Final shape: {df_clean[final_columns].shape}")
        return df_clean[final_columns]
        
    except Exception as e:
        logger.error(f"Stage 2 cleaning failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Fallback to robust cleaner
        from .cleaning_utils import robust_clean_dataframe
        return robust_clean_dataframe(df_raw)
