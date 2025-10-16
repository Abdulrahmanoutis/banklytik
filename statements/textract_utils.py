import boto3
import time
import json
import pandas as pd
from django.conf import settings

# Textract client setup
def get_textract_client():
    return boto3.client(
        "textract",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
    )

# Start Textract job
def start_textract_job(s3_key):
    textract = get_textract_client()
    response = textract.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": settings.AWS_S3_BUCKET, "Name": s3_key}},
        FeatureTypes=["TABLES"],
    )
    return response["JobId"]

# Wait for job completion
def wait_for_job(job_id, poll_interval=5):
    textract = get_textract_client()
    while True:
        response = textract.get_document_analysis(JobId=job_id)
        status = response["JobStatus"]
        if status in ["SUCCEEDED", "FAILED"]:
            return status
        time.sleep(poll_interval)

# Get all blocks from Textract
def get_all_blocks(job_id):
    textract = get_textract_client()
    blocks = []
    next_token = None
    while True:
        if next_token:
            response = textract.get_document_analysis(
                JobId=job_id, NextToken=next_token
            )
        else:
            response = textract.get_document_analysis(JobId=job_id)
        blocks.extend(response.get("Blocks", []))
        next_token = response.get("NextToken")
        if not next_token:
            break
    return blocks

# Helper function to extract text from a cell
def extract_text_from_cell(cell_block, all_blocks_dict):
    """Extract actual text content from a Textract cell block"""
    text_parts = []
    
    # Get child relationships
    if 'Relationships' in cell_block:
        for relationship in cell_block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = all_blocks_dict.get(child_id)
                    if child_block and child_block.get('BlockType') in ['WORD', 'LINE']:
                        text_parts.append(child_block.get('Text', ''))
    
    return ' '.join(text_parts) if text_parts else ""

# Convert table blocks to DataFrame
def table_blocks_to_dataframe(table_blocks, all_blocks_dict):
    """
    Convert Textract table blocks to a pandas DataFrame with actual text
    """
    if not table_blocks:
        return pd.DataFrame()
    
    # Group cells by row and column
    rows = {}
    for cell in table_blocks:
        if cell.get('BlockType') != 'CELL':
            continue
            
        row_index = cell.get('RowIndex', 0)
        col_index = cell.get('ColumnIndex', 0)
        
        # Extract actual text from cell
        text = extract_text_from_cell(cell, all_blocks_dict)
        
        if row_index not in rows:
            rows[row_index] = {}
        rows[row_index][col_index] = text
    
    if not rows:
        return pd.DataFrame()
    
    # Convert to DataFrame
    max_cols = max(max(rows[r].keys()) for r in rows) if rows else 0
    data = []
    
    for row_idx in sorted(rows.keys()):
        row_data = []
        for col_idx in range(1, max_cols + 1):
            row_data.append(rows[row_idx].get(col_idx, ''))
        data.append(row_data)
    
    return pd.DataFrame(data)

# Get page number from table blocks
def _get_table_page(table_blocks):
    """Get the page number from table blocks"""
    if table_blocks and 'Page' in table_blocks[0]:
        return table_blocks[0].get('Page', 1)
    return 1

# Extract all tables from Textract blocks
def extract_all_tables(blocks):
    """
    Extract all tables from Textract blocks and return them as separate DataFrames
    with metadata about each table.
    """
    # Create a dictionary of all blocks by ID for text lookup
    all_blocks_dict = {block['Id']: block for block in blocks if 'Id' in block}
    
    tables = []
    current_table = []
    in_table = False
    table_id = 0
    
    print(f"DEBUG: Processing {len(blocks)} blocks")
    
    for block in blocks:
        if block.get('BlockType') == 'TABLE':
            if current_table:  # Save previous table
                try:
                    df = table_blocks_to_dataframe(current_table, all_blocks_dict)
                    if not df.empty:
                        tables.append({
                            'table_id': table_id,
                            'page': _get_table_page(current_table),
                            'df': df,
                            'block_count': len(current_table)
                        })
                        table_id += 1
                        print(f"DEBUG: Found table {table_id} with {len(current_table)} blocks")
                except Exception as e:
                    print(f"DEBUG: Failed to convert table blocks to DataFrame: {e}")
                current_table = []
            in_table = True
        elif block.get('BlockType') == 'CELL' and in_table:
            current_table.append(block)
        elif in_table and block.get('BlockType') not in ['CELL', 'TABLE']:
            in_table = False
            if current_table:  # Save table when exiting table mode
                try:
                    df = table_blocks_to_dataframe(current_table, all_blocks_dict)
                    if not df.empty:
                        tables.append({
                            'table_id': table_id,
                            'page': _get_table_page(current_table),
                            'df': df,
                            'block_count': len(current_table)
                        })
                        table_id += 1
                        print(f"DEBUG: Found table {table_id} with {len(current_table)} blocks")
                except Exception as e:
                    print(f"DEBUG: Failed to convert table blocks to DataFrame: {e}")
                current_table = []
    
    # Don't forget the last table
    if current_table:
        try:
            df = table_blocks_to_dataframe(current_table, all_blocks_dict)
            if not df.empty:
                tables.append({
                    'table_id': table_id,
                    'page': _get_table_page(current_table),
                    'df': df,
                    'block_count': len(current_table)
                })
                print(f"DEBUG: Found final table {table_id} with {len(current_table)} blocks")
        except Exception as e:
            print(f"DEBUG: Failed to convert final table blocks to DataFrame: {e}")
    
    print(f"DEBUG: Extracted {len(tables)} tables from Textract")
    return tables

# Original table extraction (for fallback)
def extract_combined_table(blocks):
    """
    Original method to extract tables - used as fallback
    """
    print("DEBUG: Using original extract_combined_table (fallback)")
    
    try:
        # Try to extract tables using the new method first
        tables = extract_all_tables(blocks)
        if tables:
            # For fallback, combine all tables into one
            all_data = []
            for table in tables:
                df = table['df']
                if not df.empty:
                    all_data.append(df)
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                print(f"DEBUG: Combined {len(all_data)} tables into shape: {combined_df.shape}")
                return combined_df
    except Exception as e:
        print(f"DEBUG: New table extraction failed in fallback: {e}")
    
    # If everything fails, return empty DataFrame
    return pd.DataFrame()

# Sampling functions (for DeepSeek payload)
def sample_representative_pages(blocks, max_pages=3):
    """Sample representative pages from Textract blocks"""
    pages = {}
    for block in blocks:
        page_num = block.get("Page", 1)
        if page_num not in pages:
            pages[page_num] = []
        pages[page_num].append(block)
    
    # Return first few pages
    sampled_pages = []
    for page_num in sorted(pages.keys())[:max_pages]:
        sampled_pages.append({
            "page": page_num,
            "blocks": pages[page_num][:50]  # Limit blocks per page
        })
    return sampled_pages

def build_deepseek_sampling_payload(blocks, sampled_pages=3):
    """Build payload for DeepSeek analysis"""
    pages = sample_representative_pages(blocks, sampled_pages)
    return {"sampled_pages": pages}