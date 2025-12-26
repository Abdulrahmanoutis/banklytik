import pandas as pd
from typing import List, Dict, Any, Optional
from banklytik_core.bank_registry import get_processor
from statements.bank_detection import detect_bank_from_textract_blocks
from statements.cleaning_utils import robust_clean_dataframe


def process_statement_with_router(blocks: List[Dict[str, Any]], bank_type: str = None) -> Optional[pd.DataFrame]:
    """
    Process statement blocks using appropriate processor based on bank type.
    
    Args:
        blocks: Textract blocks
        bank_type: Pre-marked bank type from statement or None for auto-detection
        
    Returns:
        Processed DataFrame or None if processing failed
    """
    print(f"🔄 Processing statement with bank_type: {bank_type}")
    
    # Determine which processor to use
    if bank_type and bank_type != 'AUTO':
        # Use pre-marked bank type
        processor = get_processor(bank_type)
        processor_name = bank_type
    else:
        # Auto-detect bank type
        detected_bank = detect_bank_from_textract_blocks(blocks)
        processor = get_processor(detected_bank)
        processor_name = f"auto-detected {detected_bank}"
        print(f"🔍 Auto-detected bank: {detected_bank}")
    
    try:
        print(f"🏦 Using processor: {processor_name}")
        
        # Extract tables using the chosen processor
        if processor_name == 'AUTO' or 'auto-detected' in processor_name:
            # Use generic table extraction for auto-detect
            from statements.textract_utils import extract_all_tables
            tables = extract_all_tables(blocks)
            
            if not tables:
                print("❌ No tables found in statement")
                return None
                
            # Use direct processor for table merging
            from statements.direct_processor import process_tables_directly
            df_raw = process_tables_directly(tables)
        else:
            # Use specialized processor
            df_raw = processor(blocks)
        
        if df_raw is None or df_raw.empty:
            print("❌ Processor returned empty DataFrame")
            return None
            
        print(f"✅ Raw DataFrame shape: {df_raw.shape}")
        
        # Apply robust cleaning
        df_clean = robust_clean_dataframe(df_raw)
        
        if df_clean is not None and not df_clean.empty:
            print(f"✅ Clean DataFrame shape: {df_clean.shape}")
            return df_clean
        else:
            print("❌ Cleaning failed")
            return None
            
    except Exception as e:
        print(f"❌ Processing failed with {processor_name}: {e}")
        import traceback
        traceback.print_exc()
        return None


def process_statement_for_rerun(statement) -> Optional[pd.DataFrame]:
    """
    Process an existing statement for reprocessing.
    Uses the statement's bank_type field.
    """
    from statements.textract_utils import get_all_blocks, start_textract_job, wait_for_job
    from statements.views import get_s3_client
    from django.conf import settings
    import json
    
    try:
        # Get Textract data
        s3 = get_s3_client()
        json_key = f"{statement.title}.json"
        
        if s3_key_exists(settings.AWS_S3_BUCKET, json_key):
            obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET, Key=json_key)
            blocks_data = json.loads(obj["Body"].read().decode("utf-8"))
        else:
            job_id = start_textract_job(statement.title)
            wait_for_job(job_id)
            blocks_data = {"Blocks": get_all_blocks(job_id)}
        
        blocks = blocks_data.get("Blocks", blocks_data)
        
        # Process using router
        return process_statement_with_router(blocks, statement.bank_type)
        
    except Exception as e:
        print(f"❌ Rerun failed for statement {statement.pk}: {e}")
        return None


def s3_key_exists(bucket, key):
    """Check if S3 key exists"""
    s3_client = get_s3_client()
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


def get_s3_client():
    """Get S3 client"""
    import boto3
    from django.conf import settings
    
    return boto3.client(
        "s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
        endpoint_url=f"https://s3.{settings.AWS_REGION}.amazonaws.com",
    )
