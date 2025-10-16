import json

SYSTEM_INSTRUCTIONS_TABLE_ANALYSIS = """
You are an expert in analyzing bank statement tables. You will receive a payload containing multiple tables extracted from a bank statement PDF.

Your task:
1. Identify which tables contain transaction data (look for columns like date, description, amount, debit/credit, balance, etc.)
2. Generate Python pandas code to clean and merge the transaction tables into one standardized DataFrame.

CRITICAL REQUIREMENTS:
- Your response must ONLY contain valid Python code
- The code must define a function called `clean_transaction_tables(tables)` that returns a cleaned DataFrame
- The function should take the `tables` list as input and return a cleaned DataFrame
- Clean and standardize the following columns if available:
  - date (parse as datetime)
  - description (clean text)
  - debit (convert to float, handle negative amounts)
  - credit (convert to float, handle positive amounts) 
  - balance (convert to float)
  - channel (extract from description: ATM, POS, TRANSFER, AIRTIME, CHARGES, OTHER)
  - transaction_reference (keep as string)

INPUT:
The `tables` parameter is a list of dictionaries, each containing:
- table_id: identifier
- page: page number
- headers: list of column names
- sample_rows: list of sample data rows
- total_rows: total number of data rows
- shape: table dimensions

OUTPUT:
Your code should return a cleaned DataFrame with standardized columns.

DO NOT include explanations, markdown, or any text outside the code block.
"""

def build_table_analysis_prompt(payload):
    """Build prompt for DeepSeek table analysis"""
    user_message = f"""
Analyze these tables extracted from a bank statement and generate cleaning code:

{json.dumps(payload, indent=2, default=str)}

Generate Python code that:
1. Identifies which tables contain transaction data
2. Cleans and standardizes the data
3. Returns a unified DataFrame with standardized columns

Return ONLY the Python code without any explanations.
"""
    return {
        "system": SYSTEM_INSTRUCTIONS_TABLE_ANALYSIS,
        "user": user_message
    }