"""
AI Query Generator - Uses DeepSeek to generate pandas code for natural language questions.
"""

import os
from typing import Optional
import requests
import json

# DeepSeek API Configuration
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"


def generate_pandas_code(
    user_question: str, 
    transaction_schema: dict, 
    max_retries: int = 2
) -> tuple[bool, str]:
    """
    Ask DeepSeek to generate pandas code for the user's question.
    
    Args:
        user_question: Natural language question about transactions
        transaction_schema: Dict with 'columns' list and 'sample_data' examples
        max_retries: Number of retries if code generation fails
    
    Returns:
        (success: bool, code_or_error: str)
    """
    
    if not DEEPSEEK_API_KEY:
        return False, "❌ DeepSeek API key not configured"
    
    # Build detailed prompt
    columns_str = ", ".join(transaction_schema.get("columns", []))
    sample_data_str = json.dumps(transaction_schema.get("sample_data", [])[:3], indent=2, default=str)
    
    system_prompt = """You are a pandas code expert. Generate ONLY valid pandas code that answers user questions about transaction data.

IMPORTANT RULES:
1. The variable MUST be 'df' (the transaction dataframe that's already loaded)
2. The code should be a single line or multiple lines
3. The last line should produce the result (assignment or expression)
4. Use only pandas, numpy operations - NO file operations, NO network calls
5. Return ONLY the code, no explanations
6. Do NOT use backquotes or markdown formatting
7. The code will be executed directly, so it must be valid Python

Available columns: date, description, debit, credit, balance, channel, transaction_reference

Example questions and answers:
Q: "What is the highest credit I received?"
A: df[df['credit'] > 0]['credit'].max()

Q: "Top 5 debit transactions"
A: df.nlargest(5, 'debit')[['date', 'description', 'debit']]

Q: "Total spent on airtime"
A: df[df['channel'].str.contains('airtime', case=False, na=False)]['debit'].sum()

Q: "How many transactions did I make?"
A: len(df)
"""
    
    user_prompt = f"""Transaction data schema:
- Columns: {columns_str}
- Sample transactions:
{sample_data_str}

User question: "{user_question}"

Generate the pandas code:"""
    
    for attempt in range(max_retries):
        try:
            print(f"\n🤖 Generating code (attempt {attempt + 1}/{max_retries})...")
            
            response = requests.post(
                DEEPSEEK_API_URL,
                headers={
                    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "temperature": 0.3,  # Lower temperature for more consistent code
                    "max_tokens": 500,
                },
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"⚠️ DeepSeek API error: {response.status_code}")
                print(f"Response: {response.text}")
                continue
            
            data = response.json()
            code = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            
            if code:
                # Clean up the code (remove markdown formatting if present)
                code = code.strip("```python").strip("```").strip()
                print(f"✅ Generated code: {code[:100]}...")
                return True, code
            else:
                print("⚠️ Empty response from DeepSeek")
                continue
        
        except requests.exceptions.Timeout:
            print("⚠️ DeepSeek API timeout")
            continue
        except requests.exceptions.ConnectionError:
            print("⚠️ Connection error to DeepSeek API")
            continue
        except Exception as e:
            print(f"⚠️ Error calling DeepSeek: {str(e)}")
            continue
    
    return False, "❌ Failed to generate code after retries"
