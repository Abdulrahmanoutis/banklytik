"""
Code Executor - Safely executes validated pandas code in an isolated environment.
"""

import sys
import io
import traceback
import pandas as pd
import numpy as np
from typing import Tuple
from contextlib import redirect_stdout, redirect_stderr
import signal


class TimeoutException(Exception):
    """Raised when code execution exceeds timeout."""
    pass


def timeout_handler(signum, frame):
    """Signal handler for timeout."""
    raise TimeoutException("Code execution exceeded timeout limit")


def execute_pandas_code(
    df: pd.DataFrame, 
    code: str, 
    timeout_seconds: int = 10
) -> tuple[bool, str]:
    """
    Execute validated pandas code in a safe, isolated environment.
    
    Args:
        df: DataFrame containing transaction data
        code: Python code to execute (already validated)
        timeout_seconds: Maximum execution time
    
    Returns:
        (success: bool, result: str)
    """
    
    print(f"\n⚙️ Executing code (timeout: {timeout_seconds}s)...")
    
    # Create isolated namespace with only safe globals
    namespace = {
        # Data
        'df': df,
        # Libraries
        'pd': pd,
        'np': np,
        # Allowed builtins
        'len': len,
        'sum': sum,
        'max': max,
        'min': min,
        'abs': abs,
        'round': round,
        'range': range,
        'list': list,
        'dict': dict,
        'set': set,
        'tuple': tuple,
        'str': str,
        'int': int,
        'float': float,
        'sorted': sorted,
        'enumerate': enumerate,
        'zip': zip,
        'map': map,
        'filter': filter,
        'all': all,
        'any': any,
        'print': print,
        'type': type,
        'isinstance': isinstance,
    }
    
    # Capture stdout/stderr
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    
    try:
        # Set timeout (Unix only)
        try:
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)
        except (AttributeError, ValueError):
            # Windows doesn't support SIGALRM
            print("⚠️ Note: Timeout not enforced on this system")
            old_handler = None
        
        # Redirect output
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            # Execute the code
            exec(code, namespace)
        
        # Cancel alarm if set
        if old_handler is not None:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
        
        # Get output
        stdout_text = stdout_capture.getvalue()
        stderr_text = stderr_capture.getvalue()
        
        # Try to get the result of the last expression
        result_text = ""
        
        if stdout_text:
            result_text = stdout_text.strip()
        
        if not result_text:
            # Try to evaluate the last line as an expression
            lines = code.strip().split('\n')
            if lines:
                last_line = lines[-1].strip()
                try:
                    result = eval(last_line, namespace)
                    
                    # Format result nicely
                    if isinstance(result, pd.DataFrame):
                        result_text = f"DataFrame with {len(result)} rows:\n\n{result.to_string()}"
                    elif isinstance(result, pd.Series):
                        result_text = f"Series:\n\n{result.to_string()}"
                    elif isinstance(result, list):
                        result_text = f"List with {len(result)} items:\n{result}"
                    else:
                        result_text = str(result)
                except:
                    result_text = "Code executed successfully (no output)"
        
        if stderr_text:
            result_text += f"\n⚠️ Warnings:\n{stderr_text}"
        
        if not result_text:
            result_text = "Code executed successfully"
        
        print(f"✅ Execution successful!")
        print(f"Result preview: {result_text[:200]}...")
        
        return True, result_text
    
    except TimeoutException:
        if old_handler is not None:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
        
        error_msg = f"❌ Execution timeout: Code took longer than {timeout_seconds} seconds"
        print(error_msg)
        return False, error_msg
    
    except Exception as e:
        if old_handler is not None:
            try:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
            except:
                pass
        
        error_msg = f"❌ Execution error: {str(e)}\n\n{traceback.format_exc()}"
        print(error_msg)
        return False, error_msg


def format_result_for_display(result: str, max_length: int = 2000) -> str:
    """
    Format execution result for web display.
    
    Args:
        result: Raw result string
        max_length: Maximum length to display
    
    Returns:
        Formatted result string
    """
    
    # Truncate if too long
    if len(result) > max_length:
        result = result[:max_length] + f"\n... (truncated, {len(result)} total chars)"
    
    return result


# Test function
if __name__ == "__main__":
    # Create sample DataFrame
    test_df = pd.DataFrame({
        'date': pd.date_range('2025-05-14', periods=5),
        'description': ['Airtime', 'Transfer', 'Bills', 'Airtime', 'Transfer'],
        'debit': [100, 0, 1000, 100, 0],
        'credit': [0, 1000, 0, 0, 1000],
        'balance': [0, 1000, 0, 0, 1000]
    })
    
    test_cases = [
        ("df[df['credit'] > 0]['credit'].max()", "Should work"),
        ("df.nlargest(3, 'debit')", "Should work"),
        ("import os; os.system('ls')", "Should fail - forbidden import"),
    ]
    
    print("Running executor tests...\n")
    for code, description in test_cases:
        print(f"\n{'='*60}")
        print(f"Test: {description}")
        print(f"Code: {code}")
        print(f"{'='*60}")
        
        success, result = execute_pandas_code(test_df, code)
        print(f"Result: {result[:200]}")
