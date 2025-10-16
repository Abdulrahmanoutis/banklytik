"""
Safe sandbox runner for executing DeepSeek-generated cleaning code.

Usage:
    from statements.sandbox_utils import run_user_code_in_sandbox
    df_clean = run_user_code_in_sandbox(code_text, df_raw, timeout=30, debug_path="/tmp/debug")

Notes:
- This is a pragmatic sandbox for local/dev use. Python sandboxing is hard;
  this reduces risk by:
    * disallowing "import" and "from ... import ..." via AST check
    * removing most builtins and providing a small allowlist
    * executing the user code in a separate process with a timeout
    * returning only a pandas.DataFrame (ensures type)
- Still: do not run untrusted code on production hosts without stronger isolation (containers, VMs).
"""

import ast
import io
import os
import pickle
import traceback
from multiprocessing import Process, Pipe
from typing import Optional

import pandas as pd
import numpy as np
import time
from datetime import datetime


# Safe builtin functions we allow inside the sandbox
SAFE_BUILTINS = {
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "dict": dict,
    "float": float,
    "int": int,
    "len": len,
    "list": list,
    "max": max,
    "min": min,
    "pow": pow,
    "range": range,
    "reversed": reversed,
    "round": round,
    "str": str,
    "sum": sum,
    "tuple": tuple,
    # Basic conversions
    "enumerate": enumerate,
    "zip": zip,
}

# Names / AST nodes we treat as dangerous (quick heuristic)
FORBIDDEN_NAMES = {
    "open", "exec", "eval", "__import__", "compile", "input", "os", "sys",
    "subprocess", "shutil", "socket", "requests", "urllib", "ftplib",
    "multiprocessing", "threading", "ctypes", "pickle"  # pickle in sandbox membrane not allowed from user code
}

# Forbidden AST node types
FORBIDDEN_NODE_TYPES = (ast.Import, ast.ImportFrom,)


class SandboxSecurityError(Exception):
    """Raised when static checks fail (disallowed constructs present)."""
    pass


def _static_safety_check(code_text: str) -> None:
    """
    Parse the code with ast and raise SandboxSecurityError if disallowed constructs are found.
    - Disallow import statements.
    - Disallow usage of some dangerous names (open, __import__, eval, exec, subprocess, etc).
    """
    try:
        tree = ast.parse(code_text)
    except SyntaxError as e:
        raise SandboxSecurityError(f"SyntaxError in user code: {e}")

    # Check for forbidden node types (imports)
    for node in ast.walk(tree):
        if isinstance(node, FORBIDDEN_NODE_TYPES):
            raise SandboxSecurityError("Import statements are not allowed in sandboxed code.")

        # detect calls to forbidden names (simple heuristic)
        if isinstance(node, ast.Name):
            if node.id in FORBIDDEN_NAMES:
                raise SandboxSecurityError(f"Usage of '{node.id}' is not allowed in sandboxed code.")

        # detect attribute access like os.system etc (best-effort heuristic)
        if isinstance(node, ast.Attribute):
            # node.attr is last attribute name, node.value might be Name('os') etc
            try:
                if isinstance(node.value, ast.Name) and node.value.id in FORBIDDEN_NAMES:
                    raise SandboxSecurityError(f"Attribute access on '{node.value.id}' is not allowed.")
            except Exception:
                pass

        # Disallow Exec/Global usage
        if isinstance(node, (ast.Global, ast.Nonlocal)):
            raise SandboxSecurityError("Global/nonlocal statements are not allowed.")

    # Additional simple string checks for patterns that AST may miss
    lowered = code_text.lower()
    if "import " in lowered or "__import__" in lowered or "open(" in lowered or "subprocess" in lowered:
        raise SandboxSecurityError("Code contains disallowed keywords (imports/open/subprocess).")


def _worker_exec(code_text: str, pick_conn, df_raw):
    """
    Worker function executed in a subprocess. It receives:
    - code_text : str
    - pick_conn : Pipe connection to send back pickled result or error
    - df_raw : the DataFrame passed in (already a pandas DataFrame)
    Behavior:
    - Prepare a minimal globals/locals environment
    - Execute code_text
    - Expect a variable named `df_clean` to be set to the resulting pandas.DataFrame
    - Send back a (True, payload) on success where payload is pickled DataFrame bytes
      or (False, error_str) on failure.
    """
    try:
        # Minimal allowed builtins
        safe_builtins = SAFE_BUILTINS.copy()

        # Provide numpy/pandas and a copy of df_raw to the environment
        safe_globals = {
            "__builtins__": safe_builtins,
            "pd": pd,
            "np": np,
        }

        # Locals will include df_raw copy so user can operate on it
        safe_locals = {
            "df_raw": df_raw.copy() if isinstance(df_raw, pd.DataFrame) else df_raw
        }

        # Execute user code
        exec(code_text, safe_globals, safe_locals)

        # Retrieve df_clean
        df_clean = safe_locals.get("df_clean", None)

        # Validate result
        if df_clean is None:
            error = "User code did not produce variable 'df_clean'."
            pick_conn.send((False, error))
            pick_conn.close()
            return

        if not isinstance(df_clean, pd.DataFrame):
            error = f"'df_clean' exists but is not a pandas.DataFrame (got {type(df_clean)})."
            pick_conn.send((False, error))
            pick_conn.close()
            return

        # Serialize DataFrame using pickle
        payload = pickle.dumps(df_clean, protocol=4)
        pick_conn.send((True, payload))
        pick_conn.close()
    except Exception as e:
        tb = traceback.format_exc()
        pick_conn.send((False, f"{str(e)}\n{tb}"))
        pick_conn.close()


def run_user_code_in_sandbox(code_text: str, df_raw: pd.DataFrame, timeout: int = 30,
                             debug_path: Optional[str] = None) -> pd.DataFrame:
    """
    Execute `code_text` produced by DeepSeek in a restricted sandboxed subprocess.
    Parameters:
      - code_text: Python code string. MUST set df_clean to a pandas.DataFrame.
      - df_raw: the raw pandas.DataFrame to be cleaned (passed into the sandbox as df_raw).
      - timeout: seconds to wait before killing the subprocess.
      - debug_path: optional directory path where the sandbox will write debug info (errors, code).
    Returns:
      - pandas.DataFrame (df_clean) on success
    Raises:
      - SandboxSecurityError for static safety check failures
      - RuntimeError for execution/timeouts
      - ValueError for invalid df_clean types
    """
    if debug_path:
        try:
            os.makedirs(debug_path, exist_ok=True)
            with open(os.path.join(debug_path, "stage2_user_code.py"), "w", encoding="utf-8") as f:
                f.write(code_text)
        except Exception:
            # non-fatal
            pass

    # 1) Static safety check (AST + simple heuristics)
    try:
        _static_safety_check(code_text)
    except SandboxSecurityError as e:
        raise

    # 2) Start subprocess worker
    parent_conn, child_conn = Pipe()
    proc = Process(target=_worker_exec, args=(code_text, child_conn, df_raw), daemon=True)
    proc.start()
    start_time = time.time()

    try:
        # 3) Wait for result with timeout
        waited = 0.0
        poll_interval = 0.1
        while True:
            if parent_conn.poll():
                success, payload = parent_conn.recv()
                if success:
                    # payload is pickled DataFrame bytes
                    try:
                        df_clean = pickle.loads(payload)
                    except Exception as e:
                        raise RuntimeError(f"Failed to unpickle df_clean: {e}")
                    # Validate again
                    if not isinstance(df_clean, pd.DataFrame):
                        raise ValueError(f"Sandbox returned object not DataFrame: {type(df_clean)}")
                    return df_clean
                else:
                    # payload is error string
                    raise RuntimeError(f"Sandbox execution error:\n{payload}")
            if (time.time() - start_time) > timeout:
                # Timeout - kill process
                proc.terminate()
                proc.join(1)
                raise RuntimeError(f"Sandbox timed out after {timeout} seconds.")
            time.sleep(poll_interval)
    finally:
        try:
            if proc.is_alive():
                proc.terminate()
                proc.join(1)
        except Exception:
            pass
        try:
            parent_conn.close()
            child_conn.close()
        except Exception:
            pass






import pandas as pd
from datetime import datetime

def execute_cleaning_code_with_tables(code, tables):
    """
    Execute DeepSeek-generated cleaning code with table data
    """
    try:
        # Create execution environment
        local_vars = {
            'tables': tables,
            'pd': pd,
            'datetime': datetime
        }
        global_vars = {}
        
        # Execute the code
        exec(code, global_vars, local_vars)
        
        # Call the cleaning function
        if 'clean_transaction_tables' in local_vars:
            df_clean = local_vars['clean_transaction_tables'](tables)
        else:
            raise ValueError("Generated code doesn't define 'clean_transaction_tables' function")
        
        if not isinstance(df_clean, pd.DataFrame):
            raise ValueError("Cleaning function didn't return a DataFrame")
            
        return df_clean
        
    except Exception as e:
        print(f"DEBUG: Code execution failed: {e}")
        raise