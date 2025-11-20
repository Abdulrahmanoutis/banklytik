"""
Code Validator - Validates generated pandas code before execution.
Ensures code is safe and cannot harm the system.
"""

import ast
import re
from typing import Tuple


# Forbidden patterns that indicate potentially dangerous code
FORBIDDEN_PATTERNS = {
    "__": "Double underscore (potential dunder method abuse)",
    "eval": "eval() function",
    "exec": "exec() function",
    "compile": "compile() function",
    "__import__": "__import__ function",
    "globals": "globals() access",
    "locals": "locals() access",
    "vars": "vars() function",
    "dir": "dir() function",
    "getattr": "getattr() function",
    "setattr": "setattr() function",
    "delattr": "delattr() function",
    "open(": "File operations",
    "read": "File read operations (in non-pandas context)",
    "write": "File write operations",
    "import ": "Direct imports",
    "from ": "From imports",
    "os.": "OS module",
    "sys.": "sys module",
    "subprocess": "subprocess module",
    "socket": "socket module",
    "requests": "requests module (external)",
    "urllib": "urllib module (external)",
    "pickle": "pickle module (security risk)",
    "marshal": "marshal module",
    "code": "code module",
    "types": "types module",
    "inspect": "inspect module",
    "ctypes": "ctypes module",
    "multiprocessing": "multiprocessing module",
    "threading": "threading module",
    "time.sleep": "sleep function (could hang)",
}

# Built-in functions that are ALLOWED
ALLOWED_BUILTINS = {
    "len", "sum", "max", "min", "sorted", "list", "dict", "set", "tuple",
    "str", "int", "float", "bool", "abs", "round", "pow", "range",
    "enumerate", "zip", "map", "filter", "all", "any", "print",
    "type", "isinstance", "issubclass", "callable", "hasattr",
}

# Libraries/modules that are ALLOWED
ALLOWED_MODULES = {
    "pd": "pandas",
    "np": "numpy",
    "pandas": "pandas",
    "numpy": "numpy",
}


def validate_syntax(code: str) -> Tuple[bool, str]:
    """
    Validate that code is valid Python syntax.
    
    Returns: (is_valid, error_message)
    """
    try:
        ast.parse(code)
        return True, ""
    except SyntaxError as e:
        return False, f"❌ Invalid Python syntax: {e.msg} (line {e.lineno})"
    except Exception as e:
        return False, f"❌ Syntax parsing error: {str(e)}"


def validate_forbidden_patterns(code: str) -> Tuple[bool, str]:
    """
    Check for forbidden patterns that could be dangerous.
    
    Returns: (is_safe, error_message)
    """
    code_lower = code.lower()
    
    for pattern, description in FORBIDDEN_PATTERNS.items():
        if pattern in code_lower:
            return False, f"❌ Code contains forbidden pattern: {description} ('{pattern}')"
    
    return True, ""


def validate_dataframe_reference(code: str) -> Tuple[bool, str]:
    """
    Ensure code references 'df' (our dataframe variable).
    
    Returns: (is_valid, error_message)
    """
    if 'df' not in code:
        return False, "❌ Code must reference 'df' dataframe"
    
    return True, ""


def validate_code_length(code: str, max_length: int = 2000) -> Tuple[bool, str]:
    """
    Prevent excessively long code (could be attack or infinite loop).
    
    Returns: (is_valid, error_message)
    """
    if len(code) > max_length:
        return False, f"❌ Code too long ({len(code)} > {max_length} chars)"
    
    return True, ""


def validate_imports(code: str) -> Tuple[bool, str]:
    """
    Check that imports only reference allowed modules.
    
    Returns: (is_valid, error_message)
    """
    # Find all import statements
    import_pattern = r'\b(import|from)\s+([a-zA-Z0-9_.]+)'
    imports = re.findall(import_pattern, code)
    
    for import_type, module_name in imports:
        # Extract base module name
        base_module = module_name.split('.')[0]
        
        if base_module not in ALLOWED_MODULES:
            return False, f"❌ Unsupported import: '{module_name}' is not allowed"
    
    return True, ""


def validate_ast_safety(code: str) -> Tuple[bool, str]:
    """
    Use AST to check for dangerous constructs at parse level.
    
    Returns: (is_valid, error_message)
    """
    try:
        tree = ast.parse(code)
        
        for node in ast.walk(tree):
            # Check for function calls to dangerous functions
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id.lower()
                    
                    # Block dangerous built-ins
                    if func_name in ["eval", "exec", "compile", "__import__", "open"]:
                        return False, f"❌ Dangerous function call: {func_name}()"
                
                # Check for attribute access on dangerous modules
                if isinstance(node.func, ast.Attribute):
                    if hasattr(node.func, 'value') and isinstance(node.func.value, ast.Name):
                        module = node.func.value.id.lower()
                        method = node.func.attr.lower()
                        
                        # Block dangerous OS/sys calls
                        if module in ["os", "sys", "subprocess", "socket"]:
                            return False, f"❌ Blocked: {module}.{method}()"
            
            # Check for global/local access
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in ["globals", "locals", "vars"]:
                        return False, f"❌ Blocked: {node.func.id}() function"
        
        return True, ""
    
    except Exception as e:
        return False, f"❌ AST validation error: {str(e)}"


def validate_code(code: str, verbose: bool = True) -> Tuple[bool, str]:
    """
    Complete code validation pipeline.
    Runs multiple checks to ensure code is safe to execute.
    
    Args:
        code: Python code to validate
        verbose: Whether to print detailed info
    
    Returns:
        (is_safe: bool, error_message: str)
    """
    
    if verbose:
        print("\n🔍 Validating generated code...")
    
    # Check 1: Code length
    is_valid, error = validate_code_length(code)
    if not is_valid:
        if verbose:
            print(error)
        return False, error
    
    # Check 2: Syntax
    is_valid, error = validate_syntax(code)
    if not is_valid:
        if verbose:
            print(error)
        return False, error
    
    # Check 3: DataFrame reference
    is_valid, error = validate_dataframe_reference(code)
    if not is_valid:
        if verbose:
            print(error)
        return False, error
    
    # Check 4: Forbidden patterns (regex)
    is_safe, error = validate_forbidden_patterns(code)
    if not is_safe:
        if verbose:
            print(error)
        return False, error
    
    # Check 5: Imports
    is_valid, error = validate_imports(code)
    if not is_valid:
        if verbose:
            print(error)
        return False, error
    
    # Check 6: AST safety
    is_safe, error = validate_ast_safety(code)
    if not is_safe:
        if verbose:
            print(error)
        return False, error
    
    if verbose:
        print("✅ Code validation passed!")
    
    return True, ""


# Test function for debugging
if __name__ == "__main__":
    test_cases = [
        ("df[df['credit'] > 0]['credit'].max()", True),
        ("df.nlargest(5, 'debit')", True),
        ("df[df['channel'].str.contains('airtime', case=False)]['debit'].sum()", True),
        ("import os; os.system('rm -rf /')", False),
        ("df.__class__.__bases__", False),
        ("eval('print(df)')", False),
        ("df[df['balance'] > 1000]", True),
    ]
    
    print("Running validation tests...\n")
    for code, expected_safe in test_cases:
        is_safe, error = validate_code(code)
        status = "✅ PASS" if is_safe == expected_safe else "❌ FAIL"
        print(f"{status}: {code[:50]}... → Safe: {is_safe}")
