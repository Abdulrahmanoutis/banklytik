# banklytik/statements/sandbox.py
import tempfile
import subprocess
import textwrap
import os
import traceback
import json


def run_user_code_in_sandbox(code: str, df):
    """
    Runs DeepSeek-generated Pandas code safely inside an isolated subprocess.
    - `df` is provided as the starting DataFrame.
    - The DeepSeek code must assign the cleaned DataFrame (or result) to a variable named `result`.
    - The sandbox will serialize `result` to JSON if possible, otherwise as string.
    """

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "df.csv")
            code_path = os.path.join(tmpdir, "main.py")
            result_path = os.path.join(tmpdir, "result.json")

            # Save DataFrame to CSV
            df.to_csv(csv_path, index=False)

            # Wrap user code
            safe_code = f"""
import pandas as pd
import json

# Load the DataFrame
df = pd.read_csv(r'{csv_path}')

try:
{textwrap.indent(code, '    ')}

    # Ensure result variable exists
    if 'result' not in locals():
        result = "⚠️ No result was assigned to the variable 'result'"
except Exception as e:
    result = f"⚠️ Exception during execution: {{str(e)}}"

# Serialize result
try:
    if isinstance(result, pd.DataFrame):
        out = result.to_dict(orient="records")
    else:
        out = result
    with open(r'{result_path}', 'w') as f:
        f.write(json.dumps(out, ensure_ascii=False))
except Exception as e:
    with open(r'{result_path}', 'w') as f:
        f.write(json.dumps({{"error": str(e)}}))
"""

            # Write code to file
            with open(code_path, "w") as f:
                f.write(safe_code)

            # Run it in subprocess
            proc = subprocess.run(
                ["python", code_path],
                capture_output=True,
                timeout=15,
                text=True
            )

            # Read the result
            if os.path.exists(result_path):
                with open(result_path, "r") as f:
                    try:
                        return json.loads(f.read())
                    except Exception as e:
                        return f"❌ Failed to parse sandbox output as JSON: {str(e)}"
            else:
                return "⚠️ No result returned."

    except subprocess.TimeoutExpired:
        return "⏱️ Code execution timed out."
    except Exception:
        return f"❌ Sandbox crashed:\n{traceback.format_exc()}"
