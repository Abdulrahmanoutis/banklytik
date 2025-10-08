import pandas as pd
import traceback

def run_user_code_in_sandbox(code: str, df_raw: pd.DataFrame):
    """
    Safely execute user-generated code (from DeepSeek) that takes a pandas DataFrame df_raw
    and returns a cleaned DataFrame df_clean.

    The code should define a function named `clean_dataframe(df_raw)` that returns df_clean.
    """
    sandbox_env = {
        "pd": pd,
        "df_raw": df_raw.copy(),
    }

    try:
        exec(code, sandbox_env)

        # Expect the code to define a function clean_dataframe(df_raw)
        if "clean_dataframe" in sandbox_env:
            df_clean = sandbox_env["clean_dataframe"](df_raw.copy())
        else:
            # If they just returned df_clean at the end
            df_clean = sandbox_env.get("df_clean")

        # Validate
        if not isinstance(df_clean, pd.DataFrame):
            raise ValueError("DeepSeek code did not return a valid DataFrame")

        return df_clean

    except Exception as e:
        print("Sandbox execution failed:", str(e))
        print(traceback.format_exc())
        return {"error": str(e)}
