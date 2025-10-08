import pandas as pd
import re

def robust_clean_dataframe(df_raw):
    print("DEBUG: robust_clean_dataframe input shape:", df_raw.shape)
    print(df_raw.head(10))

    # === Step 1: Detect header row ===
    expected_headers = [
        "trans", "value", "desc", "debit", "credit",
        "balance", "channel", "reference"
    ]
    header_idx = None

    for idx, row in df_raw.iterrows():
        cells = [str(c).lower().strip() for c in row.values if pd.notna(c)]
        matches = sum(any(eh in cell for cell in cells) for eh in expected_headers)
        if matches >= 3:
            header_idx = idx
            break

    if header_idx is None:
        print("DEBUG: Header row not detected — returning empty DataFrame")
        return pd.DataFrame(columns=[
            "date", "value_date", "description", "debit",
            "credit", "balance", "channel", "transaction_reference"
        ])

    # === Step 2: Slice below header ===
    df = df_raw.iloc[header_idx + 1:].copy().reset_index(drop=True)

    # === Step 3: Assign column names dynamically ===
    # Handle both 6-, 7-, or 8-column layouts
    num_cols = len(df.columns)
    base_cols = [
        "date", "value_date", "description",
        "debit_credit", "balance", "channel",
        "transaction_reference"
    ]

    if num_cols < len(base_cols):
        df.columns = base_cols[:num_cols]
    else:
        df.columns = base_cols + [f"extra_{i}" for i in range(num_cols - len(base_cols))]

    # === Step 4: Handle Debit/Credit(₦) splitting ===
    if "debit_credit" in df.columns:
        debit_vals, credit_vals = [], []
        for val in df["debit_credit"]:
            if pd.isna(val):
                debit_vals.append(0.0)
                credit_vals.append(0.0)
                continue
            s = str(val).replace(",", "").replace("₦", "").replace(" ", "")
            try:
                f = float(s)
            except Exception:
                f = 0.0
            if "-" in str(val) or f < 0:
                debit_vals.append(abs(f))
                credit_vals.append(0.0)
            else:
                debit_vals.append(0.0)
                credit_vals.append(f)
        df["debit"] = debit_vals
        df["credit"] = credit_vals
        df.drop(columns=["debit_credit"], inplace=True, errors="ignore")
    else:
        if "debit" not in df.columns: df["debit"] = 0.0
        if "credit" not in df.columns: df["credit"] = 0.0

    # === Step 5: Clean numeric fields ===
    for col in ["debit", "credit", "balance"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "")
                .str.replace("₦", "")
                .str.extract(r"(-?\d+\.?\d*)")[0]
                .astype(float)
                .fillna(0.0)
            )

    # === Step 6: Parse dates ===
    def parse_date_safe(x):
        if pd.isna(x):
            return pd.NaT
        s = str(x).strip().replace("- ", "-")
        s = re.sub(r"[^\w\s:/-]", "", s)
        return pd.to_datetime(s, errors="coerce", infer_datetime_format=True, dayfirst=True)

    if "date" in df.columns:
        df["date"] = df["date"].apply(parse_date_safe)
    if "value_date" in df.columns:
        df["value_date"] = df["value_date"].apply(parse_date_safe)

    # === Step 7: Extract transaction channel ===
    def extract_channel(desc):
        d = str(desc).upper()
        if "AIRTIME" in d:
            return "AIRTIME"
        if "TRANSFER" in d:
            return "TRANSFER"
        if "POS" in d:
            return "POS"
        if "ATM" in d:
            return "ATM"
        if "CHARGES" in d or "FEE" in d:
            return "CHARGES"
        if "REVERSAL" in d:
            return "REVERSAL"
        return "OTHER"

    if "channel" in df.columns:
        df["channel"] = df["description"].apply(extract_channel)

    # === Step 8: Fill missing columns ===
    for col in ["transaction_reference", "value_date"]:
        if col not in df.columns:
            df[col] = None

    # === Step 9: Drop rows without valid date ===
    df = df.dropna(subset=["date"]).reset_index(drop=True)

    # === Step 10: Reorder columns for consistency ===
    final_cols = [
        "date", "value_date", "description",
        "debit", "credit", "balance",
        "channel", "transaction_reference"
    ]
    df = df[[c for c in final_cols if c in df.columns]]

    # === Step 11: Final sanity checks ===
    print("DEBUG: Cleaned DataFrame shape:", df.shape)
    print(df.head(10))
    print("DEBUG: Debit total:", df["debit"].sum(), "Credit total:", df["credit"].sum())

    return df
