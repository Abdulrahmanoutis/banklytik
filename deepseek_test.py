# deepseek_test.py
import json
import pandas as pd
import re
from statements.textract_utils import extract_combined_table

# 1. Load the Textract JSON
with open("analyzeDocResponse.json", "r") as f:
    data = json.load(f)

blocks = data.get("Blocks", [])
df = extract_combined_table(blocks)

print("Raw df shape:", df.shape)
print(df.head(10))
print("\n--- Cleaning transactions ---\n")

# 2. Detect header row
expected_headers = ["post date", "transac", "doc", "value date", "dr", "cr", "balance"]

header_idx = None
for idx, row in df.iterrows():
    cells = [str(c).lower().strip() for c in row.values if pd.notna(c)]
    matches = sum(any(eh in cell for cell in cells) for eh in expected_headers)
    if matches >= 5:  # at least 5 of expected headers present
        header_idx = idx
        break

if header_idx is None:
    raise RuntimeError("Header row not detected in DataFrame")

# 3. Slice to get only transactions
transactions_df = df.iloc[header_idx+1:].copy()
transactions_df = transactions_df.reset_index(drop=True)

# 4. Assign clean column names
transactions_df.columns = [
    "date", "description", "transaction_reference", 
    "value_date", "debit", "credit", "balance"
][:len(transactions_df.columns)]

# 5. Clean amounts
def clean_amount(val):
    if pd.isna(val) or val == "":
        return 0.0
    val = str(val).replace(",", "").replace(" ", "")
    try:
        return float(val)
    except:
        return 0.0

for col in ["debit", "credit", "balance"]:
    if col in transactions_df.columns:
        transactions_df[col] = transactions_df[col].apply(clean_amount)

# 6. Parse dates
def parse_date(x):
    if pd.isna(x):
        return pd.NaT
    s = str(x).strip().replace("- ", "-")
    return pd.to_datetime(s, errors="coerce", dayfirst=True)

transactions_df["date"] = transactions_df["date"].apply(parse_date)

# 7. Extract channel
def extract_channel(desc):
    d = str(desc).upper()
    if "MOBILE/UNION" in d:
        return "MOBILE/UNION"
    if "NXG" in d:
        return "NXG"
    if "ATM" in d:
        return "ATM"
    if "POS" in d:
        return "POS"
    if "TRANSFER" in d:
        return "TRANSFER"
    if "CHARGES" in d:
        return "CHARGES"
    return "OTHER"

transactions_df["channel"] = transactions_df["description"].apply(extract_channel)

# 8. Finalize cleaned DataFrame
transactions_df = transactions_df.dropna(subset=["date"])
transactions_df = transactions_df[[
    "date", "description", "debit", "credit", "balance", "channel", "transaction_reference"
]].reset_index(drop=True)

print("\n--- Cleaned DataFrame ---\n")
print(transactions_df.head(20))

# Assign to result for consistency
result = transactions_df

# Save cleaned output
result.to_csv("cleaned_statement.csv", index=False)
print("\nSaved cleaned transactions to cleaned_statement.csv")