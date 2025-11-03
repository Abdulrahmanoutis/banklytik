### Remove Naira Symbol
Regex: ₦
Replace: 
Notes: Removes ₦ symbols before parsing numeric values.

### Remove Commas
Regex: ,
Replace: 
Notes: Strips commas to normalize 1,000 → 1000.

### Handle Negative Signs
Regex: \(([\d.]+)\)
Replace: -\1
Notes: Converts (100.00) → -100.00 for debit formatting.
