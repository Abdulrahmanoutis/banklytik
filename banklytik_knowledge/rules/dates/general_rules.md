# Banklytik Date Parsing — General Rules

## Rule 1: Fix missing space between day and time
**Description:** Handles OCR output like `2025 Feb 2310:00 48`.
**Regex:** `(\d{4}\s+[A-Za-z]{3,}\s+)(\d{2})(\d{2}:\d{2}\s+\d{2})`
**Replacement:** `\1\2 \3`
**Example:** `2025 Feb 2310:00 48` → `2025 Feb 23 10:00 48`

---

## Rule 2: Remove extra spaces inside time parts
**Description:** Fixes cases like `2025 Feb 23 20:11: 58`
**Regex:** `r'\s*:\s*' → ':'`
**Example:** `2025 Feb 23 20:11: 58` → `2025 Feb 23 20:11:58`

---

## Rule 3: Compact day-month patterns
**Description:** Converts `24Feb 2025` → `24 Feb 2025`
**Regex:** `r'(\d{2})([A-Za-z]{3,})(\s*\d{4})' → '\1 \2 \3'`

---

## Notes
- All dates assumed **Day–Month–Year**
- `dateparser` used first; `strptime` and `pandas` are fallback layers
- Future banks can override or extend these rules
