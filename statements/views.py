from django.shortcuts import redirect, get_object_or_404
from django.conf import settings
import json, os
from .models import BankStatement
from .textract_utils import (
    extract_all_tables,
    extract_combined_table,
    get_all_blocks,
    start_textract_job,
    wait_for_job,
)
from .cleaning_utils import robust_clean_dataframe
#from .deepseek_loader import get_deepseek_patterns
from .direct_processor import process_tables_directly
#from .utils import save_debug_textract_json, save_transactions_from_dataframe
#from .s3_utils import get_s3_client, s3_key_exists
#xxxxx
import json
import pandas as pd
import re
import logging
from datetime import datetime

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.views.generic import ListView, CreateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy
from django.conf import settings
from django.utils import timezone

import tempfile
import os
import uuid
import boto3
import dateparser
import pytz

from .models import BankStatement, Transaction
from .forms import BankStatementUploadForm
from .textract_utils import (
    start_textract_job,
    wait_for_job,
    get_all_blocks,
    extract_combined_table,
    extract_all_tables,
)
from .cleaning_utils import robust_clean_dataframe
from .direct_processor import process_tables_directly
from .cleaning_utils import parse_date_str
from banklytik_core.deepseek_adapter import get_deepseek_patterns


logger = logging.getLogger(__name__)


# ------------------ AWS HELPERS ------------------

def get_s3_client():
    """Return a boto3 S3 client configured with correct region and endpoint"""
    region = getattr(settings, "AWS_REGION", getattr(settings, "AWS_S3_REGION_NAME", "eu-west-2"))
    return boto3.client(
        "s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=region,
        endpoint_url=f"https://s3.{region}.amazonaws.com",
    )


def s3_key_exists(bucket, key):
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False
    
    
def save_debug_textract_json(blocks_data, stmt_pk):
    """
    Save the raw Textract JSON output locally for debugging and AI training.
    """
    try:
        debug_dir = os.path.join(getattr(settings, "BASE_DIR", "."), "debug_exports")
        os.makedirs(debug_dir, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(debug_dir, f"analyzeDocResponse_{stmt_pk}_{timestamp}.json")

        with open(path, "w", encoding="utf-8") as f:
            json.dump(blocks_data, f, indent=2)
        print(f"✅ Saved local Textract JSON to {path}")
    except Exception as e:
        print(f"⚠️ Failed to save local Textract JSON: {e}")

def fix_missing_space_date(date_str):
    """
    Fix OCR spacing and colon issues in date strings.
    This helps DeepSeek pattern parsing and fallback regex cleaning.
    """
    import re
    if not isinstance(date_str, str):
        return date_str

    s = date_str.strip()

    # Fix common missing spaces between day/time
    s = re.sub(r"(\d{2})(?=\d{2}:\d{2})", r"\1 ", s)
    s = re.sub(r"(\d{4})(?=[A-Za-z]{3,})", r"\1 ", s)
    s = re.sub(r"(\d{2}:\d{2})(?=\d{2})", r"\1 ", s)
    s = re.sub(r"\s{2,}", " ", s)

    return s.strip()


def normalize_text(value):
    """Normalize OCR text by stripping spaces, newlines, and hidden characters."""
    if pd.isna(value):
        return ""
    s = str(value).strip()
    s = s.replace("\n", " ").replace("\r", " ").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\x20-\x7E\u00A0-\uFFFF]", "", s)
    return s.strip()


def clean_amount(value):
    """Convert ₦ amounts to float safely."""
    if pd.isna(value):
        return 0.0

    s = str(value).replace("₦", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        s_clean = re.sub(r"[^0-9.\-]", "", s)
        try:
            return float(s_clean)
        except Exception:
            return 0.0

def extract_channel(desc):
    """Guess transaction channel based on description text."""
    if pd.isna(desc) or not str(desc).strip():
        return "EMPTY"
    d = str(desc).upper()
    if "AIRTIME" in d:
        return "AIRTIME"
    if "TRANSFER" in d:
        return "TRANSFER"
    if "POS" in d:
        return "POS"
    if "ATM" in d:
        return "ATM"
    if "CHARGE" in d or "FEE" in d or "USSD" in d:
        return "CHARGES"
    if "REVERSAL" in d:
        return "REVERSAL"
    return "OTHER"




# ------------------ DATE PARSING HELPERS ------------------

def enhanced_date_parsing(date_str):
    """
    Cleans and parses date strings using multiple strategies and normalizes to UTC.
    """
    if not date_str or str(date_str).strip() in ["NaT", "None", ""]:
        return None

    cleaned = str(date_str).strip()
    cleaned = re.sub(r"(\d{2})(?=\d{2}:\d{2})", r"\1 ", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    cleaned = cleaned.replace("INVALID_DATE:", "").strip()

    parsed = None
    try:
        parsed = dateparser.parse(
            cleaned,
            settings={
                "DATE_ORDER": "DMY",
                "PREFER_DATES_FROM": "current_period",
                "RETURN_AS_TIMEZONE_AWARE": True,
            },
        )
    except Exception:
        pass

    if not parsed:
        known_formats = [
            "%Y %b %d %H:%M %S",
            "%Y %b %d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d %b %Y",
            "%b %d, %Y",
            "%d%b%Y",
        ]
        for fmt in known_formats:
            try:
                parsed = datetime.strptime(cleaned, fmt)
                break
            except Exception:
                continue

    if not parsed:
        try:
            parsed_pd = pd.to_datetime(cleaned, errors="coerce", dayfirst=True)
            if not pd.isna(parsed_pd):
                parsed = parsed_pd.to_pydatetime()
        except Exception:
            pass

    if not parsed:
        return None

    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone.get_current_timezone())

    return parsed.astimezone(pytz.UTC)


def save_transactions_from_dataframe(stmt, df_clean):
    """
    Saves transactions safely, handling invalid/NaT dates.
    Prevents Series/array ambiguity by coercing all values to strings or floats.
    """
    stmt.transactions.all().delete()
    transactions_created = 0
    flagged_transactions = 0

    for _, row in df_clean.iterrows():
        # Defensive getters (avoid Series ambiguity)
        def safe_get(col, default=""):
            val = row.get(col, default)
            # If Series or list → pick first valid item
            if isinstance(val, (pd.Series, list)):
                if len(val) > 0:
                    return val.iloc[0] if isinstance(val, pd.Series) else val[0]
                return default
            # Handle NaN and weird Pandas types
            if pd.isna(val):
                return default
            return val

        raw_date_text = str(safe_get("date", "")).strip()
        parsed_date = parse_date_str(raw_date_text)

        parsed_date_value = None
        if parsed_date and not pd.isna(parsed_date):
            try:
                parsed_date_value = parsed_date.date()
            except Exception:
                parsed_date_value = None
                flagged_transactions += 1
        else:
            parsed_date_value = None  # FIXED: avoid empty string
            flagged_transactions += 1


        # Coerce numeric and text fields safely
        try:
            debit_val = float(safe_get("debit", 0.0) or 0.0)
        except Exception:
            debit_val = 0.0

        try:
            credit_val = float(safe_get("credit", 0.0) or 0.0)
        except Exception:
            credit_val = 0.0

        try:
            balance_val = float(safe_get("balance", 0.0) or 0.0)
        except Exception:
            balance_val = 0.0

        Transaction.objects.create(
            statement=stmt,
            date=parsed_date_value,
            raw_date=raw_date_text,
            value_date=safe_get("value_date", None),
            description=str(safe_get("description", "")).strip(),
            debit=debit_val,
            credit=credit_val,
            balance=balance_val,
            channel=str(safe_get("channel", "EMPTY") or "EMPTY"),
            transaction_reference=str(safe_get("transaction_reference", "") or ""),
        )

        transactions_created += 1

    stmt.processed = True
    stmt.error_message = ""
    stmt.save()

    print(f"DEBUG: ✅ Created {transactions_created} transactions (⚠️ {flagged_transactions} invalid dates)")
    return transactions_created, flagged_transactions


# ------------------ MAIN VIEWS ------------------

class StatementListView(LoginRequiredMixin, ListView):
    model = BankStatement
    template_name = "statements/statement_list.html"
    context_object_name = "statements"

    def get_queryset(self):
        return BankStatement.objects.filter(user=self.request.user).order_by("-uploaded_at")


class StatementUploadView(LoginRequiredMixin, CreateView):
    model = BankStatement
    form_class = BankStatementUploadForm
    template_name = "statements/upload.html"
    success_url = reverse_lazy("statements:list")

    def form_valid(self, form):
        statement = form.save(commit=False)
        statement.user = self.request.user

        file_obj = self.request.FILES["pdf_file"]
        s3_key = f"statements/user_{statement.user.id}/{uuid.uuid4()}.pdf"

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            for chunk in file_obj.chunks():
                temp_file.write(chunk)
            temp_file_path = temp_file.name

        s3 = get_s3_client()
        try:
            with open(temp_file_path, "rb") as file_to_upload:
                s3.upload_fileobj(
                    file_to_upload,
                    settings.AWS_S3_BUCKET,
                    s3_key,
                    ExtraArgs={"ContentType": "application/pdf"},
                )
        except Exception as e:
            form.add_error(None, f"S3 upload failed: {str(e)}")
            return self.form_invalid(form)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

        statement.title = s3_key
        statement.save()
        return redirect("statements:process", pk=statement.pk)


@login_required
def reprocess_statement(request, pk):
    """
    Clears old transactions, resets the statement, and re-runs processing.
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    stmt.transactions.all().delete()
    stmt.processed = False
    stmt.error_message = ""
    stmt.save()

    print(f"DEBUG: ♻️ Reprocessing statement {stmt.pk}")

    return redirect("statements:process", pk=stmt.pk)




# Replace the process_statement view body in statements/views.py with this function


@login_required
def process_statement(request, pk):
    """Main statement processing function (offline-ready)."""
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    if stmt.processed:
        return redirect("statements:review", pk=stmt.pk)

    # --- DeepSeek preload (keep as you had) ---
    try:
        deepseek_rules = get_deepseek_patterns()
        print(f"✅ DeepSeek preloaded with {len(deepseek_rules)} rules.")
    except Exception as e:
        print(f"⚠️ DeepSeek preload failed: {e}")

    # --- Load Textract JSON (offline or from S3) ---
    try:
        s3 = get_s3_client()
        json_key = f"{stmt.title}.json"
        if s3_key_exists(settings.AWS_S3_BUCKET, json_key):
            obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET, Key=json_key)
            blocks_data = json.loads(obj["Body"].read().decode("utf-8"))
        else:
            job_id = start_textract_job(stmt.title)
            wait_for_job(job_id)
            blocks_data = {"Blocks": get_all_blocks(job_id)}
            # save to S3 for offline reuse
            s3.put_object(
                Bucket=settings.AWS_S3_BUCKET,
                Key=json_key,
                Body=json.dumps(blocks_data).encode("utf-8"),
                ContentType="application/json",
            )
    except Exception as e:
        print(f"⚠️ Textract fetch failed: {e}")
        traceback.print_exc()
        return redirect("statements:detail", pk=stmt.pk)

    blocks = blocks_data.get("Blocks", blocks_data)
    save_debug_textract_json(blocks_data, stmt.pk)

    # --- Extract tables ---
    tables = extract_all_tables(blocks)
    print(f"🧾 Extracted {len(tables)} tables from Textract.")

    df_clean = None

    # --- Try direct processor first ---
    if tables:
        try:
            df_direct = process_tables_directly(tables)
            if df_direct is not None and not df_direct.empty:
                print(f"✅ Direct processor produced shape: {df_direct.shape}")
                print("DEBUG: Direct processor head:\n", df_direct.head(10))
                # attempt to clean using robust_clean_dataframe (it expects a DataFrame)
                df_clean = robust_clean_dataframe(df_direct)
                print(f"✅ After robust_clean_dataframe: {getattr(df_clean,'shape', None)}")
            else:
                print("⚠️ Direct processor returned empty DataFrame")
                df_clean = None
        except Exception as e:
            print(f"⚠️ Direct processor error: {e}")
            traceback.print_exc()
            df_clean = None

    # --- Fallback to combined extraction ---
    if df_clean is None or (hasattr(df_clean, "empty") and df_clean.empty):
        print("⚙️ Using fallback extract_combined_table()...")
        try:
            df_raw = extract_combined_table(blocks)
            print(f"DEBUG: extract_combined_table returned shape: {getattr(df_raw, 'shape', None)}")
            if df_raw is not None and not df_raw.empty:
                print("DEBUG: extract_combined_table head:\n", df_raw.head(20))
                df_clean = robust_clean_dataframe(df_raw)
                print(f"✅ Fallback robust_clean produced: {getattr(df_clean,'shape', None)}")
            else:
                print("❌ Fallback also returned empty DataFrame")
                df_clean = None
        except Exception as e:
            print(f"❌ Critical failure in fallback processing: {e}")
            traceback.print_exc()
            return redirect("statements:detail", pk=stmt.pk)

    # --- Save transactions to DB if any ---
    if df_clean is not None and not (hasattr(df_clean, "empty") and df_clean.empty):
        try:
            save_transactions_from_dataframe(stmt, df_clean)
        except Exception as e:
            print(f"⚠️ Could not save transactions: {e}")
            traceback.print_exc()
    else:
        print("❌ No data to save - all processing methods failed")
        stmt.error_message = "No transaction data could be extracted from the PDF"
        stmt.save()

    return redirect("statements:detail", pk=stmt.pk)

def robust_clean_dataframe(df_raw):
    """
    Clean and normalize extracted bank statement tables.
    Filters junk rows and ensures consistent canonical format.
    """
    import pandas as pd
    from datetime import datetime

    print("DEBUG: robust_clean_dataframe input shape:", getattr(df_raw, "shape", None))
    df = df_raw.copy() if df_raw is not None else pd.DataFrame()

    if df is None or df.empty:
        cols = [
            "date", "raw_date", "value_date", "description",
            "debit", "credit", "balance", "channel",
            "transaction_reference", "row_issue",
        ]
        return pd.DataFrame(columns=cols)

    # Normalize text
    df = df.map(lambda v: normalize_text(v) if pd.notna(v) else "")

    # Assign fallback headers if needed
    if not any("date" in str(c).lower() for c in df.columns):
        df.columns = [
            "Trans. Time", "Value Date", "Description",
            "Debit/Credit(W)", "Balance(N)", "Channel", "Transaction Reference"
        ][: len(df.columns)]

    # Fill required columns
    required = {
        "Trans. Time": "",
        "Value Date": "",
        "Description": "",
        "Debit/Credit(W)": "",
        "Balance(N)": "0",
        "Channel": "",
        "Transaction Reference": "",
    }
    for c, default in required.items():
        if c not in df.columns:
            df[c] = default

    # Parse and clean
    df["raw_date"] = df["Trans. Time"].astype(str)
    df["date"] = df["raw_date"].apply(lambda v: parse_date_str(v) if str(v).strip() else None)
    df["value_date"] = df["Value Date"].apply(lambda v: parse_date_str(v) if str(v).strip() else None)
    df["description"] = df["Description"].astype(str)
    df["balance"] = df["Balance(N)"].apply(clean_amount)

    dc = df["Debit/Credit(W)"].astype(str)
    df["debit"] = dc.apply(lambda x: clean_amount(x) if "-" in x else 0.0)
    df["credit"] = dc.apply(lambda x: clean_amount(x) if "+" in x else 0.0)
    df["channel"] = df["Channel"].apply(extract_channel)
    df["transaction_reference"] = df["Transaction Reference"].astype(str)

    # --- Filter invalid / junk rows ---
    before = len(df)
    df = df[
        df["date"].notna() |
        df["debit"].astype(float).ne(0) |
        df["credit"].astype(float).ne(0) |
        df["balance"].astype(float).ne(0)
    ]
    df = df[df["description"].str.strip() != ""]
    after = len(df)
    print(f"🧹 Filtered junk rows: {before - after} removed, {after} kept")

    # Detect row issues
    def _detect_issues(r):
        issues = []
        if r["date"] is None:
            issues.append("invalid_date")
        if r.get("value_date") is None:
            issues.append("invalid_value_date")
        if r.get("channel") == "EMPTY":
            issues.append("missing_channel")
        return ", ".join(issues) if issues else ""

    df["row_issue"] = df.apply(_detect_issues, axis=1)

    # Canonical order
    cols = [
        "date", "raw_date", "value_date", "description",
        "debit", "credit", "balance", "channel",
        "transaction_reference", "row_issue",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    final_df = df[cols]

    # Save debug snapshot
    try:
        DEBUG_DIR = os.path.join(getattr(settings, "BASE_DIR", "."), "debug_exports")
        os.makedirs(DEBUG_DIR, exist_ok=True)
        stamp = int(datetime.utcnow().timestamp())
        path = os.path.join(DEBUG_DIR, f"robust_clean_snapshot_{stamp}.csv")
        final_df.to_csv(path, index=False)
        print(f"💾 Saved cleaned snapshot → {path}")
    except Exception as e:
        print(f"⚠️ Snapshot save failed: {e}")

    return final_df


@login_required
def statement_detail(request, pk):
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    transactions = stmt.transactions.all().order_by("date", "id")
    return render(
        request,
        "statements/detail.html",
        {"statement": stmt, "transactions": transactions},
    )


# ------------------ EXPORT TABLES FOR DEEPSEEK ------------------

@login_required
def export_tables_for_deepseek(request, pk):
    """
    Exports extracted tables for a given statement (for debugging or DeepSeek AI pipeline)
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)

    try:
        s3 = get_s3_client()
        json_key = f"{stmt.title}.json"

        if not s3_key_exists(settings.AWS_S3_BUCKET, json_key):
            return render(
                request,
                "statements/export_error.html",
                {"error": "No Textract data found for this statement."},
            )

        obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET, Key=json_key)
        blocks_data = json.loads(obj["Body"].read().decode("utf-8"))
        blocks = blocks_data.get("Blocks", blocks_data)

        tables = extract_all_tables(blocks)

        try:
            from .table_serializer import save_complete_tables_for_deepseek
            payload = save_complete_tables_for_deepseek(tables, stmt.pk)
            table_count = len(payload.get("tables", []))
        except Exception:
            table_count = len(tables)

        return render(
            request,
            "statements/export_success.html",
            {
                "statement": stmt,
                "table_count": table_count,
                "file_path": f"debug_exports/complete_tables_{stmt.pk}.json",
            },
        )

    except Exception as e:
        logger.error(f"Export failed: {e}")
        return render(
            request,
            "statements/export_error.html",
            {"error": str(e)},
        )

@login_required
def review_statement(request, pk):
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)

    # Load debug_exports/analyzeDocResponse_<id>_latest.json
    # Or load from S3 if needed
    # Or re-run Textract if no debug file exists

    return render(request, "statements/review.html", {
        "statement": stmt,
        "tables": [],  # will fill next
    })
