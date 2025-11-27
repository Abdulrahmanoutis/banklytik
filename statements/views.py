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
    import traceback
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    
    if stmt.processed:
        return redirect("statements:review", pk=stmt.pk)

    # --- DeepSeek preload ---
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

            # save JSON for offline reuse
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

    # --- Extract tables from Textract ---
    tables = extract_all_tables(blocks)
    print(f"🧾 Extracted {len(tables)} tables from Textract.")
    df_clean = None

    # =====================================================
    # 🔥 TRY DIRECT PROCESSOR FIRST
    # =====================================================
    if tables:
        try:
            df_direct = process_tables_directly(tables)

            if df_direct is not None and not df_direct.empty:
                print(f"✅ Direct processor produced shape: {df_direct.shape}")
                print("DEBUG: Direct processor head:\n", df_direct.head(10))

                # ⭐ WRITE MERGED DEBUG FOR ME TO INSPECT ⭐
                try:
                    debug_path = os.path.join("debug_exports", "merged_debug.csv")
                    os.makedirs("debug_exports", exist_ok=True)
                    df_direct.to_csv(debug_path, index=False)
                    print(f"⭐ WROTE merged_debug.csv → {debug_path}")
                except Exception as e:
                    print(f"⚠️ Failed writing merged_debug.csv: {e}")

                # Now pass into robust cleaner
                df_clean = robust_clean_dataframe(df_direct)
                print(f"✅ After robust_clean_dataframe: {df_clean.shape}")

            else:
                print("⚠️ Direct processor returned empty DataFrame")
                df_clean = None

        except Exception as e:
            print(f"⚠️ Direct processor error: {e}")
            traceback.print_exc()
            df_clean = None

    # =====================================================
    # 🔥 FALLBACK TO COMBINED EXTRACTOR
    # =====================================================
    if df_clean is None or df_clean.empty:
        print("⚙️ Using fallback extract_combined_table()...")

        try:
            df_raw = extract_combined_table(blocks)
            print(f"DEBUG: extract_combined_table returned shape: {getattr(df_raw, 'shape', None)}")

            if df_raw is not None and not df_raw.empty:
                print("DEBUG: extract_combined_table head:\\n", df_raw.head(20))
                df_clean = robust_clean_dataframe(df_raw)
                print(f"✅ Fallback robust_clean produced: {df_clean.shape}")
            else:
                print("❌ Fallback returned empty DataFrame")
                df_clean = None

        except Exception as e:
            print(f"❌ Critical failure in fallback cleaning: {e}")
            traceback.print_exc()
            return redirect("statements:detail", pk=stmt.pk)

    # =====================================================
    # 🔥 SAVE TRANSACTIONS IF ANY
    # =====================================================
    if df_clean is not None and not df_clean.empty:
        try:
            save_transactions_from_dataframe(stmt, df_clean)
        except Exception as e:
            print(f"⚠️ Could not save transactions: {e}")
            traceback.print_exc()
    else:
        print("❌ No data to save — all processing failed")
        stmt.error_message = "No transaction data could be extracted from the PDF"
        stmt.save()

    return redirect("statements:detail", pk=stmt.pk)


# =====================================================
# 🔥 NEW MANUAL TABLE SELECTION WORKFLOW
# =====================================================

from .table_scorer import score_all_tables
from .selection_session import get_session, clear_session
from .table_merger import merge_selected_tables
from .column_mapper import analyze_merged_table, ColumnMapper

@login_required
def start_table_selection(request, pk):
    """
    Start the manual table selection workflow.
    Extracts tables and redirects to table selection page.
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    
    # Clear any existing session
    clear_session(stmt.pk, request.user.id)
    
    # Load Textract data
    try:
        s3 = get_s3_client()
        json_key = f"{stmt.title}.json"

        if s3_key_exists(settings.AWS_S3_BUCKET, json_key):
            obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET, Key=json_key)
            blocks_data = json.loads(obj["Body"].read().decode("utf-8"))
        else:
            # If no Textract data exists, run Textract
            job_id = start_textract_job(stmt.title)
            wait_for_job(job_id)
            blocks_data = {"Blocks": get_all_blocks(job_id)}
            
            # Save JSON for future use
            s3.put_object(
                Bucket=settings.AWS_S3_BUCKET,
                Key=json_key,
                Body=json.dumps(blocks_data).encode("utf-8"),
                ContentType="application/json",
            )

        blocks = blocks_data.get("Blocks", blocks_data)
        save_debug_textract_json(blocks_data, stmt.pk)
        
        # Extract and score tables
        tables = extract_all_tables(blocks)
        scored_tables = score_all_tables(tables)
        
        # Store only metadata in session (no DataFrames)
        session = get_session(stmt.pk, request.user.id)
        
        # Create serializable version without DataFrames
        serializable_tables = []
        for table in scored_tables:
            serializable_table = {
                'table_id': table.get('table_id'),
                'page': table.get('page'),
                'score': table.get('score', 0),
                'confidence': table.get('confidence', 'low'),
                'reasons': table.get('reasons', []),
                'row_count': table.get('row_count', 0),
                'column_count': table.get('column_count', 0),
                'score_breakdown': table.get('score_breakdown', {}),
                'preview_data': table.get('preview_data', [])
            }
            serializable_tables.append(serializable_table)
        
        session.set_extracted_tables(serializable_tables)
        
        # Store the Textract JSON data for re-extraction when needed
        session.state['textract_json'] = blocks_data
        session.save()
        
        return redirect("statements:select_tables", pk=stmt.pk)
        
    except Exception as e:
        logger.error(f"Table selection start failed: {e}")
        return render(
            request,
            "statements/process_error.html",
            {"error": f"Failed to extract tables: {str(e)}"},
        )


@login_required
def select_tables(request, pk):
    """
    Display all extracted tables for user selection.
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    session = get_session(stmt.pk, request.user.id)
    
    # Get table metadata from session
    tables_metadata = session.get_extracted_tables()
    
    if request.method == "POST":
        # Get selected table IDs
        selected_ids = request.POST.getlist("selected_tables")
        selected_ids = [int(id) for id in selected_ids if id.isdigit()]
        
        if not selected_ids:
            return render(
                request,
                "statements/select_tables.html",
                {
                    "statement": stmt,
                    "tables": tables_metadata,
                    "error": "Please select at least one table containing transactions."
                },
            )
        
        # Store selected tables
        session.set_selected_tables(selected_ids)
        
        # Re-extract tables from stored Textract JSON data
        blocks_data = session.state.get('textract_json')
        if not blocks_data:
            return render(
                request,
                "statements/select_tables.html",
                {
                    "statement": stmt,
                    "tables": tables_metadata,
                    "error": "Failed to retrieve table data. Please restart the selection process."
                },
            )
        
        # Extract tables again and filter by selected IDs
        blocks = blocks_data.get("Blocks", blocks_data)
        tables = extract_all_tables(blocks)
        selected_tables = [t for t in tables if t.get('table_id') in selected_ids]
        
        merged_df = merge_selected_tables(selected_tables)
        
        if merged_df is None or merged_df.empty:
            return render(
                request,
                "statements/select_tables.html",
                {
                    "statement": stmt,
                    "tables": tables_metadata,
                    "error": "Failed to merge selected tables. Please try different table selection."
                },
            )
        
        session.set_merged_data(merged_df)
        
        return redirect("statements:map_columns", pk=stmt.pk)
    
    # GET request - show table selection
    return render(
        request,
        "statements/select_tables.html",
        {
            "statement": stmt,
            "tables": tables_metadata,
            "selected_count": len(session.state.get('selected_table_ids', [])),
        },
    )


@login_required
def map_columns(request, pk):
    """
    Column mapping interface for the merged table.
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    session = get_session(stmt.pk, request.user.id)
    
    merged_df = session.get_merged_data()
    
    if merged_df is None or merged_df.empty:
        return redirect("statements:select_tables", pk=stmt.pk)
    
    if request.method == "POST":
        # Get column mappings from form
        column_mappings = {}
        for key, value in request.POST.items():
            if key.startswith("column_"):
                column_name = key.replace("column_", "")
                if value and value != "unknown":
                    column_mappings[column_name] = value
        
        if not column_mappings:
            return render(
                request,
                "statements/map_columns.html",
                {
                    "statement": stmt,
                    "analysis": analyze_merged_table(merged_df),
                    "error": "Please map at least one column to a transaction field."
                },
            )
        
        # Apply mappings and create standardized dataframe
        mapper = ColumnMapper()
        standardized_df = mapper.apply_column_mapping(merged_df, column_mappings)
        
        # Store in session
        session.set_column_mappings(column_mappings)
        session.set_final_dataframe(standardized_df)
        
        return redirect("statements:preview_data", pk=stmt.pk)
    
    # GET request - show column mapping interface
    analysis = analyze_merged_table(merged_df)
    
    return render(
        request,
        "statements/map_columns.html",
        {
            "statement": stmt,
            "analysis": analysis,
            "existing_mappings": session.get_column_mappings(),
        },
    )


@login_required
def preview_data(request, pk):
    """
    Preview the final cleaned data before saving.
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    session = get_session(stmt.pk, request.user.id)
    
    final_df = session.get_final_dataframe()
    
    if final_df is None or final_df.empty:
        redirect("statements:map_columns", pk=stmt.pk)
    
    if request.method == "POST":
        # User confirmed - save transactions
        try:
            # Apply final cleaning
            df_clean = robust_clean_dataframe(final_df)
            
            if df_clean is not None and not df_clean.empty:
                transactions_created, flagged_transactions = save_transactions_from_dataframe(stmt, df_clean)
                
                # Clear session
                clear_session(stmt.pk, request.user.id)
                
                return render(
                    request,
                    "statements/preview_success.html",
                    {
                        "statement": stmt,
                        "transactions_created": transactions_created,
                        "flagged_transactions": flagged_transactions,
                    },
                )
            else:
                return render(
                    request,
                    "statements/preview_data.html",
                    {
                        "statement": stmt,
                        "final_data": final_df.to_dict(orient='records'),
                        "data_shape": final_df.shape,
                        "error": "Failed to clean data. Please review your column mappings."
                    },
                )
                
        except Exception as e:
            logger.error(f"Failed to save transactions: {e}")
            return render(
                request,
                "statements/preview_data.html",
                {
                    "statement": stmt,
                    "final_data": final_df.to_dict(orient='records'),
                    "data_shape": final_df.shape,
                    "error": f"Failed to save transactions: {str(e)}"
                },
            )
    
    # GET request - show data preview
    field_labels = [
        ('date', 'Date'),
        ('description', 'Description'), 
        ('debit', 'Debit'),
        ('credit', 'Credit'),
        ('amount', 'Amount'),
        ('balance', 'Balance'),
        ('reference', 'Reference'),
    ]

    # Convert DataFrame to records and apply cleaning for validation
    df_clean = robust_clean_dataframe(final_df)
    final_data = df_clean.to_dict(orient='records') if df_clean is not None else []
    
    # Extract validation summary if available
    validation_summary = {
        'total': len(final_data),
        'with_warnings': sum(1 for row in final_data if row.get('date_validation_warning') == 'ERROR'),
        'suspicious': sum(1 for row in final_data if row.get('date_validation_warning') == 'WARNING'),
        'valid': sum(1 for row in final_data if row.get('date_validation_warning') == 'INFO'),
    }

    return render(
        request,
        "statements/preview_data.html",
        {
            "statement": stmt,
            "final_data": final_data,
            "data_shape": df_clean.shape if df_clean is not None else (0, 0),
            "column_mappings": session.get_column_mappings(),
            "field_labels": field_labels,
            "validation_summary": validation_summary,
        },
    )


@login_required
def cancel_selection(request, pk):
    """
    Cancel the manual table selection workflow.
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    clear_session(stmt.pk, request.user.id)
    
    return redirect("statements:detail", pk=stmt.pk)


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


# =====================================================
# 🤖 AI-POWERED CHAT FOR TRANSACTION ANALYSIS
# =====================================================

from .ai_query_generator import generate_pandas_code
from .code_validator import validate_code
from .code_executor import execute_pandas_code
import time

@login_required
def chat(request, pk):
    """
    AI-powered chat interface for natural language queries about transactions.
    
    Uses DeepSeek to generate pandas code that analyzes transactions.
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    
    # Check if statement has transactions
    transaction_count = stmt.transactions.count()
    if transaction_count == 0:
        return render(
            request,
            "statements/chat.html",
            {
                "statement": stmt,
                "error": "❌ No transactions in this statement. Please import transactions first.",
                "chat_history": [],
            },
        )
    
    answer = None
    code = None
    error = None
    execution_time = None
    
    if request.method == "POST":
        question = request.POST.get("question", "").strip()
        
        if not question:
            return render(
                request,
                "statements/chat.html",
                {
                    "statement": stmt,
                    "error": "❌ Please enter a question.",
                    "chat_history": stmt.chats.all()[:10],
                },
            )
        
        # Limit question length to prevent token waste
        if len(question) > 500:
            return render(
                request,
                "statements/chat.html",
                {
                    "statement": stmt,
                    "error": "❌ Question is too long (max 500 characters).",
                    "chat_history": stmt.chats.all()[:10],
                },
            )
        
        print(f"\n{'='*80}")
        print(f"🤖 CHAT QUERY: {question}")
        print(f"{'='*80}")
        
        # Step 1: Get transaction data as DataFrame
        try:
            transactions = stmt.transactions.values()
            df = pd.DataFrame(transactions)
            
            # Remove Django-specific fields
            df = df[['date', 'description', 'debit', 'credit', 'balance', 'channel', 'transaction_reference']]
            
            print(f"📊 Loaded {len(df)} transactions")
            print(f"   Columns: {list(df.columns)}")
            
        except Exception as e:
            error = f"❌ Failed to load transaction data: {str(e)}"
            print(error)
            return render(
                request,
                "statements/chat.html",
                {
                    "statement": stmt,
                    "error": error,
                    "chat_history": stmt.chats.all()[:10],
                },
            )
        
        # Step 2: Generate pandas code using DeepSeek
        schema = {
            'columns': df.columns.tolist(),
            'sample_data': df.head(5).to_dict('records')
        }
        
        success, generated_code = generate_pandas_code(question, schema)
        
        if not success:
            error = generated_code
            print(error)
            return render(
                request,
                "statements/chat.html",
                {
                    "statement": stmt,
                    "question": question,
                    "error": error,
                    "chat_history": stmt.chats.all()[:10],
                },
            )
        
        code = generated_code
        print(f"\n✅ Generated code:\n{code}\n")
        
        # Step 3: Validate generated code
        is_safe, validation_error = validate_code(code, verbose=True)
        
        if not is_safe:
            error = validation_error
            print(error)
            
            # Save failed attempt to history
            from .models import ChatHistory
            ChatHistory.objects.create(
                user=request.user,
                statement=stmt,
                question=question,
                code=code,
                result=error,
            )
            
            return render(
                request,
                "statements/chat.html",
                {
                    "statement": stmt,
                    "question": question,
                    "code": code,
                    "error": error,
                    "chat_history": stmt.chats.all()[:10],
                },
            )
        
        # Step 4: Execute the code safely
        start_time = time.time()
        success, result = execute_pandas_code(df, code, timeout_seconds=10)
        execution_time = round(time.time() - start_time, 2)
        
        if not success:
            error = result
            print(error)
            answer = None
        else:
            answer = result
            print(f"\n✅ Result:\n{answer}\n")
        
        # Step 5: Save to chat history
        from .models import ChatHistory
        ChatHistory.objects.create(
            user=request.user,
            statement=stmt,
            question=question,
            code=code,
            result=answer or error,
        )
        
        print(f"{'='*80}\n")
        
    # Render chat page with history
    chat_history = stmt.chats.all().order_by('-created_at')[:20]
    
    return render(
        request,
        "statements/chat.html",
        {
            "statement": stmt,
            "question": question if request.method == "POST" else None,
            "answer": answer,
            "code": code,
            "error": error,
            "execution_time": execution_time,
            "chat_history": chat_history,
            "transaction_count": transaction_count,
        },
    )


@login_required
def chat_history(request, pk):
    """
    View full chat history for a statement.
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    chat_history = stmt.chats.all().order_by('-created_at')
    
    return render(
        request,
        "statements/history.html",
        {
            "statement": stmt,
            "chat_history": chat_history,
        },
    )
