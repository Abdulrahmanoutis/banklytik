

from django.shortcuts import render, redirect, get_object_or_404
from django.views.generic import ListView, CreateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy
from django.contrib.auth.decorators import login_required
from django.conf import settings
from datetime import datetime

import tempfile
import os
import uuid
import boto3
import pandas as pd
import json
import logging

from .models import BankStatement, Transaction, ChatHistory
from .forms import BankStatementUploadForm
from .textract_utils import (
    start_textract_job,
    wait_for_job,
    get_all_blocks,
    extract_combined_table,
    sample_representative_pages,
)
from .cleaning_utils import robust_clean_dataframe   # ✅ fallback cleaner

logger = logging.getLogger(__name__)

# ---------- S3 HELPERS ----------
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
    )

def s3_key_exists(bucket, key):
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


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
    Allow user to re-run the statement processing using cached Textract JSON.
    Deletes old transactions, sets processed=False, then calls process_statement.
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    stmt.transactions.all().delete()
    stmt.processed = False
    stmt.save()
    return redirect("statements:process", pk=stmt.pk)


@login_required
def process_statement(request, pk):
    """
    New processing pipeline with table-based approach
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)

    if stmt.processed:
        return redirect("statements:list")

    s3 = get_s3_client()
    json_key = f"{stmt.title}.json"

    try:
        # 1️⃣ Load Textract JSON (same as before)
        if s3_key_exists(settings.AWS_S3_BUCKET, json_key):
            obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET, Key=json_key)
            blocks_data = json.loads(obj["Body"].read().decode("utf-8"))
        else:
            job_id = start_textract_job(stmt.title)
            wait_for_job(job_id)
            blocks_data = {"Blocks": get_all_blocks(job_id)}
            s3.put_object(
                Bucket=settings.AWS_S3_BUCKET,
                Key=json_key,
                Body=json.dumps(blocks_data).encode("utf-8"),
                ContentType="application/json",
            )

        blocks = blocks_data.get("Blocks", blocks_data)
        if isinstance(blocks, str):
            blocks = json.loads(blocks)

        print(f"DEBUG: Textract returned {len(blocks)} blocks")

        # In your process_statement function, add this after table extraction:

# 2️⃣ Extract ALL tables (NEW)
        from .textract_utils import extract_all_tables
        tables = extract_all_tables(blocks)

        # Debug: Print table info
        for table in tables:
            print(f"DEBUG: Table {table['table_id']} - Page {table['page']} - Shape: {table['df'].shape}")
            print(f"DEBUG: Headers: {table['df'].iloc[0].tolist() if not table['df'].empty else 'Empty'}")

        # 🆕 NEW: Save complete tables for DeepSeek
        from .table_serializer import save_complete_tables_for_deepseek
        complete_tables_payload = save_complete_tables_for_deepseek(tables, stmt.pk)

# 3️⃣ Process tables directly (continue with your existing code...)

        # 3️⃣ Process tables directly (NEW - skip DeepSeek for now)
        try:
            from .direct_processor import process_tables_directly
            df_clean = process_tables_directly(tables)
            print(f"DEBUG: Direct processor returned shape: {df_clean.shape}")
            
            # If direct processor fails, fallback to combined table
            if df_clean.empty:
                raise ValueError("Direct processor returned empty DataFrame")
                
        except Exception as e:
            print(f"DEBUG: Direct processor failed: {e}")
            # Fallback to original approach
            from .textract_utils import extract_combined_table
            from .cleaning_utils import robust_clean_dataframe
            df_raw = extract_combined_table(blocks)
            df_clean = robust_clean_dataframe(df_raw)

        print(f"DEBUG: Final cleaned DataFrame shape: {df_clean.shape}")

    except Exception as e:
        print(f"DEBUG: Processing failed: {e}")
        # Fallback to original approach
        from .textract_utils import extract_combined_table
        from .cleaning_utils import robust_clean_dataframe
        df_raw = extract_combined_table(blocks)
        df_clean = robust_clean_dataframe(df_raw)

    # 6️⃣ Save transactions (same as before)
    try:
        stmt.transactions.all().delete()
        transactions_created = 0
        skipped_transactions = 0
        
        for _, row in df_clean.iterrows():
            # Get date value and handle conversion SAFELY
            date_val = row.get("date")
            
            # Skip rows with invalid dates
            if (pd.isna(date_val) or 
                date_val is None or
                str(date_val) in ['NaT', 'None', ''] or
                str(date_val).startswith('NaT')):
                skipped_transactions += 1
                continue
                
            # Convert to Python datetime SAFELY
            try:
                if hasattr(date_val, 'to_pydatetime'):
                    date_val = date_val.to_pydatetime()
                elif isinstance(date_val, pd.Timestamp):
                    date_val = date_val.to_pydatetime()
                elif isinstance(date_val, datetime):
                    # Already a Python datetime, no conversion needed
                    pass
                else:
                    # Try to parse as string
                    date_val = datetime.fromisoformat(str(date_val))
            except Exception as e:
                print(f"DEBUG: Failed to convert date {date_val}: {e}")
                skipped_transactions += 1
                continue
                
            Transaction.objects.create(
                statement=stmt,
                date=date_val,
                description=row.get("description", ""),
                debit=row.get("debit", 0.0),
                credit=row.get("credit", 0.0),
                balance=row.get("balance", 0.0),
                channel=row.get("channel", "OTHER"),
                transaction_reference=row.get("transaction_reference", ""),
            )
            transactions_created += 1

        stmt.processed = True
        stmt.error_message = ""
        stmt.save()
        
        print(f"DEBUG: Successfully created {transactions_created} transactions")
        print(f"DEBUG: Skipped {skipped_transactions} invalid transactions")
        
    except Exception as e:
        stmt.error_message = f"Failed saving transactions: {e}"
        stmt.save()
        return render(
            request,
            "statements/process_error.html",
            {"error_message": str(e), "statement": stmt},
        )

    return redirect("statements:detail", pk=stmt.pk)


@login_required
def statement_detail(request, pk):
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    transactions = stmt.transactions.all()
    return render(
        request,
        "statements/detail.html",
        {"statement": stmt, "transactions": transactions},
    )


@login_required
def export_tables_for_deepseek(request, pk):
    """
    Export all extracted tables as JSON for DeepSeek analysis
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    
    try:
        # Load Textract JSON
        s3 = get_s3_client()
        json_key = f"{stmt.title}.json"
        
        if s3_key_exists(settings.AWS_S3_BUCKET, json_key):
            obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET, Key=json_key)
            blocks_data = json.loads(obj["Body"].read().decode("utf-8"))
            blocks = blocks_data.get("Blocks", blocks_data)
            
            # Extract tables
            from .textract_utils import extract_all_tables
            tables = extract_all_tables(blocks)
            
            # Save complete tables
            from .table_serializer import save_complete_tables_for_deepseek
            payload = save_complete_tables_for_deepseek(tables, stmt.pk)
            
            return render(request, 'statements/export_success.html', {
                'statement': stmt,
                'table_count': len(payload['tables']),
                'file_path': f"debug_exports/complete_tables_{stmt.pk}.json"
            })
        else:
            return render(request, 'statements/export_error.html', {
                'error': 'No Textract data found for this statement'
            })
            
    except Exception as e:
        return render(request, 'statements/export_error.html', {
            'error': str(e)
        })