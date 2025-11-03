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
    """
    stmt.transactions.all().delete()
    transactions_created = 0
    flagged_transactions = 0

    for _, row in df_clean.iterrows():
        raw_date_text = str(row.get("date", "")).strip()
        parsed_date = parse_date_str(raw_date_text)

        parsed_date_value = None
        if parsed_date and not pd.isna(parsed_date):
            try:
                parsed_date_value = parsed_date.date()
            except Exception:
                parsed_date_value = None
                flagged_transactions += 1
        else:
            flagged_transactions += 1

        Transaction.objects.create(
            statement=stmt,
            date=parsed_date_value,
            raw_date=raw_date_text,
            # 🆕 Include parsed value date
            value_date=row.get("value_date") if "value_date" in row else None,
            description=row.get("description", "") or "",
            debit=row.get("debit", 0.0) or 0.0,
            credit=row.get("credit", 0.0) or 0.0,
            balance=row.get("balance", 0.0) or 0.0,
            channel=row.get("channel", "EMPTY") or "EMPTY",
            transaction_reference=row.get("transaction_reference", "") or "",
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
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    stmt.transactions.all().delete()
    stmt.processed = False
    stmt.save()
    print(f"DEBUG: ♻️ Reprocessing statement {stmt.pk}")
    return redirect("statements:process", pk=stmt.pk)


@login_required
def process_statement(request, pk):
    # Ensure DeepSeek rules are loaded before cleaning
    try:
        deepseek_rules = get_deepseek_patterns()
        print(f"✅ DeepSeek preloaded with {len(deepseek_rules)} rules.")
    except Exception as e:
        print(f"⚠️ DeepSeek preload failed: {e}")
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)
    if stmt.processed:
        return redirect("statements:list")

    s3 = get_s3_client()
    json_key = f"{stmt.title}.json"

    try:
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

        tables = extract_all_tables(blocks)

        try:
            df_clean = process_tables_directly(tables)
            if df_clean.empty:
                raise ValueError("Direct processor returned empty DataFrame")
        except Exception:
            df_raw = extract_combined_table(blocks)
            df_clean = robust_clean_dataframe(df_raw)

    except Exception as e:
        df_raw = extract_combined_table(blocks)
        df_clean = robust_clean_dataframe(df_raw)

    save_transactions_from_dataframe(stmt, df_clean)
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
