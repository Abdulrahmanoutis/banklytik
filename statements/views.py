from django.shortcuts import render, redirect, get_object_or_404
from django.views.generic import ListView, CreateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings

import tempfile
import os
import uuid
import boto3
import pandas as pd
import io
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
from .deepseek_utils import build_sample_json, build_deepseek_prompt, call_deepseek
from .sandbox import run_user_code_in_sandbox
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

    # Clear old transactions
    stmt.transactions.all().delete()

    # Reset processed flag
    stmt.processed = False
    stmt.save()

    # Redirect to the existing process pipeline
    return redirect("statements:process", pk=stmt.pk)


@login_required
def process_statement(request, pk):
    """
    Processing pipeline:
    1. Load Textract blocks from cache (S3) if available, otherwise call Textract & save JSON.
    2. Extract combined table -> df_raw.
    3. Send sampled JSON to DeepSeek -> get cleaning code.
    4. Execute cleaning code in sandbox.
    5. If sandbox fails, fallback to robust_clean_dataframe.
    6. Save transactions to DB.
    """
    stmt = get_object_or_404(BankStatement, pk=pk, user=request.user)

    if stmt.processed:
        return redirect("statements:list")

    s3 = get_s3_client()
    blocks = None
    json_key = f"{stmt.title}.json"

    # 1. Use cached JSON if available
    try:
        if s3_key_exists(settings.AWS_S3_BUCKET, json_key):
            obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET, Key=json_key)
            blocks_data = json.loads(obj["Body"].read().decode("utf-8"))
        else:
            job_id = start_textract_job(stmt.title)
            wait_for_job(job_id)
            blocks_data = {"Blocks": get_all_blocks(job_id)}

            # cache in S3
            s3.put_object(
                Bucket=settings.AWS_S3_BUCKET,
                Key=json_key,
                Body=json.dumps(blocks_data).encode("utf-8"),
                ContentType="application/json",
            )

        # ✅ Always extract "Blocks" list
        blocks = blocks_data.get("Blocks", blocks_data)
        from .deepseek_column_detection import run_column_detection_with_deepseek

        print("DEBUG: Type of blocks before DeepSeek:", type(blocks))
        if isinstance(blocks, str):
            try:
                blocks = json.loads(blocks)
                print("DEBUG: Parsed stringified blocks into JSON successfully.")
            except Exception as e:
                print("DEBUG: Failed to parse blocks JSON:", e)

# 🔍 TEMP TEST: Run DeepSeek to detect transaction columns
        column_detection_result = run_column_detection_with_deepseek(blocks, stmt.pk)
        print("DEBUG: DeepSeek column detection result:", column_detection_result)

        if not isinstance(blocks, list):
            raise ValueError("Textract JSON did not contain a valid 'Blocks' list.")

        # 🔍 Debug logs
        logger.info("Textract returned %s blocks", len(blocks))
        print("DEBUG: Textract returned", len(blocks), "blocks")

    except Exception as e:
        stmt.error_message = f"Textract failed: {e}"
        stmt.save()
        return render(
            request,
            "statements/process_error.html",
            {"error_message": str(e), "statement": stmt},
        )

      # 2. Build raw df
    try:
        df_raw = extract_combined_table(blocks)
        logger.info("Extracted raw DF shape: %s", df_raw.shape)
        print("DEBUG: Raw DataFrame shape:", df_raw.shape)
        print("DEBUG: Raw DataFrame head:\n", df_raw.head(10))

        # 💾 DEBUG: Save extracted Textract blocks and raw table for offline analysis
        try:
            debug_dir = os.path.join(settings.BASE_DIR, "debug_exports")
            os.makedirs(debug_dir, exist_ok=True)

            # Save Textract blocks (full response)
            textract_json_path = os.path.join(debug_dir, f"textract_blocks_{stmt.pk}.json")
            with open(textract_json_path, "w") as f:
                json.dump(blocks, f, indent=2)

            # Save extracted raw table
            raw_df_json_path = os.path.join(debug_dir, f"raw_table_{stmt.pk}.json")
            df_raw.to_json(raw_df_json_path, orient="records", indent=2)

            print(f"DEBUG: Saved Textract blocks → {textract_json_path}")
            print(f"DEBUG: Saved raw DataFrame → {raw_df_json_path}")
        except Exception as debug_e:
            print("DEBUG: Failed to save debug JSON:", debug_e)

    except Exception as e:
        stmt.error_message = f"Failed to parse tables: {e}"
        stmt.save()
        return render(
            request,
            "statements/process_error.html",
            {"error_message": str(e), "statement": stmt},
        )

    # 3. Call DeepSeek
    try:
        sampled_blocks = sample_representative_pages(blocks)
        sampled_pages = sorted({b.get("Page") for b in sampled_blocks if b.get("Page")})
        sample_json = build_sample_json(blocks, sampled_pages)

        prompt = build_deepseek_prompt(sample_json)
        cleaning_code = call_deepseek(prompt)

        # 4️⃣ Run DeepSeek Stage 2 Cleaning
        from .deepseek_cleaning_generation import run_stage2_cleaning_with_deepseek

        df_clean = run_stage2_cleaning_with_deepseek(df_raw, deepseek_stage1_result, stmt_pk=statement.pk)


    except Exception as e:
        logger.warning("Sandbox cleaning failed or returned invalid result, falling back: %s", e)
        print("DEBUG: Entering fallback cleaner because sandbox failed.")
        df_clean = robust_clean_dataframe(df_raw)
        print("DEBUG: Fallback cleaner returned shape:", df_clean.shape)
        print(df_clean.head(10))

    # 5. Save cleaned transactions
    try:
        for _, row in df_clean.iterrows():
            Transaction.objects.create(
                statement=stmt,
                date=row.get("date"),
                description=row.get("description", ""),
                debit=row.get("debit", 0.0),
                credit=row.get("credit", 0.0),
                balance=row.get("balance", 0.0),
                channel=row.get("channel", "OTHER"),
                transaction_reference=row.get("transaction_reference", ""),
            )

        stmt.processed = True
        stmt.error_message = ""
        stmt.save()

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

    
