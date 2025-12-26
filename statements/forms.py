from django import forms
from .models import BankStatement
from banklytik_core.bank_registry import get_active_banks

class BankStatementUploadForm(forms.ModelForm):
    class Meta:
        model = BankStatement
        fields = ["title", "bank_type", "pdf_file"]
        widgets = {
            'bank_type': forms.Select(attrs={'class': 'form-select'})
        }
