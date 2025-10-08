from django import forms
from .models import BankStatement

class BankStatementUploadForm(forms.ModelForm):
    class Meta:
        model = BankStatement
        fields = ["title", "pdf_file"]
