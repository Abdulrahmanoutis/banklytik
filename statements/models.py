from django.db import models
from django.contrib.auth.models import User


def user_statement_path(instance, filename):
    return f"statements/user_{instance.user.id}/{filename}"


class BankStatement(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="statements")
    title = models.CharField(max_length=255)
    uploaded_at = models.DateTimeField(auto_now_add=True)
    pdf_file = models.FileField(upload_to=user_statement_path)
    processed = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.title} ({self.user.username})"


class Transaction(models.Model):
    statement = models.ForeignKey(BankStatement, on_delete=models.CASCADE, related_name="transactions")
    
    # 🗓 Parsed datetime (only valid if parsing succeeded)
    date = models.DateField(blank=True, null=True)
    
    # 🆕 Raw date text exactly as extracted from PDF
    raw_date = models.CharField(max_length=255, blank=True, null=True)
    
    value_date = models.DateField(null=True, blank=True)
    
    # 💬 Other fields
    description = models.TextField()
    debit = models.FloatField(default=0)
    credit = models.FloatField(default=0)
    balance = models.FloatField(default=0)
    channel = models.CharField(max_length=255, blank=True, null=True)
    transaction_reference = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        date_display = (
            self.date.strftime("%Y-%m-%d")
            if self.date
            else (self.raw_date or "Unknown Date")
        )
        return f"{date_display} - {self.description[:40]}"


class ChatHistory(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    statement = models.ForeignKey("BankStatement", on_delete=models.CASCADE, related_name="chats")
    question = models.TextField()
    code = models.TextField(blank=True)
    result = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"Q: {self.question[:30]}... | {self.created_at.strftime('%Y-%m-%d %H:%M')}"
