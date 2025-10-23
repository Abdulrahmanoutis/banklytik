from django.contrib import admin

from .models import BankStatement, Transaction, ChatHistory

@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ("statement", "date", "raw_date", "description", "debit", "credit", "balance", "channel")


admin.site.register(BankStatement)
admin.site.register(ChatHistory)
