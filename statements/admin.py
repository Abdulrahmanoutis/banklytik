from django.contrib import admin

from .models import BankStatement, Transaction

admin.site.register(BankStatement)
admin.site.register(Transaction)

