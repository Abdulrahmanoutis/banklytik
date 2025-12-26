from django.core.management.base import BaseCommand
from statements.models import BankStatement


class Command(BaseCommand):
    help = 'Mark test statements with their bank types'

    def handle(self, *args, **options):
        try:
            # Mark OPAY statement (ID: 3)
            opay_stmt = BankStatement.objects.get(pk=3)
            opay_stmt.bank_type = 'OPAY'
            opay_stmt.save()
            self.stdout.write(self.style.SUCCESS(f'✅ Marked statement ID 3 as OPAY: {opay_stmt.title}'))
            
            # Mark KUDA statement (ID: 19)
            kuda_stmt = BankStatement.objects.get(pk=19)
            kuda_stmt.bank_type = 'KUDA'
            kuda_stmt.save()
            self.stdout.write(self.style.SUCCESS(f'✅ Marked statement ID 19 as KUDA: {kuda_stmt.title}'))
            
            self.stdout.write(self.style.SUCCESS('🎉 All test statements have been marked with bank types!'))
            
        except BankStatement.DoesNotExist as e:
            self.stdout.write(self.style.ERROR(f'❌ Statement not found: {e}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error marking statements: {e}'))
