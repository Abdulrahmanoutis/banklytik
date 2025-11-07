from django.core.management.base import BaseCommand
from pathlib import Path

class Command(BaseCommand):
    help = "Check DeepSeek initialization status"

    def handle(self, *args, **options):
        log = Path("deepseek_ready.log")
        if log.exists():
            self.stdout.write(self.style.SUCCESS("✅ DeepSeek is initialized."))
            self.stdout.write(log.read_text())
        else:
            self.stdout.write(self.style.WARNING("⚠️ DeepSeek not ready or failed."))
