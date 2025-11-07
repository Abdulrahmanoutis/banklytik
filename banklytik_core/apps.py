# banklytik_core/apps.py
from django.apps import AppConfig
import logging
logger = logging.getLogger(__name__)

class BanklytikCoreConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "banklytik_core"

    def ready(self):
        """
        Schedule delayed DeepSeek initialization on startup.
        """
        try:
            from banklytik_core.startup_loader import safe_initialize_deepseek
            safe_initialize_deepseek()
        except Exception as e:
            logger.warning(f"⚠️ DeepSeek startup scheduling failed: {e}")
