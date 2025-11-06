from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)

class BanklytikCoreConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "banklytik_core"

    def ready(self):
        """
        Initialize DeepSeek on Django startup using isolated loader
        (prevents circular imports between apps and knowledge modules).
        """
        try:
            from banklytik_core.startup_loader import safe_initialize_deepseek
            safe_initialize_deepseek()
        except Exception as e:
            logger.warning(f"⚠️ DeepSeek startup failed: {e}")
