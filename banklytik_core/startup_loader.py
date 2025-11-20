# banklytik_core/startup_loader.py
import logging, threading, time, os
from pathlib import Path

logger = logging.getLogger(__name__)
READY_FILE = Path("deepseek_ready.log")

def _init_task():
    """Background task that runs DeepSeek initialization after a small delay."""
    try:
        from banklytik_core.knowledge_loader import reload_knowledge
        from banklytik_core.knowledge_registry import initialize_registry

        time.sleep(2)  # wait for Django apps to fully load
        reload_knowledge()
        initialize_registry()

        READY_FILE.write_text("DeepSeek initialized successfully.\n")
        logger.info("✅ DeepSeek fully initialized and ready.")
        print("✅ DeepSeek fully initialized and ready.")
    except Exception as e:
        logger.warning(f"⚠️ DeepSeek background initialization failed: {e}")
        print(f"⚠️ DeepSeek background initialization failed: {e}")

def safe_initialize_deepseek():
    """
    Run DeepSeek initialization in a background thread to prevent
    blocking or circular import issues during AppConfig.ready().
    """
    threading.Thread(target=_init_task, daemon=True).start()
    logger.info("🕒 DeepSeek background initialization scheduled.")
    print("🕒 DeepSeek background initialization scheduled.")
