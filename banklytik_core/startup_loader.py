# banklytik_core/startup_loader.py
import logging

logger = logging.getLogger(__name__)

def safe_initialize_deepseek():
    """
    Safely initialize the DeepSeek learning system at Django startup.
    This function delays imports until runtime to prevent circular imports
    between core modules (knowledge_loader, knowledge_registry, statements).
    """
    try:
        # Lazy imports to avoid circular dependencies
        from banklytik_core.knowledge_loader import reload_knowledge
        from banklytik_core.knowledge_registry import initialize_registry

        logger.info("🔄 Initializing DeepSeek knowledge system...")

        # Step 1: Load markdown and JSON rules into memory
        reload_knowledge()

        # Step 2: Register functions and rules into AI-accessible registry
        initialize_registry()

        logger.info("✅ DeepSeek initialized successfully.")
        print("✅ DeepSeek system initialized cleanly.")

    except Exception as e:
        logger.warning(f"⚠️ DeepSeek initialization failed: {e}")
        print(f"⚠️ DeepSeek initialization failed: {e}")
