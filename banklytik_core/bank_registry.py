import importlib
from typing import Dict, Callable, Any, List, Tuple


BANK_REGISTRY = {
    'KUDA': {
        'name': 'Kuda Bank',
        'processor': 'statements.kuda_processor.process_kuda_statement',
        'display_order': 1,
        'active': True,
    },
    'OPAY': {
        'name': 'OPay',
        'processor': 'statements.opay_processor.process_opay_statement',
        'display_order': 2,
        'active': True,
    },
    'GTBANK': {
        'name': 'GTBank',
        'processor': 'statements.gtbank_processor.process_gtbank_statement',
        'display_order': 3,
        'active': False,
    },
    'ZENITH': {
        'name': 'Zenith Bank',
        'processor': 'statements.zenith_processor.process_zenith_statement',
        'display_order': 4,
        'active': False,
    },
    'ACCESS': {
        'name': 'Access Bank',
        'processor': 'statements.access_processor.process_access_statement',
        'display_order': 5,
        'active': False,
    },
    'UBA': {
        'name': 'UBA',
        'processor': 'statements.uba_processor.process_uba_statement',
        'display_order': 6,
        'active': False,
    },
    'FCMB': {
        'name': 'FCMB',
        'processor': 'statements.fcmb_processor.process_fcmb_statement',
        'display_order': 7,
        'active': False,
    },
}


def get_active_banks() -> List[Tuple[str, str]]:
    """Get list of active banks for dropdown"""
    return [
        ('AUTO', 'Auto-detect')
    ] + [
        (code, info['name']) 
        for code, info in sorted(BANK_REGISTRY.items(), key=lambda x: x[1]['display_order'])
        if info.get('active', False)
    ]


def get_processor(bank_code: str) -> Callable:
    """Get processor function for a bank"""
    if bank_code == 'AUTO':
        # Fallback to generic processor for auto-detect
        from statements.direct_processor import process_tables_directly
        return process_tables_directly
    
    bank_info = BANK_REGISTRY.get(bank_code)
    if bank_info and bank_info.get('active', False):
        try:
            module_path, function_name = bank_info['processor'].rsplit('.', 1)
            module = importlib.import_module(module_path)
            return getattr(module, function_name)
        except (ImportError, AttributeError) as e:
            print(f"⚠️ Failed to load processor for {bank_code}: {e}")
            # Fallback to generic processor
            from statements.direct_processor import process_tables_directly
            return process_tables_directly
    
    # Fallback to generic processor
    from statements.direct_processor import process_tables_directly
    return process_tables_directly


def get_all_banks() -> Dict[str, Dict[str, Any]]:
    """Get all banks (active and inactive)"""
    return BANK_REGISTRY.copy()


def is_bank_active(bank_code: str) -> bool:
    """Check if a bank is active"""
    bank_info = BANK_REGISTRY.get(bank_code)
    return bank_info.get('active', False) if bank_info else False
