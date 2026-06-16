# -*- coding: utf-8 -*-
"""Stock code normalization utilities."""


def normalize_stock_code_key(code: str) -> str:
    """Normalize stock code to canonical lowercase internal key format.

    - HK stocks: 'HK00700' → 'hk00700', 'hk00700' → 'hk00700'
    - CN/US stocks: unchanged ('600519', 'AAPL')
    """
    value = str(code or '').strip()
    if value.lower().startswith('hk'):
        return 'hk' + value[2:]
    return value


def normalize_stock_code_for_history_query(code: str) -> str:
    """Normalize stock code to uppercase DB query format for analysis_history.

    HK stocks: 'hk00700' → 'HK00700', 'HK00700' → 'HK00700'
    CN/US stocks: unchanged
    """
    value = str(code or '').strip()
    if value.lower().startswith('hk'):
        return 'HK' + value[2:]
    return value
