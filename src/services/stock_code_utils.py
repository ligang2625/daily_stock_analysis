# -*- coding: utf-8 -*-
"""
Shared stock code utilities.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from data_provider.base import is_bse_code

logger = logging.getLogger(__name__)


# Known exchange prefixes (case-insensitive) and the digit lengths they accept.
# e.g. SH600519 -> 600519, HK00700 -> 00700
_PREFIX_DIGIT_LENS: dict = {
    "SH": (6,),
    "SZ": (6,),
    "SS": (6,),
    "BJ": (6,),
    "HK": (1, 2, 3, 4, 5),
}

_SUFFIX_DIGIT_LENS: dict = {
    ".SH": (6,),
    ".SZ": (6,),
    ".SS": (6,),
    ".BJ": (6,),
    ".HK": (1, 2, 3, 4, 5),
}


def _valid_exchange_code(exchange: str, base: str, digit_lens: tuple[int, ...]) -> bool:
    if not (base.isdigit() and len(base) in digit_lens):
        return False
    if exchange == "BJ":
        return is_bse_code(base)
    return True


def _strip_exchange_prefix(text: str) -> Optional[str]:
    """Strip leading exchange prefix (SH/SZ/HK etc.) and return the bare digits, or None."""
    for prefix, digit_lens in _PREFIX_DIGIT_LENS.items():
        if text.startswith(prefix):
            base = text[len(prefix):]
            if _valid_exchange_code(prefix, base, digit_lens):
                return base.zfill(5) if prefix == "HK" else base
    return None


def _strip_exchange_suffix(text: str) -> Optional[str]:
    """Strip exchange suffix (.SH/.SZ/.SS/.HK) and return normalized bare digits, or None."""
    for suffix, digit_lens in _SUFFIX_DIGIT_LENS.items():
        if text.endswith(suffix):
            base = text[: -len(suffix)].strip()
            exchange = suffix.lstrip(".")
            if _valid_exchange_code(exchange, base, digit_lens):
                return base.zfill(5) if suffix == ".HK" else base
    return None


def is_code_like(value: str) -> bool:
    """Check if string looks like a stock code (5-6 digits, 1-5 letters, or prefixed code)."""
    text = value.strip().upper()
    if not text:
        return False
    if text.isdigit() and len(text) in (5, 6):
        return True
    if _strip_exchange_suffix(text) is not None:
        return True
    if re.match(r"^[A-Z]{1,5}(?:\.(?:US|[A-Z]))?$", text):
        return True
    # Support exchange-prefixed codes: SH600519, SZ000001, BJ920493, HK00700
    if _strip_exchange_prefix(text) is not None:
        return True
    return False


def normalize_code(raw: str) -> Optional[str]:
    """Normalize and validate a single stock code.

    Supports:
    - Plain digit codes: 600519, 00700
    - Suffix format: 600519.SH, 600519.SZ, 920493.BJ, 00700.HK
    - Prefix format: SH600519, SZ000001, BJ920493, HK00700 (case-insensitive)
    - US ticker symbols: AAPL, TSLA
    """
    text = raw.strip().upper()
    if not text:
        return None
    if text.isdigit() and len(text) in (5, 6):
        return text
    if re.match(r"^[A-Z]{1,5}(?:\.(?:US|[A-Z]))?$", text):
        return text
    stripped_suffix = _strip_exchange_suffix(text)
    if stripped_suffix is not None:
        return stripped_suffix
    # Support exchange-prefixed codes: SH600519 -> 600519, BJ920493 -> 920493
    stripped = _strip_exchange_prefix(text)
    if stripped is not None:
        return stripped
    return None


# ---------------------------------------------------------------------------
# Persistence-facing helpers (Phase 1)
# ---------------------------------------------------------------------------

_US_TICKER_RE = re.compile(r"^[A-Z]{1,5}$")


def normalize_market_region(value: Optional[str], code: Optional[str] = None) -> Optional[str]:
    """Return one of: 'cn', 'hk', 'us', or None.

    Infers market region from an explicit market value or from the stock code.
    """
    if value is not None:
        v = str(value).strip().lower()
        if v in ("cn", "hk", "us"):
            return v
        # Accept common alias forms
        if v in ("sh", "sz", "bj", "china", "a股"):
            return "cn"
        if v == "hong kong":
            return "hk"
        if v in ("united states", "usa"):
            return "us"

    if code is not None:
        c = str(code).strip().upper()
        if not c:
            return None
        if c.startswith("HK") and c[2:].isdigit():
            return "hk"
        if c.isdigit():
            if len(c) == 5:
                # Short HK code like 00700
                return "hk"
            if len(c) == 6:
                try:
                    if is_bse_code(c):
                        return "cn"
                except Exception:
                    pass
                return "cn"
        if _US_TICKER_RE.match(c):
            return "us"

    return None


def normalize_stock_code_for_storage(code: Optional[str], market: Optional[str] = None) -> Optional[str]:
    """Return canonical DB key for persistence.

    Canonical format:
      CN: 600519 (6 digits as-is)
      HK: HK00700 (uppercase HK prefix + 5 digits zero-padded)
      US: AAPL (uppercase ticker)

    This is a persistence-facing helper and does not change the public
    behavior of existing input-munging functions.
    """
    if code is None:
        return None

    raw = str(code).strip()
    if not raw:
        return None

    upper = raw.upper()

    # Already canonical: HK + digits
    if upper.startswith("HK") and len(upper) > 2 and upper[2:].isdigit():
        digits = upper[2:].zfill(5)
        return f"HK{digits}"

    # Suffix .HK: 00700.HK, 1810.HK -> HK00700, HK01810
    if "." in upper:
        base, suffix = upper.rsplit(".", 1)
        if suffix == "HK" and base.isdigit():
            digits = base.zfill(5)
            return f"HK{digits}"

    # 1-5 digit numeric: HK code (e.g. 00700, 0700, 1810)
    # BUT: if caller explicitly passes market, respect it first
    if upper.isdigit() and 1 <= len(upper) <= 5:
        if market is not None:
            mkt = normalize_market_region(market, code=raw)
            if mkt != "hk":
                if mkt == "cn":
                    return raw.zfill(6)  # treat as CN short code
                if mkt == "us":
                    return upper  # unlikely but safe
        return f"HK{upper.zfill(5)}"

    # 6-digit numeric: CN code
    if upper.isdigit() and len(upper) == 6:
        return upper

    # Strip SH/SZ/BJ prefix for CN codes (e.g. SH600519 -> 600519)
    if upper.startswith(("SH", "SZ", "BJ")) and len(upper) > 2:
        candidate = upper[2:]
        if candidate.isdigit() and len(candidate) == 6:
            return candidate

    # US ticker
    if _US_TICKER_RE.match(upper):
        return upper

    # Try to infer market and format
    if market is not None:
        mkt = normalize_market_region(market, code=raw)
        if mkt == "hk" and raw.isdigit():
            digits = raw.zfill(5)
            return f"HK{digits}"
        if mkt == "us":
            return upper
        if mkt == "cn" and raw.isdigit():
            return raw.zfill(6)

    return upper
