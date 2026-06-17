# -*- coding: utf-8 -*-
"""Tests for code normalization and migration compatibility (Phase 1)."""

from __future__ import annotations

import pytest

from src.services.stock_code_utils import (
    normalize_stock_code_for_storage,
    normalize_market_region,
)


class TestNormalizeStockCodeForStorage:
    """Test canonical stock code normalization for persistence."""

    # --- HK codes ---

    def test_hk_lowercase_with_prefix(self):
        assert normalize_stock_code_for_storage("hk00700") == "HK00700"

    def test_hk_uppercase_with_prefix(self):
        assert normalize_stock_code_for_storage("HK00700") == "HK00700"

    def test_hk_suffix_dot_format(self):
        assert normalize_stock_code_for_storage("00700.HK") == "HK00700"

    def test_hk_short_digits_with_suffix(self):
        assert normalize_stock_code_for_storage("1810.HK") == "HK01810"

    def test_hk_short_digits_with_prefix(self):
        assert normalize_stock_code_for_storage("HK700") == "HK00700"

    def test_hk_five_digit_raw(self):
        assert normalize_stock_code_for_storage("00700") == "HK00700"

    def test_hk_four_digit_raw(self):
        assert normalize_stock_code_for_storage("0700") == "HK00700"

    # --- CN codes ---

    def test_cn_six_digit(self):
        assert normalize_stock_code_for_storage("600519") == "600519"

    def test_cn_leading_zero(self):
        assert normalize_stock_code_for_storage("000001") == "000001"

    def test_cn_sh_prefixed(self):
        # SH prefix is stripped by normalize_stock_code_for_storage → returns digits
        result = normalize_stock_code_for_storage("SH600519")
        assert result == "600519"

    # --- US codes ---

    def test_us_lowercase(self):
        assert normalize_stock_code_for_storage("aapl") == "AAPL"

    def test_us_uppercase(self):
        assert normalize_stock_code_for_storage("AAPL") == "AAPL"

    def test_us_mixed_case(self):
        assert normalize_stock_code_for_storage("Tsla") == "TSLA"

    # --- Edge cases ---

    def test_none_returns_none(self):
        assert normalize_stock_code_for_storage(None) is None

    def test_empty_string_returns_none(self):
        assert normalize_stock_code_for_storage("") is None

    def test_whitespace_returns_none(self):
        assert normalize_stock_code_for_storage("   ") is None

    def test_digits_only_cn(self):
        assert normalize_stock_code_for_storage("300750") == "300750"

    def test_digits_only_hk_pad(self):
        assert normalize_stock_code_for_storage("00001") == "HK00001"


class TestNormalizeMarketRegion:
    """Test market region normalization."""

    def test_explicit_cn(self):
        assert normalize_market_region("cn") == "cn"

    def test_explicit_hk(self):
        assert normalize_market_region("hk") == "hk"

    def test_explicit_us(self):
        assert normalize_market_region("us") == "us"

    def test_alias_sh(self):
        assert normalize_market_region("sh") == "cn"

    def test_alias_sz(self):
        assert normalize_market_region("sz") == "cn"

    def test_from_hk_code(self):
        assert normalize_market_region(None, code="HK00700") == "hk"

    def test_from_hk_code_lower(self):
        assert normalize_market_region(None, code="hk00700") == "hk"

    def test_from_cn_code(self):
        assert normalize_market_region(None, code="600519") == "cn"

    def test_from_us_code(self):
        assert normalize_market_region(None, code="AAPL") == "us"

    def test_from_five_digit_code(self):
        assert normalize_market_region(None, code="00700") == "hk"

    def test_none_returns_none(self):
        assert normalize_market_region(None) is None

    def test_empty_returns_none(self):
        assert normalize_market_region("") is None


class TestIdempotency:
    """Verifying idempotency of normalization."""

    def test_hk_idempotent(self):
        first = normalize_stock_code_for_storage("hk00700")
        second = normalize_stock_code_for_storage(first)
        assert first == second == "HK00700"

    def test_us_idempotent(self):
        first = normalize_stock_code_for_storage("aapl")
        second = normalize_stock_code_for_storage(first)
        assert first == second == "AAPL"

    def test_cn_idempotent(self):
        first = normalize_stock_code_for_storage("600519")
        second = normalize_stock_code_for_storage(first)
        assert first == second == "600519"

    def test_hk_dot_suffix_idempotent(self):
        first = normalize_stock_code_for_storage("1810.HK")
        assert first == "HK01810"
        second = normalize_stock_code_for_storage(first)
        assert first == second == "HK01810"
