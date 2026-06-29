# -*- coding: utf-8 -*-
"""
Tests for intraday decision content integrity:
- _extract_stock_codes_from_decision_content()
- _validate_official_decision_completeness()
- DecisionContentIntegrity dataclass
- LLM truncation metadata
"""

import pytest
from src.core.intraday_monitor import (
    DecisionContentIntegrity,
    IntradayMonitor,
)
from unittest.mock import MagicMock


class TestExtractStockCodes:
    """Tests for _extract_stock_codes_from_decision_content()."""

    def test_extract_a_share_codes(self):
        content = "建议买入 **600519**，当前价 1800.00。观望 **000858**。"
        codes = IntradayMonitor._extract_stock_codes_from_decision_content(content)
        assert "600519" in codes
        assert "000858" in codes

    def test_extract_hk_codes(self):
        content = "建议买入 **HK00700**，当前价 320.00。"
        codes = IntradayMonitor._extract_stock_codes_from_decision_content(content)
        assert "HK00700" in codes

    def test_extract_parenthesis_pattern_half_width(self):
        content = "- **贵州茅台(600519)**：当前价 1800.00，建议买入。"
        codes = IntradayMonitor._extract_stock_codes_from_decision_content(content)
        assert "600519" in codes

    def test_extract_parenthesis_pattern_full_width(self):
        content = "- **腾讯控股（HK00700）**：当前价 320.00，建议观望。"
        codes = IntradayMonitor._extract_stock_codes_from_decision_content(content)
        assert "HK00700" in codes

    def test_extract_us_ticker(self):
        content = "- **Apple（AAPL）**：当前价 220.00，建议买入。"
        codes = IntradayMonitor._extract_stock_codes_from_decision_content(content)
        assert "AAPL" in codes

    def test_filter_sentinel_codes(self):
        content = "<!-- INTRADAY_DECISION_COMPLETE expected=5 covered=5 -->\n建议买入 600519"
        codes = IntradayMonitor._extract_stock_codes_from_decision_content(content)
        assert "INTRADAY" not in codes
        assert "DECISION" not in codes
        assert "COMPLETE" not in codes
        assert "600519" in codes


class TestValidateDecisionCompleteness:
    """Tests for _validate_official_decision_completeness()."""

    @staticmethod
    def _validate(content, expected_codes):
        """Helper: call _validate_official_decision_completeness with a dummy self."""
        monitor = MagicMock()
        return IntradayMonitor._validate_official_decision_completeness(
            monitor, content, expected_codes,
        )

    def test_complete_with_sentinel(self):
        content = (
            "## 决策分析过程\n"
            "- **贵州茅台(600519)**：建议买入。\n"
            "- **五粮液(000858)**：建议观望。\n"
            "<!-- INTRADAY_DECISION_COMPLETE expected=2 covered=2 -->"
        )
        result = self._validate(content, {"600519", "000858"})
        assert result.ok is True
        assert result.sentinel_ok is True
        assert len(result.missing_codes) == 0

    def test_missing_stock_no_sentinel(self):
        content = (
            "## 决策分析过程\n"
            "- **贵州茅台(600519)**：建议买入。\n"
        )
        result = self._validate(content, {"600519", "000858"})
        assert result.ok is False
        assert result.sentinel_ok is False
        assert "000858" in result.missing_codes
        # 1 missing out of 2: max(2*0.2,1)=1, 1>1=False => not truncated
        assert result.truncated_suspected is False

    def test_truncated_multiple_missing_no_sentinel(self):
        """With 5 stocks and 2+ missing without sentinel, truncation is suspected."""
        content = (
            "## 决策分析过程\n"
            "- **600519**：建议买入。\n"
            "- **000858**：建议买入。\n"
            "- **002594**：建议买入。\n"
        )
        result = self._validate(
            content, {"600519", "000858", "002594", "300750", "601318"},
        )
        assert result.ok is False
        assert result.sentinel_ok is False
        assert len(result.missing_codes) == 2
        # 2 missing out of 5: max(5*0.2,1)=1, 2>1=True
        assert result.truncated_suspected is True

    def test_missing_single_stock_with_sentinel(self):
        content = (
            "## 决策分析过程\n"
            "- **600519**：建议买入。\n"
            "- **000858**：建议买入。\n"
            "- **002594**：建议买入。\n"
            "- **300750**：建议买入。\n"
            "- **601318**：建议买入。\n"
            "<!-- INTRADAY_DECISION_COMPLETE expected=6 covered=5 -->"
        )
        result = self._validate(
            content, {"600519", "000858", "002594", "300750", "601318", "600000"},
        )
        assert result.ok is False
        assert result.sentinel_ok is True
        assert "600000" in result.missing_codes
        assert result.truncated_suspected is False  # sentinel present, so not truncated

    def test_decision_content_integrity_dataclass(self):
        integrity = DecisionContentIntegrity(
            ok=True,
            expected_codes={"600519", "000858"},
            covered_codes={"600519", "000858"},
            missing_codes=set(),
            sentinel_ok=True,
            truncated_suspected=False,
            reason="passed",
        )
        assert integrity.ok is True
        assert integrity.reason == "passed"
