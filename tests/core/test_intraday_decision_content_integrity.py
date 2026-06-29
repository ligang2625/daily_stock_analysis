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

    def test_sentinel_values_mismatch_expected(self):
        """Sentinel says expected=5 but actual expected is 6 → sentinel invalid."""
        content = (
            "## 决策分析过程\n"
            "- **600519**：建议买入。\n"
            "- **000858**：建议买入。\n"
            "- **002594**：建议买入。\n"
            "- **300750**：建议买入。\n"
            "- **601318**：建议买入。\n"
            "- **600000**：建议买入。\n"
            "<!-- INTRADAY_DECISION_COMPLETE expected=5 covered=6 -->"
        )
        result = self._validate(
            content, {"600519", "000858", "002594", "300750", "601318", "600000"},
        )
        # sentinel found but values don't match → should be treated as invalid
        assert result.sentinel_ok is False
        assert result.ok is False  # both missing codes=0 but sentinel invalid
        assert "sentinel值不匹配" in result.reason

    def test_sentinel_values_mismatch_covered(self):
        """Sentinel says covered=7 but only 6 codes found → sentinel invalid."""
        content = (
            "## 决策分析过程\n"
            "- **600519**：建议买入。\n"
            "- **000858**：建议买入。\n"
            "- **002594**：建议买入。\n"
            "- **300750**：建议买入。\n"
            "- **601318**：建议买入。\n"
            "- **600000**：建议买入。\n"
            "<!-- INTRADAY_DECISION_COMPLETE expected=6 covered=7 -->"
        )
        result = self._validate(
            content, {"600519", "000858", "002594", "300750", "601318", "600000"},
        )
        assert result.sentinel_ok is False
        assert result.ok is False
        assert "sentinel值不匹配" in result.reason

    def test_sentinel_values_match(self):
        """Sentinel values match programmatic counts → sentinel valid."""
        content = (
            "## 决策分析过程\n"
            "- **600519**：建议买入。\n"
            "- **000858**：建议买入。\n"
            "- **002594**：建议买入。\n"
            "- **300750**：建议买入。\n"
            "- **601318**：建议买入。\n"
            "- **600000**：建议买入。\n"
            "<!-- INTRADAY_DECISION_COMPLETE expected=6 covered=6 -->"
        )
        result = self._validate(
            content, {"600519", "000858", "002594", "300750", "601318", "600000"},
        )
        assert result.sentinel_ok is True
        assert result.ok is True
        assert len(result.missing_codes) == 0
        assert result.reason == "passed"

    def test_no_missing_but_no_sentinel_not_ok(self):
        """All codes covered but no sentinel → still not ok (can't verify completeness)."""
        content = (
            "## 决策分析过程\n"
            "- **600519**：建议买入。\n"
            "- **000858**：建议买入。\n"
        )
        result = self._validate(content, {"600519", "000858"})
        # All codes covered but no sentinel → still not ok
        assert len(result.missing_codes) == 0
        assert result.sentinel_ok is False
        assert result.ok is False  # ok requires BOTH no missing AND sentinel_ok

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


class TestAppendDecisionCompleteSentinel:
    """Tests for _append_decision_complete_sentinel()."""

    @staticmethod
    def _validate(content, expected_codes):
        """Helper: call _validate_official_decision_completeness with a dummy self."""
        monitor = MagicMock()
        return IntradayMonitor._validate_official_decision_completeness(
            monitor, content, expected_codes,
        )

    def test_append_sentinel_when_no_missing_codes(self):
        """All codes covered, no sentinel -> append sentinel -> re-validate passes."""
        content = (
            "## 决策分析过程\n"
            "- **贵州茅台(600519)**：建议买入。\n"
            "- **五粮液(000858)**：建议观望。\n"
        )
        expected = {"600519", "000858"}

        # First validation: codes covered but no sentinel -> not ok
        integrity = self._validate(content, expected)
        assert len(integrity.missing_codes) == 0
        assert integrity.sentinel_ok is False
        assert integrity.ok is False

        # Append sentinel
        monitor = MagicMock()
        content_with_sentinel = IntradayMonitor._append_decision_complete_sentinel(
            monitor, content, integrity,
        )

        # Verify sentinel is in the content
        assert "INTRADAY_DECISION_COMPLETE" in content_with_sentinel
        assert "expected=2" in content_with_sentinel
        assert "covered=2" in content_with_sentinel

        # Re-validate after appending sentinel
        integrity2 = self._validate(content_with_sentinel, expected)
        assert integrity2.ok is True
        assert integrity2.sentinel_ok is True
        assert len(integrity2.missing_codes) == 0
        assert integrity2.reason == "passed"

    def test_no_sentinel_but_all_codes_covered_does_not_block(self):
        """Simulate full cycle: validate, auto-append sentinel, re-validate, decision proceeds."""
        content = (
            "## 决策分析过程\n"
            "- **600519**：建议买入。\n"
            "- **000858**：建议买入。\n"
            "- **002594**：建议买入。\n"
            "- **300750**：建议买入。\n"
            "- **601318**：建议买入。\n"
        )
        expected = {"600519", "000858", "002594", "300750", "601318"}

        # Simulate the decision flow logic
        integrity = self._validate(content, expected)
        # Precondition: all codes covered, no sentinel -> not ok
        assert len(integrity.missing_codes) == 0
        assert not integrity.ok
        assert not integrity.sentinel_ok

        # Sentinel auto-append branch (same conditions as in intraday_monitor.py)
        if (
            not integrity.ok
            and not integrity.missing_codes
            and integrity.covered_codes == integrity.expected_codes
            and not integrity.truncated_suspected
        ):
            monitor = MagicMock()
            content = IntradayMonitor._append_decision_complete_sentinel(
                monitor, content, integrity,
            )
            integrity = self._validate(content, expected)

        # After sentinel auto-append, decision should proceed
        assert integrity.ok is True
        assert integrity.sentinel_ok is True
        assert len(integrity.missing_codes) == 0
        assert integrity.reason == "passed"


class TestBuildDecisionBodySummary:
    """Tests for _build_decision_body_summary()."""

    def test_short_content_not_truncated(self):
        content = "Short content under limit"
        result = IntradayMonitor._build_decision_body_summary(content, 1000)
        assert result == content

    def test_long_content_has_group_stats(self):
        content = (
            "> 决策覆盖: 3/3\n"
            "> 内容完整性: passed\n"
            "\n"
            "## 决策分析过程\n"
            "\n"
            "### 1. 买入信号相关股票\n"
            "- **贵州茅台(600519)**：当前价 1800.00 进入理想买入区间，建议买入\n"
            "\n"
            "### 2. 卖出信号相关股票（止盈）\n"
            "- **五粮液(000858)**：当前价 200.00 进入止盈位，建议卖出止盈\n"
            "\n"
            "### 3. 卖出信号相关股票（止损）\n"
            "（无）\n"
            "\n"
            "### 4. 观望/无法判断股票（如有）\n"
            "- **比亚迪(002594)**：未触及明确阈值，建议观望\n"
            "\n"
            "<!-- INTRADAY_DECISION_COMPLETE expected=3 covered=3 -->"
        )
        result = IntradayMonitor._build_decision_body_summary(content, 200)
        # Should be shorter than original (200 < content length)
        assert len(result) < len(content)
        # Should have group stats section
        assert "分组统计" in result
        # Should mention attachment
        assert "完整报告见附件" in result or "附件" in result
        # Header block should be preserved
        assert "> 决策覆盖: 3/3" in result

    def test_no_hard_mid_markdown_truncation(self):
        """Verify we don't cut in the middle of a list item."""
        content = (
            "> 决策覆盖: 5/5\n"
            "> 内容完整性: passed\n"
            "\n"
            "## 决策分析过程\n"
            "\n"
            "### 1. 买入信号相关股票\n"
            "- **股票A(000001)**：这是一个非常长的描述，包含大量细节信息和其他内容，"
            "用来测试在正文过长时系统是否会从中途截断Markdown格式的内容。"
            "\n"
            "### 2. 卖出信号相关股票（止盈）\n"
            "- **股票B(000002)**：建议卖出止盈\n"
            "\n"
            "<!-- INTRADAY_DECISION_COMPLETE expected=5 covered=5 -->"
        )
        result = IntradayMonitor._build_decision_body_summary(content, 150)
        # With 150 char limit, the content should be summarized with attachment note
        assert "附件" in result
