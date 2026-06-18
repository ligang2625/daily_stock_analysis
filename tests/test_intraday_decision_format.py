# -*- coding: utf-8 -*-
"""Tests for intraday decision email grouped format."""

import pytest
from unittest.mock import MagicMock
import types

from src.core.intraday_prompt import build_intraday_prompt
from src.core.intraday_monitor import IntradayMonitor, IntradayEvent


class TestPromptGroupedFormat:
    """Prompt should not request markdown table and should define grouped output."""

    def test_prompt_no_markdown_table_requirement(self):
        prompt = build_intraday_prompt(events=[], yesterday_analysis={})
        assert "以Markdown表格形式输出最终决策摘要" not in prompt

    def test_prompt_contains_grouped_headers(self):
        prompt = build_intraday_prompt(events=[], yesterday_analysis={})
        assert "决策分析过程" in prompt
        assert "买入信号相关股票" in prompt
        assert "卖出信号相关股票（止盈）" in prompt
        assert "卖出信号相关股票（止损）" in prompt

    def test_prompt_allows_watch_group(self):
        prompt = build_intraday_prompt(events=[], yesterday_analysis={})
        assert "观望/无法判断" in prompt

    def test_prompt_buy_sell_grouping_instruction(self):
        prompt = build_intraday_prompt(events=[], yesterday_analysis={})
        assert "按买入/卖出止盈/卖出止损/观望分组" in prompt

    def test_prompt_no_markdown_table_example(self):
        prompt = build_intraday_prompt(events=[], yesterday_analysis={})
        assert "| 股票 | 当前价 |" not in prompt
        assert "|------|--------|" not in prompt

    def test_prompt_empty_and_normal_params(self):
        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={"600519": {"ideal_buy": 10.0}},
            stock_timelines={
                "600519": {
                    "stock_name": "茅台",
                    "points": [{"price": 10.5, "change_pct": 0.5, "timestamp": "2026-01-01T10:00:00"}],
                }
            },
        )
        assert len(prompt) > 100
        assert "600519" in prompt


def _make_monitor_with_normalizer():
    """Create a mock IntradayMonitor bound with the real normalizer."""
    monitor = MagicMock(spec=IntradayMonitor)
    monitor._normalize_official_decision_content = types.MethodType(
        IntradayMonitor._normalize_official_decision_content, monitor
    )
    return monitor


class TestNormalizeOfficialDecision:
    """Test the normalizer function."""

    def test_empty_content(self):
        monitor = _make_monitor_with_normalizer()
        assert monitor._normalize_official_decision_content("") == ""

    def test_none_content(self):
        monitor = _make_monitor_with_normalizer()
        assert monitor._normalize_official_decision_content(None) is None

    def test_already_grouped_preserved(self):
        grouped = (
            "## 决策分析过程\n\n"
            "### 1. 买入信号相关股票\n"
            "- **测试股票（000001）**：当前价 10.0，建议买入。\n\n"
            "### 2. 卖出信号相关股票（止盈）\n"
            "（无）\n\n"
            "### 3. 卖出信号相关股票（止损）\n"
            "（无）\n\n"
            "### 4. 观望/无法判断股票（如有）\n"
            "（无）"
        )
        monitor = _make_monitor_with_normalizer()
        result = monitor._normalize_official_decision_content(grouped)
        assert "买入信号相关股票" in result
        assert "测试股票" in result
        assert "卖出信号相关股票（止损）" in result

    def test_already_grouped_strips_lingering_table(self):
        content = (
            "## 决策摘要\n\n"
            "### 1. 买入信号相关股票\n"
            "- **股票A**：买入\n\n"
            "| 股票 | 当前价 | 建议 |\n"
            "|------|--------|------|\n"
        )
        monitor = _make_monitor_with_normalizer()
        result = monitor._normalize_official_decision_content(content)
        assert "买入信号相关股票" in result
        assert "股票A" in result
        assert "| 股票 | 当前价 |" not in result

    def test_legacy_table_converted(self):
        legacy = (
            "## 决策摘要\n\n"
            "| 股票 | 当前价 | 日内走势 | 大盘环境 | 相对强弱 | 数据质量 | 建议 | 理由 |\n"
            "|------|--------|----------|----------|----------|----------|------|------|\n"
            "| 600519(茅台) | 1850.00 | 震荡 | 上证+0.5% | 弱于大盘 | 正常 | 买入 | 触及次级买入区间 |\n"
            "| 000002(万科) | 8.50 | 下跌 | 上证-0.3% | 同步大盘 | 正常 | 卖出止盈 | 突破止盈位 |\n"
            "| 000003(某股) | 5.00 | 下跌 | 上证-0.3% | 弱于大盘 | 正常 | 卖出止损 | 跌破止损位 |\n"
            "| 000004(持有) | 12.00 | 震荡 | 上证+0.1% | 同步 | 正常 | 持有 | 未触及阈值 |\n\n"
            "> 风险提示\n"
        )
        monitor = _make_monitor_with_normalizer()
        result = monitor._normalize_official_decision_content(legacy)
        assert "买入信号相关股票" in result
        assert "卖出信号相关股票（止盈）" in result
        assert "卖出信号相关股票（止损）" in result
        assert "600519" in result or "茅台" in result
        assert "000002" in result or "万科" in result
        assert "000003" in result
        assert "000004" in result or "持有" in result
        assert "| 股票 | 当前价 |" not in result

    def test_unparseable_passthrough(self):
        monitor = _make_monitor_with_normalizer()
        gibberish = "Some random text\nwith no recognizable format\n"
        result = monitor._normalize_official_decision_content(gibberish)
        assert result == gibberish


class TestClassifyEventTypeForDisplay:
    """Test event type classification."""

    def test_buy_types(self):
        from src.core.intraday_monitor import classify_event_type_for_display
        assert classify_event_type_for_display("enter_ideal_buy") == "buy"
        assert classify_event_type_for_display("enter_secondary_buy") == "buy"
        assert classify_event_type_for_display("near_buy_zone") == "buy"

    def test_take_profit(self):
        from src.core.intraday_monitor import classify_event_type_for_display
        assert classify_event_type_for_display("enter_take_profit") == "take_profit"

    def test_stop_loss(self):
        from src.core.intraday_monitor import classify_event_type_for_display
        assert classify_event_type_for_display("break_stop_loss") == "stop_loss"

    def test_unusual_volume(self):
        from src.core.intraday_monitor import classify_event_type_for_display
        assert classify_event_type_for_display("unusual_volume") == "volume"

    def test_price_only(self):
        from src.core.intraday_monitor import classify_event_type_for_display
        assert classify_event_type_for_display("price_only") == "watch"

    def test_unknown_type(self):
        from src.core.intraday_monitor import classify_event_type_for_display
        assert classify_event_type_for_display("some_random_type") == "watch"


class TestAggregateEventsForDisplay:
    """Test event aggregation merges unusual_volume correctly."""

    def test_single_stock_buy(self):
        from src.core.intraday_monitor import aggregate_events_for_display
        from datetime import datetime
        ev = IntradayEvent(
            timestamp=datetime.now(),
            stock_code="000001",
            stock_name="平安银行",
            current_price=12.0,
            volume_ratio=1.5,
            change_pct=0.5,
            event_type="enter_ideal_buy",
            description="test",
        )
        result = aggregate_events_for_display([ev])
        assert len(result["buy"]) == 1
        assert result["buy"][0]["stock_code"] == "000001"
        assert result["buy"][0]["has_unusual_volume"] is False

    def test_volume_merged_into_price_event(self):
        from src.core.intraday_monitor import aggregate_events_for_display
        from datetime import datetime
        now = datetime.now()
        price_ev = IntradayEvent(
            timestamp=now,
            stock_code="000001",
            stock_name="平安银行",
            current_price=12.0,
            volume_ratio=3.5,
            change_pct=0.5,
            event_type="enter_take_profit",
            description="price event",
        )
        volume_ev = IntradayEvent(
            timestamp=now,
            stock_code="000001",
            stock_name="平安银行",
            current_price=12.0,
            volume_ratio=3.5,
            change_pct=0.5,
            event_type="unusual_volume",
            description="unusual volume",
        )
        result = aggregate_events_for_display([price_ev, volume_ev])
        assert len(result["take_profit"]) == 1
        tp_entry = result["take_profit"][0]
        assert tp_entry["has_unusual_volume"] is True
        assert tp_entry["max_volume_ratio"] == 3.5
        assert len(result["buy"]) == 0
        assert len(result["stop_loss"]) == 0

    def test_two_stocks_different_groups(self):
        from src.core.intraday_monitor import aggregate_events_for_display
        from datetime import datetime
        now = datetime.now()
        ev1 = IntradayEvent(
            timestamp=now, stock_code="000001", stock_name="A",
            current_price=10.0, volume_ratio=1.0, change_pct=0.5,
            event_type="enter_ideal_buy", description="buy",
        )
        ev2 = IntradayEvent(
            timestamp=now, stock_code="000002", stock_name="B",
            current_price=8.0, volume_ratio=1.0, change_pct=-1.0,
            event_type="break_stop_loss", description="stop loss",
        )
        result = aggregate_events_for_display([ev1, ev2])
        assert len(result["buy"]) == 1
        assert len(result["stop_loss"]) == 1
        assert result["buy"][0]["stock_code"] == "000001"
        assert result["stop_loss"][0]["stock_code"] == "000002"

    def test_unusual_volume_only_goes_to_watch(self):
        from src.core.intraday_monitor import aggregate_events_for_display
        from datetime import datetime
        ev = IntradayEvent(
            timestamp=datetime.now(), stock_code="000001", stock_name="A",
            current_price=10.0, volume_ratio=4.0, change_pct=0.5,
            event_type="unusual_volume", description="just volume",
        )
        result = aggregate_events_for_display([ev])
        assert len(result["watch"]) == 1
        assert result["watch"][0]["has_unusual_volume"] is True
        assert len(result["buy"]) == 0
        assert len(result["take_profit"]) == 0
        assert len(result["stop_loss"]) == 0

    def test_empty_events(self):
        from src.core.intraday_monitor import aggregate_events_for_display
        result = aggregate_events_for_display([])
        assert len(result["buy"]) == 0
        assert len(result["take_profit"]) == 0
        assert len(result["stop_loss"]) == 0
        assert len(result["watch"]) == 0
