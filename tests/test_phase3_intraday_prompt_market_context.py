# -*- coding: utf-8 -*-
"""Tests for Phase 3: build_intraday_prompt market context sections.

Focus: market_timeline_stats + market_timelines coexistence,
historical_snapshot_count=0/1/2 paths, and relative strength injection.
"""

from __future__ import annotations

import pytest


class TestPhase3PromptMarketContext:
    """Test build_intraday_prompt market index and snapshot count sections."""

    @pytest.fixture
    def stock_timelines(self):
        return {
            "600519": {
                "stock_name": "贵州茅台",
                "market": "cn",
                "points": [
                    {"price": 1500.0, "change_pct": 0.0, "timestamp": "10:00"},
                    {"price": 1510.0, "change_pct": 0.67, "timestamp": "14:20"},
                ],
            }
        }

    @pytest.fixture
    def market_timelines(self):
        return {
            "cn": [
                {
                    "index_code": "000001",
                    "index_name": "上证指数",
                    "points": [
                        {"price": 3000.0, "change_pct": 0.0, "timestamp": "09:30"},
                        {"price": 3010.0, "change_pct": 0.33, "timestamp": "14:20"},
                    ],
                }
            ]
        }

    @pytest.fixture
    def market_timeline_stats(self):
        return {
            "cn": {
                "expected_indices": ["000001", "399001"],
                "valid_snapshot_count": 2,
                "total_snapshots": 4,
                "failed_indices": [],
                "unavailable_indices": ["399001"],
            }
        }

    def test_prompt_contains_both_stats_and_trends(self, stock_timelines, market_timelines, market_timeline_stats):
        """When both stats and timelines provided, prompt must contain both data quality and trends."""
        from src.core.intraday_prompt import build_intraday_prompt

        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=["10:00", "14:20"],
            markets={"cn"},
            stock_timelines=stock_timelines,
            market_timelines=market_timelines,
            market_timeline_stats=market_timeline_stats,
        )
        # Must have data quality section
        assert "数据质量" in prompt
        # Must have index trend table
        assert "指数走势" in prompt
        # Must have actual index name and price
        assert "上证指数" in prompt
        assert "3000" in prompt or "3010" in prompt
        # Must NOT be just coverage stats
        assert "2/4 有效" in prompt or "2/4" in prompt

    def test_prompt_trends_without_stats(self, stock_timelines, market_timelines):
        """Without stats but with market_timelines, trends table should appear directly."""
        from src.core.intraday_prompt import build_intraday_prompt

        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=["10:00"],
            markets={"cn"},
            stock_timelines=stock_timelines,
            market_timelines=market_timelines,
            market_timeline_stats=None,
        )
        # Must have trends even without stats
        assert "指数走势" in prompt or "大盘指数" in prompt
        assert "上证指数" in prompt
        # No ### 数据质量 heading when stats absent (text may appear elsewhere)
        assert "### 数据质量" not in prompt

    def test_no_market_timelines_fallback(self, stock_timelines):
        """When market_timelines is missing, prompt must warn against speculation."""
        from src.core.intraday_prompt import build_intraday_prompt

        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=["10:00"],
            markets={"cn"},
            stock_timelines=stock_timelines,
            market_timelines=None,
        )
        assert "勿臆测大盘方向" in prompt or "数据缺失" in prompt

    def test_historical_snapshot_count_zero(self, stock_timelines, market_timelines):
        """When historical_snapshot_count=0, prompt must warn about single-point data."""
        from src.core.intraday_prompt import build_intraday_prompt

        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=["10:00"],
            markets={"cn"},
            stock_timelines=stock_timelines,
            market_timelines=market_timelines,
            historical_snapshot_count=0,
        )
        assert "仅有当前快照" in prompt or "仅有决策快照" in prompt
        assert "趋势不确定" in prompt or "谨慎" in prompt

    def test_historical_snapshot_count_one(self, stock_timelines, market_timelines):
        """When historical_snapshot_count=1, prompt must warn about limited data."""
        from src.core.intraday_prompt import build_intraday_prompt

        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=["10:00", "14:20"],
            markets={"cn"},
            stock_timelines=stock_timelines,
            market_timelines=market_timelines,
            historical_snapshot_count=1,
        )
        assert "走势数据有限" in prompt
        assert "谨慎" in prompt

    def test_historical_snapshot_count_two(self, stock_timelines, market_timelines):
        """When historical_snapshot_count>=2, no data-insufficient warning."""
        from src.core.intraday_prompt import build_intraday_prompt

        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=["10:00", "14:20"],
            markets={"cn"},
            stock_timelines=stock_timelines,
            market_timelines=market_timelines,
            historical_snapshot_count=2,
        )
        assert "日内走势数据不足" not in prompt
        assert "仅有当前快照" not in prompt
        assert "走势数据有限" not in prompt

    def test_relative_strength_summary_injected(self, stock_timelines, market_timelines):
        """When relative_strength_summary provided, it should appear in prompt."""
        from src.core.intraday_prompt import build_intraday_prompt

        rs_section = "### 个股相对强弱\n- 600519: 强于大盘"
        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=["10:00", "14:20"],
            markets={"cn"},
            stock_timelines=stock_timelines,
            market_timelines=market_timelines,
            relative_strength_summary=rs_section,
        )
        assert "个股相对大盘强弱（系统计算）" in prompt
        assert "强于大盘" in prompt
