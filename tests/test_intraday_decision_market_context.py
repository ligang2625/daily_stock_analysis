# -*- coding: utf-8 -*-
"""Tests for intraday decision market context: prompt contains market index and relative strength."""

from __future__ import annotations

from datetime import datetime

import pytest


class TestIntradayDecisionMarketContext:
    """Test that build_intraday_prompt includes market context correctly."""

    @pytest.fixture
    def stock_timelines(self):
        return {
            '600519': {
                'stock_name': '贵州茅台', 'market': 'cn',
                'points': [
                    {'price': 1500.0, 'change_pct': 0.0, 'timestamp': '10:00'},
                    {'price': 1510.0, 'change_pct': 0.67, 'timestamp': '10:30'},
                    {'price': 1520.0, 'change_pct': 1.33, 'timestamp': '14:20'},
                ],
            },
            'HK00700': {
                'stock_name': '腾讯', 'market': 'hk',
                'points': [
                    {'price': 380.0, 'change_pct': 0.0, 'timestamp': '10:00'},
                    {'price': 385.0, 'change_pct': 1.32, 'timestamp': '14:20'},
                ],
            },
        }

    @pytest.fixture
    def market_timelines(self):
        return {
            'cn': [{
                'index_code': '000001', 'index_name': '上证指数',
                'points': [
                    {'price': 3000.0, 'change_pct': 0.0, 'timestamp': '09:30'},
                    {'price': 3010.0, 'change_pct': 0.33, 'timestamp': '14:20'},
                ],
            }],
            'hk': [{
                'index_code': 'HSI', 'index_name': '恒生指数',
                'points': [
                    {'price': 20000.0, 'change_pct': 0.0, 'timestamp': '09:30'},
                    {'price': 20100.0, 'change_pct': 0.5, 'timestamp': '14:20'},
                ],
            }],
        }

    def test_prompt_contains_market_summary(self, stock_timelines, market_timelines):
        """Prompt should include market index summary section."""
        from src.core.intraday_prompt import build_intraday_prompt
        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=['10:00', '14:20'],
            markets={'cn', 'hk'},
            stock_timelines=stock_timelines,
            market_timelines=market_timelines,
        )
        assert '大盘指数走势摘要' in prompt
        assert '上证指数' in prompt
        assert '恒生指数' in prompt

    def test_prompt_contains_stock_timeline(self, stock_timelines, market_timelines):
        """Prompt should include stock intraday timeline summary."""
        from src.core.intraday_prompt import build_intraday_prompt
        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=['10:00', '14:20'],
            markets={'cn', 'hk'},
            stock_timelines=stock_timelines,
            market_timelines=market_timelines,
        )
        assert '当日个股走势摘要' in prompt
        assert '600519' in prompt
        assert 'HK00700' in prompt

    def test_prompt_contains_relative_strength_when_data_available(self, stock_timelines, market_timelines):
        """Prompt should have relative strength section when both timelines exist."""
        from src.core.intraday_prompt import build_intraday_prompt
        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=['10:00', '14:20'],
            markets={'cn', 'hk'},
            stock_timelines=stock_timelines,
            market_timelines=market_timelines,
        )
        assert '个股相对大盘强弱' in prompt

    def test_missing_market_data_warning(self, stock_timelines):
        """When market_timelines is empty, prompt should warn about missing data."""
        from src.core.intraday_prompt import build_intraday_prompt
        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=['10:00'],
            markets={'cn'},
            stock_timelines=stock_timelines,
            market_timelines=None,
        )
        assert '缺失' in prompt or '数据缺失' in prompt

    def test_no_stock_data_fallback(self, market_timelines):
        """When stock_timelines is empty, prompt should show fallback message."""
        from src.core.intraday_prompt import build_intraday_prompt
        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=['10:00'],
            markets={'cn'},
            stock_timelines={},
            market_timelines=market_timelines,
        )
        assert '无当日个股走势数据' in prompt

    def test_prompt_no_real_llm_call(self):
        """Test builds prompt without calling any real LLM API."""
        from src.core.intraday_prompt import build_intraday_prompt
        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=['10:00'],
            markets={'cn'},
        )
        assert isinstance(prompt, str)
        assert len(prompt) > 100
