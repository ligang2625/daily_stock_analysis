# -*- coding: utf-8 -*-
"""Tests for intraday_relative_strength summarizer and prompt injection."""

from __future__ import annotations

import pytest


class TestRelativeStrength:
    """Test summarize_relative_strength with various market conditions."""

    def _make_stock_tl(self, code, market, changes, name=None):
        return {
            code: {
                'stock_name': name or code,
                'market': market,
                'points': [{'price': 100.0 + i, 'change_pct': chg} for i, chg in enumerate(changes)],
            }
        }

    def _make_market_tl(self, market, code, benchmark_changes):
        return {
            market: [{
                'index_code': code,
                'index_name': f'Benchmark {code}',
                'points': [{'price': 3000.0 + i, 'change_pct': chg} for i, chg in enumerate(benchmark_changes)],
            }]
        }

    def test_outperforming_market(self):
        """Stock up 3%, benchmark up 0.5% → outperforming."""
        from src.core.intraday_relative_strength import summarize_relative_strength
        stock_tl = self._make_stock_tl('600519', 'cn', [0.5, 1.0, 3.0])
        market_tl = self._make_market_tl('cn', '000001', [0.0, 0.2, 0.5])
        rs = summarize_relative_strength(stock_tl, market_tl)
        assert rs['600519']['relative_label'] == 'outperforming_market'
        assert rs['600519']['divergence_flag'] == 'same_direction'

    def test_underperforming_market(self):
        """Stock down 3%, benchmark down 0.5% → underperforming."""
        from src.core.intraday_relative_strength import summarize_relative_strength
        stock_tl = self._make_stock_tl('600519', 'cn', [-0.5, -1.0, -3.0])
        market_tl = self._make_market_tl('cn', '000001', [0.0, -0.2, -0.5])
        rs = summarize_relative_strength(stock_tl, market_tl)
        assert rs['600519']['relative_label'] == 'underperforming_market'
        assert rs['600519']['divergence_flag'] == 'same_direction'

    def test_stock_up_market_down_divergence(self):
        """Stock up 2%, benchmark down 1% → divergence."""
        from src.core.intraday_relative_strength import summarize_relative_strength
        stock_tl = self._make_stock_tl('HK00700', 'hk', [-0.5, 0.5, 2.0])
        market_tl = self._make_market_tl('hk', 'HSI', [0.0, -0.5, -1.0])
        rs = summarize_relative_strength(stock_tl, market_tl)
        assert rs['HK00700']['divergence_flag'] == 'stock_up_market_down'

    def test_stock_down_market_up_divergence(self):
        """Stock down 2%, benchmark up 1% → divergence."""
        from src.core.intraday_relative_strength import summarize_relative_strength
        stock_tl = self._make_stock_tl('AAPL', 'us', [0.5, -1.0, -2.0])
        market_tl = self._make_market_tl('us', '^GSPC', [0.0, 0.5, 1.0])
        rs = summarize_relative_strength(stock_tl, market_tl)
        assert rs['AAPL']['divergence_flag'] == 'stock_down_market_up'

    def test_market_data_missing(self):
        """No market timelines → market_data_missing label."""
        from src.core.intraday_relative_strength import summarize_relative_strength
        stock_tl = self._make_stock_tl('600519', 'cn', [0.5, 1.0, 2.0])
        rs = summarize_relative_strength(stock_tl, {})
        assert rs['600519']['relative_label'] == 'market_data_missing'
        assert rs['600519']['divergence_flag'] == 'insufficient'

    def test_insufficient_stock_data(self):
        """No stock change_pct data → insufficient."""
        from src.core.intraday_relative_strength import summarize_relative_strength
        stock_tl = {
            '600519': {
                'stock_name': '贵州茅台', 'market': 'cn',
                'points': [{'price': 100.0}],  # no change_pct
            }
        }
        market_tl = self._make_market_tl('cn', '000001', [0.5, 1.0])
        rs = summarize_relative_strength(stock_tl, market_tl)
        assert rs['600519']['stock_change_pct'] is None
        assert rs['600519']['stock_path_label'] == 'insufficient'

    def test_empty_inputs(self):
        """Empty stock_timelines returns empty dict."""
        from src.core.intraday_relative_strength import summarize_relative_strength
        rs = summarize_relative_strength({}, {})
        assert rs == {}

    def test_format_markdown_section(self):
        """format_relative_strength_section should produce a markdown table."""
        from src.core.intraday_relative_strength import (
            summarize_relative_strength,
            format_relative_strength_section,
        )
        stock_tl = self._make_stock_tl('600519', 'cn', [0.5, 1.0, 2.0])
        market_tl = self._make_market_tl('cn', '000001', [0.0, 0.2, 0.5])
        rs = summarize_relative_strength(stock_tl, market_tl)
        section = format_relative_strength_section(rs)
        assert '600519' in section
        assert '强于大盘' in section or '跟随大盘' in section
        assert '个股涨跌幅' in section
        assert '基准指数涨跌幅' in section

    def test_prompt_contains_relative_strength(self):
        """build_intraday_prompt should include system-computed relative strength."""
        from src.core.intraday_prompt import build_intraday_prompt
        stock_tl = self._make_stock_tl('600519', 'cn', [0.0, 0.5, 1.0])
        market_tl = self._make_market_tl('cn', '000001', [0.0, 0.2, 0.5])
        from src.core.intraday_relative_strength import (
            summarize_relative_strength,
            format_relative_strength_section,
        )
        rs = summarize_relative_strength(stock_tl, market_tl)
        section = format_relative_strength_section(rs)
        prompt = build_intraday_prompt(
            events=[],
            yesterday_analysis={},
            snapshot_times=['10:00', '14:20'],
            markets={'cn'},
            stock_timelines=stock_tl,
            market_timelines=market_tl,
            relative_strength_summary=section,
        )
        assert '个股相对大盘强弱（系统计算）' in prompt
        assert '600519' in prompt
