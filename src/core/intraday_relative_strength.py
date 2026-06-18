# -*- coding: utf-8 -*-
"""
Relative strength summarizer for intraday decision prompt.

Computes structured stock-vs-market relative strength labels so the LLM
does not have to infer them from raw timeline tables.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Market → default benchmark index code mapping
BENCHMARK_INDEX: Dict[str, str] = {
    'cn': '000001',
    'hk': 'HSI',
    'us': '^GSPC',
}


def _stock_path_label(points: List[Dict[str, Any]]) -> str:
    """Classify stock intraday path based on change_pct trajectory."""
    changes = [p.get('change_pct') for p in points if p.get('change_pct') is not None]
    if not changes:
        return 'insufficient'

    latest = changes[-1]
    # Volatile if large swings
    if len(changes) >= 3:
        max_chg = max(changes)
        min_chg = min(changes)
        if (max_chg - min_chg) > 5:
            return 'volatile'

    if latest > 2:
        return 'rising'
    elif latest < -2:
        return 'falling'
    elif latest > 0.5:
        return 'rising'
    elif latest < -0.5:
        return 'falling'
    else:
        return 'flat'


def _get_benchmark_change(market_timelines: Dict[str, List[Dict[str, Any]]], market: str) -> Optional[float]:
    """Get the latest change_pct of the primary benchmark index for a market."""
    indices = market_timelines.get(market, [])
    benchmark_code = BENCHMARK_INDEX.get(market)
    if not benchmark_code:
        return None
    for idx in indices:
        if idx.get('index_code') == benchmark_code:
            points = idx.get('points', [])
            if points and points[-1].get('change_pct') is not None:
                return float(points[-1]['change_pct'])
    return None


def summarize_relative_strength(
    stock_timelines: Dict[str, Dict[str, Any]],
    market_timelines: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    """
    Compute relative strength summary for each stock vs its market benchmark.

    Args:
        stock_timelines: {stock_code: {stock_name, market, points: [{price, change_pct, ...}]}}
        market_timelines: {market: [{index_code, index_name, points: [{price, change_pct, ts}]}]}

    Returns:
        {stock_code: {stock_name, market, stock_change_pct, benchmark_change_pct,
                      relative_spread, relative_label, divergence_flag, stock_path_label}}
    """
    if not stock_timelines:
        return {}

    results: Dict[str, Dict[str, Any]] = {}

    for code, info in stock_timelines.items():
        market = info.get('market', 'cn')
        points = info.get('points', [])
        changes = [p.get('change_pct') for p in points if p.get('change_pct') is not None]
        stock_change_pct = changes[-1] if changes else None

        stock_path = _stock_path_label(points)

        benchmark_change = _get_benchmark_change(market_timelines, market)

        if stock_change_pct is not None and benchmark_change is not None:
            relative_spread = round(stock_change_pct - benchmark_change, 2)
            if relative_spread > 1:
                relative_label = 'outperforming_market'
            elif relative_spread < -1:
                relative_label = 'underperforming_market'
            else:
                relative_label = 'moving_with_market'

            # Divergence detection
            if stock_change_pct > 0 and benchmark_change < 0:
                divergence_flag = 'stock_up_market_down'
            elif stock_change_pct < 0 and benchmark_change > 0:
                divergence_flag = 'stock_down_market_up'
            else:
                divergence_flag = 'same_direction'
        else:
            relative_spread = None
            relative_label = 'market_data_missing'
            divergence_flag = 'insufficient'

        results[code] = {
            'stock_name': info.get('stock_name', code),
            'market': market,
            'stock_change_pct': stock_change_pct,
            'stock_path_label': stock_path,
            'benchmark_change_pct': benchmark_change,
            'relative_spread': relative_spread,
            'relative_label': relative_label,
            'divergence_flag': divergence_flag,
        }

    return results


def format_relative_strength_section(
    relative_strength: Dict[str, Dict[str, Any]],
) -> str:
    """Format the relative strength summary as a markdown section for the prompt."""
    if not relative_strength:
        return "（无个股数据，无法计算相对强弱）"

    lines: List[str] = []
    lines.append("| 股票 | 市场 | 个股涨跌幅 | 走势 | 基准指数涨跌幅 | 相对强弱 | 背离 |")
    lines.append("|------|------|-----------|------|---------------|----------|------|")

    market_labels = {'cn': 'A股', 'hk': '港股', 'us': '美股'}

    for code in sorted(relative_strength.keys()):
        rs = relative_strength[code]
        name = rs.get('stock_name', code)
        market_label = market_labels.get(rs.get('market', ''), rs.get('market', '').upper())

        stock_chg = f"{rs['stock_change_pct']:+.2f}%" if rs['stock_change_pct'] is not None else "-"
        bench_chg = f"{rs['benchmark_change_pct']:+.2f}%" if rs['benchmark_change_pct'] is not None else "-"

        path_label_map = {'rising': '上涨', 'falling': '下跌', 'flat': '震荡', 'volatile': '剧烈波动', 'insufficient': '数据不足'}
        path_label = path_label_map.get(rs.get('stock_path_label', ''), rs.get('stock_path_label', ''))

        relative_label_map = {
            'outperforming_market': '强于大盘',
            'underperforming_market': '弱于大盘',
            'moving_with_market': '跟随大盘',
            'market_data_missing': '大盘数据缺失',
        }
        rel_label = relative_label_map.get(rs.get('relative_label', ''), rs.get('relative_label', ''))

        divergence_map = {
            'stock_up_market_down': '⚠️ 个股涨/大盘跌',
            'stock_down_market_up': '⚠️ 个股跌/大盘涨',
            'same_direction': '同向',
            'insufficient': '无法判断',
        }
        div_label = divergence_map.get(rs.get('divergence_flag', ''), rs.get('divergence_flag', ''))

        lines.append(
            f"| {code}({name}) | {market_label} | {stock_chg} | {path_label} | "
            f"{bench_chg} | {rel_label} | {div_label} |"
        )

    return "\n".join(lines)
