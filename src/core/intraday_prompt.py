# -*- coding: utf-8 -*-
"""
===================================
盘中决策 Prompt 模板
===================================

构造 14:20 LLM 决策所用的 prompt，包含：
1. 昨日盘后分析结论（阈值摘要）
2. 当日盘中事件时间线
3. 当前价格与量比快照
4. 开盘价 vs 昨日收盘价对比（跳空检测）
5. 大盘指数上下文（上证指数、深证成指、创业板指、恒生指数、标普500等）
6. 当日个股走势摘要（所有快照的价格/涨跌幅/量比路径）
7. 大盘指数走势摘要（所有快照的指数点位变化路径）
8. 个股相对大盘强弱分析
"""

from datetime import datetime, date
from typing import Any, Dict, List, Optional


def format_intraday_event_time(event: Any, include_market: bool = False) -> str:
    """Format event time: prefer market_local_timestamp, fallback to event timestamp."""
    mlt = getattr(event, 'market_local_timestamp', None)
    time_str = "--:--"
    if mlt:
        try:
            dt = datetime.fromisoformat(str(mlt))
            time_str = dt.strftime("%H:%M")
        except (ValueError, TypeError):
            pass
    if time_str == "--:--" and hasattr(event, 'timestamp') and event.timestamp:
        time_str = event.timestamp.strftime("%H:%M")

    if include_market:
        market = getattr(event, 'market', None)
        if market:
            market_label = market.upper()
            if market == 'us':
                return f"{market_label} {time_str} ET"
            return f"{market_label} {time_str}"
    return time_str


def _fmt_price(val: Optional[float]) -> str:
    """Format a float price value for display."""
    if val is None:
        return "-"
    return f"{val:.2f}"


def _fmt_pct(val: Optional[float]) -> str:
    """Format a percentage value for display with +/- sign."""
    if val is None:
        return "-"
    return f"{val:+.2f}%"


def _fmt_stock_timeline_summary(timelines: Dict[str, Dict[str, Any]]) -> str:
    """Build a compact per-stock day timeline summary for the prompt.

    Each stock gets one line with: code, name, open, high, low, latest, day_change,
    and a compact price path showing price points across snapshots.
    """
    if not timelines:
        return "（无当日个股走势数据）"

    lines: List[str] = []
    lines.append("| 股票 | 开盘 | 最高 | 最低 | 最新 | 日涨跌 | 走势路径 |")
    lines.append("|------|------|------|------|------|--------|----------|")
    for code in sorted(timelines.keys()):
        info = timelines[code]
        name = info.get('stock_name', code)
        points = info.get('points', [])
        # Collect valid price points
        prices = [p['price'] for p in points if p.get('price') is not None]
        changes = [p['change_pct'] for p in points if p.get('change_pct') is not None]
        if not prices:
            lines.append(f"| {code}({name}) | - | - | - | - | - | 无有效行情 |")
            continue
        open_price = prices[0]
        high_price = max(prices)
        low_price = min(prices)
        latest_price = prices[-1]
        day_change = changes[-1] if changes else None
        # Build compact price path (up to 8 points)
        path_points = prices[:8] if len(prices) <= 8 else prices[::max(len(prices)//7, 1)][:7] + [prices[-1]]
        ts_labels = []
        for p in points[:8] if len(points) <= 8 else points[::max(len(points)//7, 1)][:7]:
            ts = p.get('timestamp', '')
            if ts:
                try:
                    dt = datetime.fromisoformat(str(ts))
                    ts_labels.append(dt.strftime("%H:%M"))
                except (ValueError, TypeError):
                    ts_labels.append('')
            else:
                ts_labels.append('')
        if len(points) > 8:
            ts_labels.append('')
        path_str = " -> ".join(f"{ts_labels[i]}:{p:.2f}" if ts_labels[i] else f"{p:.2f}"
                               for i, p in enumerate(path_points))
        lines.append(
            f"| {code}({name}) | {_fmt_price(open_price)} | {_fmt_price(high_price)} | "
            f"{_fmt_price(low_price)} | {_fmt_price(latest_price)} | {_fmt_pct(day_change)} | {path_str} |"
        )
    return "\n".join(lines)


def _fmt_market_timeline_summary(market_timelines: Dict[str, List[Dict[str, Any]]]) -> str:
    """Build a per-market index timeline summary for the prompt.

    Each index gets one line: index name, open, high, low, latest, day_change, and trend.
    Data quality status is included when indices are missing or unavailable.
    """
    if not market_timelines:
        return "（无大盘指数数据 — 不得臆测大盘方向）"

    lines: List[str] = []
    lines.append("| 市场 | 指数 | 开盘 | 最高 | 最低 | 最新 | 涨跌幅 | 趋势 |")
    lines.append("|------|------|------|------|------|------|--------|------|")
    for market in sorted(market_timelines.keys()):
        indices = market_timelines[market]
        if not indices:
            market_label = {'cn': 'A股', 'hk': '港股', 'us': '美股'}.get(market, market.upper())
            lines.append(f"| {market_label} | - | - | - | - | - | - | 无指数数据 |")
            continue
        market_label = {'cn': 'A股', 'hk': '港股', 'us': '美股'}.get(market, market.upper())
        for idx in indices:
            points = idx.get('points', [])
            if not points:
                lines.append(
                    f"| {market_label} | {idx.get('index_name', idx['index_code'])} | - | - | - | - | - | 无快照数据 |"
                )
                continue
            first = points[0]
            last = points[-1]
            # Compute high/low across all snapshot points
            all_prices = [p['price'] for p in points if p.get('price') is not None]
            open_p = first.get('price')
            high_p = max(all_prices) if all_prices else None
            low_p = min(all_prices) if all_prices else None
            latest_p = last.get('price')
            day_chg = last.get('change_pct')

            # Determine trend
            if len(all_prices) >= 2 and all_prices[0] and all_prices[-1]:
                start_val, end_val = all_prices[0], all_prices[-1]
                if end_val > start_val * 1.005:
                    trend = "上涨"
                elif end_val < start_val * 0.995:
                    trend = "下跌"
                else:
                    trend = "震荡"
            else:
                trend = "数据不足"

            lines.append(
                f"| {market_label} | {idx.get('index_name', idx['index_code'])} | "
                f"{_fmt_price(open_p)} | {_fmt_price(high_p)} | {_fmt_price(low_p)} | "
                f"{_fmt_price(latest_p)} | {_fmt_pct(day_chg)} | {trend} |"
            )
    return "\n".join(lines)


def build_intraday_prompt(
    *,
    events: List[Any],
    yesterday_analysis: Dict[str, Dict[str, Optional[float]]],
    snapshot_times: Optional[List[str]] = None,
    markets: Optional[set] = None,
    stock_timelines: Optional[Dict[str, Dict[str, Any]]] = None,
    market_timelines: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    market_timeline_stats: Optional[Dict[str, Any]] = None,
    historical_snapshot_count: int = 0,
) -> str:
    """
    Build the LLM prompt for 14:20 intraday decision.

    Args:
        events: List of IntradayEvent objects for today.
        yesterday_analysis: {code: {ideal_buy, secondary_buy, stop_loss, take_profit}}.
        snapshot_times: List of HH:MM snapshot times executed today.
        markets: Set of market codes (e.g. {'cn', 'hk'}) for context-aware prompt.
        stock_timelines: {stock_code: {stock_name, market, points: [{price, change_pct, volume_ratio, ts, ...}]}}
        market_timelines: {market: [{index_code, index_name, points: [{price, change_pct, ts}], open_price, ...}]}

    Returns:
        Prompt string ready for litellm.completion().
    """
    markets_set = markets or {'cn'}
    has_cn = 'cn' in markets_set
    has_hk = 'hk' in markets_set
    has_us = 'us' in markets_set
    is_mixed = len(markets_set) > 1

    today_str = date.today().isoformat()

    if is_mixed:
        advisor = "多市场股票交易分析助手"
    elif has_hk:
        advisor = "专业港股交易顾问"
    elif has_us:
        advisor = "专业美股交易顾问"
    else:
        advisor = "专业A股交易顾问"

    lines: List[str] = [
        f"# 盘中监控决策请求 — {today_str}",
        "",
        f"你是一位{advisor}。请根据以下盘中事件和昨日分析结论，生成买卖决策建议。",
        "",
        "## 指令",
        "",
        "1. 综合昨日盘后分析阈值和今日盘中实时走势，给出每支监控股票的买卖建议（买入/卖出/持有）。",
        "2. 买入建议需说明：触及哪个买入区间、当前价格、量比是否支持。",
        "3. 卖出建议需说明：触及止损位还是止盈位。",
        "4. 如有异常放量（量比>=3），单独评价其对买卖信号的影响。",
        '5. 如有停牌或数据缺失的股票，标注为"无法判断"。',
        '6. 如果开盘价与昨日收盘价跳空超过3%，请标注"阈值可能已失效"',
        "7. 参考大盘指数方向（如"
        + (
            "各市场主要指数" if is_mixed
            else "上证指数SSE Composite、深证成指、创业板指" if has_cn
            else "恒生指数HSI、恒生科技指数HSTECH" if has_hk
            else "标普500 S&P 500、纳斯达克NASDAQ及相关行业ETF"
        )
        + "）判断市场整体情绪。",
        "8. 结合当日个股走势路径（开盘→高/低点→当前）判断趋势方向，不要只依赖当前单点价格。",
        "9. 观察个股相对大盘强弱：若大盘上涨而个股下跌（或反之），在理由中说明背离风险。",
        "10. 以Markdown表格形式输出最终决策摘要，包含日内走势、大盘环境和相对强弱。",
        '11. 每行必须引用至少一个指数状态，或说明"指数数据缺失"。',
        "",
        "## 输出格式",
        "",
        "| 股票 | 当前价 | 日内走势 | 大盘环境 | 相对强弱 | 数据质量 | 建议 | 理由 |",
        "|------|--------|----------|----------|----------|----------|------|------|",
        "| 600519(茅台) | 1850.00 | 开1830→高1860→低1820→收1850(震荡) | 上证+0.5% 上行 | 弱于大盘 | 正常 | 买入 | 触及次级买入区间，量比正常 |",
        "",
        "---",
        "",
    ]

    # --- Yesterday analysis summary ---
    lines.append("## 昨日盘后分析阈值")
    lines.append("")
    if yesterday_analysis:
        lines.append("| 股票代码 | 理想买入 | 次级买入 | 止损位 | 止盈位 |")
        lines.append("|----------|----------|----------|--------|--------|")
        for code, points in sorted(yesterday_analysis.items()):
            ib = _fmt(points.get("ideal_buy"))
            sb = _fmt(points.get("secondary_buy"))
            sl = _fmt(points.get("stop_loss"))
            tp = _fmt(points.get("take_profit"))
            lines.append(f"| {code} | {ib} | {sb} | {sl} | {tp} |")
    else:
        lines.append("（无昨日分析数据）")
    lines.append("")

    # --- Snapshot times ---
    if snapshot_times:
        lines.append("## 今日快照时间点")
        lines.append("")
        lines.append(", ".join(sorted(set(snapshot_times))))
        lines.append("")

    # --- Historical snapshot context (standalone decision) ---
    if 0 < historical_snapshot_count < 2:
        lines.append("## ⚠️ 日内走势数据不足")
        lines.append("")
        if historical_snapshot_count == 0:
            lines.append("当前仅有决策快照，缺少日内历史走势数据。请基于当前单点价格和昨日阈值给出判断，并在理由中注明'仅有当前快照，趋势不确定'。")
        else:
            lines.append("仅有{historical_snapshot_count}个历史快照和当前决策快照，走势数据有限。请谨慎推断趋势方向。")
        lines.append("")

    # --- Market index timeline summary ---
    lines.append("## 大盘指数走势摘要")
    lines.append("")
    if market_timeline_stats:
        market_label = {'cn': 'A股', 'hk': '港股', 'us': '美股'}
        for market in sorted(market_timelines.keys()):
            ms = market_timeline_stats.get(market, {})
            label = market_label.get(market, market.upper())
            if not ms or not ms.get('expected_indices'):
                lines.append(f"- {label}: 📡 大盘指数未采集")
            elif ms.get('failed_indices') and len(ms.get('failed_indices', [])) == len(ms.get('expected_indices', [])):
                lines.append(f"- {label}: ⚠️ 大盘指数采集失败: {ms['failed_indices']}")
            elif ms.get('valid_snapshot_count', 0) > 0:
                lines.append(f"- {label}: 📊 大盘指数走势 ({ms['valid_snapshot_count']}/{ms.get('total_snapshots', 0)} 有效)")
            else:
                lines.append(f"- {label}: 📡 大盘指数无有效数据")
            if ms.get('unavailable_indices'):
                lines.append(f"  - 未采集到: {ms['unavailable_indices']}")
        lines.append("")
    elif market_timelines:
        lines.append(_fmt_market_timeline_summary(market_timelines))
    else:
        lines.append("（大盘指数数据缺失，请勿臆测大盘方向，仅基于个股数据给出判断）")
    lines.append("")

    # --- Stock intraday timeline summary ---
    lines.append("## 当日个股走势摘要")
    lines.append("")
    if stock_timelines:
        lines.append(_fmt_stock_timeline_summary(stock_timelines))
    else:
        lines.append("（无当日个股走势数据）")
    lines.append("")

    # --- Relative strength analysis ---
    lines.append("## 个股相对大盘强弱")
    lines.append("")
    if market_timelines and stock_timelines:
        lines.append("请对比上面的「个股走势摘要」与「大盘指数走势摘要」，判断每支股票的日内走势是否与大盘同步：")
        lines.append("- 若个股跟随大盘上涨/下跌，趋势可信度较高")
        lines.append("- 若个股逆大盘上涨（大盘跌个股涨），可能受到特殊消息驱动，关注持续性")
        lines.append("- 若个股逆大盘下跌（大盘涨个股跌），可能表明资金流出，需警惕风险")
        lines.append("- 若指数数据缺失，不得臆测大盘方向，仅基于个股自身走势判断")
    else:
        lines.append("（大盘指数数据缺失，无法计算相对强弱，请仅基于个股自身走势判断）")
    lines.append("")

    # --- Intraday event timeline (latest snapshot events for detail) ---
    lines.append("## 盘中事件时间线（最新快照详情）")
    lines.append("")
    if events:
        lines.append("| 时间 | 股票 | 价格 | 量比 | 涨跌幅 | 事件类型 | 描述 |")
        lines.append("|------|------|------|------|--------|----------|------|")
        for e in events:
            vol = f"{e.volume_ratio:.1f}" if e.volume_ratio is not None else "-"
            chg = f"{e.change_pct:+.2f}%" if e.change_pct is not None else "-"
            lines.append(
                f"| {format_intraday_event_time(e, include_market=is_mixed)} | {e.stock_name}({e.stock_code}) | "
                f"{e.current_price:.2f} | {vol} | {chg} | {e.event_type} | {e.description} |"
            )
    else:
        lines.append("（无盘中事件）")
    lines.append("")

    # --- Market context note ---
    lines.append("## 市场背景")
    lines.append("")
    if is_mixed:
        lines.append("请结合各市场主要指数的整体方向与当日走势路径，判断个股走势是否与大盘同步。")
    elif has_cn:
        lines.append("请结合当日上证指数（SSE Composite）、深证成指和创业板指的整体方向与走势路径，判断个股走势是否与大盘同步。")
    elif has_hk:
        lines.append("请结合当日恒生指数（HSI）和恒生科技指数（HSTECH）的整体方向与走势路径，判断个股走势是否与大盘同步。")
    elif has_us:
        lines.append("请结合当日标普500（S&P 500）、纳斯达克指数（NASDAQ）和相关板块ETF的整体方向与走势路径，判断个股走势是否与大盘同步。")
    lines.append("若个股与大盘背离，在理由中说明。")
    lines.append("")

    # --- Gap validation ---
    lines.append("## 跳空验证")
    lines.append("")
    lines.append('对比开盘价与昨日收盘价：若跳空幅度超过3%，标注"阈值可能已失效（跳空>3%）"，')
    lines.append("此时昨日盘后分析得出的买入/止损/止盈阈值可能不再适用。")
    lines.append("")

    # --- Risk disclaimer ---
    lines.append("## 风险提示")
    lines.append("")
    lines.append("> 本报告由AI自动生成，仅供参考，不构成投资建议。")
    if has_cn:
        lines.append("> 场外基金申购截止时间为每个交易日15:00，请在此时间前完成操作。")
    lines.append("> 投资有风险，入市需谨慎。")

    return "\n".join(lines)


def _fmt(val: Optional[float]) -> str:
    """Format float for display, '-' if None."""
    if val is None:
        return "-"
    return f"{val:.2f}"
