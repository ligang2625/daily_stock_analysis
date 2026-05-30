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
5. 大盘指数上下文（SSE Composite 方向）
"""

from datetime import datetime, date
from typing import Any, Dict, List, Optional


def build_intraday_prompt(
    *,
    events: List[Any],
    yesterday_analysis: Dict[str, Dict[str, Optional[float]]],
    snapshot_times: Optional[List[str]] = None,
) -> str:
    """
    Build the LLM prompt for 14:20 intraday decision.

    Args:
        events: List of IntradayEvent objects for today.
        yesterday_analysis: {code: {ideal_buy, secondary_buy, stop_loss, take_profit}}.
        snapshot_times: List of HH:MM snapshot times executed today.

    Returns:
        Prompt string ready for litellm.completion().
    """
    today_str = date.today().isoformat()

    lines: List[str] = [
        f"# 盘中监控决策请求 — {today_str}",
        "",
        "你是一位专业A股交易顾问。请根据以下盘中事件和昨日分析结论，生成买卖决策建议。",
        "",
        "## 指令",
        "",
        "1. 综合昨日盘后分析阈值和今日盘中实时走势，给出每支监控股票的买卖建议（买入/卖出/持有）。",
        "2. 买入建议需说明：触及哪个买入区间、当前价格、量比是否支持。",
        "3. 卖出建议需说明：触及止损位还是止盈位。",
        "4. 如有异常放量（量比>=3），单独评价其对买卖信号的影响。",
        '5. 如有停牌或数据缺失的股票，标注为"无法判断"。',
        '6. 如果开盘价与昨日收盘价跳空超过3%，请标注"阈值可能已失效"',
        "7. 参考大盘指数方向（如SSE Composite）判断市场整体情绪。",
        "8. 以Markdown表格形式输出最终决策摘要，并在表格后附简短理由。",
        "",
        "## 输出格式",
        "",
        "| 股票 | 当前价 | 建议 | 理由 |",
        "|------|--------|------|------|",
        "| 600519(茅台) | 1850.00 | 买入 | 触及次级买入区间，量比正常 |",
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
        lines.append(", ".join(snapshot_times))
        lines.append("")

    # --- Intraday event timeline ---
    lines.append("## 盘中事件时间线")
    lines.append("")
    if events:
        lines.append("| 时间 | 股票 | 价格 | 量比 | 涨跌幅 | 事件类型 | 描述 |")
        lines.append("|------|------|------|------|--------|----------|------|")
        for e in events:
            vol = f"{e.volume_ratio:.1f}" if e.volume_ratio is not None else "-"
            chg = f"{e.change_pct:+.2f}%" if e.change_pct is not None else "-"
            lines.append(
                f"| {e.timestamp.strftime('%H:%M')} | {e.stock_name}({e.stock_code}) | "
                f"{e.current_price:.2f} | {vol} | {chg} | {e.event_type} | {e.description} |"
            )
    else:
        lines.append("（无盘中事件）")
    lines.append("")

    # --- Market context note ---
    lines.append("## 市场背景")
    lines.append("")
    lines.append("请结合当日上证指数（SSE Composite）和板块指数的整体方向，判断个股走势是否与大盘同步。")
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
    lines.append("> 场外基金申购截止时间为每个交易日15:00，请在此时间前完成操作。")
    lines.append("> 投资有风险，入市需谨慎。")

    return "\n".join(lines)


def _fmt(val: Optional[float]) -> str:
    """Format float for display, '-' if None."""
    if val is None:
        return "-"
    return f"{val:.2f}"
