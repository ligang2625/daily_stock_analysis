# -*- coding: utf-8 -*-
"""
===================================
盘中监控模块 (IntradayMonitor)
===================================

职责：
1. 交易日盘中定时快照（每30分钟），比对实时价格与昨日分析阈值
2. 14:20 汇总当天盘中事件，调用 LLM 生成买卖决策
3. 通过 EmailSender 发送决策邮件

设计要点：
- 纯数值比对（无需 LLM），仅在 14:20 汇总时调一次 LLM
- SQLite 持久化盘中事件，进程重启不丢失
- threading.Lock 序列化 monitor_snapshot() 和 final_decision()
- get_realtime_quote() 调用包装 10s 超时
- 多市场支持：CN/HK/US 股票各自使用对应交易日历
- 市场本地日期：事件 query_date 使用市场本地日期，避免服务器时区偏移
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
import atexit
import os
import uuid
import sqlite3
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import litellm
from src.core.intraday_prompt import format_intraday_event_time
from data_provider.realtime_types import SnapshotQuoteProcessResult, SnapshotQuoteStatus
from src.utils.stock_code import normalize_stock_code_key, normalize_stock_code_for_history_query

logger = logging.getLogger(__name__)


def _log_ctx(monitor: "IntradayMonitor", **extra) -> dict:
    """Build structured log context dict."""
    ctx = {
        "run_id": getattr(monitor, '_current_run_id', None),
        "snapshot_id": getattr(monitor, '_current_snapshot_id', None),
    }
    ctx.update(extra)
    return ctx


# Module-level executor for realtime quote timeout (shared across instances)
_QUOTE_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4)


def shutdown_quote_executor() -> None:
    """Shutdown the module-level quote executor. Safe to call multiple times."""
    global _QUOTE_EXECUTOR
    if _QUOTE_EXECUTOR is not None:
        _QUOTE_EXECUTOR.shutdown(wait=False)
        _QUOTE_EXECUTOR = None


def reset_quote_executor() -> None:
    """Reset executor for testing. Shuts down old and creates new."""
    shutdown_quote_executor()
    global _QUOTE_EXECUTOR
    _QUOTE_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4)


atexit.register(shutdown_quote_executor)


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

@dataclass
class IntradayEvent:
    """单条盘中事件记录"""
    timestamp: datetime
    stock_code: str
    stock_name: str
    current_price: float
    volume_ratio: Optional[float]
    change_pct: Optional[float]
    event_type: str  # break_stop_loss / enter_secondary_buy / enter_ideal_buy /
                     # near_buy_zone / enter_take_profit / unusual_volume / price_only
    description: str
    market_local_timestamp: Optional[str] = None  # ISO format in market local timezone
    market: Optional[str] = None
    snapshot_id: Optional[str] = None
    run_id: Optional[str] = None
    snapshot_status: Optional[str] = None  # "valid" | "data_unavailable" | "suspended" | "timeout"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "current_price": self.current_price,
            "volume_ratio": self.volume_ratio,
            "change_pct": self.change_pct,
            "event_type": self.event_type,
            "description": self.description,
            "market_local_timestamp": self.market_local_timestamp,
            "market": self.market,
            "snapshot_id": self.snapshot_id,
            "run_id": self.run_id,
            "snapshot_status": self.snapshot_status,
        }


@dataclass
class SnapshotState:
    snapshot_id: str
    run_id: str
    status: str = "running"  # running | completed | partial | expired
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: Optional[datetime] = None
    expected_codes: List[str] = field(default_factory=list)
    completed_codes: List[str] = field(default_factory=list)
    valid_quote_codes: List[str] = field(default_factory=list)
    failed_codes: List[str] = field(default_factory=list)
    markets: List[str] = field(default_factory=list)
    market_dates: Dict[str, str] = field(default_factory=dict)


@dataclass
class DecisionContentIntegrity:
    """Result of validating official decision content completeness."""
    ok: bool
    expected_codes: set
    covered_codes: set
    missing_codes: set
    sentinel_ok: bool
    truncated_suspected: bool
    reason: str


class EmailReportType(Enum):
    OFFICIAL_DECISION = "official_decision"
    DATA_QUALITY_ALERT = "data_quality_alert"
    LLM_FAILURE_ALERT = "llm_failure_alert"
    NO_BASELINE_ALERT = "no_baseline_alert"
    SNAPSHOT_INCOMPLETE_ALERT = "snapshot_incomplete_alert"


@dataclass
class LLMResult:
    status: str  # "success" | "config_error" | "rate_limited" | "network_error" | "empty_response"
    content: Optional[str] = None
    error_message: Optional[str] = None
    provider: Optional[str] = None       # "primary" | "fallback"
    model: Optional[str] = None
    base_url_host: Optional[str] = None  # host from base_url (no API key leakage)
    attempts: int = 1
    primary_error_message: Optional[str] = None
    fallback_error_message: Optional[str] = None
    # Extended fields for dual-failure reporting
    primary_status: Optional[str] = None  # status of primary (e.g. "rate_limited")
    fallback_status: Optional[str] = None  # status of fallback (e.g. "network_error")
    fallback_attempted: bool = False
    fallback_skipped_reason: Optional[str] = None
    # Truncation and integrity fields
    finish_reason: Optional[str] = None  # "stop" | "length" | "max_tokens" | None
    response_chars: int = 0
    usage: Optional[Dict[str, Any]] = None
    completion_tokens: int = 0
    truncated_suspected: bool = False
    expected_codes: Optional[set] = None
    covered_codes: Optional[set] = None
    missing_codes: Optional[set] = None


class LLMConfigError(Exception):
    pass


# ---------------------------------------------------------------------------
# Threshold comparison (pure function, testable without mocks)
# ---------------------------------------------------------------------------

def _compare_with_thresholds(
    stock_code: str,
    stock_name: str,
    current_price: float,
    volume_ratio: Optional[float],
    change_pct: Optional[float],
    yesterday: Dict[str, Optional[float]],
    now: Optional[datetime] = None,
) -> List[IntradayEvent]:
    """
    Compare current price / volume_ratio against yesterday's sniper points.

    Data convention: stop_loss < secondary_buy < ideal_buy < take_profit

    Returns list of events (most severe price event + optional volume event).
    """
    if now is None:
        now = datetime.now()

    ideal_buy = yesterday.get("ideal_buy")
    secondary_buy = yesterday.get("secondary_buy")
    stop_loss = yesterday.get("stop_loss")
    take_profit = yesterday.get("take_profit")

    events: List[IntradayEvent] = []

    # --- Price events (most severe wins) ---
    price_event: Optional[IntradayEvent] = None

    if stop_loss is not None and current_price <= stop_loss:
        price_event = IntradayEvent(
            timestamp=now,
            stock_code=stock_code,
            stock_name=stock_name,
            current_price=current_price,
            volume_ratio=volume_ratio,
            change_pct=change_pct,
            event_type="break_stop_loss",
            description=f"当前价 {current_price} <= 止损位 {stop_loss}",
        )
    elif secondary_buy is not None and stop_loss is not None and secondary_buy >= current_price > stop_loss:
        price_event = IntradayEvent(
            timestamp=now,
            stock_code=stock_code,
            stock_name=stock_name,
            current_price=current_price,
            volume_ratio=volume_ratio,
            change_pct=change_pct,
            event_type="enter_secondary_buy",
            description=f"当前价 {current_price} 进入次级买入区间 ({stop_loss}, {secondary_buy}]",
        )
    elif ideal_buy is not None and secondary_buy is not None and ideal_buy >= current_price > secondary_buy:
        price_event = IntradayEvent(
            timestamp=now,
            stock_code=stock_code,
            stock_name=stock_name,
            current_price=current_price,
            volume_ratio=volume_ratio,
            change_pct=change_pct,
            event_type="enter_ideal_buy",
            description=f"当前价 {current_price} 进入理想买入区间 ({secondary_buy}, {ideal_buy}]",
        )
    elif take_profit is not None and current_price >= take_profit:
        price_event = IntradayEvent(
            timestamp=now,
            stock_code=stock_code,
            stock_name=stock_name,
            current_price=current_price,
            volume_ratio=volume_ratio,
            change_pct=change_pct,
            event_type="enter_take_profit",
            description=f"当前价 {current_price} >= 止盈位 {take_profit}",
        )
    elif ideal_buy is not None and ideal_buy < current_price <= ideal_buy * 1.02:
        price_event = IntradayEvent(
            timestamp=now,
            stock_code=stock_code,
            stock_name=stock_name,
            current_price=current_price,
            volume_ratio=volume_ratio,
            change_pct=change_pct,
            event_type="near_buy_zone",
            description=f"当前价 {current_price} 接近买入区间 (理想买入 {ideal_buy}, 偏离 {(current_price / ideal_buy - 1) * 100:.1f}%)",
        )
    else:
        # No threshold hit, record price-only
        price_event = IntradayEvent(
            timestamp=now,
            stock_code=stock_code,
            stock_name=stock_name,
            current_price=current_price,
            volume_ratio=volume_ratio,
            change_pct=change_pct,
            event_type="price_only",
            description=f"当前价 {current_price}，未触及任何阈值",
        )

    # --- Anomalous config warnings ---
    if take_profit is not None and ideal_buy is not None and take_profit <= ideal_buy:
        logger.warning(
            "异常阈值配置 [%s]: take_profit(%.2f) <= ideal_buy(%.2f)，止盈可能被买入区间覆盖",
            stock_code, take_profit, ideal_buy,
        )
    if secondary_buy is not None and ideal_buy is not None and secondary_buy >= ideal_buy:
        logger.warning(
            "异常阈值配置 [%s]: secondary_buy(%.2f) >= ideal_buy(%.2f)",
            stock_code, secondary_buy, ideal_buy,
        )
    if stop_loss is not None and secondary_buy is not None and stop_loss >= secondary_buy:
        logger.warning(
            "异常阈值配置 [%s]: stop_loss(%.2f) >= secondary_buy(%.2f)",
            stock_code, stop_loss, secondary_buy,
        )

    events.append(price_event)

    # --- Volume event (independent dimension) ---
    if volume_ratio is not None and volume_ratio >= 3.0:
        events.append(IntradayEvent(
            timestamp=now,
            stock_code=stock_code,
            stock_name=stock_name,
            current_price=current_price,
            volume_ratio=volume_ratio,
            change_pct=change_pct,
            event_type="unusual_volume",
            description=f"量比 {volume_ratio:.1f} >= 3.0，异常放量",
        ))

    return events


def _find_sniper_points_in_dict(d: dict) -> Dict[str, Optional[float]]:
    """Recursively search for sniper_points in a nested dict (mirrors _find_sniper_in_dashboard)."""
    if not isinstance(d, dict):
        return {}

    # Direct: has sniper keys at top level
    if "ideal_buy" in d:
        return {
            "ideal_buy": _safe_float(d.get("ideal_buy")),
            "secondary_buy": _safe_float(d.get("secondary_buy")),
            "stop_loss": _safe_float(d.get("stop_loss")),
            "take_profit": _safe_float(d.get("take_profit")),
        }

    # d.sniper_points
    sp = d.get("sniper_points")
    if isinstance(sp, dict) and sp:
        return _find_sniper_points_in_dict(sp)

    # d.battle_plan.sniper_points
    bp = d.get("battle_plan")
    if isinstance(bp, dict):
        result = _find_sniper_points_in_dict(bp)
        if result:
            return result

    # d.dashboard.battle_plan.sniper_points
    inner = d.get("dashboard")
    if isinstance(inner, dict):
        return _find_sniper_points_in_dict(inner)

    return {}


def _safe_float(val: Any) -> Optional[float]:
    """Safe float conversion, returns None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Event display helpers
# ---------------------------------------------------------------------------


def classify_event_type_for_display(event_type: str) -> str:
    """Map event type to display group: buy/take_profit/stop_loss/volume/watch."""
    buy_types = {"enter_ideal_buy", "enter_secondary_buy", "near_buy_zone"}
    if event_type in buy_types:
        return "buy"
    if event_type == "enter_take_profit":
        return "take_profit"
    if event_type == "break_stop_loss":
        return "stop_loss"
    if event_type == "unusual_volume":
        return "volume"
    return "watch"


def aggregate_events_for_display(events: list) -> dict:
    """Aggregate events by stock, merging unusual_volume into main event.

    Returns dict with keys: buy, take_profit, stop_loss, watch.
    Each value is list of dicts: stock_code, stock_name, main_event,
    has_unusual_volume, max_volume_ratio, current_price.
    """
    from collections import OrderedDict

    stocks = OrderedDict()

    priority = {
        "break_stop_loss": 5,
        "enter_take_profit": 4,
        "enter_ideal_buy": 3,
        "enter_secondary_buy": 2,
        "near_buy_zone": 1,
        "price_only": 0,
        "unusual_volume": -1,
    }

    for event in events:
        code = event.stock_code
        if code not in stocks:
            stocks[code] = {
                "stock_code": code,
                "stock_name": event.stock_name,
                "main_event": None,
                "main_priority": -1,
                "has_unusual_volume": False,
                "max_volume_ratio": None,
                "all_events": [],
                "current_price": event.current_price,
            }

        info = stocks[code]
        info["all_events"].append(event)
        info["current_price"] = event.current_price

        ev_priority = priority.get(event.event_type, -1)
        if ev_priority > info["main_priority"]:
            info["main_event"] = event
            info["main_priority"] = ev_priority

        if event.event_type == "unusual_volume":
            info["has_unusual_volume"] = True
            if event.volume_ratio is not None:
                if info["max_volume_ratio"] is None or event.volume_ratio > info["max_volume_ratio"]:
                    info["max_volume_ratio"] = event.volume_ratio

    result = {"buy": [], "take_profit": [], "stop_loss": [], "watch": []}
    for code, info in stocks.items():
        main_ev = info["main_event"]
        if main_ev is None:
            group = "watch"
        else:
            group = classify_event_type_for_display(main_ev.event_type)
            if group == "volume":
                group = "watch"

        entry = {
            "stock_code": info["stock_code"],
            "stock_name": info["stock_name"],
            "main_event": info["main_event"],
            "has_unusual_volume": info["has_unusual_volume"],
            "max_volume_ratio": info["max_volume_ratio"],
            "current_price": info["current_price"],
        }
        result[group].append(entry)

    return result


# ---------------------------------------------------------------------------
# IntradayMonitor
# ---------------------------------------------------------------------------

INTRADAY_SYSTEM_PROMPT = """你是一个专业的盘中股票分析助手。你的任务是基于盘中实时数据、昨日技术分析结果和盘中事件，生成客观、审慎的盘中决策参考。

规则：
1. 所有判断必须基于输入数据，不得虚构价格、成交量、技术指标数值或新闻事件。
2. 必须明确区分：事实陈述（基于数据）、风险提示（基于阈值偏离）、操作建议（基于多因素推理，需有条件说明）。
3. 不得输出确定性收益承诺或保证性预测。
4. 对数据不足或信号矛盾的标的，应明确表示无法判断，而不是强行给出结论。"""


class IntradayMonitor:
    """
    盘中监控器

    Lifecycle:
    1. 首次 monitor_snapshot() 时自动 load_yesterday_analysis() + 新交易日清空事件
    2. 后续快照直接比对写入
    3. 14:20 final_decision() 读取全天事件 → LLM → 发邮件

    Multi-market: CN/HK/US stocks each use their own trading calendar.
    Market-local dates: event query_date uses market timezone, not server local time.
    """

    def __init__(
        self,
        config: Any,
        fetcher_manager: Any,
        db_manager: Any,
        email_sender: Any = None,
        llm_analyzer: Any = None,
    ):
        self._config = config
        self._fetcher = fetcher_manager
        self._db = db_manager
        self._email_sender = email_sender
        self._llm_analyzer = llm_analyzer  # Injected GeminiAnalyzer; lazy-init fallback if None
        self._yesterday_analysis: Dict[str, Dict[str, Optional[float]]] = {}
        self._initialized: bool = False
        self._lock = threading.Lock()
        self._snapshot_times: List[str] = []  # record snapshot times for the day
        self._daily_init_marker: Optional[str] = None  # tracks which trading date was initialized
        self._expired_requests: set = set()
        self._current_run_id = uuid.uuid4().hex[:12]
        self._current_snapshot_id: Optional[str] = None

        # Startup logging for fallback LLM config
        fb_enabled = getattr(self._config, 'intraday_llm_fallback_enabled', False)
        if fb_enabled:
            fb_model = getattr(self._config, 'intraday_llm_fallback_model', None) or "not set"
            fb_host = "not set"
            fb_base_url = getattr(self._config, 'intraday_llm_fallback_base_url', None)
            if fb_base_url:
                try:
                    from urllib.parse import urlparse
                    fb_host = urlparse(fb_base_url).netloc or "unknown"
                except Exception:
                    fb_host = "parse-error"
            fb_retry_on = getattr(self._config, 'intraday_llm_fallback_retry_on', '')
            logger.info(
                "Fallback LLM enabled: model=%s base_url_host=%s retry_on=%s",
                fb_model, fb_host, fb_retry_on,
            )
        else:
            logger.info("Fallback LLM disabled")

    def _get_llm_analyzer(self):
        """Return the injected analyzer, or create one lazily from config (fallback)."""
        if self._llm_analyzer is not None:
            return self._llm_analyzer
        from src.analyzer import GeminiAnalyzer
        self._llm_analyzer = GeminiAnalyzer(config=self._config)
        return self._llm_analyzer

    def set_llm_analyzer(self, analyzer: Any) -> None:
        """Set or replace the LLM analyzer (e.g., after config reload in schedule mode)."""
        self._llm_analyzer = analyzer

    # ------------------------------------------------------------------
    # Market resolution (per-stock)
    # ------------------------------------------------------------------

    @staticmethod
    def _get_market_for_stock(code: str) -> Optional[str]:
        """Infer market for a stock code. Returns 'cn' | 'hk' | 'us' | None."""
        try:
            from src.core.trading_calendar import get_market_for_stock
            return get_market_for_stock(code)
        except Exception:
            return None

    def _resolve_quote_timeout(self, code: str, source_hint: Optional[str] = None) -> float:
        market = self._get_market_for_stock(code)
        if market == 'cn':
            return getattr(self._config, 'intraday_quote_timeout_cn_fast', 12.0)
        elif market == 'hk':
            if source_hint in ('stock_hk_spot',):
                return getattr(self._config, 'intraday_quote_timeout_hk_full', 60.0)
            return getattr(self._config, 'intraday_quote_timeout_hk_fast', 15.0)
        elif market == 'us':
            return getattr(self._config, 'intraday_quote_timeout_us', 15.0)
        return getattr(self._config, 'intraday_quote_timeout_default', 20.0)

    def _stamp_event(self, event: IntradayEvent, snapshot_status: Optional[str] = None) -> IntradayEvent:
        """Stamp event with current snapshot_id, run_id, and optional snapshot_status."""
        event.snapshot_id = self._current_snapshot_id
        event.run_id = self._current_run_id
        event.snapshot_status = snapshot_status
        return event

    @staticmethod
    def _extract_quote_data(quote) -> Dict[str, Any]:
        """Extract supplemental quote fields from a UnifiedRealtimeQuote into a dict for raw_quote storage.

        Returns dict with keys: source, open, prev_close, high, low, volume, amount, turnover_rate.
        Fields that are None or missing are excluded.
        """
        data: Dict[str, Any] = {}
        try:
            data['source'] = getattr(quote, 'source', None)
        except Exception:
            pass
        for field in ('open', 'prev_close', 'high', 'low', 'volume', 'amount', 'turnover_rate'):
            try:
                val = getattr(quote, field, None)
                if val is not None:
                    data[field] = float(val) if isinstance(val, (int, float)) else val
            except Exception:
                pass
        return data

    @staticmethod
    def _get_market_query_date(market: str) -> str:
        """Return market-local date string (YYYY-MM-DD) for the given market."""
        try:
            from src.core.trading_calendar import get_market_now
            return get_market_now(market).date().isoformat()
        except Exception:
            return date.today().isoformat()

    def _get_stock_markets(self, stock_codes: List[str]) -> Dict[str, Optional[str]]:
        """Map stock codes to their market regions. None = unknown market."""
        return {code: self._get_market_for_stock(code) for code in stock_codes}

    def _resolve_unknown_market_policy(self) -> str:
        """Return the policy for unknown-market stocks: 'skip' | 'cn_compat'."""
        policy = getattr(self._config, 'intraday_unknown_market_policy', 'skip')
        if policy in ('skip', 'cn_compat'):
            return policy
        return 'skip'

    def _filter_unknown_market_stocks(self, stock_codes: List[str]) -> List[str]:
        """Filter out stocks with unrecognized market codes."""
        policy = self._resolve_unknown_market_policy()
        if policy == 'cn_compat':
            return stock_codes
        markets = self._get_stock_markets(stock_codes)
        known = [code for code in stock_codes if markets.get(code)]
        unknown = [code for code in stock_codes if not markets.get(code)]
        if unknown:
            logger.warning(
                "跳过 %d 个无法识别市场的股票 (policy=skip): %s",
                len(unknown), ', '.join(unknown),
            )
        return known

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def load_yesterday_analysis(self, stock_codes: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
        """
        Load the most recent completed trading day's postmarket analysis.

        Multi-market: groups stocks by market, uses per-market effective trading date.

        Primary query: effective_trading_date == target AND analysis_phase == 'postmarket'.
        Fallback query (old records only): analysis_phase IS NULL AND effective_trading_date IS NULL
          AND func.date(created_at) >= target_date_str.
        Primary records take precedence over fallback for the same code.

        Returns {code: {ideal_buy, secondary_buy, stop_loss, take_profit}}
        """
        if not stock_codes:
            return {}

        result: Dict[str, Dict[str, Optional[float]]] = {}
        stock_markets = self._get_stock_markets(stock_codes)

        try:
            from src.core.trading_calendar import (
                get_intraday_baseline_trading_date,
                get_effective_trading_date,
            )
        except Exception:
            get_intraday_baseline_trading_date = None
            get_effective_trading_date = None

        # Group stocks by market (None/unknown → logged, skipped unless cn_compat policy)
        unknown_market_policy = getattr(self._config, 'intraday_unknown_market_policy', 'skip')
        by_market: Dict[str, List[str]] = {}
        unknown_codes: List[str] = []
        for code in stock_codes:
            mkt = stock_markets.get(code)
            if mkt is None:
                if unknown_market_policy == 'cn_compat':
                    mkt = 'cn'
                else:
                    unknown_codes.append(code)
                    continue
            by_market.setdefault(mkt, []).append(code)

        if unknown_codes:
            logger.warning(
                "盘中监控: %d 支股票市场未知，跳过昨日分析加载 (policy=%s): %s",
                len(unknown_codes), unknown_market_policy,
                ", ".join(sorted(unknown_codes)),
            )

        # Compute canonical code_norm for each stock
        code_norm_map: Dict[str, str] = {}
        try:
            from src.services.stock_code_utils import normalize_stock_code_for_storage
            for code in stock_codes:
                norm = normalize_stock_code_for_storage(code)
                if norm:
                    code_norm_map[code] = norm
        except Exception:
            pass

        try:
            with self._db.session_scope() as session:
                from src.storage import AnalysisHistory
                from sqlalchemy import and_, func, or_

                all_primary: list = []
                all_fallback: list = []
                fallback_warning_codes: Set[str] = set()
                allow_fallback = getattr(self._config, 'intraday_legacy_fallback_enabled', False)

                for market, codes_in_market in by_market.items():
                    if not codes_in_market:
                        continue

                    # Per-market target trading date
                    if get_intraday_baseline_trading_date:
                        try:
                            target_trading_date = get_intraday_baseline_trading_date(market)
                        except Exception:
                            target_trading_date = date.today() - timedelta(days=1)
                    else:
                        target_trading_date = date.today() - timedelta(days=1)

                    target_date_str = target_trading_date.strftime("%Y-%m-%d")

                    # Normalize codes for DB query (HK uppercase for analysis_history)
                    query_codes = [normalize_stock_code_for_history_query(c) for c in codes_in_market]

                    # Build canonical code_norm list for indexed lookup
                    norm_codes = []
                    for c in codes_in_market:
                        norm = code_norm_map.get(c)
                        if norm:
                            norm_codes.append(norm)

                    # --- Primary query: code_norm first (hits compound index), then code fallback ---
                    primary_conditions = [
                        AnalysisHistory.effective_trading_date == target_trading_date,
                        AnalysisHistory.analysis_phase == 'postmarket',
                    ]
                    code_conditions = []
                    if norm_codes:
                        code_conditions.append(AnalysisHistory.code_norm.in_(norm_codes))
                    if query_codes:
                        code_conditions.append(AnalysisHistory.code.in_(query_codes))
                    if code_conditions:
                        primary_conditions.append(or_(*code_conditions))

                    primary_rows = (
                        session.query(AnalysisHistory)
                        .filter(and_(*primary_conditions))
                        .order_by(AnalysisHistory.created_at.desc())
                        .all()
                    )
                    if len(primary_rows) == 0 and codes_in_market:
                        sample = codes_in_market[:3]
                        logger.warning(
                            "load_yesterday_analysis market=%s date=%s input_codes=%d matched=0 sample_input=%s hint='try uppercase HK prefix'",
                            market, target_date_str, len(codes_in_market), sample,
                        )
                    all_primary.extend(primary_rows)
                    primary_codes = {normalize_stock_code_key(str(row.code or "").strip()) for row in primary_rows}

                    # --- Fallback: old records only (no new columns) ---
                    remaining_codes = [c for c in codes_in_market if normalize_stock_code_key(c) not in primary_codes]
                    if remaining_codes and allow_fallback:
                        remaining_query_codes = [normalize_stock_code_for_history_query(c) for c in remaining_codes]
                        fallback_rows = (
                            session.query(AnalysisHistory)
                            .filter(
                                and_(
                                    AnalysisHistory.code.in_(remaining_query_codes),
                                    AnalysisHistory.analysis_phase.is_(None),
                                    AnalysisHistory.effective_trading_date.is_(None),
                                    func.date(AnalysisHistory.created_at) >= target_date_str,
                                )
                            )
                            .order_by(AnalysisHistory.created_at.desc())
                            .all()
                        )
                        all_fallback.extend(fallback_rows)
                        if fallback_rows:
                            fallback_warning_codes.update(
                                str(row.code or "").strip() for row in fallback_rows
                            )

                if fallback_warning_codes:
                    logger.warning(
                        "盘中监控: %d 支股票命中 legacy fallback（旧记录，无 analysis_phase）: %s",
                        len(fallback_warning_codes),
                        ", ".join(sorted(fallback_warning_codes)),
                    )

                # Combine: primary first, then fallback; keep first per code
                all_rows = list(all_primary) + list(all_fallback)

                # --- Diagnostic logging when primary query misses ---
                primary_codes_all = {normalize_stock_code_key(str(row.code or "").strip()) for row in all_primary}
                missed_codes: Dict[str, List[str]] = {}
                for market, codes_in_market in by_market.items():
                    missed = [c for c in codes_in_market if normalize_stock_code_key(c) not in primary_codes_all]
                    if missed:
                        missed_codes[market] = missed

                if missed_codes:
                    logger.warning(
                        "盘中监控: primary query 未命中 %d 支股票，开始诊断...",
                        sum(len(v) for v in missed_codes.values()),
                    )
                    # Diagnostic 1: same code + effective_trading_date but different phase
                    for market, missed_list in missed_codes.items():
                        missed_query_codes = [normalize_stock_code_for_history_query(c) for c in missed_list]
                        if get_effective_trading_date:
                            try:
                                ttd = get_effective_trading_date(market)
                            except Exception:
                                ttd = date.today() - timedelta(days=1)
                        else:
                            ttd = date.today() - timedelta(days=1)
                        diag_rows = (
                            session.query(
                                AnalysisHistory.code,
                                AnalysisHistory.analysis_phase,
                                AnalysisHistory.effective_trading_date,
                                AnalysisHistory.created_at,
                            )
                            .filter(
                                and_(
                                    AnalysisHistory.code.in_(missed_query_codes),
                                    AnalysisHistory.effective_trading_date == ttd,
                                    AnalysisHistory.analysis_phase != 'postmarket',
                                    AnalysisHistory.analysis_phase.isnot(None),
                                )
                            )
                            .all()
                        )
                        if diag_rows:
                            logger.warning(
                                "诊断: 存在相同交易日但 phase!=postmarket 的记录: %s",
                                ", ".join(
                                    f"{r.code}(phase={r.analysis_phase}, created={r.created_at})"
                                    for r in diag_rows
                                ),
                            )

                        # Diagnostic 2: analysis_phase='auto' records
                        auto_rows = (
                            session.query(
                                AnalysisHistory.code,
                                AnalysisHistory.created_at,
                            )
                            .filter(
                                and_(
                                    AnalysisHistory.code.in_(missed_query_codes),
                                    AnalysisHistory.analysis_phase == 'auto',
                                )
                            )
                            .all()
                        )
                        if auto_rows:
                            logger.warning(
                                "诊断: 存在 analysis_phase='auto' 的记录（应尽早修复）: %s",
                                ", ".join(
                                    f"{r.code}(created={r.created_at})" for r in auto_rows
                                ),
                            )

                        # Diagnostic 3: NULL effective_trading_date on new-style records
                        null_date_rows = (
                            session.query(
                                AnalysisHistory.code,
                                AnalysisHistory.analysis_phase,
                                AnalysisHistory.created_at,
                            )
                            .filter(
                                and_(
                                    AnalysisHistory.code.in_(missed_query_codes),
                                    AnalysisHistory.effective_trading_date.is_(None),
                                    AnalysisHistory.analysis_phase.isnot(None),
                                )
                            )
                            .all()
                        )
                        if null_date_rows:
                            logger.warning(
                                "诊断: 存在 effective_trading_date=NULL 的新记录: %s",
                                ", ".join(
                                    f"{r.code}(phase={r.analysis_phase}, created={r.created_at})"
                                    for r in null_date_rows
                                ),
                            )

                # Keep latest per code
                seen: set = set()
                for row in all_rows:
                    code = str(row.code or "").strip()
                    if not code:
                        continue
                    key = normalize_stock_code_key(code)
                    if key in seen:
                        continue
                    seen.add(key)

                    ideal = float(row.ideal_buy) if row.ideal_buy is not None else None
                    secondary = float(row.secondary_buy) if row.secondary_buy is not None else None
                    stop = float(row.stop_loss) if row.stop_loss is not None else None
                    profit = float(row.take_profit) if row.take_profit is not None else None

                    # Fallback: try raw_result JSON for sniper points
                    if not any([ideal, secondary, stop, profit]) and row.raw_result:
                        try:
                            raw = json.loads(row.raw_result) if isinstance(row.raw_result, str) else row.raw_result
                            if isinstance(raw, dict):
                                sp = _find_sniper_points_in_dict(raw)
                                ideal = sp.get("ideal_buy")
                                secondary = sp.get("secondary_buy")
                                stop = sp.get("stop_loss")
                                profit = sp.get("take_profit")
                        except Exception:
                            pass

                    result[key] = {
                        "ideal_buy": ideal,
                        "secondary_buy": secondary,
                        "stop_loss": stop,
                        "take_profit": profit,
                    }

            logger.info("加载昨日分析: %d 支股票 (primary=%d, fallback=%d, markets=%s)",
                         len(result), len(all_primary), len(all_fallback),
                         ", ".join(f"{m}={len(c)}" for m, c in by_market.items()))
            full_sniper = sum(1 for v in result.values() if all(v.values()))
            partial_sniper = sum(1 for v in result.values() if any(v.values()) and not all(v.values()))
            no_sniper = sum(1 for v in result.values() if not any(v.values()))
            logger.info(
                "昨日分析详情: full_sniper=%d partial_sniper=%d no_sniper=%d total=%d",
                full_sniper, partial_sniper, no_sniper, len(result),
            )
        except Exception as exc:
            logger.exception("加载昨日分析失败: %s", exc)

        self._yesterday_analysis = result
        return result

    def _get_daily_init_key(self, market: str) -> str:
        """Return a unique key for today's initialization state for a given market."""
        return f"{market}:{self._get_market_query_date(market)}"

    def _resolve_primary_market(self) -> str:
        """Determine primary market for global checks (config-driven)."""
        region = getattr(self._config, 'market_review_region', None)
        if region and region in ('cn', 'hk', 'us'):
            return region
        return 'cn'

    def _should_clear_events(self, market: str) -> bool:
        """Return True only on genuinely new trading day for a given market (persistent marker doesn't match).

        On process restart within same trading day, the persistent marker in DB
        matches today's key → returns False (events preserved).
        """
        today_key = self._get_daily_init_key(market)
        persistent_key = f"daily_init:{market}"
        try:
            persistent_value = self._db.get_intraday_state(persistent_key)
        except Exception:
            persistent_value = None

        if persistent_value == today_key:
            return False  # Already initialized today, even across restarts

        # Genuinely new trading day (or first-ever run)
        return True

    def _persist_init_marker(self, market: str) -> None:
        """Write persistent init marker for a given market so restarts don't re-clear today's events."""
        today_key = self._get_daily_init_key(market)
        persistent_key = f"daily_init:{market}"
        try:
            self._db.set_intraday_state(persistent_key, today_key)
            self._daily_init_marker = today_key
            logger.info("Persisted intraday init marker for %s: %s", market, today_key)
        except Exception as exc:
            logger.warning("Failed to persist intraday init marker for %s: %s", market, exc)
            self._daily_init_marker = today_key  # Still set memory marker on failure

    def clear_today_events(self, markets: Optional[List[str]] = None, reset_marker: bool = False) -> None:
        """Delete today's intraday events for given markets (or all markets if None).

        Set reset_marker=True to also clear the persistent init marker, effectively
        allowing re-initialization on next snapshot (used by --reset-intraday-events).
        """
        if markets is None:
            markets = ['cn', 'hk', 'us']
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                for mkt in markets:
                    mkt_date = self._get_market_query_date(mkt)
                    conn.execute(
                        sa_text("DELETE FROM intraday_events WHERE query_date = :qd AND market = :mkt"),
                        {"qd": mkt_date, "mkt": mkt},
                    )
                session.commit()
            logger.info("已清空当天盘中事件: markets=%s reset_marker=%s", markets, reset_marker)
        except Exception as exc:
            logger.warning("清空当天盘中事件失败（可能表不存在）: %s", exc)

        # Reset marker independent of event deletion success
        if reset_marker:
            for mkt in ['cn', 'hk', 'us']:
                persistent_key = f"daily_init:{mkt}"
                try:
                    self._db.set_intraday_state(persistent_key, "")
                except Exception:
                    pass
            self._daily_init_marker = None
            logger.info("已重置所有市场持久化 init marker")

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def monitor_snapshot(self) -> None:
        """Entry point for scheduled snapshot (called by Scheduler)."""
        self._ensure_intraday_table()
        self._ensure_intraday_snapshots_table()
        ttl = getattr(self._config, 'intraday_snapshot_lock_ttl_seconds', 120)
        if not self._acquire_process_lock("intraday_snapshot", ttl_seconds=ttl):
            logger.warning("monitor_snapshot: 无法获取进程锁（另一个实例正在执行），跳过本次快照")
            return
        acquired = self._lock.acquire(timeout=30)
        if not acquired:
            logger.warning("monitor_snapshot: 无法获取锁（可能 final_decision 正在执行），跳过本次快照")
            self._release_process_lock("intraday_snapshot")
            return
        try:
            self._monitor_snapshot_locked()
        finally:
            self._lock.release()
            self._release_process_lock("intraday_snapshot")

    def _monitor_snapshot_locked(self) -> None:
        """Snapshot implementation (caller must hold self._lock)."""

        snapshot_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self._current_snapshot_id = snapshot_id
        snapshot_start = datetime.now()

        stock_codes = self._get_stock_codes()
        if not stock_codes:
            return

        # Filter out stocks with unknown markets
        stock_codes = self._filter_unknown_market_stocks(stock_codes)
        if not stock_codes:
            logger.info("所有股票市场均无法识别，跳过盘中快照")
            return

        # Per-stock trading day check: only snapshot stocks in open markets
        tradable_codes = [c for c in stock_codes if self._is_stock_trading_today(c)]
        if not tradable_codes:
            all_closed = all(not self._is_stock_trading_today(c) for c in stock_codes)
            if all_closed:
                logger.info("所有监控股票对应市场今日均休市，跳过盘中快照")
            return

        # Check for explicit reset (--reset-intraday-events or intraday_reset_on_start)
        reset_requested = getattr(self._config, 'intraday_reset_on_start', False)
        if reset_requested:
            logger.warning("intraday_reset_on_start=True: 强制清空当天盘中事件并重置 marker")
            self.clear_today_events(reset_marker=True)
            # Reset the flag so it only fires once
            try:
                self._config.intraday_reset_on_start = False
            except Exception:
                pass

        # Per-market lazy init
        stock_markets = self._get_stock_markets(stock_codes)
        markets_seen: set = set()
        for stock_code in stock_codes:
            mkt = stock_markets.get(stock_code)
            if mkt and mkt not in markets_seen:
                markets_seen.add(mkt)
                if self._should_clear_events(mkt):
                    self.clear_today_events(markets=[mkt])
                    self._persist_init_marker(mkt)
                elif self._daily_init_marker is None:
                    self._persist_init_marker(mkt)

        if not self._initialized:
            self.load_yesterday_analysis(stock_codes)
            self._initialized = True
            logger.info("盘中监控初始化完成，监控 %d 支股票", len(stock_codes))

        now = datetime.now()
        self._snapshot_times.append(now.strftime("%H:%M"))
        logger.info(
            "盘中快照开始: %s, 候选 %d 支 (可交易 %d 支)",
            now.strftime("%H:%M:%S"), len(stock_codes), len(tradable_codes),
            extra=_log_ctx(self),
        )

        # Run snapshot with state persistence
        results, snapshot_state = self._run_snapshot_with_state_persistence(
            codes=tradable_codes, now=now, snapshot_id=snapshot_id, source="monitor",
        )
        completed_codes = results.get("completed_codes", [])
        valid_quote_codes = results.get("valid_quote_codes", [])
        failed_codes = results.get("failed_codes", [])

        # Group tradable_codes by market for batch fetch
        stock_markets_all = self._get_stock_markets(tradable_codes)
        by_market: Dict[str, List[str]] = {}
        for code in tradable_codes:
            mkt = stock_markets_all.get(code)
            if mkt:
                by_market.setdefault(mkt, []).append(code)

        # Log batch-first verification
        logger.info(
            "batch_first_summary: snapshot=%s market_count=%d target_count=%d "
            "batch_hit=%d batch_miss=%d completed=%d valid=%d failed=%d",
            snapshot_id, len(by_market), len(tradable_codes),
            results.get("batch_matched_count", 0),
            results.get("fallback_count", 0),
            len(completed_codes), len(valid_quote_codes), len(failed_codes),
        )

        # Clear per-snapshot cache
        try:
            self._fetcher.clear_snapshot_cache(snapshot_id)
        except Exception:
            pass

        # Per-snapshot summary
        total_elapsed = (datetime.now() - snapshot_start).total_seconds()
        target_count = len(tradable_codes)
        success_count = len(valid_quote_codes)
        failed_count = len(failed_codes)
        _CI = os.environ.get('CI', '').lower() == 'true' or os.environ.get('GITHUB_ACTIONS', '').lower() == 'true'
        logger.info(
            "snapshot_summary: target=%d success=%d failed=%d timeout=%d suspended=%d total_time=%.1fs coverage=%.1f%%",
            target_count, success_count, failed_count,
            results.get("timeout_count", 0), results.get("suspended_count", 0),
            total_elapsed,
            (success_count / max(target_count, 1)) * 100,
            extra=_log_ctx(self),
        )
        if _CI:
            logger.info("[CI] snapshot=%s target=%s done=%s", snapshot_id, target_count, success_count)

    def _run_snapshot_for_codes(
        self, tradable_codes: List[str], now: datetime, snapshot_id: str,
    ) -> Dict[str, Any]:
        """Run batch-first snapshot for a list of stock codes.

        Groups by market, fetches via batch, processes matched stocks via
        _snapshot_one_stock_with_quote, and runs fallback for unmatched.
        Shared by _monitor_snapshot_locked, run_one_shot_snapshot, and
        run_one_shot_decision.

        Returns dict with keys: completed_codes, valid_quote_codes, failed_codes,
        batch_matched_count, fallback_count.
        """
        stock_markets_all = self._get_stock_markets(tradable_codes)
        by_market: Dict[str, List[str]] = {}
        for code in tradable_codes:
            mkt = stock_markets_all.get(code)
            if mkt:
                by_market.setdefault(mkt, []).append(code)

        # Batch fetch per market
        all_quotes: Dict[str, Any] = {}
        for mkt, codes_in_market in by_market.items():
            try:
                batch = self._fetcher.get_realtime_quotes_batch(codes_in_market, {mkt}, snapshot_id)
                if batch is not None and batch.quotes:
                    all_quotes.update(batch.quotes)
            except Exception:
                logger.exception("批量行情获取失败 [market=%s], 该市场股票将逐个 fallback 到单股接口", mkt)
            # Fill in missing stocks
            for code in codes_in_market:
                if code not in all_quotes:
                    all_quotes[code] = None

        # Process each code
        completed_codes: List[str] = []
        valid_quote_codes: List[str] = []
        failed_codes: List[str] = []
        timeout_count = 0
        suspended_count = 0
        batch_matched_count = 0
        fallback_count = 0

        for code in tradable_codes:
            quote = all_quotes.get(code)
            if quote is not None:
                batch_matched_count += 1
                try:
                    result = self._snapshot_one_stock_with_quote(code, now, quote)
                    completed_codes.append(code)
                    if result.status == SnapshotQuoteStatus.VALID:
                        valid_quote_codes.append(code)
                    elif result.status == SnapshotQuoteStatus.SUSPENDED:
                        suspended_count += 1
                except Exception as exc:
                    logger.error("盘中快照异常 [%s]: %s", code, exc)
                    failed_codes.append(code)
            else:
                fallback_count += 1
                try:
                    result = self._fallback_single_quote_without_bulk_sources(code, now)
                    completed_codes.append(code)
                    if result.status == SnapshotQuoteStatus.VALID:
                        valid_quote_codes.append(code)
                except Exception as exc:
                    logger.error("盘中快照fallback异常 [%s]: %s", code, exc)
                    failed_codes.append(code)

        return {
            "completed_codes": completed_codes,
            "valid_quote_codes": valid_quote_codes,
            "failed_codes": failed_codes,
            "timeout_count": timeout_count,
            "suspended_count": suspended_count,
            "batch_matched_count": batch_matched_count,
            "fallback_count": fallback_count,
        }

    def _run_snapshot_with_state_persistence(
        self,
        codes: List[str],
        now: datetime,
        snapshot_id: Optional[str] = None,
        source: str = "monitor",
    ) -> Tuple[Dict[str, Any], "SnapshotState"]:
        """Run snapshot with full state persistence lifecycle.

        Creates SnapshotState (running) -> persists -> runs snapshot -> updates to
        completed -> persists. On exception, persists failed/partial state, then re-raises.

        Returns tuple of (snapshot_results_dict, SnapshotState).
        """
        if snapshot_id is None:
            snapshot_id = f"{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self._current_snapshot_id = snapshot_id

        snapshot_state = SnapshotState(
            snapshot_id=snapshot_id,
            run_id=self._current_run_id,
            expected_codes=list(codes),
        )
        # Compute markets and market_dates from monitored stock codes
        markets_set: set = set()
        for code in codes:
            mkt = self._get_market_for_stock(code)
            if mkt:
                markets_set.add(mkt)
        snapshot_state.markets = sorted(markets_set)
        snapshot_state.market_dates = {
            mkt: self._get_market_query_date(mkt) for mkt in sorted(markets_set)
        }
        self._persist_snapshot_state(snapshot_state)

        logger.info(
            "快照状态已持久化: snapshot_id=%s status=%s expected=%d source=%s",
            snapshot_id, snapshot_state.status, len(codes), source,
        )

        results: Dict[str, Any] = {}
        try:
            results = self._run_snapshot_for_codes(codes, now, snapshot_id)
        except Exception as exc:
            snapshot_state.status = "failed"
            snapshot_state.finished_at = datetime.now()
            snapshot_state.completed_codes = results.get("completed_codes", [])
            snapshot_state.valid_quote_codes = results.get("valid_quote_codes", [])
            snapshot_state.failed_codes = results.get("failed_codes", []) or list(codes)
            self._persist_snapshot_state(snapshot_state)
            logger.error(
                "快照状态已持久化: snapshot_id=%s status=failed expected=%d error=%s",
                snapshot_id, len(codes), exc,
            )
            raise

        completed_codes = results.get("completed_codes", [])
        valid_quote_codes = results.get("valid_quote_codes", [])
        failed_codes = results.get("failed_codes", [])

        expected = len(codes)
        valid = len(valid_quote_codes)
        if valid == 0:
            snapshot_state.status = "failed"
        elif valid < expected:
            snapshot_state.status = "partial"
        else:
            snapshot_state.status = "completed"
        snapshot_state.finished_at = datetime.now()
        snapshot_state.completed_codes = completed_codes
        snapshot_state.valid_quote_codes = valid_quote_codes
        snapshot_state.failed_codes = failed_codes
        self._persist_snapshot_state(snapshot_state)

        coverage = len(valid_quote_codes) / max(len(codes), 1)
        logger.info(
            "快照状态已持久化: snapshot_id=%s status=%s expected=%d valid=%d failed=%d coverage=%.1f%% source=%s",
            snapshot_id, snapshot_state.status, len(codes), len(valid_quote_codes), len(failed_codes), coverage * 100, source,
        )

        # Fetch and persist market index snapshots (fail-open, after stock snapshot is done)
        try:
            self._run_market_index_snapshot(snapshot_id, self._current_run_id, snapshot_state.markets)
        except Exception as exc:
            logger.warning("[市场指数] 指数快照阶段异常（不影响个股决策）: %s", exc)

        return results, snapshot_state

    def _snapshot_one_stock_with_quote(self, code: str, now: datetime, quote) -> SnapshotQuoteProcessResult:
        """Process a pre-fetched quote for one stock against thresholds.

        Called for stocks that were hit by batch prefetch.
        Quote must be a UnifiedRealtimeQuote (not None).

        Returns SnapshotQuoteProcessResult with status and event count.
        """
        if quote is None:
            logger.error("_snapshot_one_stock_with_quote called with None quote [%s]", code)
            self._save_event_unavailable(code, now)
            return SnapshotQuoteProcessResult(
                code=code, status=SnapshotQuoteStatus.ERROR,
                message="None quote passed to method",
            )

        # Suspended detection
        if quote.price is None or quote.price <= 0:
            self._save_event(self._stamp_event(IntradayEvent(
                timestamp=now,
                stock_code=code,
                stock_name=quote.name or "",
                current_price=0.0,
                volume_ratio=None,
                change_pct=None,
                event_type="suspended",
                description="停牌或无有效价格",
            ), snapshot_status="suspended"), raw_quote_data=self._extract_quote_data(quote))
            return SnapshotQuoteProcessResult(
                code=code, status=SnapshotQuoteStatus.SUSPENDED,
                source=getattr(quote, 'source', None),
            )

        # Sanitize values
        price = float(quote.price)
        vol_ratio = float(quote.volume_ratio) if quote.volume_ratio is not None else None
        change = float(quote.change_pct) if quote.change_pct is not None else None
        name = str(quote.name or "")

        # Compare with thresholds
        key = normalize_stock_code_key(code)
        yesterday = (
            self._yesterday_analysis.get(key)
            or self._yesterday_analysis.get(key.upper())
            or self._yesterday_analysis.get(key.lower())
            or {}
        )
        event_count = 0
        if not any(yesterday.values()):
            event = IntradayEvent(
                timestamp=now,
                stock_code=code,
                stock_name=name,
                current_price=price,
                volume_ratio=vol_ratio,
                change_pct=change,
                event_type="price_only",
                description=f"当前价 {price}（无昨日分析数据）",
            )
            self._save_event(self._stamp_event(event, snapshot_status="valid"), raw_quote_data=self._extract_quote_data(quote))
            event_count = 1
            return SnapshotQuoteProcessResult(
                code=code, status=SnapshotQuoteStatus.VALID, event_count=event_count,
                source=getattr(quote, 'source', None),
            )

        events = _compare_with_thresholds(code, name, price, vol_ratio, change, yesterday, now=now)
        for event in events:
            self._save_event(self._stamp_event(event, snapshot_status="valid"), raw_quote_data=self._extract_quote_data(quote))
            event_count += 1

        return SnapshotQuoteProcessResult(
            code=code, status=SnapshotQuoteStatus.VALID, event_count=event_count,
            source=getattr(quote, 'source', None),
        )

    def _fallback_single_quote_without_bulk_sources(self, code: str, now: datetime) -> SnapshotQuoteProcessResult:
        """Fallback for stocks not hit by batch prefetch.

        Uses only single-stock-capable fetchers via get_realtime_quote_single_only
        (no bulk full-market interfaces like stock_hk_spot).
        """
        logger.info(
            "[单股行情] 单股逐笔获取 [%s] (batch未命中, 使用单股快接口, 不含全市场批量源)",
            code, extra=_log_ctx(self),
        )
        timeout = self._resolve_quote_timeout(code)
        quote = self._get_realtime_with_timeout(
            code, timeout=timeout,
            fetch_fn=self._fetcher.get_realtime_quote_single_only,
        )

        if quote is None:
            self._save_event_unavailable(code, now)
            return SnapshotQuoteProcessResult(
                code=code, status=SnapshotQuoteStatus.TIMEOUT,
                message="single-stock fallback returned None",
            )

        # Delegate to shared processing (same as batch-matched path)
        if quote.price is None or quote.price <= 0:
            self._save_event(self._stamp_event(IntradayEvent(
                timestamp=now,
                stock_code=code,
                stock_name=quote.name or "",
                current_price=0.0,
                volume_ratio=None,
                change_pct=None,
                event_type="suspended",
                description="停牌或无有效价格(单股逐笔)",
            ), snapshot_status="suspended"), raw_quote_data=self._extract_quote_data(quote))
            return SnapshotQuoteProcessResult(
                code=code, status=SnapshotQuoteStatus.SUSPENDED,
                source=getattr(quote, 'source', None),
            )

        price = float(quote.price)
        vol_ratio = float(quote.volume_ratio) if quote.volume_ratio is not None else None
        change = float(quote.change_pct) if quote.change_pct is not None else None
        name = str(quote.name or "")

        key = normalize_stock_code_key(code)
        yesterday = (
            self._yesterday_analysis.get(key)
            or self._yesterday_analysis.get(key.upper())
            or self._yesterday_analysis.get(key.lower())
            or {}
        )
        event_count = 0
        if not any(yesterday.values()):
            event = IntradayEvent(
                timestamp=now,
                stock_code=code,
                stock_name=name,
                current_price=price,
                volume_ratio=vol_ratio,
                change_pct=change,
                event_type="price_only",
                description=f"当前价 {price}（无昨日分析数据, 单股逐笔）",
            )
            self._save_event(self._stamp_event(event, snapshot_status="valid"), raw_quote_data=self._extract_quote_data(quote))
            event_count = 1
            return SnapshotQuoteProcessResult(
                code=code, status=SnapshotQuoteStatus.VALID, event_count=event_count,
                source=getattr(quote, 'source', None),
            )

        events = _compare_with_thresholds(code, name, price, vol_ratio, change, yesterday, now=now)
        for event in events:
            self._save_event(self._stamp_event(event, snapshot_status="valid"), raw_quote_data=self._extract_quote_data(quote))
            event_count += 1

        return SnapshotQuoteProcessResult(
            code=code, status=SnapshotQuoteStatus.VALID, event_count=event_count,
            source=getattr(quote, 'source', None),
        )

    def _save_event_unavailable(self, code: str, now: datetime) -> None:
        """Save a data_unavailable event for a stock. Shared by batch and fallback paths."""
        self._save_event(self._stamp_event(IntradayEvent(
            timestamp=now,
            stock_code=code,
            stock_name="",
            current_price=0.0,
            volume_ratio=None,
            change_pct=None,
            event_type="data_unavailable",
            description="实时行情获取超时或所有数据源失败",
        ), snapshot_status="timeout"))

    def _get_realtime_with_timeout(self, code: str, timeout: float = 10.0,
                                    snapshot_id: Optional[str] = None,
                                    fetch_fn: Optional[Callable[[str], Any]] = None):
        """Call get_realtime_quote (or custom fetch_fn) with timeout protection.

        Uses a module-level executor so shutdown does not block on stuck threads.

        Args:
            code: stock code to fetch
            timeout: max wait seconds
            snapshot_id: optional snapshot context for logging
            fetch_fn: optional callable to use instead of self._fetcher.get_realtime_quote

        Note: Python Futures cannot be truly cancelled for non-cancellable I/O
        (e.g. socket operations). The underlying network call may continue
        running even after timeout. The `_expired_requests` set guards against
        late results being consumed after timeout.
        """
        fn = fetch_fn or self._fetcher.get_realtime_quote
        request_id = f"{snapshot_id or 'nosnap'}:{code}:{time.time()}"
        future = _QUOTE_EXECUTOR.submit(fn, code)
        try:
            result = future.result(timeout=timeout)
            if request_id in self._expired_requests:
                logger.warning("get_realtime_quote [%s] late_result_ignored (request_id=%s)", code, request_id)
                return None
            return result
        except concurrent.futures.TimeoutError:
            self._expired_requests.add(request_id)
            logger.warning("get_realtime_quote [%s] 超时 (timeout=%ss, request_id=%s)", code, timeout, request_id)
            future.cancel()
            return None
        except Exception as exc:
            logger.warning("get_realtime_quote [%s] 异常: %s", code, exc)
            return None

    # ------------------------------------------------------------------
    # Final decision
    # ------------------------------------------------------------------

    def final_decision(self) -> None:
        """14:20 entry point: read today's events → LLM decision → email."""
        self._ensure_intraday_table()
        self._ensure_intraday_snapshots_table()
        ttl = getattr(self._config, 'intraday_decision_lock_ttl_seconds', 300)
        if not self._acquire_process_lock("intraday_decision", ttl_seconds=ttl):
            logger.warning("final_decision: 无法获取进程锁（另一个实例正在执行），放弃本次决策")
            return
        acquired = self._lock.acquire(timeout=60)
        if not acquired:
            logger.error("final_decision: 无法获取锁（monitor_snapshot 可能卡死），放弃本次决策")
            self._release_process_lock("intraday_decision")
            return
        try:
            self._final_decision_locked()
        finally:
            self._lock.release()
            self._release_process_lock("intraday_decision")

    def _final_decision_locked(self, preferred_snapshot_id: Optional[str] = None) -> None:
        """Final decision implementation (caller must hold self._lock)."""
        stock_codes = self._get_stock_codes()
        if stock_codes and not self._yesterday_analysis:
            logger.info("_yesterday_analysis 为空，尝试加载...")
            self.load_yesterday_analysis(stock_codes)

        baseline_status = "available" if self._yesterday_analysis else "missing"

        # Check trading day
        tradable = [c for c in stock_codes if self._is_stock_trading_today(c)]
        if not tradable:
            logger.info("所有监控股票对应市场今日均休市，跳过盘中决策")
            if not getattr(self._config, 'intraday_force_run', False):
                return

        logger.info(
            "决策摘要: stock_codes=%d tradable=%d baseline=%s",
            len(stock_codes), len(tradable), baseline_status,
        )

        # 数据库状态摘要
        if self._db and self._db.session_scope:
            try:
                with self._db.session_scope() as session:
                    ah_count = session.execute("SELECT COUNT(*) FROM analysis_history").scalar()
                    snap_count = session.execute("SELECT COUNT(*) FROM intraday_snapshots").scalar()
                    logger.info("数据库状态: analysis_history=%d intraday_snapshots=%d", ah_count, snap_count)
            except Exception:
                pass

        # Get snapshot: preferred if provided, otherwise latest completed.
        # When preferred_snapshot_id is specified but the snapshot is not usable,
        # alert immediately instead of silently falling back to an older snapshot.
        snapshot = None
        if preferred_snapshot_id:
            snapshot = self._get_usable_snapshot_by_id(preferred_snapshot_id)
            if snapshot:
                logger.info("使用本次决策快照: snapshot_id=%s", preferred_snapshot_id)
            else:
                logger.warning(
                    "本次决策快照不可用，不使用旧快照替代: snapshot_id=%s",
                    preferred_snapshot_id,
                )
                self._send_decision_email(
                    content=f"本次决策快照不可用，无法生成正式决策报告。snapshot_id={preferred_snapshot_id}",
                    report_type=EmailReportType.SNAPSHOT_INCOMPLETE_ALERT,
                    snapshot_id=preferred_snapshot_id,
                    baseline_status=baseline_status,
                    market_dates=None,
                )
                return
        else:
            snapshot = self._get_latest_usable_snapshot()
        if snapshot is not None:
            expected_count = len(snapshot["expected_codes"])
            valid_count = len(snapshot["valid_quote_codes"])
            logger.info(
                "决策使用快照: snapshot_id=%s expected=%d valid=%d coverage=%.1f%%",
                snapshot["snapshot_id"], expected_count,
                valid_count,
                (valid_count / max(expected_count, 1)) * 100,
            )
        if snapshot is None:
            logger.warning("no_usable_snapshot: 当前没有可用快照，跳过正式决策邮件")
            # Send incomplete alert
            self._send_decision_email(
                content="当日尚无可用盘中快照，无法生成正式决策报告。",
                report_type=EmailReportType.SNAPSHOT_INCOMPLETE_ALERT,
                snapshot_id=None,
                baseline_status=baseline_status,
                market_dates=None,
            )
            return

        snapshot_id = snapshot["snapshot_id"]
        expected_count = len(snapshot["expected_codes"])
        valid_count = len(snapshot["valid_quote_codes"])
        coverage_ratio = valid_count / max(expected_count, 1)
        failed_codes = snapshot["failed_codes"]
        snapshot_market_dates = snapshot.get("market_dates") if snapshot else None

        # Coverage gate
        min_coverage = getattr(self._config, 'intraday_min_quote_coverage', 0.8)

        logger.info(
            "快照覆盖率检查: snapshot_id=%s expected=%d valid=%d failed=%d coverage=%.1f%% min_coverage=%.0f%%",
            snapshot_id, expected_count, valid_count, len(failed_codes),
            coverage_ratio * 100, min_coverage * 100,
        )

        if coverage_ratio < min_coverage:
            logger.warning(
                "行情覆盖率不足: expected_count=%d valid_count=%d failed_count=%d coverage_ratio=%.2f snapshot_id=%s",
                expected_count, valid_count, len(failed_codes), coverage_ratio, snapshot_id,
            )
            self._send_decision_email(
                content=(
                    f"行情覆盖率不足，无法生成正式决策报告。\n"
                    f"目标股票数: {expected_count}, 有效行情: {valid_count}, "
                    f"失败: {len(failed_codes)}, 覆盖率: {coverage_ratio:.1%}\n"
                    f"最低覆盖率要求: {min_coverage:.0%}"
                ),
                report_type=EmailReportType.DATA_QUALITY_ALERT,
                snapshot_id=snapshot_id,
                coverage_ratio=coverage_ratio,
                valid_count=valid_count,
                expected_count=expected_count,
                failed_codes=failed_codes,
                baseline_status=baseline_status,
                market_dates=snapshot_market_dates,
            )
            return

        # 决策状态摘要
        report_type = "pending"
        logger.info(
            "决策状态摘要: baseline=%s snapshot_id=%s status=%s expected=%d valid=%d coverage=%.1f%% report_type=%s",
            baseline_status, snapshot["snapshot_id"], snapshot["status"],
            expected_count, valid_count, coverage_ratio * 100,
            report_type,
        )

        # Load events for this completed snapshot
        all_events = self._load_events_for_snapshot(snapshot_id)

        if not all_events:
            logger.info("无盘中事件(snapshot=%s)，跳过决策邮件", snapshot_id)
            return

        # If no baseline, downgrade
        if baseline_status == "missing":
            logger.warning("baseline_status=missing: 无昨日基准分析，降级为无基准摘要")
            no_baseline_markets = {self._get_market_for_stock(c) for c in stock_codes if self._get_market_for_stock(c)}
            raw = self._build_raw_summary(all_events, markets=no_baseline_markets)
            self._send_decision_email(
                content=raw,
                report_type=EmailReportType.NO_BASELINE_ALERT,
                snapshot_id=snapshot_id,
                coverage_ratio=coverage_ratio,
                valid_count=valid_count,
                expected_count=expected_count,
                failed_codes=failed_codes,
                baseline_status=baseline_status,
                market_dates=snapshot_market_dates,
            )
            return

        # Build prompt (filter out data_unavailable)
        from src.core.intraday_prompt import build_intraday_prompt

        # Rebuild snapshot_times from DB instead of relying on in-memory state
        db_snapshot_times = self._rebuild_snapshot_times_from_db()
        merged_times = db_snapshot_times if db_snapshot_times else self._snapshot_times

        # Determine markets and market_dates for timeline queries
        decision_markets = list(snapshot["markets"]) if snapshot.get("markets") else []
        decision_market_dates = snapshot.get("market_dates") if snapshot.get("market_dates") else {}

        # Load stock timelines (all snapshots across the day)
        stock_timelines = self._load_stock_timelines_for_decision(decision_markets, decision_market_dates)

        # Load market index timelines
        market_timelines = self._load_market_timelines_for_decision(decision_markets, decision_market_dates)

        # --- Market index coverage diagnostics ---
        market_coverage_parts: List[str] = []
        market_index_coverage_ratio: Optional[float] = None
        mkt_coverage_info: Dict[str, Any] = {
            'ratio': None, 'summary': None, 'quality_alert': False,
        }
        if decision_market_dates and decision_markets:
            expected_indices_per_market = {}
            for mkt in decision_markets:
                indices = self._get_market_indices(mkt)
                if indices:
                    expected_indices_per_market[mkt] = len(indices)
            expected_snapshots = len(merged_times) if merged_times else 1
            total_valid = 0
            total_expected = 0
            for market in decision_markets:
                timelines = market_timelines.get(market, [])
                total_points = sum(len(t.get('points', [])) for t in timelines)
                valid_points = sum(
                    1 for t in timelines
                    for p in t.get('points', [])
                    if p.get('status', 'valid') != 'fetch_failed'
                )
                logger.info(
                    "market_timeline_summary market=%s query_date=%s index_count=%d total_points=%d valid_points=%d",
                    market, decision_market_dates.get(market, '?'), len(timelines),
                    total_points, valid_points,
                )
                exp = expected_indices_per_market.get(market, 1) or 1
                total_expected += exp * expected_snapshots
                total_valid += valid_points
                market_coverage_parts.append(
                    f"  {market.upper()}: {valid_points}/{exp * expected_snapshots} 有效快照"
                )
            if total_expected > 0:
                market_index_coverage_ratio = total_valid / total_expected
                logger.info(
                    "market_index_coverage total_valid=%d total_expected=%d ratio=%.1f%%",
                    total_valid, total_expected, market_index_coverage_ratio * 100,
                )
        min_mkt_cov = getattr(self._config, 'intraday_min_market_index_coverage', 0.0)
        market_quality_alert = False
        if market_index_coverage_ratio is not None and min_mkt_cov > 0 and market_index_coverage_ratio < min_mkt_cov:
            if getattr(self._config, 'intraday_data_quality_alert_enabled', True):
                logger.warning(
                    "DATA_QUALITY_ALERT: market_index_coverage=%.1f%% < threshold=%.0f%%. "
                    "Decision email will include quality warning.",
                    market_index_coverage_ratio * 100, min_mkt_cov * 100,
                )
                market_quality_alert = True
        mkt_coverage_info = {
            'ratio': market_index_coverage_ratio,
            'summary': "\n".join(market_coverage_parts) if market_coverage_parts else None,
            'quality_alert': market_quality_alert,
        }

        # --- Historical snapshot count (standalone decision) ---
        historical_snapshot_count = 0
        try:
            with self._db.session_scope() as session:
                from sqlalchemy import text as sa_text
                market_dates_list = list(decision_market_dates.values()) if decision_market_dates else [datetime.now().strftime('%Y-%m-%d')]
                placeholders = ", ".join([f":qd{i}" for i in range(len(market_dates_list))])
                params = {f"qd{i}": d for i, d in enumerate(market_dates_list)}
                row = session.execute(
                    sa_text(
                        f"SELECT COUNT(*) FROM intraday_snapshots "
                        f"WHERE query_date IN ({placeholders}) AND status IN ('completed', 'partial')"
                    ),
                    params,
                ).scalar()
                historical_snapshot_count = row or 0
                logger.info(
                    "historical_snapshots count=%d for market_dates=%s",
                    historical_snapshot_count, market_dates_list,
                )
        except Exception as exc:
            logger.warning("查询历史快照数失败: %s", exc)

        # --- Load market timeline stats for prompt ---
        market_timeline_stats = self._load_market_timeline_stats_for_decision(
            market_timelines, decision_markets, decision_market_dates,
        )

        # --- Relative strength summary ---
        relative_strength_summary = None
        try:
            from src.core.intraday_relative_strength import (
                summarize_relative_strength,
                format_relative_strength_section,
            )
            rs = summarize_relative_strength(stock_timelines, market_timelines)
            if rs:
                relative_strength_summary = format_relative_strength_section(rs)
        except Exception as exc:
            logger.debug("Relative strength summary skipped: %s", exc)
            relative_strength_summary = None

        prompt = build_intraday_prompt(
            events=all_events,
            yesterday_analysis=self._yesterday_analysis,
            snapshot_times=merged_times,
            markets={self._get_market_for_stock(c) for c in stock_codes if self._get_market_for_stock(c)},
            stock_timelines=stock_timelines,
            market_timelines=market_timelines,
            market_timeline_stats=market_timeline_stats,
            historical_snapshot_count=historical_snapshot_count,
            relative_strength_summary=relative_strength_summary,
            expected_codes=list({e.stock_code for e in all_events}),
        )

        # Call LLM via unified analyzer with fallback
        result = self._call_llm_with_fallback(prompt)

        if result.status == "success":
            # Check for truncation: if response truncated, degrade to alert
            if result.truncated_suspected:
                logger.warning(
                    "LLM response truncated (finish_reason=%s chars=%d). Degrading to LLM_FAILURE_ALERT.",
                    result.finish_reason, result.response_chars,
                )
                trunc_content = (
                    f"## LLM 响应截断告警\n\n"
                    f"主LLM和备用LLM的输出均被截断，无法生成完整的盘中决策。\n\n"
                    f"- 主LLM finish_reason: {result.finish_reason or 'unknown'}\n"
                    f"- 响应字符数: {result.response_chars}\n"
                    f"- 建议: 增大 INTRADAY_LLM_MAX_TOKENS（当前 {self._config.intraday_llm_max_tokens}），或减少监控股票数量\n"
                )
                self._send_decision_email(
                    content=trunc_content,
                    report_type=EmailReportType.LLM_FAILURE_ALERT,
                    snapshot_id=snapshot_id,
                    coverage_ratio=coverage_ratio,
                    valid_count=valid_count,
                    expected_count=expected_count,
                    llm_status="truncated",
                    baseline_status=baseline_status,
                    market_dates=snapshot_market_dates,
                )
            else:
                content = self._normalize_official_decision_content(result.content or "")
                # Prepend fallback indicator if applicable
                if result.provider == "fallback":
                    fallback_lines = [
                        f"> LLM来源: fallback",
                        f"> 主LLM状态: {result.primary_error_message or 'unknown'}",
                        f"> 备用模型: {result.model or 'unknown'}",
                        "",
                    ]
                    content = "\n".join(fallback_lines) + content

                # Build expected decision codes and validate completeness
                completeness_enabled = getattr(self._config, 'intraday_decision_completeness_enabled', True)
                if completeness_enabled:
                    expected_scope = getattr(self._config, 'intraday_decision_expected_scope', 'events')
                    if expected_scope == 'valid_quotes':
                        expected_decision_codes = set(snapshot.get("valid_quote_codes", []))
                    else:
                        # Default: events
                        expected_decision_codes = set(
                            e.stock_code for e in all_events
                        )
                    integrity = self._validate_official_decision_completeness(
                        content, expected_decision_codes,
                    )
                    # Log integrity result
                    logger.info(
                        "Decision content integrity: ok=%s expected=%d covered=%d missing=%d sentinel_ok=%s reason=%s",
                        integrity.ok, len(integrity.expected_codes),
                        len(integrity.covered_codes), len(integrity.missing_codes),
                        integrity.sentinel_ok, integrity.reason,
                    )
                    # Add coverage display to email header
                    coverage_lines = [
                        f"> 决策覆盖: {len(integrity.covered_codes)}/{len(integrity.expected_codes)}",
                        f"> 内容完整性: {'passed' if integrity.ok else integrity.reason}",
                    ]
                    if result.finish_reason:
                        coverage_lines.append(
                            f"> LLM finish_reason: {result.finish_reason} (chars={result.response_chars})"
                        )
                    coverage_lines.append("")
                    content = "\n".join(coverage_lines) + "\n" + content

                self._send_decision_email(
                    content=content,
                    report_type=EmailReportType.OFFICIAL_DECISION,
                    snapshot_id=snapshot_id,
                    coverage_ratio=coverage_ratio,
                    valid_count=valid_count,
                    expected_count=expected_count,
                    failed_codes=failed_codes,
                    llm_status="success",
                    baseline_status=baseline_status,
                    market_dates=snapshot_market_dates,
                    market_index_coverage=mkt_coverage_info,
                )
        elif result.status == "config_error":
            logger.error("LLM config error, no email sent: %s", result.error_message)
            if getattr(self._config, 'intraday_send_llm_failure_alert', True):
                # Build content with fallback details if available
                if result.fallback_error_message:
                    fallback_status_display = result.fallback_status or "config_error"
                    fail_content = (
                        f"## LLM 调用失败（配置错误）\n\n"
                        f"### 主 LLM\n"
                        f"- 状态: {result.primary_status or 'config_error'}\n"
                        f"- 错误: {result.primary_error_message or result.error_message}\n\n"
                        f"### 备用 LLM\n"
                        f"- 状态: {fallback_status_display}\n"
                        f"- 错误: {result.fallback_error_message}\n"
                        f"- 模型: {result.model or 'N/A'}\n\n"
                        f"### 建议检查\n"
                        f"- INTRADAY_LLM_FALLBACK_ENABLED\n"
                        f"- INTRADAY_LLM_FALLBACK_BASE_URL\n"
                        f"- INTRADAY_LLM_FALLBACK_API_KEY\n"
                        f"- INTRADAY_LLM_FALLBACK_MODEL\n"
                    )
                elif result.provider == "fallback":
                    skip_reason = getattr(result, 'fallback_skipped_reason', None) or "配置不完整或未启用"
                    fail_content = (
                        f"## LLM 调用失败\n\n"
                        f"### 主 LLM\n"
                        f"- 状态: {result.primary_status or 'config_error'}\n"
                        f"- 错误: {result.primary_error_message or result.error_message}\n\n"
                        f"### 备用 LLM\n"
                        f"- 未尝试（{skip_reason}）\n"
                    )
                else:
                    fail_content = f"LLM调用失败（配置错误），请检查模型配置。\n错误: {result.error_message}"
                self._send_decision_email(
                    content=fail_content,
                    report_type=EmailReportType.LLM_FAILURE_ALERT,
                    snapshot_id=snapshot_id,
                    coverage_ratio=coverage_ratio,
                    valid_count=valid_count,
                    expected_count=expected_count,
                    llm_status="config_error",
                    baseline_status=baseline_status,
                    market_dates=snapshot_market_dates,
                )
        else:
            # network_error, rate_limited, empty_response
            if getattr(self._config, 'intraday_send_llm_failure_alert', True):
                if result.fallback_error_message:
                    # Both primary and fallback failed — show separate statuses
                    fallback_status_display = result.fallback_status or result.status
                    raw = (
                        f"## LLM 调用失败\n\n"
                        f"### 主 LLM\n"
                        f"- 状态: {result.primary_status or result.status}\n"
                        f"- 错误: {result.primary_error_message or result.error_message}\n\n"
                        f"### 备用 LLM\n"
                        f"- 状态: {fallback_status_display}\n"
                        f"- 错误: {result.fallback_error_message}\n"
                        f"- 模型: {result.model or 'N/A'}\n"
                        f"- 主机: {result.base_url_host or 'N/A'}\n"
                    )
                elif result.provider == "fallback":
                    skip_reason = getattr(result, 'fallback_skipped_reason', None) or "配置不完整或未启用"
                    raw = (
                        f"## LLM 调用失败\n\n"
                        f"### 主 LLM\n"
                        f"- 状态: {result.primary_status or result.status}\n"
                        f"- 错误: {result.primary_error_message or result.error_message}\n\n"
                        f"### 备用 LLM\n"
                        f"- 未尝试（{skip_reason}）\n"
                    )
                else:
                    if getattr(self._config, 'intraday_send_raw_summary_on_llm_failure', False):
                        llm_fail_markets = {self._get_market_for_stock(c) for c in stock_codes if self._get_market_for_stock(c)}
                        raw = self._build_raw_summary(all_events, markets=llm_fail_markets)
                    else:
                        raw = f"LLM调用失败: {result.status}\n{result.error_message}"
                self._send_decision_email(
                    content=raw,
                    report_type=EmailReportType.LLM_FAILURE_ALERT,
                    snapshot_id=snapshot_id,
                    coverage_ratio=coverage_ratio,
                    valid_count=valid_count,
                    expected_count=expected_count,
                    llm_status=result.status,
                    baseline_status=baseline_status,
                    market_dates=snapshot_market_dates,
                )

    def _call_llm(self, prompt: str) -> LLMResult:
        """Call LLM via the unified GeminiAnalyzer.generate_text_with_metadata() pipeline.

        Uses the injected or lazily-initialized analyzer so that model
        resolution, Router, API keys, fallback, and retry logic are all
        handled by the same code path as the post-market analysis.

        Passes explicit max_tokens, temperature, and system_prompt to prevent
        report truncation and ensure consistent generation parameters.

        Returns an LLMResult with finish_reason, response_chars, usage, and
        completion_tokens populated for truncation detection.
        """
        try:
            analyzer = self._get_llm_analyzer()
        except Exception as exc:
            logger.error("LLM analyzer init failed: %s", exc)
            return LLMResult(status="config_error", error_message=str(exc))

        if not analyzer.is_available():
            return LLMResult(
                status="config_error",
                error_message="LLM analyzer not available (no Router and no legacy litellm fallback)",
            )

        try:
            result = analyzer.generate_text_with_metadata(
                prompt,
                max_tokens=self._config.intraday_llm_max_tokens,
                temperature=self._config.llm_temperature,
                system_prompt=INTRADAY_SYSTEM_PROMPT,
                call_type="intraday_decision",
                raise_on_error=True,
            )
        except Exception as exc:
            return self._classify_llm_error(exc)

        if result is None or not result.text or not result.text.strip():
            return LLMResult(status="empty_response", error_message="Empty response from LLM")

        # Detect truncation
        finish_reason = result.finish_reason
        truncated = finish_reason in ("length", "max_tokens")
        if truncated:
            logger.warning(
                "LLM response truncated: finish_reason=%s chars=%d tokens=%d",
                finish_reason, result.response_chars, result.completion_tokens,
            )

        return LLMResult(
            status="success",
            content=str(result.text),
            finish_reason=finish_reason,
            response_chars=result.response_chars,
            usage=result.usage,
            completion_tokens=result.completion_tokens,
            truncated_suspected=truncated,
        )

    def _classify_llm_error(self, exc: Exception) -> LLMResult:
        """Classify an LLM exception into an LLMResult with appropriate status."""
        error_str = str(exc)
        if self._is_config_error(error_str):
            logger.error("LLM config/auth error: %s", exc)
            return LLMResult(status="config_error", error_message=error_str)
        if "ratelimit" in error_str.lower() or "rate_limit" in error_str.lower() or "429" in error_str:
            logger.warning("LLM rate limited: %s", exc)
            return LLMResult(status="rate_limited", error_message=error_str)
        if "timeout" in error_str.lower() or "connection" in error_str.lower():
            logger.warning("LLM network error: %s", exc)
            return LLMResult(status="network_error", error_message=error_str)
        logger.error("LLM call failed: %s", exc)
        return LLMResult(status="network_error", error_message=error_str)

    @staticmethod
    def _is_config_error(error_str: str) -> bool:
        """Check if an error string indicates a configuration or authentication error.

        Uses precise keyword matching. Does NOT use bare "auth" which is too broad
        and would also match generic auth errors unrelated to configuration.
        """
        lowered = error_str.lower()
        # Exact config/credential errors
        config_keywords = (
            "authenticationerror", "unauthorized", "forbidden",
            "invalid api key", "incorrect api key", "credential",
            "invalid x-goog", "permission denied", "access denied",
            "apikey", "no litellm router", "no models configured",
            "missing", "not configured", "no api key",
        )
        # Model-not-found errors (often indicate config drift, not network issues)
        model_keywords = (
            "notfounderror", "model is not found", "model not found",
            "model_not_found", "invalid model", "unknown model",
        )
        for kw in config_keywords:
            if kw in lowered:
                return True
        for kw in model_keywords:
            if kw in lowered:
                return True
        # Do NOT use bare "auth" — too broad
        return False

    def _call_llm_with_fallback(self, prompt: str) -> LLMResult:
        """Call primary LLM, fall back to backup LLM on failure or truncation if configured."""
        primary = self._call_llm(prompt)
        primary.provider = "primary"
        primary.primary_status = primary.status

        # Truncation is treated as a retryable condition
        if primary.status == "success" and primary.truncated_suspected:
            logger.warning(
                "Primary LLM truncated: finish_reason=%s chars=%d. Will attempt fallback retry.",
                primary.finish_reason, primary.response_chars,
            )
            # Fall through to fallback attempt below
        elif primary.status == "success":
            primary.fallback_attempted = False
            return primary

        # Check if we should try fallback (for error statuses or truncation)
        try_fallback = False
        if primary.status != "success":
            try_fallback = self._should_try_fallback_llm(primary.status)
        elif primary.truncated_suspected:
            try_fallback = self._should_try_fallback_llm(primary.status)

        if not try_fallback:
            primary.fallback_attempted = False
            primary.fallback_skipped_reason = getattr(self, '_fallback_skipped_reason', None)
            return primary

        # Fallback attempted
        self._fallback_attempted = True
        fallback = self._call_fallback_llm(prompt, primary)
        if fallback.status == "success":
            fallback.provider = "fallback"
            fallback.primary_error_message = primary.error_message
            fallback.attempts = primary.attempts + fallback.attempts
            fallback.fallback_attempted = True
            logger.info(
                "Fallback LLM succeeded. provider=%s model=%s base_url=%s",
                fallback.provider, fallback.model, fallback.base_url_host,
            )
            return fallback

        # Both failed — merge errors with separate statuses
        # If primary truncated and fallback also failed, flag it
        if primary.truncated_suspected:
            merged_status = "truncated_response"
            merged_error = (
                f"主LLM截断: {primary.finish_reason or 'unknown'}; "
                f"备用LLM: {fallback.error_message}"
            )
        else:
            merged_status = primary.status
            merged_error = f"主LLM: {primary.error_message}; 备用LLM: {fallback.error_message}"

        merged = LLMResult(
            status=merged_status,
            fallback_status=fallback.status,
            content=None,
            error_message=merged_error,
            provider="fallback",
            model=fallback.model,
            base_url_host=fallback.base_url_host,
            attempts=primary.attempts + fallback.attempts,
            primary_error_message=primary.error_message,
            fallback_error_message=fallback.error_message,
            primary_status=primary.status,
            truncated_suspected=primary.truncated_suspected,
            fallback_attempted=True,
        )
        return merged

    def _should_try_fallback_llm(self, primary_status: str) -> bool:
        """Check whether fallback LLM should be attempted based on config and primary status.

        Validates: enabled, model, api_key, base_url (with scheme check), protocol.
        Sets self._fallback_skipped_reason on False returns for diagnostics.
        """
        self._fallback_skipped_reason = None
        config = self._config
        if not getattr(config, 'intraday_llm_fallback_enabled', False):
            self._fallback_skipped_reason = "fallback disabled (INTRADAY_LLM_FALLBACK_ENABLED=false)"
            logger.info("Fallback LLM disabled (INTRADAY_LLM_FALLBACK_ENABLED=false)")
            return False
        if not getattr(config, 'intraday_llm_fallback_model', None) or not getattr(config, 'intraday_llm_fallback_api_key', None):
            self._fallback_skipped_reason = "incomplete config: model or api_key missing"
            logger.warning(
                "Fallback LLM enabled but incomplete config: model=%s api_key=%s",
                getattr(config, 'intraday_llm_fallback_model', None),
                "set" if getattr(config, 'intraday_llm_fallback_api_key', None) else "missing",
            )
            return False
        # Validate base_url is present and has valid scheme
        fb_base_url = getattr(config, 'intraday_llm_fallback_base_url', None)
        if not fb_base_url or not fb_base_url.strip():
            self._fallback_skipped_reason = "base_url missing or empty"
            logger.warning("Fallback LLM skipped: base_url is missing or empty")
            return False
        if not (fb_base_url.startswith("http://") or fb_base_url.startswith("https://")):
            self._fallback_skipped_reason = f"base_url has invalid scheme (must be http:// or https://)"
            logger.warning("Fallback LLM skipped: base_url has invalid scheme (no http:// or https://)")
            return False
        # Validate protocol
        fb_protocol = getattr(config, 'intraday_llm_fallback_protocol', 'openai')
        if fb_protocol != "openai":
            self._fallback_skipped_reason = f"unsupported protocol: {fb_protocol} (only 'openai' supported)"
            logger.warning("Fallback LLM skipped: unsupported protocol=%s", fb_protocol)
            return False
        # Check if this status qualifies for retry
        retry_on = set(
            s.strip() for s in getattr(config, 'intraday_llm_fallback_retry_on', '').split(",") if s.strip()
        )
        should_retry = primary_status in retry_on
        if not should_retry:
            self._fallback_skipped_reason = f"primary_status={primary_status} not in retry_on={retry_on}"
        logger.info(
            "Fallback decision: status=%s retry_on=%s should_retry=%s",
            primary_status, retry_on, should_retry,
        )
        return should_retry

    def _call_fallback_llm(self, prompt: str, primary_result: LLMResult) -> LLMResult:
        """Call the fallback LLM using direct litellm.completion()."""
        config = self._config
        fallback_model = getattr(config, 'intraday_llm_fallback_model', None) or ""
        fallback_api_key = getattr(config, 'intraday_llm_fallback_api_key', None) or ""
        fallback_base_url = getattr(config, 'intraday_llm_fallback_base_url', None) or ""
        fallback_timeout = getattr(config, 'intraday_llm_fallback_timeout_sec', 60)
        fallback_protocol = getattr(config, 'intraday_llm_fallback_protocol', 'openai')

        # Ensure model name has provider prefix for openai protocol
        if fallback_protocol == "openai" and fallback_model and "/" not in fallback_model:
            fallback_model = f"openai/{fallback_model}"
            logger.info("Added openai/ prefix to fallback model: %s", fallback_model)

        # Determine max_tokens
        fallback_max_tokens = config.intraday_llm_fallback_max_tokens or config.intraday_llm_max_tokens

        # Determine temperature
        fallback_temperature = config.llm_temperature
        if config.intraday_llm_fallback_temperature is not None:
            fallback_temperature = config.intraday_llm_fallback_temperature

        # Extract base_url host for logging (no API key leakage)
        base_url_host = None
        try:
            from urllib.parse import urlparse
            parsed = urlparse(fallback_base_url)
            base_url_host = parsed.netloc or parsed.hostname
        except Exception:
            base_url_host = fallback_base_url.split("://")[-1].split("/")[0] if "://" in fallback_base_url else fallback_base_url

        logger.info(
            "Calling fallback LLM: model=%s base_url=%s max_tokens=%s temperature=%s timeout=%s",
            fallback_model, base_url_host, fallback_max_tokens, fallback_temperature, fallback_timeout,
        )

        try:
            response = litellm.completion(
                model=fallback_model,
                api_key=fallback_api_key,
                api_base=fallback_base_url,
                messages=[
                    {"role": "system", "content": INTRADAY_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=fallback_max_tokens,
                temperature=fallback_temperature,
                timeout=fallback_timeout,
            )
            text = response.choices[0].message.content if response.choices else None
            if not text or not text.strip():
                logger.warning("Fallback LLM returned empty response")
                return LLMResult(
                    status="empty_response",
                    error_message="Fallback LLM returned empty response",
                    model=fallback_model,
                    base_url_host=base_url_host,
                    provider="fallback",
                )
            logger.info("Fallback LLM call succeeded: model=%s", fallback_model)
            return LLMResult(
                status="success",
                content=str(text),
                model=fallback_model,
                base_url_host=base_url_host,
                provider="fallback",
            )
        except Exception as exc:
            error_str = str(exc)
            logger.error("Fallback LLM call failed: %s", error_str)
            # Classify the error
            status = "network_error"
            lowered = error_str.lower()
            config_keywords = (
                "authenticationerror", "unauthorized", "forbidden",
                "invalid api key", "incorrect api key", "credential",
                "invalid x-goog", "permission denied", "access denied",
                "apikey",
            )
            if any(kw in lowered for kw in config_keywords):
                status = "config_error"
            elif "ratelimit" in lowered or "rate_limit" in lowered or "429" in error_str:
                status = "rate_limited"
            elif "timeout" in lowered or "connection" in lowered:
                status = "network_error"
            return LLMResult(
                status=status,
                error_message=error_str,
                model=fallback_model,
                base_url_host=base_url_host,
                provider="fallback",
            )

    def _build_raw_summary(self, events: List[IntradayEvent], markets: Optional[set] = None) -> str:
        """Build raw data summary when LLM fails."""
        is_mixed = len(markets) > 1 if markets else False
        lines = [
            "# 盘中监控原始数据摘要",
            "",
            "> 注意：LLM 调用失败，以下为原始盘中事件数据，仅供参考。",
            "",
            "| 时间 | 股票 | 价格 | 量比 | 涨跌幅 | 事件类型 | 描述 |",
            "|------|------|------|------|--------|----------|------|",
        ]
        for e in events:
            vol = f"{e.volume_ratio:.1f}" if e.volume_ratio is not None else "-"
            chg = f"{e.change_pct:+.2f}%" if e.change_pct is not None else "-"
            lines.append(
                f"| {format_intraday_event_time(e, include_market=is_mixed)} | {e.stock_name}({e.stock_code}) | "
                f"{e.current_price:.2f} | {vol} | {chg} | {e.event_type} | {e.description} |"
            )
        return "\n".join(lines)

    def _normalize_official_decision_content(self, raw_content: str) -> str:
        """Ensure official decision email uses grouped list format.

        1. If content already contains target group headers, return cleaned original.
        2. If content contains old Markdown table header, parse and regroup.
        3. If unparseable, return original with warning.
        """
        if not raw_content:
            return raw_content

        # Check if already in grouped format (with wider matching)
        grouped_headers = [
            "买入信号相关股票",
            "卖出信号相关股票（止盈）",
            "卖出信号相关股票（止损）",
            "卖出信号相关股票(止盈)",
            "卖出信号相关股票(止损)",
            "观望/无法判断",
            "观望 / 无法判断",
            "观望 无法判断",
            "买入信号", "卖出止盈", "卖出止损", "观望",
        ]
        if any(h in raw_content for h in grouped_headers):
            lines = raw_content.split("\n")
            cleaned = [l for l in lines if not l.startswith("| 股票 | 当前价 |") and not l.startswith("|------|--------|")]
            return "\n".join(cleaned)

        # Check for legacy table format
        if "| 股票 | 当前价 |" in raw_content:
            logger.warning("Detected legacy table format in LLM output — attempting to normalize to grouped list")
            lines = raw_content.split("\n")
            table_start = -1
            table_end = -1
            header_line = None
            for i, line in enumerate(lines):
                if line.startswith("| 股票 | 当前价 |"):
                    table_start = i
                    header_line = line
                if table_start >= 0 and i > table_start and not line.strip():
                    table_end = i
                    break
                if table_start >= 0 and i > table_start and not line.startswith("|"):
                    table_end = i
                    break
            if table_end == -1:
                table_end = len(lines)

            if header_line:
                cols = [c.strip() for c in header_line.strip("|").split("|")]
            else:
                cols = ["股票", "当前价", "日内走势", "大盘环境", "相对强弱", "数据质量", "建议", "理由"]

            try:
                stock_idx = next(i for i, c in enumerate(cols) if "股票" in c)
                price_idx = next(i for i, c in enumerate(cols) if "当前价" in c)
                suggestion_idx = next(i for i, c in enumerate(cols) if "建议" in c)
                reason_idx = next(i for i, c in enumerate(cols) if "理由" in c)
            except StopIteration:
                logger.warning("Cannot parse legacy table columns, returning original")
                return raw_content

            buy_stocks = []
            take_profit_stocks = []
            stop_loss_stocks = []
            watch_stocks = []

            for row_line in lines[table_start + 2:table_end]:
                row_line = row_line.strip()
                if not row_line.startswith("|"):
                    continue
                cells = [c.strip() for c in row_line.strip("|").split("|")]
                if len(cells) <= max(stock_idx, suggestion_idx, reason_idx):
                    continue
                stock = cells[stock_idx]
                price = cells[price_idx] if len(cells) > price_idx else ""
                suggestion = cells[suggestion_idx]
                reason = cells[reason_idx] if len(cells) > reason_idx else ""
                context = f"当前价 {price}，{reason}"

                if "买入" in suggestion:
                    buy_stocks.append(f"- **{stock}**：{context}")
                elif "止盈" in suggestion or ("卖出" in suggestion and "止盈" in reason):
                    take_profit_stocks.append(f"- **{stock}**：{context}")
                elif "止损" in suggestion or ("卖出" in suggestion and "止损" in reason):
                    stop_loss_stocks.append(f"- **{stock}**：{context}")
                else:
                    watch_stocks.append(f"- **{stock}**：{context}")

            result_lines = []
            result_lines.append("## 决策分析过程")
            result_lines.append("")
            result_lines.append("基于规则，对每只触发了事件的股票进行逐一分析：")
            result_lines.append("")
            result_lines.append("### 1. 买入信号相关股票")
            result_lines.append("")
            result_lines.extend(buy_stocks) if buy_stocks else result_lines.append("（无）")
            result_lines.append("")
            result_lines.append("### 2. 卖出信号相关股票（止盈）")
            result_lines.append("")
            result_lines.extend(take_profit_stocks) if take_profit_stocks else result_lines.append("（无）")
            result_lines.append("")
            result_lines.append("### 3. 卖出信号相关股票（止损）")
            result_lines.append("")
            result_lines.extend(stop_loss_stocks) if stop_loss_stocks else result_lines.append("（无）")
            result_lines.append("")
            result_lines.append("### 4. 观望/无法判断股票（如有）")
            result_lines.append("")
            result_lines.extend(watch_stocks) if watch_stocks else result_lines.append("（无）")

            prefix = [l for l in lines[:table_start] if l.strip()]
            suffix = [l for l in lines[table_end:] if l.strip()]

            final_lines = prefix + [""] + result_lines + [""] + suffix
            return "\n".join(final_lines)

        logger.warning("Cannot normalize decision content: no recognized format found")
        return raw_content

    @staticmethod
    def _extract_stock_codes_from_decision_content(content: str) -> set:
        """Extract stock codes from decision content text.

        Supports multiple code formats:
        - A股: 6-digit codes (600xxx, 000xxx, 300xxx, 601xxx, 603xxx, 002xxx)
        - 港股: HK-prefixed codes (HK00700, hk00700)
        - 美股: uppercase tickers (AAPL, TSLA)
        - 名称(代码) and 名称（代码） patterns
        - Plain codes embedded in text

        Returns a set of normalized stock code strings (uppercase).
        """
        import re as _re
        codes: set = set()

        patterns: list = [
            # 名称(代码) or **名称（代码）** — full-width or half-width brackets
            _re.compile(r'[*_]{0,2}[^(（\s]+[（(]\s*([A-Za-z]{0,6}\d{5,6})\s*[)）]'),
            # Plain A股 6-digit codes (standalone)
            _re.compile(r'\b([36]0[012]\d{3}|00[0123]\d{3}|60[013]\d{3})\b'),
            # HK codes: HKxxxxx or hkxxxxx (preserve HK prefix)
            _re.compile(r'\b([Hh][Kk]\d{5,6})\b'),
            # US tickers (uppercase letters, at least 2, typically not pure digits)
            _re.compile(r'\b([A-Z]{2,6})\b'),
        ]

        # Common markdown keywords to filter out
        _filter_keywords = {
            'INTRADAY', 'DECISION', 'COMPLETE', 'COVERED', 'EXPECTED', 'LLM', 'API',
            'HTML', 'HTTP', 'HTTPS', 'JSON', 'CSV', 'XML', 'NOTE', 'TODO',
        }

        for pat in patterns:
            for m in pat.finditer(content):
                if m.lastindex:
                    code = m.group(1).upper().strip()
                else:
                    code = m.group(0).upper().strip()
                # Filter obvious non-code matches
                if not code or code.startswith('<!--'):
                    continue
                # Allow shorter codes for US tickers (2-6 alpha chars)
                if code in _filter_keywords:
                    continue
                if code not in codes:
                    codes.add(code)

        return codes

    def _validate_official_decision_completeness(
        self,
        content: str,
        expected_codes: set,
    ) -> DecisionContentIntegrity:
        """Validate that a decision content covers all expected stock codes.

        Checks:
        1. Sentinel line: <!-- INTRADAY_DECISION_COMPLETE expected={N} covered={N} -->
        2. Stock code coverage in content
        3. Truncation suspicion based on sentinel absence

        Returns a DecisionContentIntegrity dataclass.
        """
        # Check sentinel
        sentinel_pattern = re.compile(
            r'<!--\s*INTRADAY_DECISION_COMPLETE\s+expected=(\d+)\s+covered=(\d+)\s*-->'
        )
        sentinel_match = sentinel_pattern.search(content)
        sentinel_ok = sentinel_match is not None

        # Extract covered codes from content
        covered_codes = IntradayMonitor._extract_stock_codes_from_decision_content(content)

        # Cross-reference with expected codes (normalize for comparison)
        expected_normalized = {c.upper().strip() for c in expected_codes}
        covered_normalized = {c.upper().strip() for c in covered_codes}
        missing_codes = expected_normalized - covered_normalized

        # Truncation suspicion: no sentinel and significant coverage gap
        truncated_suspected = (
            not sentinel_ok
            and len(missing_codes) > max(len(expected_normalized) * 0.2, 1)
        )

        ok = len(missing_codes) == 0 and sentinel_ok

        reason_parts: list = []
        if not sentinel_ok:
            reason_parts.append("缺少完整性标记")
        if missing_codes:
            reason_parts.append(f"缺失股票: {sorted(missing_codes)}")
        if truncated_suspected:
            reason_parts.append("疑似截断")

        return DecisionContentIntegrity(
            ok=ok,
            expected_codes=expected_normalized,
            covered_codes=covered_normalized,
            missing_codes=missing_codes,
            sentinel_ok=sentinel_ok,
            truncated_suspected=truncated_suspected,
            reason="; ".join(reason_parts) if reason_parts else "passed",
        )

    def _complete_missing_decision_sections(
        self,
        prompt_context: Dict[str, Any],
        partial_content: str,
        missing_codes: set,
    ) -> Optional[str]:
        """Attempt to complete truncated decision content for missing stock codes.

        Strategy:
        1. Retry primary LLM with continuation prompt (only missing codes)
        2. Fallback LLM (if configured)
        3. Deterministic raw summary append

        Returns merged content on success, or None if completion failed.
        """
        retry_count = getattr(self._config, 'intraday_decision_completion_retry', 1)
        if retry_count <= 0:
            return None

        covered_codes = partial_content and self._extract_stock_codes_from_decision_content(partial_content) or set()
        continuation_prompt = (
            "上一轮盘中 decision 输出不完整。已覆盖股票：" + ", ".join(sorted(covered_codes)) + "\n"
            "请只为以下缺失股票生成建议，不要重复已覆盖股票：" + ", ".join(sorted(missing_codes)) + "\n"
            "沿用同样四个分组标题；如果某组没有缺失股票，写（无）。\n"
        )

        # Try primary LLM completion
        logger.info(
            "Attempting decision completion: missing=%d retry_count=%d",
            len(missing_codes), retry_count,
        )
        for attempt in range(retry_count):
            logger.info("Completion attempt %d/%d", attempt + 1, retry_count)
            result = self._call_llm(continuation_prompt)
            if result.status == "success" and result.content:
                merged = partial_content + "\n\n## 补全: 缺失股票\n\n" + result.content
                logger.info(
                    "Completion succeeded on attempt %d: chars=%d",
                    attempt + 1, len(merged),
                )
                return merged
            logger.warning("Completion attempt %d failed: status=%s", attempt + 1, result.status)

        # Try fallback LLM for missing codes
        use_fallback = getattr(self._config, 'intraday_decision_completion_use_fallback', True)
        if use_fallback and self._should_try_fallback_llm("empty_response"):
            logger.info("Attempting fallback LLM for completion")
            fallback = self._call_fallback_llm(
                continuation_prompt,
                LLMResult(status="empty_response", error_message="completion needed"),
            )
            if fallback.status == "success" and fallback.content:
                merged = partial_content + "\n\n## 补全（备用LLM）: 缺失股票\n\n" + fallback.content
                logger.info("Fallback completion succeeded: chars=%d", len(merged))
                return merged

        # Deterministic raw summary append
        append_raw = getattr(self._config, 'intraday_decision_append_raw_for_missing', True)
        if append_raw:
            raw_lines = [
                "\n\n## 原始数据补充: 缺失股票",
                "",
                "以下股票在 LLM 决策中缺失，以下为原始数据摘要：",
                "",
            ]
            raw_lines.append("| 股票 | 价格 | 量比 | 涨跌幅 | 事件类型 |")
            raw_lines.append("|------|------|------|--------|----------|")
            for miss_code in sorted(missing_codes):
                raw_lines.append(f"| {miss_code} | - | - | - | 数据缺失 |")
            logger.info("Appended raw summary for %d missing codes", len(missing_codes))
            return partial_content + "\n".join(raw_lines)

        return None

    def _send_decision_email(
        self,
        content: str,
        *,
        report_type: EmailReportType = EmailReportType.OFFICIAL_DECISION,
        snapshot_id: Optional[str] = None,
        coverage_ratio: Optional[float] = None,
        valid_count: int = 0,
        expected_count: int = 0,
        failed_codes: Optional[List[str]] = None,
        llm_status: Optional[str] = None,
        baseline_status: Optional[str] = None,
        market_dates: Optional[Dict[str, str]] = None,
        market_index_coverage: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Send decision email via EmailSender with structured params."""
        if self._email_sender is None:
            logger.warning("EmailSender 未配置，跳过邮件发送")
            return

        # Build subject
        prefix_map = {
            EmailReportType.OFFICIAL_DECISION: "盘中监控报告",
            EmailReportType.DATA_QUALITY_ALERT: "[数据质量告警] 盘中监控",
            EmailReportType.LLM_FAILURE_ALERT: "[LLM故障告警] 盘中监控",
            EmailReportType.NO_BASELINE_ALERT: "[无基线告警] 盘中监控",
            EmailReportType.SNAPSHOT_INCOMPLETE_ALERT: "[快照未完成告警] 盘中监控",
        }
        prefix = prefix_map.get(report_type, "盘中监控通知")
        subject = prefix

        coverage_str = f" (覆盖率{coverage_ratio:.0%})" if coverage_ratio is not None else ""
        # Use provided market_dates or fallback to primary market
        if not market_dates:
            primary_market = self._resolve_primary_market()
            mkt_date = self._get_market_query_date(primary_market)
            market_dates = {primary_market: mkt_date}
        market_str = " / ".join(sorted(f"{m.upper()}:{d}" for m, d in market_dates.items()))
        subject = f"{prefix} - {market_str}{coverage_str}"

        # Prepend coverage summary for official
        if report_type == EmailReportType.OFFICIAL_DECISION:
            summary_lines = [
                f"> 快照覆盖率: {valid_count}/{expected_count} ({coverage_ratio:.0%})",
                f"> 快照ID: {snapshot_id}",
                f"> LLM状态: {llm_status or 'unknown'}",
                f"> 基线状态: {baseline_status or 'unknown'}",
            ]
            if market_index_coverage:
                if market_index_coverage.get('quality_alert'):
                    summary_lines.append("> ⚠️ 大盘指数数据不足，本决策仅供参考")
                if market_index_coverage.get('summary'):
                    for line in market_index_coverage['summary'].split('\n'):
                        summary_lines.append(f"> {line}")
                if market_index_coverage.get('ratio') is not None:
                    summary_lines.append(
                        f"> 大盘指数有效占比: {market_index_coverage['ratio']:.1%}"
                    )
            summary_lines.append("")
            content = "\n".join(summary_lines) + "\n" + content

        try:
            # Check body size for attachment mode
            body_mode = getattr(self._config, 'intraday_email_body_mode', 'full')
            body_max_chars = getattr(self._config, 'intraday_email_body_max_chars', 60000)
            attach_full = getattr(self._config, 'intraday_email_attach_full_report', True)

            if body_mode == 'summary_with_attachment' or len(content) > body_max_chars:
                # Send summary body + full report as attachment
                summary_len = min(len(content), body_max_chars)
                summary_content = (
                    content[:summary_len]
                    + "\n\n---\n> 正文过长，完整报告见附件。"
                ) if len(content) > body_max_chars else content

                attachments = []
                if attach_full:
                    attachments.append(
                        ("intraday_decision.md", content.encode("utf-8"), "text/markdown"),
                    )
                    # Also attach normalized HTML
                    from src.formatters import markdown_to_html_document
                    html_content = markdown_to_html_document(content)
                    attachments.append(
                        ("intraday_decision.html", html_content.encode("utf-8"), "text/html"),
                    )

                logger.info(
                    "决策邮件body_size=%d max=%d attachments=%d",
                    len(content), body_max_chars, len(attachments),
                )

                if hasattr(self._email_sender, 'send_to_email_with_attachments'):
                    success = self._email_sender.send_to_email_with_attachments(
                        content=summary_content,
                        subject=subject,
                        attachments=attachments if attach_full else None,
                    )
                else:
                    # Fallback to regular send (no attachment support)
                    success = self._email_sender.send_to_email(
                        content=summary_content,
                        subject=subject,
                    )
            else:
                success = self._email_sender.send_to_email(
                    content=content,
                    subject=subject,
                )

            if success:
                logger.info(
                    "盘中决策邮件发送成功: report_type=%s snapshot_id=%s coverage=%.1f%%",
                    report_type.value, snapshot_id or "none", (coverage_ratio or 0) * 100,
                )
                # Save complete report to logs/ for GitHub Actions artifact capture
                try:
                    import os as _os
                    log_dir = getattr(self._config, 'log_dir', './logs')
                    _os.makedirs(log_dir, exist_ok=True)
                    report_path = _os.path.join(log_dir, f"intraday_decision_{snapshot_id or 'latest'}.md")
                    with open(report_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    logger.info("Decision report saved to %s (%d chars)", report_path, len(content))
                except Exception as save_exc:
                    logger.debug("Failed to save decision report to file: %s", save_exc)
            else:
                logger.warning("盘中决策邮件发送失败: report_type=%s", report_type.value)
        except Exception as exc:
            logger.exception("盘中决策邮件发送异常: report_type=%s: %s", report_type.value, exc)

    # ------------------------------------------------------------------
    # SQLite helpers
    # ------------------------------------------------------------------

    def _save_event(self, event: IntradayEvent, raw_quote_data: Optional[Dict[str, Any]] = None) -> None:
        """Persist one intraday event to SQLite. Uses market-local date.

        Args:
            event: The intraday event to persist.
            raw_quote_data: Optional dict with extra quote fields (open, prev_close, high, low,
                           volume, amount, turnover_rate, source) to save as raw_quote JSON.
        """
        market = self._get_market_for_stock(event.stock_code)
        if market is None:
            policy = self._resolve_unknown_market_policy()
            if policy == 'cn_compat':
                market = 'cn'
            else:
                logger.warning(
                    "跳过无法识别市场的股票事件 [%s] (policy=skip)", event.stock_code
                )
                return
        query_date = self._get_market_query_date(market)

        # Compute market-local timestamp
        try:
            from src.core.trading_calendar import get_market_now
            market_now = get_market_now(market)
            event.market_local_timestamp = market_now.isoformat()
        except Exception:
            event.market_local_timestamp = event.timestamp.isoformat()

        # Build raw_quote JSON from provided extra fields
        raw_quote_str = None
        if raw_quote_data:
            # Filter None values and serialize
            clean_data = {k: v for k, v in raw_quote_data.items() if v is not None}
            if clean_data:
                raw_quote_str = json.dumps(clean_data, default=str)

        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                conn.execute(
                    sa_text(
                        "INSERT INTO intraday_events "
                        "(query_date, market, timestamp, market_local_timestamp, stock_code, stock_name, "
                        "current_price, volume_ratio, change_pct, event_type, description, raw_quote, "
                        "snapshot_id, run_id, snapshot_status) "
                        "VALUES (:qd, :mkt, :ts, :mlt, :sc, :sn, :cp, :vr, :cpct, :et, :desc, :rq, "
                        ":sid, :rid, :ss)"
                    ),
                    {
                        "qd": query_date,
                        "mkt": market,
                        "ts": event.timestamp.isoformat(),
                        "mlt": event.market_local_timestamp,
                        "sc": event.stock_code,
                        "sn": event.stock_name,
                        "cp": event.current_price,
                        "vr": event.volume_ratio,
                        "cpct": event.change_pct,
                        "et": event.event_type,
                        "desc": event.description,
                        "rq": raw_quote_str,
                        "sid": event.snapshot_id,
                        "rid": event.run_id,
                        "ss": event.snapshot_status,
                    },
                )
                session.commit()
        except Exception as exc:
            logger.error("保存盘中事件失败 [%s]: %s", event.stock_code, exc)

    def _load_today_events(self, query_date: str, market: Optional[str] = None) -> List[IntradayEvent]:
        """Load all intraday events for a given date (and optionally market) from SQLite."""
        events: List[IntradayEvent] = []
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                if market:
                    rows = conn.execute(
                        sa_text(
                            "SELECT timestamp, stock_code, stock_name, current_price, market_local_timestamp, "
                            "volume_ratio, change_pct, event_type, description, market "
                            "FROM intraday_events "
                            "WHERE query_date = :qd AND market = :mkt ORDER BY timestamp"
                        ),
                        {"qd": query_date, "mkt": market},
                    ).fetchall()
                else:
                    rows = conn.execute(
                        sa_text(
                            "SELECT timestamp, stock_code, stock_name, current_price, market_local_timestamp, "
                            "volume_ratio, change_pct, event_type, description, market "
                            "FROM intraday_events WHERE query_date = :qd ORDER BY timestamp"
                        ),
                        {"qd": query_date},
                    ).fetchall()

                for row in rows:
                    ts = row[0]
                    if isinstance(ts, str):
                        ts = datetime.fromisoformat(ts)
                    events.append(IntradayEvent(
                        timestamp=ts,
                        stock_code=str(row[1] or ""),
                        stock_name=str(row[2] or ""),
                        current_price=float(row[3] or 0),
                        volume_ratio=float(row[5]) if row[5] is not None else None,
                        change_pct=float(row[6]) if row[6] is not None else None,
                        event_type=str(row[7] or "price_only"),
                        description=str(row[8] or ""),
                    ))
                    if len(row) > 4 and row[4] is not None:
                        events[-1].market_local_timestamp = str(row[4])
                    if len(row) > 9 and row[9] is not None:
                        events[-1].market = str(row[9])
        except Exception as exc:
            logger.warning("加载盘中事件失败: %s", exc)

        return events

    def _load_events_for_snapshot(self, snapshot_id: str) -> List[IntradayEvent]:
        events: List[IntradayEvent] = []
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                rows = conn.execute(
                    sa_text(
                        "SELECT query_date, timestamp, stock_code, stock_name, current_price, market_local_timestamp, "
                        "volume_ratio, change_pct, event_type, description, snapshot_id, run_id, snapshot_status, market "
                        "FROM intraday_events WHERE snapshot_id = :sid ORDER BY timestamp"
                    ),
                    {"sid": snapshot_id},
                ).fetchall()
                for row in rows:
                    ts = row[1]
                    if isinstance(ts, str):
                        ts = datetime.fromisoformat(ts)
                    events.append(IntradayEvent(
                        timestamp=ts,
                        stock_code=str(row[2] or ""),
                        stock_name=str(row[3] or ""),
                        current_price=float(row[4] or 0),
                        volume_ratio=float(row[6]) if row[6] is not None else None,
                        change_pct=float(row[7]) if row[7] is not None else None,
                        event_type=str(row[8] or "price_only"),
                        description=str(row[9] or ""),
                        snapshot_id=str(row[10]) if len(row) > 10 and row[10] is not None else None,
                        run_id=str(row[11]) if len(row) > 11 and row[11] is not None else None,
                        snapshot_status=str(row[12]) if len(row) > 12 and row[12] is not None else None,
                    ))
                    if len(row) > 5 and row[5] is not None:
                        events[-1].market_local_timestamp = str(row[5])
                    if len(row) > 13 and row[13] is not None:
                        events[-1].market = str(row[13])
        except Exception as exc:
            logger.warning("加载盘中事件失败(snapshot=%s): %s", snapshot_id, exc)
        return events

    def _get_latest_completed_snapshot(self, markets=None) -> Optional[dict]:
        """Return the most recent completed snapshot state dict or None."""
        self._ensure_intraday_snapshots_table()
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                rows = conn.execute(
                    sa_text(
                        "SELECT snapshot_id, run_id, status, started_at, finished_at, "
                        "expected_codes, completed_codes, valid_quote_codes, failed_codes, query_date, "
                        "markets, market_dates "
                        "FROM intraday_snapshots WHERE status = 'completed' ORDER BY started_at DESC LIMIT 1"
                    ),
                ).fetchall()
                if rows:
                    import json
                    row = rows[0]
                    snapshot = {
                        "snapshot_id": row[0],
                        "run_id": row[1],
                        "status": row[2],
                        "started_at": row[3],
                        "finished_at": row[4],
                        "expected_codes": json.loads(row[5]) if row[5] else [],
                        "completed_codes": json.loads(row[6]) if row[6] else [],
                        "valid_quote_codes": json.loads(row[7]) if row[7] else [],
                        "failed_codes": json.loads(row[8]) if row[8] else [],
                        "query_date": row[9],
                        "markets": json.loads(row[10]) if len(row) > 10 and row[10] else [],
                        "market_dates": json.loads(row[11]) if len(row) > 11 and row[11] else {},
                    }
                    expected_count = len(snapshot["expected_codes"])
                    valid_count = len(snapshot["valid_quote_codes"])
                    coverage = valid_count / max(expected_count, 1)
                    logger.info(
                        "获取最新completed快照成功: snapshot_id=%s expected=%d valid=%d coverage=%.1f%%",
                        snapshot["snapshot_id"], expected_count, valid_count, coverage * 100,
                    )
                    return snapshot
                logger.info("无 completed 快照记录（表存在但无 completed 行）")
        except sqlite3.OperationalError as exc:
            error_text = str(exc).lower()
            if "no such table" in error_text:
                logger.warning("intraday_snapshots 表不存在（schema bug），自动建表后重查")
                self._ensure_intraday_snapshots_table()
                try:
                    return self._get_latest_completed_snapshot(markets)
                except RecursionError:
                    logger.warning("_get_latest_completed_snapshot 重查递归溢出，放弃")
                    return None
            else:
                logger.warning("获取最新completed快照失败 (OperationalError): %s", exc)
        except Exception as exc:
            logger.warning("获取最新completed快照失败: %s", exc)
        return None

    def _get_latest_usable_snapshot(self, markets=None) -> Optional[dict]:
        """Return the most recent usable (completed or partial) snapshot state dict or None."""
        self._ensure_intraday_snapshots_table()
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                rows = conn.execute(
                    sa_text(
                        "SELECT snapshot_id, run_id, status, started_at, finished_at, "
                        "expected_codes, completed_codes, valid_quote_codes, failed_codes, query_date, "
                        "markets, market_dates "
                        "FROM intraday_snapshots WHERE status IN ('completed', 'partial') "
                        "ORDER BY CASE WHEN status = 'completed' THEN 0 ELSE 1 END, started_at DESC LIMIT 1"
                    ),
                ).fetchall()
                if rows:
                    import json
                    row = rows[0]
                    snapshot = {
                        "snapshot_id": row[0],
                        "run_id": row[1],
                        "status": row[2],
                        "started_at": row[3],
                        "finished_at": row[4],
                        "expected_codes": json.loads(row[5]) if row[5] else [],
                        "completed_codes": json.loads(row[6]) if row[6] else [],
                        "valid_quote_codes": json.loads(row[7]) if row[7] else [],
                        "failed_codes": json.loads(row[8]) if row[8] else [],
                        "query_date": row[9],
                        "markets": json.loads(row[10]) if len(row) > 10 and row[10] else [],
                        "market_dates": json.loads(row[11]) if len(row) > 11 and row[11] else {},
                    }
                    expected_count = len(snapshot["expected_codes"])
                    valid_count = len(snapshot["valid_quote_codes"])
                    coverage = valid_count / max(expected_count, 1)
                    logger.info(
                        "获取最新可用快照成功: snapshot_id=%s expected=%d valid=%d coverage=%.1f%%",
                        snapshot["snapshot_id"], expected_count, valid_count, coverage * 100,
                    )
                    return snapshot
                logger.info("无 可用快照记录（表存在但无 completed/partial 行）")
        except sqlite3.OperationalError as exc:
            error_text = str(exc).lower()
            if "no such table" in error_text:
                logger.warning("intraday_snapshots 表不存在（schema bug），自动建表后重查")
                self._ensure_intraday_snapshots_table()
                try:
                    return self._get_latest_usable_snapshot(markets)
                except RecursionError:
                    logger.warning("_get_latest_usable_snapshot 重查递归溢出，放弃")
                    return None
            else:
                logger.warning("获取最新可用快照失败 (OperationalError): %s", exc)
        except Exception as exc:
            logger.warning("获取最新可用快照失败: %s", exc)
        return None

    def _get_completed_snapshot_by_id(self, snapshot_id: str) -> Optional[dict]:
        """Return completed snapshot by exact snapshot_id, or None."""
        self._ensure_intraday_snapshots_table()
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                rows = conn.execute(
                    sa_text(
                        "SELECT snapshot_id, run_id, status, started_at, finished_at, "
                        "expected_codes, completed_codes, valid_quote_codes, failed_codes, query_date, "
                        "markets, market_dates "
                        "FROM intraday_snapshots WHERE snapshot_id = :sid AND status = 'completed'"
                    ),
                    {"sid": snapshot_id},
                ).fetchall()
                if rows:
                    import json
                    row = rows[0]
                    return {
                        "snapshot_id": row[0], "run_id": row[1], "status": row[2],
                        "started_at": row[3], "finished_at": row[4],
                        "expected_codes": json.loads(row[5]) if row[5] else [],
                        "completed_codes": json.loads(row[6]) if row[6] else [],
                        "valid_quote_codes": json.loads(row[7]) if row[7] else [],
                        "failed_codes": json.loads(row[8]) if row[8] else [],
                        "query_date": row[9],
                        "markets": json.loads(row[10]) if len(row) > 10 and row[10] else [],
                        "market_dates": json.loads(row[11]) if len(row) > 11 and row[11] else {},
                    }
        except sqlite3.OperationalError as exc:
            logger.warning("按ID查询快照失败: %s", exc)
        except Exception as exc:
            logger.warning("按ID查询快照失败: %s", exc)
        return None

    def _get_usable_snapshot_by_id(self, snapshot_id: str) -> Optional[dict]:
        """Return usable (completed or partial) snapshot by exact snapshot_id, or None."""
        self._ensure_intraday_snapshots_table()
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                rows = conn.execute(
                    sa_text(
                        "SELECT snapshot_id, run_id, status, started_at, finished_at, "
                        "expected_codes, completed_codes, valid_quote_codes, failed_codes, query_date, "
                        "markets, market_dates "
                        "FROM intraday_snapshots WHERE snapshot_id = :sid AND status IN ('completed', 'partial')"
                    ),
                    {"sid": snapshot_id},
                ).fetchall()
                if rows:
                    import json
                    row = rows[0]
                    return {
                        "snapshot_id": row[0], "run_id": row[1], "status": row[2],
                        "started_at": row[3], "finished_at": row[4],
                        "expected_codes": json.loads(row[5]) if row[5] else [],
                        "completed_codes": json.loads(row[6]) if row[6] else [],
                        "valid_quote_codes": json.loads(row[7]) if row[7] else [],
                        "failed_codes": json.loads(row[8]) if row[8] else [],
                        "query_date": row[9],
                        "markets": json.loads(row[10]) if len(row) > 10 and row[10] else [],
                        "market_dates": json.loads(row[11]) if len(row) > 11 and row[11] else {},
                    }
        except sqlite3.OperationalError as exc:
            logger.warning("按ID查询可用快照失败: %s", exc)
        except Exception as exc:
            logger.warning("按ID查询可用快照失败: %s", exc)
        return None

    def _ensure_intraday_table(self) -> None:
        """Create intraday_events table if not exists (with market and market_local_timestamp columns)."""
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                conn.execute(sa_text(
                    "CREATE TABLE IF NOT EXISTS intraday_events ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "query_date TEXT NOT NULL, "
                    "market TEXT NOT NULL DEFAULT 'cn', "
                    "timestamp TEXT NOT NULL, "
                    "market_local_timestamp TEXT, "
                    "stock_code TEXT NOT NULL, "
                    "stock_name TEXT, "
                    "current_price REAL, "
                    "volume_ratio REAL, "
                    "change_pct REAL, "
                    "event_type TEXT NOT NULL, "
                    "description TEXT, "
                    "raw_quote TEXT"
                    ")"
                ))
                # Migrate: add missing columns for older tables
                info = conn.execute(sa_text("PRAGMA table_info('intraday_events')")).fetchall()
                existing_cols = {row[1] for row in info}
                if 'market' not in existing_cols:
                    conn.execute(sa_text(
                        "ALTER TABLE intraday_events ADD COLUMN market TEXT NOT NULL DEFAULT 'cn'"
                    ))
                    logger.info("Schema migration: added intraday_events.market")
                if 'market_local_timestamp' not in existing_cols:
                    conn.execute(sa_text(
                        "ALTER TABLE intraday_events ADD COLUMN market_local_timestamp TEXT"
                    ))
                    logger.info("Schema migration: added intraday_events.market_local_timestamp")
                if 'snapshot_id' not in existing_cols:
                    conn.execute(sa_text(
                        "ALTER TABLE intraday_events ADD COLUMN snapshot_id TEXT"
                    ))
                    logger.info("Schema migration: added intraday_events.snapshot_id")
                if 'run_id' not in existing_cols:
                    conn.execute(sa_text(
                        "ALTER TABLE intraday_events ADD COLUMN run_id TEXT"
                    ))
                    logger.info("Schema migration: added intraday_events.run_id")
                if 'snapshot_status' not in existing_cols:
                    conn.execute(sa_text(
                        "ALTER TABLE intraday_events ADD COLUMN snapshot_status TEXT"
                    ))
                    logger.info("Schema migration: added intraday_events.snapshot_status")
                conn.execute(sa_text(
                    "CREATE INDEX IF NOT EXISTS idx_intraday_date_code "
                    "ON intraday_events(query_date, stock_code)"
                ))
                conn.execute(sa_text(
                    "CREATE INDEX IF NOT EXISTS idx_intraday_date_market "
                    "ON intraday_events(query_date, market)"
                ))
                conn.execute(sa_text(
                    "CREATE INDEX IF NOT EXISTS idx_intraday_snapshot "
                    "ON intraday_events(snapshot_id)"
                ))
                # Process-locks table for cross-instance concurrency control
                conn.execute(sa_text(
                    "CREATE TABLE IF NOT EXISTS process_locks ("
                    "lock_name TEXT PRIMARY KEY, "
                    "holder TEXT NOT NULL, "
                    "acquired_at TEXT NOT NULL, "
                    "expires_at TEXT NOT NULL)"
                ))
                session.commit()
                logger.info("intraday_events 表已就绪")
        except Exception as exc:
            logger.error("创建 intraday_events 表失败: %s", exc)

    def _ensure_intraday_snapshots_table(self) -> None:
        """Create intraday_snapshots table if not exists."""
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                conn.execute(sa_text(
                    "CREATE TABLE IF NOT EXISTS intraday_snapshots ("
                    "snapshot_id TEXT PRIMARY KEY, "
                    "run_id TEXT NOT NULL, "
                    "status TEXT NOT NULL DEFAULT 'running', "
                    "started_at TEXT NOT NULL, "
                    "finished_at TEXT, "
                    "expected_codes TEXT, "
                    "completed_codes TEXT, "
                    "valid_quote_codes TEXT, "
                    "failed_codes TEXT, "
                    "query_date TEXT NOT NULL, "
                    "markets TEXT, "
                    "market_dates TEXT)"
                ))
                session.commit()
                # Schema migration: add markets/market_dates if missing
                info = conn.execute(sa_text("PRAGMA table_info('intraday_snapshots')")).fetchall()
                existing_cols = {row[1] for row in info}
                if 'markets' not in existing_cols:
                    conn.execute(sa_text(
                        "ALTER TABLE intraday_snapshots ADD COLUMN markets TEXT"
                    ))
                    logger.info("Schema migration: added intraday_snapshots.markets")
                if 'market_dates' not in existing_cols:
                    conn.execute(sa_text(
                        "ALTER TABLE intraday_snapshots ADD COLUMN market_dates TEXT"
                    ))
                    logger.info("Schema migration: added intraday_snapshots.market_dates")
                session.commit()
            logger.info("intraday_snapshots 表已就绪")
        except Exception as exc:
            logger.error("创建 intraday_snapshots 表失败: %s", exc)

    def _persist_snapshot_state(self, state: SnapshotState) -> None:
        """Write snapshot state to intraday_snapshots table."""
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                # Compute query_date: use first market's local date if available, else fallback
                query_date = state.started_at.strftime("%Y-%m-%d")
                if state.markets:
                    first_mkt = state.markets[0]
                    query_date = self._get_market_query_date(first_mkt)
                conn.execute(
                    sa_text("INSERT OR REPLACE INTO intraday_snapshots "
                            "(snapshot_id, run_id, status, started_at, finished_at, "
                            "expected_codes, completed_codes, valid_quote_codes, failed_codes, query_date, "
                            "markets, market_dates) "
                            "VALUES (:sid, :rid, :st, :sa, :fa, :ec, :cc, :vc, :fc, :qd, "
                            ":mkts, :mdates)"),
                    {
                        "sid": state.snapshot_id,
                        "rid": state.run_id,
                        "st": state.status,
                        "sa": state.started_at.isoformat(),
                        "fa": state.finished_at.isoformat() if state.finished_at else None,
                        "ec": json.dumps(state.expected_codes),
                        "cc": json.dumps(state.completed_codes),
                        "vc": json.dumps(state.valid_quote_codes),
                        "fc": json.dumps(state.failed_codes),
                        "qd": query_date,
                        "mkts": json.dumps(state.markets) if state.markets else None,
                        "mdates": json.dumps(state.market_dates) if state.market_dates else None,
                    },
                )
                session.commit()
        except Exception as exc:
            logger.warning("持久化快照状态失败: %s", exc)

    def _ensure_intraday_market_snapshots_table(self) -> None:
        """Create intraday_market_snapshots table if not exists.

        Note: intraday_market_snapshots is the canonical market-index snapshot table
        (see persistence_optimization_strategy.md for naming context).
        """
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                conn.execute(sa_text(
                    "CREATE TABLE IF NOT EXISTS intraday_market_snapshots ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "snapshot_id TEXT NOT NULL, "
                    "run_id TEXT, "
                    "query_date TEXT NOT NULL, "
                    "market TEXT NOT NULL, "
                    "timestamp TEXT, "
                    "market_local_timestamp TEXT, "
                    "index_code TEXT NOT NULL, "
                    "index_name TEXT, "
                    "current_price REAL, "
                    "open_price REAL, "
                    "prev_close REAL, "
                    "high_price REAL, "
                    "low_price REAL, "
                    "change_pct REAL, "
                    "volume REAL, "
                    "amount REAL, "
                    "source TEXT, "
                    "error_message TEXT, "
                    "status TEXT NOT NULL DEFAULT 'valid', "
                    "raw_quote TEXT, "
                    "created_at TEXT NOT NULL, "
                    "UNIQUE(snapshot_id, market, index_code))"
                ))
                # Idempotent column migration for existing tables
                info = conn.execute(sa_text(
                    "PRAGMA table_info('intraday_market_snapshots')"
                )).fetchall()
                existing_cols = {row[1] for row in info}
                if 'timestamp' not in existing_cols:
                    conn.execute(sa_text(
                        "ALTER TABLE intraday_market_snapshots ADD COLUMN timestamp TEXT"
                    ))
                if 'error_message' not in existing_cols:
                    conn.execute(sa_text(
                        "ALTER TABLE intraday_market_snapshots ADD COLUMN error_message TEXT"
                    ))
                conn.execute(sa_text(
                    "CREATE INDEX IF NOT EXISTS idx_intraday_market_snapshots_day "
                    "ON intraday_market_snapshots(query_date, market, index_code)"
                ))
                conn.execute(sa_text(
                    "CREATE INDEX IF NOT EXISTS idx_intraday_market_snapshots_snapshot "
                    "ON intraday_market_snapshots(snapshot_id, market)"
                ))
                conn.execute(sa_text(
                    "CREATE INDEX IF NOT EXISTS idx_intraday_market_snapshots_timeline "
                    "ON intraday_market_snapshots(query_date, market, index_code, market_local_timestamp)"
                ))
                conn.execute(sa_text(
                    "CREATE INDEX IF NOT EXISTS idx_intraday_market_snapshots_status "
                    "ON intraday_market_snapshots(query_date, market, status)"
                ))
                session.commit()
                logger.info("intraday_market_snapshots 表已就绪")
        except Exception as exc:
            logger.error("创建 intraday_market_snapshots 表失败: %s", exc)

    def _get_market_indices(self, market: str) -> List[str]:
        """Get configured index codes for a market."""
        if market == 'cn':
            codes_str = getattr(self._config, 'intraday_market_indices_cn', '') or ''
        elif market == 'hk':
            codes_str = getattr(self._config, 'intraday_market_indices_hk', '') or ''
        elif market == 'us':
            codes_str = getattr(self._config, 'intraday_market_indices_us', '') or ''
        else:
            return []
        return [c.strip() for c in codes_str.split(',') if c.strip()]

    def _fetch_index_quotes(self, market: str) -> List[Dict[str, Any]]:
        """Fetch realtime index quotes for a single market. Fail-open: returns [] on error."""
        enabled = getattr(self._config, 'intraday_market_snapshot_enabled', True)
        if not enabled:
            return []
        index_codes = self._get_market_indices(market)
        if not index_codes:
            return []
        results: List[Dict[str, Any]] = []
        for code in index_codes:
            try:
                quote = self._fetch_index_quote(market, code)
                if quote is None:
                    logger.info("[市场指数] %s index_code=%s: 无行情数据", market.upper(), code)
                    results.append({
                        'index_code': code, 'status': 'data_unavailable',
                        'market': market, 'error_message': 'no quote data returned',
                    })
                    continue
                results.append(quote)
            except Exception as exc:
                logger.warning("[市场指数] %s index_code=%s 获取失败: %s", market.upper(), code, exc)
                results.append({
                    'index_code': code, 'status': 'fetch_failed',
                    'market': market, 'error_message': str(exc)[:500],
                })
        return results

    def _fetch_index_quote(self, market: str, index_code: str) -> Optional[Dict[str, Any]]:
        """Fetch single index quote using configured data-source strategy.

        intraday_index_data_source strategies:
        - 'dedicated': market-specific dedicated fetcher (AkShare/yfinance)
        - 'realtime': old get_realtime_quote path
        - 'auto': try dedicated first, fall back to realtime
        """
        data_source = getattr(self._config, 'intraday_index_data_source', 'dedicated')

        def _try_realtime():
            try:
                quote = self._fetcher.get_realtime_quote(index_code, log_final_failure=False)
                if quote is None:
                    return None
                return {
                    'index_code': index_code,
                    'index_name': getattr(quote, 'name', '') or '',
                    'current_price': float(quote.price) if quote.price is not None else None,
                    'open_price': float(quote.open) if getattr(quote, 'open', None) is not None else None,
                    'prev_close': float(quote.prev_close) if getattr(quote, 'prev_close', None) is not None else None,
                    'high_price': float(quote.high) if getattr(quote, 'high', None) is not None else None,
                    'low_price': float(quote.low) if getattr(quote, 'low', None) is not None else None,
                    'change_pct': float(quote.change_pct) if getattr(quote, 'change_pct', None) is not None else None,
                    'volume': float(quote.volume) if getattr(quote, 'volume', None) is not None else None,
                    'amount': float(quote.amount) if getattr(quote, 'amount', None) is not None else None,
                    'source': getattr(quote, 'source', '') or '',
                    'status': 'valid',
                    'market': market,
                }
            except Exception:
                return None

        if data_source == 'realtime':
            return _try_realtime()

        result = None
        try:
            if market == 'cn':
                result = self._fetch_cn_index_quote(index_code)
            elif market == 'hk':
                result = self._fetch_hk_index_quote(index_code)
            elif market == 'us':
                result = self._fetch_us_index_quote(index_code)
        except Exception as exc:
            logger.warning("[专用指数] %s index_code=%s 获取失败: %s", market.upper(), index_code, exc)
            result = None

        if result is None and data_source == 'auto':
            logger.info("[专用指数] %s index_code=%s 专用路由失败，fallback到realtime", market.upper(), index_code)
            return _try_realtime()
        return result

    def _fetch_cn_index_quote(self, index_code: str) -> Optional[Dict[str, Any]]:
        """Fetch CN market index using AkShare index-specific API."""
        try:
            import akshare as ak
            try:
                df = ak.stock_zh_index_spot_em()
                if df is not None and not df.empty:
                    code_map = {
                        '000001': '上证指数', '399001': '深证成指', '399006': '创业板指',
                        '000016': '上证50', '000300': '沪深300', '000688': '科创50',
                        '000905': '中证500', '000852': '中证1000',
                    }
                    name = code_map.get(index_code, '')
                    if name and '名称' in df.columns:
                        row = df[df['名称'] == name]
                    elif '代码' in df.columns:
                        row = df[df['代码'] == index_code]
                    else:
                        row = None
                    if row is not None and not row.empty:
                        r = row.iloc[0]
                        return {
                            'index_code': index_code, 'index_name': name,
                            'current_price': float(r.get('最新价', 0)) if '最新价' in r else None,
                            'open_price': float(r.get('今开', 0)) if '今开' in r else None,
                            'prev_close': float(r.get('昨收', 0)) if '昨收' in r else None,
                            'high_price': float(r.get('最高', 0)) if '最高' in r else None,
                            'low_price': float(r.get('最低', 0)) if '最低' in r else None,
                            'change_pct': float(r.get('涨跌幅', 0)) if '涨跌幅' in r else None,
                            'volume': float(r.get('成交量', 0)) if '成交量' in r else None,
                            'amount': float(r.get('成交额', 0)) if '成交额' in r else None,
                            'source': 'akshare_spot_em',
                            'status': 'valid',
                        }
            except Exception:
                pass
            df = ak.stock_zh_index_daily_em(symbol=index_code)
            if df is not None and not df.empty:
                latest = df.iloc[0]
                return {
                    'index_code': index_code, 'index_name': '',
                    'current_price': float(latest.get('close', 0)) if 'close' in latest else None,
                    'open_price': float(latest.get('open', 0)) if 'open' in latest else None,
                    'high_price': float(latest.get('high', 0)) if 'high' in latest else None,
                    'low_price': float(latest.get('low', 0)) if 'low' in latest else None,
                    'change_pct': float(latest.get('pct_chg', 0)) if 'pct_chg' in latest else None,
                    'volume': float(latest.get('volume', 0)) if 'volume' in latest else None,
                    'amount': float(latest.get('amount', 0)) if 'amount' in latest else None,
                    'source': 'akshare_daily_em',
                    'status': 'valid',
                }
        except Exception as exc:
            logger.warning("[CN指数专用] index_code=%s 失败: %s", index_code, exc)
        return None

    def _fetch_hk_index_quote(self, index_code: str) -> Optional[Dict[str, Any]]:
        """Fetch HK market index using yfinance with proper symbol mapping."""
        try:
            import yfinance as yf
            symbol_map = {'HSI': '^HSI', 'HSTECH': '^HSTECH', 'HSCEI': '^HSCEI'}
            yf_symbol = symbol_map.get(index_code, index_code)
            ticker = yf.Ticker(yf_symbol)
            info = ticker.info
            fast = ticker.fast_info
            current = fast.get('lastPrice') or info.get('regularMarketPrice') or info.get('currentPrice')
            if current is None:
                return None
            return {
                'index_code': index_code,
                'index_name': info.get('shortName', '') or info.get('longName', ''),
                'current_price': float(current),
                'open_price': float(fast.get('open')) if fast.get('open') is not None else None,
                'prev_close': float(fast.get('previousClose')) if fast.get('previousClose') is not None else None,
                'high_price': float(fast.get('dayHigh')) if fast.get('dayHigh') is not None else None,
                'low_price': float(fast.get('dayLow')) if fast.get('dayLow') is not None else None,
                'volume': float(fast.get('volume')) if fast.get('volume') is not None else None,
                'source': 'yfinance_hk',
                'status': 'valid',
            }
        except Exception as exc:
            logger.warning("[HK指数专用] index_code=%s 失败: %s", index_code, exc)
        return None

    def _fetch_us_index_quote(self, index_code: str) -> Optional[Dict[str, Any]]:
        """Fetch US market index using yfinance."""
        try:
            import yfinance as yf
            ticker = yf.Ticker(index_code)
            info = ticker.info
            fast = ticker.fast_info
            current = fast.get('lastPrice') or info.get('regularMarketPrice')
            if current is None:
                return None
            return {
                'index_code': index_code,
                'index_name': info.get('shortName', '') or info.get('longName', ''),
                'current_price': float(current),
                'open_price': float(fast.get('open')) if fast.get('open') is not None else None,
                'prev_close': float(fast.get('previousClose')) if fast.get('previousClose') is not None else None,
                'high_price': float(fast.get('dayHigh')) if fast.get('dayHigh') is not None else None,
                'low_price': float(fast.get('dayLow')) if fast.get('dayLow') is not None else None,
                'volume': float(fast.get('volume')) if fast.get('volume') is not None else None,
                'source': 'yfinance_us',
                'status': 'valid',
            }
        except Exception as exc:
            logger.warning("[US指数专用] index_code=%s 失败: %s", index_code, exc)
        return None

    def _persist_market_snapshots(
        self, snapshot_id: str, run_id: str, market: str,
        index_quotes: List[Dict[str, Any]],
    ) -> None:
        """Persist market index snapshots to intraday_market_snapshots table.

        intraday_market_snapshots is canonical market-index snapshot table
        (see persistence_optimization_strategy.md).
        """
        self._ensure_intraday_market_snapshots_table()
        query_date = self._get_market_query_date(market)
        try:
            from src.core.trading_calendar import get_market_now
            market_local_ts = get_market_now(market).isoformat()
        except Exception:
            market_local_ts = datetime.now().isoformat()
        created_at = datetime.now().isoformat()
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                for idx in index_quotes:
                    raw = None
                    if idx.get('status') == 'valid':
                        raw_data = {k: v for k, v in idx.items()
                                    if k not in ('status', 'market') and v is not None}
                        raw = json.dumps(raw_data, default=str) if raw_data else None
                    conn.execute(sa_text(
                        "INSERT OR REPLACE INTO intraday_market_snapshots "
                        "(snapshot_id, run_id, query_date, market, market_local_timestamp, "
                        "index_code, index_name, current_price, open_price, prev_close, "
                        "high_price, low_price, change_pct, volume, amount, source, "
                        "status, raw_quote, error_message, timestamp, created_at) "
                        "VALUES (:sid, :rid, :qd, :mkt, :mlt, :ic, :iname, :cp, :op, :pc, "
                        ":hp, :lp, :chpct, :vol, :amt, :src, :st, :rq, :err, :ts, :ca)"
                    ), {
                        "sid": snapshot_id, "rid": run_id, "qd": query_date,
                        "mkt": market, "mlt": market_local_ts,
                        "ic": idx.get('index_code', ''),
                        "iname": idx.get('index_name') or None,
                        "cp": idx.get('current_price'),
                        "op": idx.get('open_price'),
                        "pc": idx.get('prev_close'),
                        "hp": idx.get('high_price'),
                        "lp": idx.get('low_price'),
                        "chpct": idx.get('change_pct'),
                        "vol": idx.get('volume'),
                        "amt": idx.get('amount'),
                        "src": idx.get('source') or None,
                        "st": idx.get('status', 'data_unavailable'),
                        "rq": raw,
                        "err": idx.get('error_message'),
                        "ts": idx.get('timestamp') or market_local_ts,
                        "ca": created_at,
                    })
                session.commit()
            if any(idx.get('status') == 'valid' for idx in index_quotes):
                valid_count = sum(1 for idx in index_quotes if idx.get('status') == 'valid')
                logger.info(
                    "[市场指数] 持久化成功: snapshot_id=%s market=%s valid=%d/%d",
                    snapshot_id, market.upper(), valid_count, len(index_quotes),
                )
        except Exception as exc:
            logger.warning("[市场指数] 持久化失败 snapshot_id=%s market=%s: %s", snapshot_id, market.upper(), exc)

    def _run_market_index_snapshot(self, snapshot_id: str, run_id: str, markets: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch and persist market index snapshots for all given markets.
        Fail-open: individual market failure does not block others.
        Returns dict mapping market -> list of index quote dicts.
        """
        all_results: Dict[str, List[Dict[str, Any]]] = {}
        enabled = getattr(self._config, 'intraday_market_snapshot_enabled', True)
        if not enabled:
            logger.info("[市场指数] 市场指数快照已禁用 (intraday_market_snapshot_enabled=false)")
            return all_results
        for market in markets:
            try:
                quotes = self._fetch_index_quotes(market)
                all_results[market] = quotes
                self._persist_market_snapshots(snapshot_id, run_id, market, quotes)
            except Exception as exc:
                logger.warning("[市场指数] 市场 %s 指数快照失败: %s", market.upper(), exc)
                all_results[market] = [{'index_code': 'ALL', 'status': 'fetch_failed', 'market': market}]
        return all_results

    def _load_market_timelines_for_decision(self, markets: List[str], market_dates: Dict[str, str]) -> Dict[str, List[Dict[str, Any]]]:
        """Load all market index snapshots for the decision day, grouped by market+index_code.

        Tries repository-backed path first (unified main+historical), falls back
        to direct main DB SQL. Returns {market: [{index_code, index_name, points: [{ts, price, change_pct}], ...}]}
        """
        timelines: Dict[str, List[Dict[str, Any]]] = {}

        # Try repository-backed path first
        repo_result = self._try_repo_market_timelines(markets, market_dates)
        if repo_result is not None:
            return repo_result

        # Fallback: direct main DB SQL
        self._ensure_intraday_market_snapshots_table()
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                for market in markets:
                    query_date = market_dates.get(market) or self._get_market_query_date(market)
                    rows = conn.execute(sa_text(
                        "SELECT index_code, index_name, current_price, change_pct, "
                        "market_local_timestamp, status, open_price, prev_close, "
                        "high_price, low_price, snapshot_id "
                        "FROM intraday_market_snapshots "
                        "WHERE query_date = :qd AND market = :mkt "
                        "ORDER BY index_code, market_local_timestamp"
                    ), {"qd": query_date, "mkt": market}).fetchall()
                    if not rows:
                        continue
                    index_map: Dict[str, Dict[str, Any]] = {}
                    for row in rows:
                        ic = str(row[0] or "")
                        if not ic:
                            continue
                        if ic not in index_map:
                            index_map[ic] = {
                                'index_code': ic,
                                'index_name': str(row[1] or "") if row[1] else ic,
                                'points': [],
                                'open_price': float(row[6]) if row[6] is not None else None,
                                'prev_close': float(row[7]) if row[7] is not None else None,
                                'high_price': float(row[8]) if row[8] is not None else None,
                                'low_price': float(row[9]) if row[9] is not None else None,
                            }
                        entry = index_map[ic]
                        if row[5] == 'valid' and row[2] is not None:
                            entry['points'].append({
                                'price': float(row[2]),
                                'change_pct': float(row[3]) if row[3] is not None else 0,
                                'timestamp': str(row[4]) if row[4] else '',
                            })
                            price_val = float(row[2])
                            if entry['high_price'] is None or price_val > entry['high_price']:
                                entry['high_price'] = price_val
                            if entry['low_price'] is None or price_val < entry['low_price']:
                                entry['low_price'] = price_val
                    timelines[market] = list(index_map.values())
        except Exception as exc:
            logger.warning("[市场指数] 加载市场指数时间线失败: %s", exc)
        return timelines

    def _try_repo_market_timelines(self, markets: List[str], market_dates: Dict[str, str]) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        """Try to load market timelines via MarketDataRepository.

        Returns None if repository is unavailable or fails (caller falls back to direct SQL).
        """
        try:
            from src.services.market_data_repository import MarketDataRepository
            from src.storage_historical import HistoricalDatabaseManager

            hist_db_path = getattr(self, '_historical_db_path', None)
            if hist_db_path:
                historical_db = HistoricalDatabaseManager(hist_db_path)
            else:
                historical_db = HistoricalDatabaseManager()
            repo = MarketDataRepository(self._db, historical_db)

            timelines: Dict[str, List[Dict[str, Any]]] = {}
            for market in markets:
                query_date = market_dates.get(market) or self._get_market_query_date(market)
                rows = repo.get_market_index_trend(market=market, query_date=query_date)
                if not rows:
                    continue

                # Group by index_code
                index_map: Dict[str, Dict[str, Any]] = {}
                for r in rows:
                    ic = r.get('index_code', '')
                    if not ic:
                        continue
                    if ic not in index_map:
                        index_map[ic] = {
                            'index_code': ic,
                            'index_name': r.get('index_name') or ic,
                            'points': [],
                            'open_price': r.get('open'),
                            'prev_close': r.get('pre_close'),
                            'high_price': r.get('high'),
                            'low_price': r.get('low'),
                        }
                    entry = index_map[ic]
                    if r.get('status') == 'valid' and r.get('current_price') is not None:
                        entry['points'].append({
                            'price': float(r['current_price']),
                            'change_pct': float(r['change_pct']) if r.get('change_pct') is not None else 0,
                            'timestamp': str(r.get('market_local_timestamp') or ''),
                        })
                        price_val = float(r['current_price'])
                        if entry['high_price'] is None or price_val > entry['high_price']:
                            entry['high_price'] = price_val
                        if entry['low_price'] is None or price_val < entry['low_price']:
                            entry['low_price'] = price_val
                timelines[market] = list(index_map.values())
            return timelines
        except Exception as exc:
            logger.debug("Repository-backed market timelines unavailable, falling back to direct SQL: %s", exc)
            return None

    def _load_market_timeline_stats_for_decision(
        self,
        market_timelines: Dict[str, List[Dict[str, Any]]],
        markets: List[str],
        market_dates: Dict[str, str],
    ) -> Dict[str, Any]:
        """Build structured market timeline stats for prompt differentiation.

        Returns {market: {market, expected_indices, rows, valid_points,
                          failed_indices, unavailable_indices,
                          total_snapshots, valid_snapshot_count}}
        """
        stats: Dict[str, Any] = {}
        for market in markets:
            expected_indices = self._get_market_indices(market)
            mkt_tl = market_timelines.get(market, [])
            existing_codes = {t['index_code'] for t in mkt_tl}
            failed_indices = []
            for t in mkt_tl:
                pts = t.get('points', [])
                if not pts or all(p.get('status') == 'fetch_failed' for p in pts):
                    failed_indices.append(t['index_code'])
            unavailable_indices = [ic for ic in expected_indices if ic not in existing_codes]
            valid_count = sum(
                1 for t in mkt_tl
                for p in t.get('points', [])
                if p.get('price') is not None
            )
            total_count = sum(len(t.get('points', [])) for t in mkt_tl)
            stats[market] = {
                'market': market,
                'expected_indices': expected_indices,
                'rows': mkt_tl,
                'valid_points': [
                    p for t in mkt_tl
                    for p in t.get('points', [])
                    if p.get('price') is not None
                ],
                'failed_indices': failed_indices,
                'unavailable_indices': unavailable_indices,
                'total_snapshots': total_count,
                'valid_snapshot_count': valid_count,
            }
        return stats

    def _load_stock_timelines_for_decision(self, markets: List[str], market_dates: Dict[str, str]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """Load all intraday events across all snapshots for the decision day.

        Groups by stock_code, sorted by market_local_timestamp.
        Returns: {stock_code: {'market': str, 'points': [{price, change_pct, volume_ratio, ts, event_type, description}, ...]}}
        """
        self._ensure_intraday_table()
        timelines: Dict[str, Dict[str, Any]] = {}
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                for market in markets:
                    query_date = market_dates.get(market) or self._get_market_query_date(market)
                    rows = conn.execute(sa_text(
                        "SELECT stock_code, stock_name, current_price, change_pct, volume_ratio, "
                        "market_local_timestamp, event_type, description, snapshot_id, market "
                        "FROM intraday_events "
                        "WHERE query_date = :qd AND market = :mkt "
                        "ORDER BY stock_code, market_local_timestamp"
                    ), {"qd": query_date, "mkt": market}).fetchall()
                    for row in rows:
                        code = str(row[0] or "")
                        if not code:
                            continue
                        if code not in timelines:
                            timelines[code] = {
                                'stock_name': str(row[1] or ""),
                                'market': str(row[9] or market),
                                'points': [],
                            }
                        timelines[code]['points'].append({
                            'price': float(row[2]) if row[2] is not None else None,
                            'change_pct': float(row[3]) if row[3] is not None else None,
                            'volume_ratio': float(row[4]) if row[4] is not None else None,
                            'timestamp': str(row[5]) if row[5] else '',
                            'event_type': str(row[6] or ""),
                            'description': str(row[7] or ""),
                            'snapshot_id': str(row[8]) if row[8] else '',
                        })
        except Exception as exc:
            logger.warning("[个股时间线] 加载失败: %s", exc)
        return timelines

    def _rebuild_snapshot_times_from_db(self) -> List[str]:
        """Query intraday_snapshots to rebuild snapshot times for the day."""
        snapshot_times: List[str] = []
        self._ensure_intraday_snapshots_table()
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                rows = conn.execute(sa_text(
                    "SELECT DISTINCT started_at FROM intraday_snapshots "
                    "WHERE status IN ('completed', 'partial') ORDER BY started_at"
                )).fetchall()
                for row in rows:
                    ts_str = str(row[0]) if row[0] else ""
                    if ts_str:
                        try:
                            dt = datetime.fromisoformat(ts_str)
                            snapshot_times.append(dt.strftime("%H:%M"))
                        except (ValueError, TypeError):
                            pass
        except Exception as exc:
            logger.warning("[快照时间] 从DB重建失败: %s", exc)
        return snapshot_times

    def _update_snapshot_status(self, snapshot_id: str, status: str, finished_at: Optional[datetime] = None) -> None:
        """Update snapshot status."""
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                if finished_at:
                    conn.execute(
                        sa_text("UPDATE intraday_snapshots SET status = :st, finished_at = :fa WHERE snapshot_id = :sid"),
                        {"st": status, "fa": finished_at.isoformat(), "sid": snapshot_id},
                    )
                else:
                    conn.execute(
                        sa_text("UPDATE intraday_snapshots SET status = :st WHERE snapshot_id = :sid"),
                        {"st": status, "sid": snapshot_id},
                    )
                session.commit()
        except Exception as exc:
            logger.warning("更新快照状态失败: %s", exc)

    # ------------------------------------------------------------------
    # Process-level concurrency control
    # ------------------------------------------------------------------

    def _acquire_process_lock(self, lock_name: str, ttl_seconds: int = 300) -> bool:
        import uuid
        holder = uuid.uuid4().hex[:12]
        now = datetime.utcnow()
        expires = now.isoformat()
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                # Check if lock exists and is still valid
                existing = conn.execute(
                    sa_text("SELECT expires_at, holder FROM process_locks WHERE lock_name = :ln"),
                    {"ln": lock_name},
                ).fetchone()
                if existing:
                    if existing[0] >= now.isoformat():
                        # Lock still valid, not ours
                        return False
                    # Lock expired: overwrite
                    conn.execute(
                        sa_text("DELETE FROM process_locks WHERE lock_name = :ln"),
                        {"ln": lock_name},
                    )
                # Acquire
                conn.execute(
                    sa_text("INSERT INTO process_locks (lock_name, holder, acquired_at, expires_at) VALUES (:ln, :h, :aa, :ea)"),
                    {"ln": lock_name, "h": holder, "aa": now.isoformat(), "ea": (datetime.utcnow() + timedelta(seconds=ttl_seconds)).isoformat()},
                )
                session.commit()
                return True
        except Exception as exc:
            logger.warning("_acquire_process_lock(%s) failed: %s", lock_name, exc)
            return False

    def _release_process_lock(self, lock_name: str) -> None:
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                conn.execute(sa_text("DELETE FROM process_locks WHERE lock_name = :ln"), {"ln": lock_name})
                session.commit()
        except Exception as exc:
            logger.warning("_release_process_lock(%s) failed: %s", lock_name, exc)

    def _force_release_process_lock(self, lock_name: str) -> None:
        self._release_process_lock(lock_name)
        logger.warning("强制释放进程锁: %s", lock_name)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_stock_trading_today(self, code: str) -> bool:
        """Check if the market for a specific stock is open today. Uses strict calendar."""
        market = self._get_market_for_stock(code)
        if not market:
            logger.warning("无法识别股票 %s 的市场，按休市处理", code)
            return False
        if market not in ('cn', 'hk', 'us'):
            logger.warning("未知市场 %r for %s，按休市处理", market, code)
            return False
        try:
            from src.core.trading_calendar import is_market_open_strict
            market_date = self._get_market_query_date(market)
            market_date_obj = date.fromisoformat(market_date)
            is_open, status = is_market_open_strict(market, market_date_obj)
            if is_open is True:
                return True
            if is_open is False:
                return False
            # status is "calendar_unavailable", "unknown_market", or "error"
            fail_open = getattr(self._config, 'intraday_calendar_fail_open', False)
            if fail_open:
                logger.warning("交易日检测 [%s] 状态=%s fail-open: 按交易日处理", code, status)
                return True
            logger.warning("交易日检测 [%s] 状态=%s fail-closed: 按休市处理", code, status)
            return False
        except Exception as exc:
            fail_open = getattr(self._config, 'intraday_calendar_fail_open', False)
            if fail_open:
                logger.warning("交易日检测异常 [%s] fail-open: %s", code, exc)
                return True
            logger.warning("交易日检测异常 [%s] fail-closed: %s", code, exc)
            return False

    def _get_stock_codes(self) -> List[str]:
        """Get monitored stock codes from config."""
        codes_str = getattr(self._config, "intraday_monitor_stocks", "") or ""
        if not codes_str:
            # Fallback to config.stock_list
            stock_list = getattr(self._config, "stock_list", []) or []
            return [str(c).strip() for c in stock_list if c]
        return [c.strip() for c in codes_str.split(",") if c.strip()]

    def run_one_shot_snapshot(self) -> None:
        """Standalone snapshot: load yesterday analysis, snapshot, save to SQLite. Does NOT clear today's events."""
        stock_codes = self._get_stock_codes()
        if not stock_codes:
            logger.warning("无监控股票，跳过快照")
            return

        tradable = [c for c in stock_codes if self._is_stock_trading_today(c)]
        if not tradable:
            force = getattr(self._config, 'intraday_force_run', False)
            if not force:
                logger.info("所有监控股票今日均休市，跳过盘中快照（--force-run 可强制执行）")
                return
            logger.warning("非交易日，但 force_run 已启用，仍执行快照")
            tradable = stock_codes

        self._ensure_intraday_table()
        self._ensure_intraday_snapshots_table()
        self.load_yesterday_analysis(stock_codes)

        now = datetime.now()
        snapshot_id = f"{now.strftime('%Y%m%d_%H%M%S')}_standalone"
        self._current_snapshot_id = snapshot_id

        logger.info("盘中快照(standalone) 开始: %s, %d 支股票 (可交易 %d 支)",
                     now.strftime("%H:%M:%S"), len(stock_codes), len(tradable))

        results, snapshot_state = self._run_snapshot_with_state_persistence(
            codes=tradable, now=now, snapshot_id=snapshot_id, source="standalone",
        )

        logger.info(
            "盘中快照(standalone) 完成: batch_hit=%d fallback=%d completed=%d valid=%d failed=%d",
            results.get("batch_matched_count", 0), results.get("fallback_count", 0),
            len(results.get("completed_codes", [])), len(results.get("valid_quote_codes", [])),
            len(results.get("failed_codes", [])),
        )

        now_str = now.strftime("%H:%M")
        if not self._snapshot_times or self._snapshot_times[-1] != now_str:
            self._snapshot_times.append(now_str)

    def run_one_shot_decision(self) -> None:
        """Standalone decision: snapshot at current price, then run LLM, send email.
        Non-trading day: no events written unless --force-run.
        """
        self._ensure_intraday_table()
        self._ensure_intraday_snapshots_table()

        stock_codes = self._get_stock_codes()
        tradable = [c for c in stock_codes if self._is_stock_trading_today(c)]
        force = getattr(self._config, 'intraday_force_run', False)

        if not tradable and not force:
            logger.info("所有监控股票今日均休市，跳过盘中决策（--force-run 可强制执行）")
            return

        if force and not tradable:
            logger.warning("非交易日，但 force_run 已启用，仍执行决策快照")

        # Only snapshot tradable stocks; under force-run, snapshot all
        target_codes = stock_codes if force else tradable

        if target_codes:
            self.load_yesterday_analysis(target_codes)

        # Snapshot first to capture latest prices before decision
        now = datetime.now()
        snapshot_id = f"{now.strftime('%Y%m%d_%H%M%S')}_decision"
        self._current_snapshot_id = snapshot_id

        logger.info("盘中决策前快照(standalone): %s, %d 支股票", now.strftime("%H:%M:%S"), len(target_codes))

        results, snapshot_state = self._run_snapshot_with_state_persistence(
            codes=target_codes, now=now, snapshot_id=snapshot_id, source="decision",
        )

        logger.info(
            "决策前快照完成(standalone): batch_hit=%d fallback=%d completed=%d valid=%d",
            results.get("batch_matched_count", 0), results.get("fallback_count", 0),
            len(results.get("completed_codes", [])), len(results.get("valid_quote_codes", [])),
        )

        now_str = now.strftime("%H:%M")
        if not self._snapshot_times or self._snapshot_times[-1] != now_str:
            self._snapshot_times.append(now_str)

        self._final_decision_locked(preferred_snapshot_id=snapshot_state.snapshot_id)
