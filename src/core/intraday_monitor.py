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
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
import atexit
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

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
        }


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
# IntradayMonitor
# ---------------------------------------------------------------------------

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
    ):
        self._config = config
        self._fetcher = fetcher_manager
        self._db = db_manager
        self._email_sender = email_sender
        self._yesterday_analysis: Dict[str, Dict[str, Optional[float]]] = {}
        self._initialized: bool = False
        self._lock = threading.Lock()
        self._snapshot_times: List[str] = []  # record snapshot times for the day
        self._daily_init_marker: Optional[str] = None  # tracks which trading date was initialized

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
            from src.core.trading_calendar import get_effective_trading_date
        except Exception:
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

        try:
            with self._db.session_scope() as session:
                from src.storage import AnalysisHistory
                from sqlalchemy import and_, func

                all_primary: list = []
                all_fallback: list = []
                fallback_warning_codes: Set[str] = set()
                allow_fallback = getattr(self._config, 'intraday_legacy_fallback_enabled', False)

                for market, codes_in_market in by_market.items():
                    if not codes_in_market:
                        continue

                    # Per-market target trading date
                    if get_effective_trading_date:
                        try:
                            target_trading_date = get_effective_trading_date(market)
                        except Exception:
                            target_trading_date = date.today() - timedelta(days=1)
                    else:
                        target_trading_date = date.today() - timedelta(days=1)

                    target_date_str = target_trading_date.strftime("%Y-%m-%d")

                    # --- Primary query: new columns, per-market date ---
                    primary_rows = (
                        session.query(AnalysisHistory)
                        .filter(
                            and_(
                                AnalysisHistory.code.in_(codes_in_market),
                                AnalysisHistory.effective_trading_date == target_trading_date,
                                AnalysisHistory.analysis_phase == 'postmarket',
                            )
                        )
                        .order_by(AnalysisHistory.created_at.desc())
                        .all()
                    )
                    all_primary.extend(primary_rows)
                    primary_codes = {str(row.code or "").strip() for row in primary_rows}

                    # --- Fallback: old records only (no new columns) ---
                    remaining_codes = [c for c in codes_in_market if c not in primary_codes]
                    if remaining_codes and allow_fallback:
                        fallback_rows = (
                            session.query(AnalysisHistory)
                            .filter(
                                and_(
                                    AnalysisHistory.code.in_(remaining_codes),
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
                primary_codes_all = {str(row.code or "").strip() for row in all_primary}
                missed_codes: Dict[str, List[str]] = {}
                for market, codes_in_market in by_market.items():
                    missed = [c for c in codes_in_market if c not in primary_codes_all]
                    if missed:
                        missed_codes[market] = missed

                if missed_codes:
                    logger.warning(
                        "盘中监控: primary query 未命中 %d 支股票，开始诊断...",
                        sum(len(v) for v in missed_codes.values()),
                    )
                    # Diagnostic 1: same code + effective_trading_date but different phase
                    for market, missed_list in missed_codes.items():
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
                                    AnalysisHistory.code.in_(missed_list),
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
                                    AnalysisHistory.code.in_(missed_list),
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
                                    AnalysisHistory.code.in_(missed_list),
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
                    if not code or code in seen:
                        continue
                    seen.add(code)

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

                    result[code] = {
                        "ideal_buy": ideal,
                        "secondary_buy": secondary,
                        "stop_loss": stop,
                        "take_profit": profit,
                    }

            logger.info("加载昨日分析: %d 支股票 (primary=%d, fallback=%d, markets=%s)",
                         len(result), len(all_primary), len(all_fallback),
                         ", ".join(f"{m}={len(c)}" for m, c in by_market.items()))
        except Exception as exc:
            logger.exception("加载昨日分析失败: %s", exc)

        self._yesterday_analysis = result
        return result

    def _get_daily_init_key(self) -> str:
        """Return a unique key for today's initialization state.
        Uses the primary market's local date so multi-market setups get a stable key.
        """
        primary_market = self._resolve_primary_market()
        return f"{primary_market}:{self._get_market_query_date(primary_market)}"

    def _resolve_primary_market(self) -> str:
        """Determine primary market for global checks (config-driven)."""
        region = getattr(self._config, 'market_review_region', None)
        if region and region in ('cn', 'hk', 'us'):
            return region
        return 'cn'

    def _should_clear_events(self) -> bool:
        """Return True only on genuinely new trading day (persistent marker doesn't match).

        On process restart within same trading day, the persistent marker in DB
        matches today's key → returns False (events preserved).
        """
        today_key = self._get_daily_init_key()
        persistent_key = f"daily_init:{self._resolve_primary_market()}"
        try:
            persistent_value = self._db.get_intraday_state(persistent_key)
        except Exception:
            persistent_value = None

        if persistent_value == today_key:
            return False  # Already initialized today, even across restarts

        # Genuinely new trading day (or first-ever run)
        return True

    def _persist_init_marker(self) -> None:
        """Write persistent init marker so restarts don't re-clear today's events."""
        today_key = self._get_daily_init_key()
        persistent_key = f"daily_init:{self._resolve_primary_market()}"
        try:
            self._db.set_intraday_state(persistent_key, today_key)
            self._daily_init_marker = today_key
            logger.info("Persisted intraday init marker: %s", today_key)
        except Exception as exc:
            logger.warning("Failed to persist intraday init marker: %s", exc)
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

            if reset_marker:
                persistent_key = f"daily_init:{self._resolve_primary_market()}"
                try:
                    self._db.set_intraday_state(persistent_key, "")
                    self._daily_init_marker = None
                    logger.info("已重置持久化 init marker (--reset-intraday-events)")
                except Exception as exc:
                    logger.warning("重置持久化 init marker 失败: %s", exc)
        except Exception as exc:
            logger.warning("清空当天盘中事件失败（可能表不存在）: %s", exc)

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def monitor_snapshot(self) -> None:
        """Entry point for scheduled snapshot (called by Scheduler)."""
        acquired = self._lock.acquire(timeout=30)
        if not acquired:
            logger.warning("monitor_snapshot: 无法获取锁（可能 final_decision 正在执行），跳过本次快照")
            return
        try:
            self._monitor_snapshot_locked()
        finally:
            self._lock.release()

    def _monitor_snapshot_locked(self) -> None:
        """Snapshot implementation (caller must hold self._lock)."""
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

        # Lazy init on first call of a new trading day
        if not self._initialized or self._should_clear_events():
            self.load_yesterday_analysis(stock_codes)
            # Only clear events on genuinely new trading day (not on restart)
            if self._should_clear_events():
                self.clear_today_events()
                self._persist_init_marker()
            elif self._daily_init_marker is None:
                # First call on restart — persist existing marker to avoid future clears
                self._persist_init_marker()
            self._initialized = True
            logger.info("盘中监控初始化完成，监控 %d 支股票", len(stock_codes))

        now = datetime.now()
        self._snapshot_times.append(now.strftime("%H:%M"))
        logger.info("盘中快照开始: %s, 候选 %d 支 (可交易 %d 支)",
                     now.strftime("%H:%M:%S"), len(stock_codes), len(tradable_codes))

        for code in tradable_codes:
            try:
                self._snapshot_one_stock(code, now)
            except Exception as exc:
                logger.error("盘中快照异常 [%s]: %s", code, exc)

        logger.info("盘中快照完成: %s", now.strftime("%H:%M:%S"))

    def _snapshot_one_stock(self, code: str, now: datetime) -> None:
        """Fetch realtime quote for one stock and compare against thresholds."""
        # 10s timeout wrapper
        quote = self._get_realtime_with_timeout(code, timeout=10.0)
        if quote is None:
            logger.warning("盘中快照 [%s]: 实时行情获取失败（超时或不可用），标记 data_unavailable", code)
            self._save_event(IntradayEvent(
                timestamp=now,
                stock_code=code,
                stock_name="",
                current_price=0.0,
                volume_ratio=None,
                change_pct=None,
                event_type="data_unavailable",
                description="实时行情获取超时或所有数据源失败",
            ))
            return

        # Suspended detection
        if quote.price is None or quote.price <= 0:
            self._save_event(IntradayEvent(
                timestamp=now,
                stock_code=code,
                stock_name=quote.name or "",
                current_price=0.0,
                volume_ratio=None,
                change_pct=None,
                event_type="suspended",
                description="停牌或无有效价格",
            ))
            return

        # Sanitize values for prompt injection prevention
        price = float(quote.price)
        vol_ratio = float(quote.volume_ratio) if quote.volume_ratio is not None else None
        change = float(quote.change_pct) if quote.change_pct is not None else None
        name = str(quote.name or "")

        # Compare with thresholds
        yesterday = self._yesterday_analysis.get(code, {})
        if not any(yesterday.values()):
            # No yesterday analysis
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
            self._save_event(event)
            return

        events = _compare_with_thresholds(code, name, price, vol_ratio, change, yesterday, now=now)
        for event in events:
            self._save_event(event)

    def _get_realtime_with_timeout(self, code: str, timeout: float = 10.0):
        """Call get_realtime_quote with timeout protection.
        Uses a module-level executor so shutdown does not block on stuck threads.
        """
        future = _QUOTE_EXECUTOR.submit(self._fetcher.get_realtime_quote, code)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            logger.warning("get_realtime_quote [%s] 超时 (%ss)", code, timeout)
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
        acquired = self._lock.acquire(timeout=60)
        if not acquired:
            logger.error("final_decision: 无法获取锁（monitor_snapshot 可能卡死），放弃本次决策")
            return
        try:
            self._final_decision_locked()
        finally:
            self._lock.release()

    def _final_decision_locked(self) -> None:
        """Final decision implementation (caller must hold self._lock)."""
        # Ensure yesterday analysis is loaded for prompt construction
        stock_codes = self._get_stock_codes()
        if stock_codes and not self._yesterday_analysis:
            logger.info("_yesterday_analysis 为空，尝试加载...")
            self.load_yesterday_analysis(stock_codes)
        if not self._yesterday_analysis:
            logger.warning(
                "未加载到昨日盘后基准分析，盘中决策将在无基准的情况下生成"
            )

        # Per-market trading day check: at least one market must be trading
        tradable = [c for c in stock_codes if self._is_stock_trading_today(c)]
        if not tradable:
            logger.info("所有监控股票对应市场今日均休市，跳过盘中决策")
            if not getattr(self._config, 'intraday_force_run', False):
                return

        # Load events for all relevant market-local dates (skip unknown markets)
        markets = set()
        for c in stock_codes:
            mkt = self._get_market_for_stock(c)
            if mkt:
                markets.add(mkt)
        all_events: List[IntradayEvent] = []
        for mkt in markets:
            mkt_date = self._get_market_query_date(mkt)
            all_events.extend(self._load_today_events(mkt_date, market=mkt))

        logger.info("盘中决策: 读取到 %d 条事件 (markets=%s)", len(all_events), markets)

        if not all_events:
            logger.info("无盘中事件，跳过决策邮件")
            return

        # Build prompt
        from src.core.intraday_prompt import build_intraday_prompt
        prompt = build_intraday_prompt(
            events=all_events,
            yesterday_analysis=self._yesterday_analysis,
            snapshot_times=self._snapshot_times,
        )

        # Call LLM with 3 retries
        decision_text = self._call_llm_with_retries(prompt)
        if decision_text is None:
            # Fallback: send raw data summary
            decision_text = self._build_raw_summary(all_events)

        # Send email
        self._send_decision_email(decision_text, all_events)

    def _call_llm_with_retries(self, prompt: str) -> Optional[str]:
        """Call litellm.completion() with 3 retries, 5s interval."""
        import litellm

        for attempt in range(3):
            try:
                response = litellm.completion(
                    model=self._config.llm_model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=getattr(self._config, "llm_max_tokens", 2000),
                    temperature=getattr(self._config, "llm_temperature", 0.3),
                )
                text = response.choices[0].message.content
                logger.info("LLM 决策成功 (attempt %d/3)", attempt + 1)
                return str(text)
            except Exception as exc:
                logger.warning("LLM 调用失败 (attempt %d/3): %s", attempt + 1, exc)
                if attempt < 2:
                    time.sleep(5)

        logger.error("LLM 调用 3 次均失败，降级发送原始数据摘要")
        return None

    def _build_raw_summary(self, events: List[IntradayEvent]) -> str:
        """Build raw data summary when LLM fails."""
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
                f"| {e.timestamp.strftime('%H:%M')} | {e.stock_name}({e.stock_code}) | "
                f"{e.current_price:.2f} | {vol} | {chg} | {e.event_type} | {e.description} |"
            )
        return "\n".join(lines)

    def _send_decision_email(self, content: str, events: List[IntradayEvent]) -> None:
        """Send decision email via EmailSender. Title reflects covered markets and dates."""
        if self._email_sender is None:
            logger.warning("EmailSender 未配置，跳过邮件发送")
            return

        # Collect per-market local dates from events
        market_dates: Dict[str, str] = {}
        for e in events:
            mkt = self._get_market_for_stock(e.stock_code)
            if mkt:
                market_dates.setdefault(mkt, self._get_market_query_date(mkt))

        if not market_dates:
            primary_market = self._resolve_primary_market()
            market_dates[primary_market] = self._get_market_query_date(primary_market)

        if len(market_dates) == 1:
            mkt, mkt_date = next(iter(market_dates.items()))
            subject = f"盘中监控报告 - {mkt.upper()}:{mkt_date}"
        else:
            parts = sorted(f"{m.upper()}:{d}" for m, d in market_dates.items())
            subject = f"盘中监控报告 - {' / '.join(parts)}"

        try:
            success = self._email_sender.send_to_email(
                content=content,
                subject=subject,
            )
            if success:
                logger.info("盘中决策邮件发送成功")
            else:
                logger.warning("盘中决策邮件发送失败")
        except Exception as exc:
            logger.exception("盘中决策邮件发送异常: %s", exc)

    # ------------------------------------------------------------------
    # SQLite helpers
    # ------------------------------------------------------------------

    def _save_event(self, event: IntradayEvent) -> None:
        """Persist one intraday event to SQLite. Uses market-local date."""
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

        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                conn.execute(
                    sa_text(
                        "INSERT INTO intraday_events "
                        "(query_date, market, timestamp, market_local_timestamp, stock_code, stock_name, "
                        "current_price, volume_ratio, change_pct, event_type, description, raw_quote) "
                        "VALUES (:qd, :mkt, :ts, :mlt, :sc, :sn, :cp, :vr, :cpct, :et, :desc, :rq)"
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
                        "rq": None,
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
                            "SELECT timestamp, stock_code, stock_name, current_price, "
                            "volume_ratio, change_pct, event_type, description "
                            "FROM intraday_events "
                            "WHERE query_date = :qd AND market = :mkt ORDER BY timestamp"
                        ),
                        {"qd": query_date, "mkt": market},
                    ).fetchall()
                else:
                    rows = conn.execute(
                        sa_text(
                            "SELECT timestamp, stock_code, stock_name, current_price, "
                            "volume_ratio, change_pct, event_type, description "
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
                        volume_ratio=float(row[4]) if row[4] is not None else None,
                        change_pct=float(row[5]) if row[5] is not None else None,
                        event_type=str(row[6] or "price_only"),
                        description=str(row[7] or ""),
                    ))
        except Exception as exc:
            logger.warning("加载盘中事件失败: %s", exc)

        return events

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
                conn.execute(sa_text(
                    "CREATE INDEX IF NOT EXISTS idx_intraday_date_code "
                    "ON intraday_events(query_date, stock_code)"
                ))
                conn.execute(sa_text(
                    "CREATE INDEX IF NOT EXISTS idx_intraday_date_market "
                    "ON intraday_events(query_date, market)"
                ))
                session.commit()
                logger.info("intraday_events 表已就绪")
        except Exception as exc:
            logger.error("创建 intraday_events 表失败: %s", exc)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_trading_day(self) -> bool:
        """Check if today is a trading day for the primary configured market.
        Deprecated in favor of _is_stock_trading_today() for multi-market setups.
        """
        try:
            from src.core.trading_calendar import is_market_open
            market = self._resolve_primary_market()
            market_date = self._get_market_query_date(market)
            market_date_obj = date.fromisoformat(market_date)
            return is_market_open(market, market_date_obj)
        except Exception as exc:
            fail_open = getattr(self._config, 'intraday_calendar_fail_open', False)
            if fail_open:
                logger.warning("交易日检测异常，fail-open 按交易日处理: %s", exc)
                return True
            logger.warning("交易日检测异常，fail-closed 跳过: %s", exc)
            return False

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
        self.load_yesterday_analysis(stock_codes)

        now = datetime.now()
        logger.info("盘中快照(standalone) 开始: %s, %d 支股票 (可交易 %d 支)",
                     now.strftime("%H:%M:%S"), len(stock_codes), len(tradable))

        for code in tradable:
            try:
                self._snapshot_one_stock(code, now)
            except Exception as exc:
                logger.error("盘中快照异常 [%s]: %s", code, exc)

        logger.info("盘中快照(standalone) 完成: %s", now.strftime("%H:%M:%S"))

    def run_one_shot_decision(self) -> None:
        """Standalone decision: snapshot at current price, then run LLM, send email.
        Non-trading day: no events written unless --force-run.
        """
        self._ensure_intraday_table()

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
        logger.info("盘中决策前快照(standalone): %s, %d 支股票", now.strftime("%H:%M:%S"), len(target_codes))
        for code in target_codes:
            try:
                self._snapshot_one_stock(code, now)
            except Exception as exc:
                logger.error("决策前快照异常 [%s]: %s", code, exc)

        self._final_decision_locked()
