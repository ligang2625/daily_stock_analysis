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
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


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
    1. 首次 monitor_snapshot() 时自动 load_yesterday_analysis() + clear_today_events()
    2. 后续快照直接比对写入
    3. 14:20 final_decision() 读取全天事件 → LLM → 发邮件
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

    def _resolve_market(self) -> str:
        """Determine market region for trading calendar lookups."""
        region = getattr(self._config, 'market_review_region', None)
        if region and region in ('cn', 'hk', 'us'):
            return region
        return 'cn'

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def load_yesterday_analysis(self, stock_codes: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
        """
        Load the most recent completed trading day's postmarket analysis.

        Primary query: effective_trading_date == target AND analysis_phase == 'postmarket'.
        Fallback query (old records): func.date(created_at) >= target_date_str.
        Primary records take precedence over fallback for the same code.

        Returns {code: {ideal_buy, secondary_buy, stop_loss, take_profit}}
        """
        if not stock_codes:
            return {}

        result: Dict[str, Dict[str, Optional[float]]] = {}
        market = self._resolve_market()

        try:
            from src.core.trading_calendar import get_effective_trading_date
            target_trading_date = get_effective_trading_date(market)
        except Exception:
            target_trading_date = date.today() - timedelta(days=1)

        target_date_str = target_trading_date.strftime("%Y-%m-%d")

        try:
            with self._db.session_scope() as session:
                from src.storage import AnalysisHistory
                from sqlalchemy import or_, and_, func

                # --- Primary query: new columns ---
                primary_rows = (
                    session.query(AnalysisHistory)
                    .filter(
                        and_(
                            AnalysisHistory.code.in_(stock_codes),
                            AnalysisHistory.effective_trading_date == target_trading_date,
                            AnalysisHistory.analysis_phase == 'postmarket',
                        )
                    )
                    .order_by(AnalysisHistory.created_at.desc())
                    .all()
                )

                primary_codes = {str(row.code or "").strip() for row in primary_rows}

                # --- Fallback: old records without new columns ---
                remaining_codes = [c for c in stock_codes if c not in primary_codes]
                fallback_rows: list = []
                if remaining_codes:
                    fallback_rows = (
                        session.query(AnalysisHistory)
                        .filter(
                            and_(
                                AnalysisHistory.code.in_(remaining_codes),
                                func.date(AnalysisHistory.created_at) >= target_date_str,
                            )
                        )
                        .order_by(AnalysisHistory.created_at.desc())
                        .all()
                    )

                # Combine: primary first, then fallback; keep first per code
                all_rows = list(primary_rows) + list(fallback_rows)

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

            logger.info("加载昨日分析: %d 支股票 (primary=%d, fallback=%d)",
                         len(result), len(primary_codes), len(fallback_rows))
        except Exception as exc:
            logger.exception("加载昨日分析失败: %s", exc)

        self._yesterday_analysis = result
        return result

    def clear_today_events(self) -> None:
        """Delete today's intraday events from SQLite (idempotent)."""
        today = date.today().isoformat()
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                conn.execute(sa_text("DELETE FROM intraday_events WHERE query_date = :qd"), {"qd": today})
                session.commit()
            logger.info("已清空当天盘中事件: %s", today)
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
        # Trading day check
        if not self._is_trading_day():
            logger.info("非交易日，跳过盘中快照")
            return

        # Lazy init on first call
        if not self._initialized:
            stock_codes = self._get_stock_codes()
            self.load_yesterday_analysis(stock_codes)
            self.clear_today_events()
            self._initialized = True
            logger.info("盘中监控初始化完成，监控 %d 支股票", len(stock_codes))

        stock_codes = self._get_stock_codes()
        if not stock_codes:
            return

        now = datetime.now()
        self._snapshot_times.append(now.strftime("%H:%M"))
        logger.info("盘中快照开始: %s, 候选股 %d 支", now.strftime("%H:%M:%S"), len(stock_codes))

        for code in stock_codes:
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
        """Call get_realtime_quote with timeout protection."""
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self._fetcher.get_realtime_quote, code)
            try:
                return future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                logger.warning("get_realtime_quote [%s] 超时 (%ss)", code, timeout)
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
        if not self._is_trading_day():
            logger.info("非交易日，跳过盘中决策")
            return

        today_str = date.today().isoformat()
        events = self._load_today_events(today_str)
        logger.info("盘中决策: 读取到 %d 条事件", len(events))

        if not events:
            logger.info("无盘中事件，跳过决策邮件")
            return

        # Build prompt
        from src.core.intraday_prompt import build_intraday_prompt
        prompt = build_intraday_prompt(
            events=events,
            yesterday_analysis=self._yesterday_analysis,
            snapshot_times=self._snapshot_times,
        )

        # Call LLM with 3 retries
        decision_text = self._call_llm_with_retries(prompt)
        if decision_text is None:
            # Fallback: send raw data summary
            decision_text = self._build_raw_summary(events)

        # Send email
        self._send_decision_email(decision_text, events)

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
        """Send decision email via EmailSender."""
        if self._email_sender is None:
            logger.warning("EmailSender 未配置，跳过邮件发送")
            return

        today_str = date.today().isoformat()
        subject = f"盘中监控报告 - {today_str} 14:30"

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
        """Persist one intraday event to SQLite."""
        today = date.today().isoformat()
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                conn.execute(
                    sa_text(
                        "INSERT INTO intraday_events "
                        "(query_date, timestamp, stock_code, stock_name, current_price, "
                        "volume_ratio, change_pct, event_type, description, raw_quote) "
                        "VALUES (:qd, :ts, :sc, :sn, :cp, :vr, :cpct, :et, :desc, :rq)"
                    ),
                    {
                        "qd": today,
                        "ts": event.timestamp.isoformat(),
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

    def _load_today_events(self, query_date: str) -> List[IntradayEvent]:
        """Load all intraday events for a given date from SQLite."""
        events: List[IntradayEvent] = []
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
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
        """Create intraday_events table if not exists."""
        try:
            with self._db.session_scope() as session:
                conn = session.connection()
                from sqlalchemy import text as sa_text
                conn.execute(sa_text(
                    "CREATE TABLE IF NOT EXISTS intraday_events ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "query_date TEXT NOT NULL, "
                    "timestamp TEXT NOT NULL, "
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
                conn.execute(sa_text(
                    "CREATE INDEX IF NOT EXISTS idx_intraday_date_code "
                    "ON intraday_events(query_date, stock_code)"
                ))
                session.commit()
                logger.info("intraday_events 表已就绪")
        except Exception as exc:
            logger.error("创建 intraday_events 表失败: %s", exc)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_trading_day(self) -> bool:
        """Check if today is a trading day for the configured market."""
        try:
            from src.core.trading_calendar import is_market_open
            return is_market_open(self._resolve_market(), date.today())
        except Exception as exc:
            logger.warning("交易日检测异常，按交易日处理: %s", exc)
        return True  # fail-open

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
        if not self._is_trading_day():
            logger.info("非交易日，跳过盘中快照")
            return

        stock_codes = self._get_stock_codes()
        if not stock_codes:
            logger.warning("无监控股票，跳过快照")
            return

        self._ensure_intraday_table()
        self.load_yesterday_analysis(stock_codes)

        now = datetime.now()
        logger.info("盘中快照(standalone) 开始: %s, %d 支股票", now.strftime("%H:%M:%S"), len(stock_codes))

        for code in stock_codes:
            try:
                self._snapshot_one_stock(code, now)
            except Exception as exc:
                logger.error("盘中快照异常 [%s]: %s", code, exc)

        logger.info("盘中快照(standalone) 完成: %s", now.strftime("%H:%M:%S"))

    def run_one_shot_decision(self) -> None:
        """Standalone decision: snapshot at current price, then run LLM, send email."""
        self._ensure_intraday_table()

        stock_codes = self._get_stock_codes()
        if stock_codes:
            self.load_yesterday_analysis(stock_codes)

        # Snapshot first to capture latest prices before decision
        now = datetime.now()
        logger.info("盘中决策前快照(standalone): %s, %d 支股票", now.strftime("%H:%M:%S"), len(stock_codes))
        for code in stock_codes:
            try:
                self._snapshot_one_stock(code, now)
            except Exception as exc:
                logger.error("决策前快照异常 [%s]: %s", code, exc)

        self._final_decision_locked()
