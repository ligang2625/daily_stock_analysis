# -*- coding: utf-8 -*-
"""
MarketDataRepository — unified query layer over main + historical databases.

Routes queries by date: recent data (within retention_days) from main DB,
older data from historical DB. Merges cross-boundary results with dedup.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from src.services.stock_code_utils import (
    normalize_stock_code_for_storage,
    normalize_market_region,
)

logger = logging.getLogger(__name__)

DEFAULT_RETENTION_DAYS = 5


class MarketDataRepository:
    """
    Unified query layer for stock technical data.

    Usage:
        repo = MarketDataRepository(main_db, historical_db)
        rows = repo.get_stock_daily('HK00700', days_back=10)
    """

    def __init__(
        self,
        main_db,       # DatabaseManager instance
        historical_db, # HistoricalDatabaseManager instance
        retention_days: int = DEFAULT_RETENTION_DAYS,
    ):
        self._main_db = main_db
        self._historical_db = historical_db
        self._retention_days = retention_days

    @property
    def retention_days(self) -> int:
        return self._retention_days

    def _split_date_range(self, days_back: int) -> tuple:
        """
        Determine the boundary date for main/historical routing.

        Returns (main_start_date, boundary_date).
        - main_start_date: oldest date to check in main DB (today - retention_days)
        - boundary_date: everything before this goes to historical DB
        """
        today = date.today()
        boundary = today - timedelta(days=self._retention_days)
        main_start = today - timedelta(days=max(days_back, self._retention_days))
        return main_start, boundary

    # ------------------------------------------------------------------
    # get_stock_daily
    # ------------------------------------------------------------------

    def get_stock_daily(
        self,
        code: str,
        days_back: int = 30,
        market: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get daily stock data for a given code, up to days_back ago.

        Routes to main DB (StockDaily) for recent data and historical DB
        (HistoricalStockDaily) for older data. Results are merged and
        deduplicated by date.
        """
        from sqlalchemy import text as sa_text

        code_norm = normalize_stock_code_for_storage(code)
        if not code_norm:
            return []

        main_start, boundary = self._split_date_range(days_back)
        results: List[Dict[str, Any]] = []
        seen_dates: set = set()

        # --- Main DB query ---
        try:
            with self._main_db.session_scope() as session:
                main_rows = session.execute(
                    sa_text(
                        "SELECT code, code_norm, date, open, high, low, close, "
                        "pct_chg, volume, amount, volume_ratio, data_source "
                        "FROM stock_daily "
                        "WHERE code_norm = :cn "
                        "  AND date >= :start_date "
                        "ORDER BY date DESC"
                    ),
                    {
                        "cn": code_norm,
                        "start_date": main_start.isoformat(),
                    },
                ).fetchall()

                for row in main_rows:
                    d = self._row_date(row[2])
                    if d not in seen_dates:
                        seen_dates.add(d)
                        results.append(self._stock_daily_dict(row))
        except Exception as exc:
            logger.warning("Main DB stock_daily query failed for %s: %s", code_norm, exc)

        # --- Historical DB query ---
        try:
            with self._historical_db.get_session() as session:
                from sqlalchemy import text as sa_text_hist

                hist_rows = session.execute(
                    sa_text_hist(
                        "SELECT code, code_norm, trade_date, open, high, low, close, "
                        "pre_close, pct_chg, volume, amount, turnover_rate, "
                        "ma5, ma10, ma20, ma60, volume_ratio, bias5, bias10, bias20, "
                        "data_source "
                        "FROM historical_stock_daily "
                        "WHERE code_norm = :cn "
                        "  AND trade_date < :boundary "
                        "ORDER BY trade_date DESC"
                    ),
                    {
                        "cn": code_norm,
                        "boundary": boundary.isoformat(),
                    },
                ).fetchall()

                for row in hist_rows:
                    d = str(row[2]) if row[2] else ""
                    if d and d not in seen_dates:
                        seen_dates.add(d)
                        results.append(self._historical_stock_daily_dict(row))
        except Exception as exc:
            logger.warning("Historical DB stock_daily query failed for %s: %s", code_norm, exc)

        # Sort by date descending
        results.sort(key=lambda r: r.get('trade_date', ''), reverse=True)
        return results

    # ------------------------------------------------------------------
    # get_intraday_trend
    # ------------------------------------------------------------------

    def get_intraday_trend(
        self,
        code: str,
        query_date: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get intraday quote points for a stock.

        query_date: optional specific date (ISO format). If None, queries
                    recent data from main DB within retention_days.
        """
        from sqlalchemy import text as sa_text

        code_norm = normalize_stock_code_for_storage(code)
        if not code_norm:
            return []

        main_start, boundary = self._split_date_range(self._retention_days * 3)
        results: List[Dict[str, Any]] = []
        seen_keys: set = set()

        # --- Main DB (intraday_events raw SQL) ---
        try:
            with self._main_db.session_scope() as session:
                conn = session.connection()

                if query_date:
                    date_filter = "query_date = :qd"
                    params: dict = {"cn": code, "qd": query_date, "limit": limit}
                else:
                    date_filter = "query_date >= :start_date"
                    params = {"cn": code, "start_date": main_start.isoformat(), "limit": limit}

                main_rows = conn.execute(
                    sa_text(
                        f"SELECT query_date, market, timestamp, market_local_timestamp, "
                        f"stock_code, stock_name, current_price, volume_ratio, change_pct, "
                        f"event_type, raw_quote, snapshot_id "
                        f"FROM intraday_events "
                        f"WHERE (stock_code = :cn OR stock_code = :cn_norm) "
                        f"  AND {date_filter} "
                        f"ORDER BY timestamp DESC "
                        f"LIMIT :limit"
                    ),
                    {**params, "cn_norm": code_norm},
                ).fetchall()

                for row in main_rows:
                    d = str(row[0]) if row[0] else ""
                    ts = str(row[2]) if row[2] else ""
                    key = f"{d}_{ts}"
                    if key not in seen_keys:
                        seen_keys.add(key)
                        results.append(self._intraday_point_dict(row))
        except Exception as exc:
            logger.warning("Main DB intraday_events query failed for %s: %s", code_norm, exc)

        # --- Historical DB ---
        if not query_date or query_date < boundary.isoformat():
            try:
                with self._historical_db.get_session() as session:
                    from sqlalchemy import text as sa_text_hist

                    hist_params = {
                        "cn": code_norm,
                        "limit": limit * 2,
                    }
                    if query_date:
                        hist_date_filter = "query_date = :qd"
                        hist_params["qd"] = query_date
                    else:
                        hist_date_filter = "query_date < :boundary"
                        hist_params["boundary"] = boundary.isoformat()

                    hist_rows = session.execute(
                        sa_text_hist(
                            f"SELECT query_date, market, timestamp, market_local_timestamp, "
                            f"stock_code, stock_code_norm, stock_name, current_price, "
                            f"change_pct, volume_ratio, turnover, amount, current_volume, "
                            f"event_type, technical_position, price_position, snapshot_id "
                            f"FROM historical_intraday_quote_points "
                            f"WHERE stock_code_norm = :cn "
                            f"  AND {hist_date_filter} "
                            f"ORDER BY timestamp DESC "
                            f"LIMIT :limit"
                        ),
                        hist_params,
                    ).fetchall()

                    for row in hist_rows:
                        d = str(row[0]) if row[0] else ""
                        ts = str(row[2]) if row[2] else ""
                        key = f"{d}_{ts}"
                        if key not in seen_keys:
                            seen_keys.add(key)
                            results.append(self._historical_intraday_point_dict(row))
            except Exception as exc:
                logger.warning("Historical DB intraday query failed for %s: %s", code_norm, exc)

        return results

    # ------------------------------------------------------------------
    # get_postmarket_technical_summary
    # ------------------------------------------------------------------

    def get_postmarket_technical_summary(
        self,
        code: str,
        days_back: int = 30,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Get postmarket technical summaries for a stock.

        Returns rows from both main DB (AnalysisHistory hot fields) and
        historical DB (HistoricalPostmarketTechnicalSummary), deduplicated
        by (code_norm, trade_date, analysis_phase, report_type).
        """
        code_norm = normalize_stock_code_for_storage(code)
        if not code_norm:
            return []

        main_start, boundary = self._split_date_range(days_back)
        results: List[Dict[str, Any]] = []
        seen_keys: set = set()

        # --- Main DB ---
        try:
            from sqlalchemy import text as sa_text
            with self._main_db.session_scope() as session:
                main_rows = session.execute(
                    sa_text(
                        "SELECT id, code, code_norm, market, effective_trading_date, "
                        "report_type, analysis_phase, current_price, change_pct, "
                        "volume_ratio, turnover_rate, ideal_buy, secondary_buy, "
                        "stop_loss, take_profit, sentiment_score, operation_advice, "
                        "trend_prediction, analysis_summary, context_snapshot, "
                        "raw_result, created_at "
                        "FROM analysis_history "
                        "WHERE code_norm = :cn "
                        "  AND analysis_phase = 'postmarket' "
                        "  AND (report_type IS NULL OR report_type != 'market_review') "
                        "  AND created_at >= :start_date "
                        "ORDER BY created_at DESC "
                        "LIMIT :limit"
                    ),
                    {"cn": code_norm, "start_date": main_start.isoformat(), "limit": limit},
                ).fetchall()

                for row in main_rows:
                    trade_date = (str(row[4]) if row[4] else str(row[20])[:10]) if row[20] else ""
                    phase = str(row[6]) if row[6] else 'postmarket'
                    rtype = str(row[5]) if row[5] else ''
                    key = f"{code_norm}_{trade_date}_{phase}_{rtype}"
                    if key not in seen_keys:
                        seen_keys.add(key)
                        results.append(self._postmarket_main_dict(row, trade_date, phase, rtype))
        except Exception as exc:
            logger.warning("Main DB postmarket summary query failed for %s: %s", code_norm, exc)

        # --- Historical DB ---
        try:
            with self._historical_db.get_session() as session:
                from sqlalchemy import text as sa_text_hist

                hist_rows = session.execute(
                    sa_text_hist(
                        "SELECT source_history_id, code, code_norm, market, trade_date, "
                        "analysis_phase, report_type, close, pct_chg, volume_ratio, "
                        "ma5, ma10, ma20, ma60, bias5, bias10, bias20, "
                        "ideal_buy, secondary_buy, stop_loss, take_profit, "
                        "support_level, resistance_level, trend_prediction, "
                        "operation_advice, risk_level, technical_summary "
                        "FROM historical_postmarket_technical_summary "
                        "WHERE code_norm = :cn "
                        "  AND trade_date < :boundary "
                        "ORDER BY trade_date DESC "
                        "LIMIT :limit"
                    ),
                    {"cn": code_norm, "boundary": boundary.isoformat(), "limit": limit},
                ).fetchall()

                for row in hist_rows:
                    trade_date = str(row[4]) if row[4] else ""
                    phase = str(row[5]) if row[5] else 'postmarket'
                    rtype = str(row[6]) if row[6] else ''
                    key = f"{code_norm}_{trade_date}_{phase}_{rtype}"
                    if key not in seen_keys:
                        seen_keys.add(key)
                        results.append(self._postmarket_historical_dict(row, trade_date, phase, rtype))
        except Exception as exc:
            logger.warning("Historical DB postmarket summary query failed for %s: %s", code_norm, exc)

        results.sort(key=lambda r: r.get('trade_date', ''), reverse=True)
        return results

    # ------------------------------------------------------------------
    # get_market_light_daily
    # ------------------------------------------------------------------

    def get_market_light_daily(
        self,
        region: str = 'cn',
        days_back: int = 30,
        limit: int = 30,
    ) -> List[Dict[str, Any]]:
        """
        Get Market Light daily snapshots for a given region.

        Main DB queries via analysis_history (report_type='market_review')
        context_snapshot parsing, historical DB queries the
        historical_market_light_daily table.
        """
        region = str(region).lower()[:8]
        main_start, boundary = self._split_date_range(days_back)
        results: List[Dict[str, Any]] = []
        seen_dates: set = set()

        # --- Main DB (parse context_snapshot from market_review) ---
        try:
            from sqlalchemy import text as sa_text
            with self._main_db.session_scope() as session:
                main_rows = session.execute(
                    sa_text(
                        "SELECT id, effective_trading_date, context_snapshot, created_at "
                        "FROM analysis_history "
                        "WHERE report_type = 'market_review' "
                        "  AND created_at >= :start_date "
                        "ORDER BY created_at DESC "
                        "LIMIT :limit"
                    ),
                    {"start_date": main_start.isoformat(), "limit": limit},
                ).fetchall()

                for row in main_rows:
                    trade_date = str(row[1]) if row[1] else (str(row[3])[:10] if row[3] else "")
                    if trade_date in seen_dates:
                        continue

                    payload_str = row[2]
                    if not payload_str:
                        continue
                    try:
                        payload = json.loads(str(payload_str))
                    except (json.JSONDecodeError, TypeError):
                        continue

                    snapshots = payload.get('market_light_snapshots', {})
                    if not isinstance(snapshots, dict):
                        continue

                    snapshot = snapshots.get(region)
                    if not isinstance(snapshot, dict):
                        continue

                    seen_dates.add(trade_date)
                    results.append({
                        'region': region,
                        'trade_date': trade_date,
                        'score': snapshot.get('score'),
                        'status': str(snapshot.get('status', '')),
                        'label': str(snapshot.get('label', '')),
                        'temperature_label': str(snapshot.get('temperature_label', '')),
                        'data_quality': str(snapshot.get('data_quality', '')),
                        'source': 'main.analysis_history',
                    })
        except Exception as exc:
            logger.warning("Main DB market_light query failed for region %s: %s", region, exc)

        # --- Historical DB ---
        try:
            with self._historical_db.get_session() as session:
                from sqlalchemy import text as sa_text_hist

                hist_rows = session.execute(
                    sa_text_hist(
                        "SELECT region, trade_date, score, status, label, "
                        "temperature_label, data_quality, risk_notes "
                        "FROM historical_market_light_daily "
                        "WHERE region = :region "
                        "  AND trade_date < :boundary "
                        "ORDER BY trade_date DESC "
                        "LIMIT :limit"
                    ),
                    {"region": region, "boundary": boundary.isoformat(), "limit": limit},
                ).fetchall()

                for row in hist_rows:
                    trade_date = str(row[1]) if row[1] else ""
                    if trade_date not in seen_dates:
                        seen_dates.add(trade_date)
                        results.append({
                            'region': str(row[0]) if row[0] else region,
                            'trade_date': trade_date,
                            'score': row[2],
                            'status': str(row[3]) if row[3] else None,
                            'label': str(row[4]) if row[4] else None,
                            'temperature_label': str(row[5]) if row[5] else None,
                            'data_quality': str(row[6]) if row[6] else None,
                            'risk_notes': str(row[7]) if row[7] else None,
                            'source': 'historical.market_light_daily',
                        })
        except Exception as exc:
            logger.warning("Historical DB market_light query failed for region %s: %s", region, exc)

        results.sort(key=lambda r: r.get('trade_date', ''), reverse=True)
        return results

    # ------------------------------------------------------------------
    # Private row-to-dict converters
    # ------------------------------------------------------------------

    @staticmethod
    def _row_date(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, (date, datetime)):
            return v.isoformat()[:10]
        return str(v)[:10]

    def _stock_daily_dict(self, row) -> Dict[str, Any]:
        return {
            'code': str(row[0]) if row[0] else None,
            'code_norm': str(row[1]) if row[1] else None,
            'trade_date': self._row_date(row[2]),
            'open': row[3],
            'high': row[4],
            'low': row[5],
            'close': row[6],
            'pre_close': None,
            'pct_chg': row[7],
            'volume': row[8],
            'amount': row[9],
            'turnover_rate': None,
            'ma5': None,
            'ma10': None,
            'ma20': None,
            'ma60': None,
            'volume_ratio': row[10],
            'bias5': None,
            'bias10': None,
            'bias20': None,
            'data_source': str(row[11]) if len(row) > 11 and row[11] else 'main.stock_daily',
            'source_db': 'main',
        }

    def _historical_stock_daily_dict(self, row) -> Dict[str, Any]:
        return {
            'code': str(row[0]) if row[0] else None,
            'code_norm': str(row[1]) if row[1] else None,
            'trade_date': str(row[2]) if row[2] else '',
            'open': row[3],
            'high': row[4],
            'low': row[5],
            'close': row[6],
            'pre_close': row[7],
            'pct_chg': row[8],
            'volume': row[9],
            'amount': row[10],
            'turnover_rate': row[11],
            'ma5': row[12],
            'ma10': row[13],
            'ma20': row[14],
            'ma60': row[15],
            'volume_ratio': row[16],
            'bias5': row[17],
            'bias10': row[18],
            'bias20': row[19],
            'data_source': str(row[20]) if len(row) > 20 and row[20] else None,
            'source_db': 'historical',
        }

    @staticmethod
    def _intraday_point_dict(row) -> Dict[str, Any]:
        return {
            'query_date': str(row[0]) if row[0] else '',
            'market': str(row[1]) if len(row) > 1 and row[1] else None,
            'timestamp': str(row[2]) if row[2] else '',
            'market_local_timestamp': str(row[3]) if len(row) > 3 and row[3] else None,
            'stock_code': str(row[4]) if row[4] else '',
            'stock_name': str(row[5]) if len(row) > 5 and row[5] else None,
            'current_price': row[6] if len(row) > 6 else None,
            'volume_ratio': row[7] if len(row) > 7 else None,
            'change_pct': row[8] if len(row) > 8 else None,
            'event_type': str(row[9]) if len(row) > 9 and row[9] else None,
            'snapshot_id': str(row[11]) if len(row) > 11 and row[11] else None,
            'source_db': 'main',
        }

    @staticmethod
    def _historical_intraday_point_dict(row) -> Dict[str, Any]:
        return {
            'query_date': str(row[0]) if row[0] else '',
            'market': str(row[1]) if row[1] else '',
            'timestamp': str(row[2]) if row[2] else '',
            'market_local_timestamp': str(row[3]) if len(row) > 3 and row[3] else None,
            'stock_code': str(row[4]) if row[4] else '',
            'stock_name': str(row[6]) if len(row) > 6 and row[6] else None,
            'current_price': row[7] if len(row) > 7 else None,
            'change_pct': row[8] if len(row) > 8 else None,
            'volume_ratio': row[9] if len(row) > 9 else None,
            'turnover': row[10] if len(row) > 10 else None,
            'amount': row[11] if len(row) > 11 else None,
            'current_volume': row[12] if len(row) > 12 else None,
            'event_type': str(row[13]) if len(row) > 13 and row[13] else None,
            'technical_position': str(row[14]) if len(row) > 14 and row[14] else None,
            'price_position': str(row[15]) if len(row) > 15 and row[15] else None,
            'snapshot_id': str(row[16]) if len(row) > 16 and row[16] else None,
            'source_db': 'historical',
        }

    def _postmarket_main_dict(self, row, trade_date, phase, rtype) -> Dict[str, Any]:
        # SELECT order: id(0), code(1), code_norm(2), market(3), effective_trading_date(4),
        #   report_type(5), analysis_phase(6), current_price(7), change_pct(8),
        #   volume_ratio(9), turnover_rate(10), ideal_buy(11), secondary_buy(12),
        #   stop_loss(13), take_profit(14), sentiment_score(15), operation_advice(16),
        #   trend_prediction(17), analysis_summary(18), context_snapshot(19),
        #   raw_result(20), created_at(21)
        # Try to extract support/resistance from payload
        support_level = None
        resistance_level = None
        try:
            for payload_col in (row[19], row[20]):  # context_snapshot, raw_result
                from src.maintenance.archive_extractors import _safe_json_parse
                payload = _safe_json_parse(payload_col)
                if not payload:
                    continue
                support_level = (
                    payload.get('support_level') or
                    payload.get('support')
                )
                resistance_level = (
                    payload.get('resistance_level') or
                    payload.get('resistance')
                )
                if support_level or resistance_level:
                    break
        except Exception:
            pass

        return {
            'source_history_id': row[0],
            'code': str(row[1]) if row[1] else '',
            'code_norm': str(row[2]) if row[2] else '',
            'market': str(row[3]) if row[3] else None,
            'trade_date': trade_date,
            'analysis_phase': phase,
            'report_type': rtype,
            'close': row[7],
            'pct_chg': row[8],
            'volume_ratio': row[9],
            'ma5': None,
            'ma10': None,
            'ma20': None,
            'ma60': None,
            'bias5': None,
            'bias10': None,
            'bias20': None,
            'ideal_buy': row[11],
            'secondary_buy': row[12],
            'stop_loss': row[13],
            'take_profit': row[14],
            'support_level': support_level,
            'resistance_level': resistance_level,
            'trend_prediction': str(row[17]) if row[17] else None,
            'operation_advice': str(row[16]) if row[16] else None,
            'risk_level': row[15],  # sentiment_score
            'technical_summary': str(row[18]) if row[18] else None,
            'source_db': 'main',
        }

    def _postmarket_historical_dict(self, row, trade_date, phase, rtype) -> Dict[str, Any]:
        return {
            'source_history_id': row[0],
            'code': str(row[1]) if row[1] else '',
            'code_norm': str(row[2]) if row[2] else '',
            'market': str(row[3]) if row[3] else None,
            'trade_date': trade_date,
            'analysis_phase': phase,
            'report_type': rtype,
            'close': row[7],
            'pct_chg': row[8],
            'volume_ratio': row[9],
            'ma5': row[10],
            'ma10': row[11],
            'ma20': row[12],
            'ma60': row[13],
            'bias5': row[14],
            'bias10': row[15],
            'bias20': row[16],
            'ideal_buy': row[17],
            'secondary_buy': row[18],
            'stop_loss': row[19],
            'take_profit': row[20],
            'support_level': row[21],
            'resistance_level': row[22],
            'trend_prediction': str(row[23]) if row[23] else None,
            'operation_advice': str(row[24]) if row[24] else None,
            'risk_level': row[25],
            'technical_summary': str(row[26]) if len(row) > 26 and row[26] else None,
            'source_db': 'historical',
        }
