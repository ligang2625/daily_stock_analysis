# -*- coding: utf-8 -*-
"""
Phase 2 technical history extractors.

Each extractor reads from the main database and returns lists of dicts
suitable for upsert into historical_market.db.

The unified entry point `extract_phase2_technical_history` orchestrates
all extractors and (in non-dry-run mode) writes to the historical DB.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _compute_bias(close: Optional[float], ma: Optional[float]) -> Optional[float]:
    """Compute bias = (close - ma) / ma * 100. Returns None if inputs invalid."""
    if close is None or ma is None:
        return None
    if ma == 0:
        return None
    return round((close - ma) / ma * 100, 4)


def _coerce_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    return str(v)


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_json_parse(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        parsed = json.loads(str(raw))
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def _to_date_str(d: Any) -> Optional[str]:
    """Convert a date/datetime/string to ISO date string."""
    if d is None:
        return None
    if isinstance(d, (date, datetime)):
        return d.isoformat()[:10] if isinstance(d, datetime) else d.isoformat()
    return str(d)[:10]


# ---------------------------------------------------------------------------
# Extractor: stock_daily -> historical_stock_daily
# ---------------------------------------------------------------------------

def extract_stock_daily(
    session,
    cutoff_date: date,
) -> List[Dict[str, Any]]:
    """
    Read StockDaily rows with date <= cutoff_date, compute pre_close and
    bias, and return rows suitable for HistoricalStockDaily.

    pre_close is computed by looking up the previous trading day's close
    for the same code. In a single pass we sort by code+date and derive it.
    """
    from sqlalchemy import text as sa_text, select as sa_select
    from src.services.stock_code_utils import (
        normalize_stock_code_for_storage,
        normalize_market_region,
    )

    rows_raw = session.execute(
        sa_text(
            "SELECT id, code, date, open, high, low, close, pct_chg, volume, "
            "amount, volume_ratio, data_source, ma5, ma10, ma20 "
            "FROM stock_daily "
            "WHERE date < :cutoff "
            "ORDER BY code, date"
        ),
        {"cutoff": cutoff_date.isoformat()},
    ).fetchall()

    if not rows_raw:
        return []

    # Group by code to compute pre_close from previous row
    results: List[Dict[str, Any]] = []
    last_close_by_code: Dict[str, float] = {}

    for row in rows_raw:
        # Column order: id(0), code(1), date(2), open(3), high(4), low(5),
        #               close(6), pct_chg(7), volume(8), amount(9),
        #               volume_ratio(10), data_source(11), ma5(12), ma10(13), ma20(14)
        raw_code = str(row[1]) if row[1] else ""
        trade_date = _to_date_str(row[2])
        if not trade_date:
            continue

        code_norm = normalize_stock_code_for_storage(raw_code) or raw_code
        market = normalize_market_region(None, code=raw_code)

        close_val = _safe_float(row[6])
        pre_close = None
        if raw_code in last_close_by_code:
            pre_close = last_close_by_code[raw_code]
        if close_val is not None:
            last_close_by_code[raw_code] = close_val

        ma5 = _safe_float(row[12])
        ma10 = _safe_float(row[13])
        ma20 = _safe_float(row[14])

        results.append({
            'code': raw_code,
            'code_norm': code_norm,
            'market': market,
            'trade_date': trade_date,
            'open': _safe_float(row[3]),
            'high': _safe_float(row[4]),
            'low': _safe_float(row[5]),
            'close': close_val,
            'pre_close': pre_close,
            'pct_chg': _safe_float(row[7]),
            'volume': _safe_float(row[8]),
            'amount': _safe_float(row[9]),
            'turnover_rate': None,  # Not in stock_daily table
            'ma5': ma5,
            'ma10': ma10,
            'ma20': ma20,
            'ma60': None,  # Not in stock_daily table
            'volume_ratio': _safe_float(row[10]),
            'bias5': _compute_bias(close_val, ma5),
            'bias10': _compute_bias(close_val, ma10),
            'bias20': _compute_bias(close_val, ma20),
            'data_source': _coerce_str(row[11]) or 'main.stock_daily',
        })

    return results


# ---------------------------------------------------------------------------
# Extractor: intraday_events -> historical_intraday_quote_points
# ---------------------------------------------------------------------------

def extract_intraday_quote_points(
    connection,
    cutoff_date: date,
) -> List[Dict[str, Any]]:
    """
    Read intraday_events with query_date < cutoff_date, parse raw_quote
    JSON for turnover / amount / current_volume / technical_position /
    price_position, and return rows for HistoricalIntradayQuotePoint.
    """
    from sqlalchemy import text as sa_text
    from src.services.stock_code_utils import normalize_stock_code_for_storage

    # Check which columns exist in intraday_events
    try:
        info = connection.execute(sa_text("PRAGMA table_info('intraday_events')")).fetchall()
        existing_cols = {row[1] for row in info}
    except Exception:
        logger.warning("Cannot introspect intraday_events table")
        return []

    # Build SELECT based on available columns
    base_cols = ['id', 'query_date', 'market', 'timestamp', 'stock_code', 'stock_name',
                 'current_price', 'volume_ratio', 'change_pct', 'event_type', 'raw_quote']
    extra_cols = []
    for col in ('market_local_timestamp', 'snapshot_id'):
        if col in existing_cols:
            extra_cols.append(col)
        else:
            extra_cols.append('NULL')

    cols_str = ', '.join(base_cols + extra_cols)

    try:
        rows_raw = connection.execute(
            sa_text(
                f"SELECT {cols_str} FROM intraday_events "
                "WHERE query_date < :cutoff "
                "ORDER BY query_date, timestamp"
            ),
            {"cutoff": cutoff_date.isoformat()},
        ).fetchall()
    except Exception as exc:
        logger.warning("Failed to query intraday_events: %s", exc)
        return []

    results: List[Dict[str, Any]] = []
    col_count = len(base_cols) + len(extra_cols)

    for row in rows_raw:
        try:
            raw_code = str(row[4]) if row[4] else ""
            code_norm = normalize_stock_code_for_storage(raw_code) or raw_code

            # Parse raw_quote JSON
            raw_quote_json = _safe_json_parse(row[10])
            turnover = None
            amount_val = None
            current_volume = None
            technical_position = None
            price_position = None
            if raw_quote_json:
                turnover = _safe_float(raw_quote_json.get('turnover_rate'))
                amount_val = _safe_float(raw_quote_json.get('amount'))
                current_volume = _safe_float(raw_quote_json.get('volume'))
                technical_position = _coerce_str(raw_quote_json.get('technical_position'))
                price_position = _coerce_str(raw_quote_json.get('price_position'))

            row_id = row[0]
            snapshot_id = None
            if len(row) > 12:
                snapshot_id = row[12]
            # Normalize NULL snapshot_id to non-null for true idempotent upsert
            # (SQLite allows duplicate NULLs in unique constraints)
            if not snapshot_id:
                snapshot_id = f"legacy:{row_id}"

            results.append({
                'query_date': str(row[1]) if row[1] else '',
                'market': str(row[2]) if row[2] else None,
                'stock_code': raw_code,
                'stock_code_norm': code_norm,
                'stock_name': str(row[5]) if row[5] else None,
                'timestamp': str(row[3]) if row[3] else '',
                'market_local_timestamp': str(row[11]) if len(row) > 11 and row[11] else None,
                'snapshot_id': str(snapshot_id) if snapshot_id else None,
                'current_price': _safe_float(row[6]),
                'change_pct': _safe_float(row[8]),
                'volume_ratio': _safe_float(row[7]),
                'turnover': turnover,
                'amount': amount_val,
                'current_volume': current_volume,
                'event_type': str(row[9]) if row[9] else None,
                'technical_position': technical_position,
                'price_position': price_position,
            })
        except Exception as exc:
            logger.warning("Failed to parse intraday_events row id=%s: %s", row[0], exc)

    return results


# ---------------------------------------------------------------------------
# Extractor: analysis_history -> historical_postmarket_technical_summary
# ---------------------------------------------------------------------------

def extract_postmarket_technical_summaries(
    session,
    cutoff_date: date,
    payload_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    Read analysis_history rows (analysis_phase='postmarket', not market_review)
    with created_at <= cutoff_date. Uses hot fields first, falls back to payload
    for ma/technical_position/support/resistance if available.

    Excludes market_review type records (they are handled separately by
    market_light extractor).
    """
    from sqlalchemy import text as sa_text
    from src.services.stock_code_utils import (
        normalize_stock_code_for_storage,
        normalize_market_region,
    )

    # Try payload-aware query first; fall back to legacy query if payload table doesn't exist
    rows_raw = None
    try:
        rows_raw = session.execute(
            sa_text(
                "SELECT ah.id, ah.code, ah.code_norm, ah.market, ah.name, "
                "ah.report_type, ah.analysis_phase, "
                "ah.sentiment_score, ah.operation_advice, ah.trend_prediction, ah.analysis_summary, "
                "COALESCE(ahp.raw_result, ah.raw_result) AS raw_result, "
                "COALESCE(ahp.context_snapshot, ah.context_snapshot) AS context_snapshot, "
                "COALESCE(ahp.news_content, ah.news_content) AS news_content, "
                "ah.ideal_buy, ah.secondary_buy, ah.stop_loss, ah.take_profit, "
                "ah.current_price, ah.change_pct, ah.volume_ratio, ah.turnover_rate, "
                "ah.created_at, ah.effective_trading_date "
                "FROM analysis_history ah "
                "LEFT JOIN analysis_history_payload ahp ON ah.id = ahp.history_id "
                "WHERE ah.analysis_phase = 'postmarket' "
                "  AND (ah.report_type IS NULL OR ah.report_type != 'market_review') "
                "  AND ah.created_at < :cutoff "
                "ORDER BY ah.created_at"
            ),
            {"cutoff": cutoff_date.isoformat()},
        ).fetchall()
    except Exception as exc:
        logger.warning("Payload-aware postmarket query failed (may be old DB without payload table): %s", exc)
        rows_raw = session.execute(
            sa_text(
                "SELECT id, code, code_norm, market, name, report_type, analysis_phase, "
                "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                "raw_result, context_snapshot, news_content, "
                "ideal_buy, secondary_buy, stop_loss, take_profit, "
                "current_price, change_pct, volume_ratio, turnover_rate, "
                "created_at, effective_trading_date "
                "FROM analysis_history "
                "WHERE analysis_phase = 'postmarket' "
                "  AND (report_type IS NULL OR report_type != 'market_review') "
                "  AND created_at < :cutoff "
                "ORDER BY created_at"
            ),
            {"cutoff": cutoff_date.isoformat()},
        ).fetchall()

    results: List[Dict[str, Any]] = []
    for row in rows_raw:
        try:
            row_id = row[0]
            raw_code = str(row[1]) if row[1] else ""
            code_norm = str(row[2]) if row[2] else normalize_stock_code_for_storage(raw_code) or raw_code
            market = str(row[3]) if row[3] else None

            # Effective trading date first, fall back to created_at
            trade_date = _to_date_str(row[23]) or _to_date_str(row[22])

            close = _safe_float(row[18])
            pct_chg = _safe_float(row[19])
            volume_ratio = _safe_float(row[20])
            turnover_rate = _safe_float(row[21])

            ideal_buy = _safe_float(row[14])
            secondary_buy = _safe_float(row[15])
            stop_loss = _safe_float(row[16])
            take_profit = _safe_float(row[17])

            trend_prediction = str(row[9]) if row[9] else None
            operation_advice = str(row[8]) if row[8] else None
            risk_level = row[7]  # sentiment_score as risk_level proxy
            if risk_level is not None:
                try:
                    risk_level = int(risk_level)
                except (TypeError, ValueError):
                    risk_level = None

            technical_summary = str(row[10]) if row[10] else None

            # Payload fallback for ma/technical positioning
            ma5 = ma10 = ma20 = ma60 = None
            bias5 = bias10 = bias20 = None
            support_level = None
            resistance_level = None

            if payload_fallback:
                # Try context_snapshot first (structured), then raw_result
                # Accumulate from both columns instead of early-breaking,
                # so partial data in one column doesn't block the other.
                for raw_payload in (row[12], row[11]):  # context_snapshot, raw_result
                    payload = _safe_json_parse(raw_payload)
                    if not payload:
                        continue

                    # Try to find nested technical data
                    enhanced = payload.get('enhanced_context')
                    tech_data = None
                    if isinstance(enhanced, dict):
                        tech_data = enhanced.get('technical')
                    if isinstance(tech_data, dict):
                        ma5 = ma5 or _safe_float(tech_data.get('ma5'))
                        ma10 = ma10 or _safe_float(tech_data.get('ma10'))
                        ma20 = ma20 or _safe_float(tech_data.get('ma20'))
                        ma60 = ma60 or _safe_float(tech_data.get('ma60'))
                        support_level = support_level or _safe_float(tech_data.get('support'))
                        resistance_level = resistance_level or _safe_float(tech_data.get('resistance'))

                    # Also check top-level payload keys
                    ma5 = ma5 or _safe_float(payload.get('ma5'))
                    ma10 = ma10 or _safe_float(payload.get('ma10'))
                    ma20 = ma20 or _safe_float(payload.get('ma20'))
                    ma60 = ma60 or _safe_float(payload.get('ma60'))
                    support_level = support_level or _safe_float(payload.get('support_level')) or _safe_float(payload.get('support'))
                    resistance_level = resistance_level or _safe_float(payload.get('resistance_level')) or _safe_float(payload.get('resistance'))

            bias5 = _compute_bias(close, ma5)
            bias10 = _compute_bias(close, ma10)
            bias20 = _compute_bias(close, ma20)

            results.append({
                'source_history_id': row_id,
                'code': raw_code,
                'code_norm': code_norm,
                'market': market,
                'trade_date': trade_date,
                'analysis_phase': str(row[6]) if row[6] else 'postmarket',
                'report_type': str(row[5]) if row[5] else None,
                'close': close,
                'pct_chg': pct_chg,
                'volume_ratio': volume_ratio,
                'ma5': ma5,
                'ma10': ma10,
                'ma20': ma20,
                'ma60': ma60,
                'bias5': bias5,
                'bias10': bias10,
                'bias20': bias20,
                'ideal_buy': ideal_buy,
                'secondary_buy': secondary_buy,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'support_level': support_level,
                'resistance_level': resistance_level,
                'trend_prediction': trend_prediction,
                'operation_advice': operation_advice,
                'risk_level': risk_level,
                'technical_summary': technical_summary,
            })
        except Exception as exc:
            logger.warning("Failed to extract postmarket summary for analysis_history id=%s: %s",
                           row[0] if row else '?', exc)

    return results


# ---------------------------------------------------------------------------
# Extractor: market_light_snapshots -> historical_market_light_daily
# ---------------------------------------------------------------------------

def extract_market_light_daily(
    session,
    cutoff_date: date,
) -> List[Dict[str, Any]]:
    """
    Read analysis_history rows with report_type='market_review', parse
    context_snapshot.market_light_snapshots, and return one row per region
    for HistoricalMarketLightDaily.
    """
    from sqlalchemy import text as sa_text

    # Try payload-aware query first; fall back to legacy query if payload table doesn't exist
    rows_raw = None
    try:
        rows_raw = session.execute(
            sa_text(
                "SELECT ah.id, ah.code, ah.code_norm, ah.market, ah.effective_trading_date, "
                "COALESCE(ahp.context_snapshot, ah.context_snapshot) AS context_snapshot, "
                "ah.created_at "
                "FROM analysis_history ah "
                "LEFT JOIN analysis_history_payload ahp ON ah.id = ahp.history_id "
                "WHERE ah.report_type = 'market_review' "
                "  AND ah.created_at < :cutoff "
                "ORDER BY ah.created_at"
            ),
            {"cutoff": cutoff_date.isoformat()},
        ).fetchall()
    except Exception as exc:
        logger.warning("Payload-aware market_review query failed (may be old DB without payload table): %s", exc)
        rows_raw = session.execute(
            sa_text(
                "SELECT id, code, code_norm, market, effective_trading_date, "
                "context_snapshot, created_at "
                "FROM analysis_history "
                "WHERE report_type = 'market_review' "
                "  AND created_at < :cutoff "
                "ORDER BY created_at"
            ),
            {"cutoff": cutoff_date.isoformat()},
        ).fetchall()

    results: List[Dict[str, Any]] = []
    for row in rows_raw:
        try:
            trade_date = _to_date_str(row[4]) or _to_date_str(row[6])
            payload = _safe_json_parse(row[5])
            if not payload:
                continue

            snapshots = payload.get('market_light_snapshots')
            if not isinstance(snapshots, dict):
                continue

            for region_key, snapshot in snapshots.items():
                if not isinstance(snapshot, dict):
                    continue
                region = str(region_key).lower()[:8]
                if not region:
                    region = 'cn'

                # Build risk_notes from reasons/guidance
                risk_notes = None
                try:
                    reasons = snapshot.get('reasons', [])
                    guidance = snapshot.get('guidance', '')
                    if reasons or guidance:
                        risk_obj = {
                            'reasons': reasons if isinstance(reasons, list) else [],
                            'guidance': str(guidance) if guidance else '',
                        }
                        risk_notes = json.dumps(risk_obj, ensure_ascii=False)
                except Exception:
                    pass

                results.append({
                    'region': region,
                    'trade_date': trade_date,
                    'score': snapshot.get('score'),
                    'status': str(snapshot.get('status') or ''),
                    'label': str(snapshot.get('label') or ''),
                    'temperature_label': str(snapshot.get('temperature_label') or ''),
                    'data_quality': str(snapshot.get('data_quality') or ''),
                    'risk_notes': risk_notes,
                })
        except Exception as exc:
            logger.warning("Failed to extract market_light for analysis_history id=%s: %s",
                           row[0] if row else '?', exc)

    return results


# ---------------------------------------------------------------------------
# Extractor: intraday_market_snapshots -> historical_market_index_points
# ---------------------------------------------------------------------------

def extract_market_index_points(
    connection,
    cutoff_date: date,
) -> List[Dict[str, Any]]:
    """
    Read intraday_market_snapshots with query_date < cutoff_date, map fields
    to HistoricalMarketIndexPoint schema, and compute derived labels.

    Returns a list of dicts suitable for
    HistoricalDatabaseManager.upsert_market_index_points_batch().
    """
    from sqlalchemy import text as sa_text

    try:
        rows_raw = connection.execute(
            sa_text(
                "SELECT query_date, market, index_code, index_name, "
                "timestamp, market_local_timestamp, snapshot_id, "
                "current_price, change_pct, open_price, high_price, low_price, "
                "prev_close, volume, amount, status "
                "FROM intraday_market_snapshots "
                "WHERE query_date < :cutoff "
                "ORDER BY market, index_code, market_local_timestamp"
            ),
            {"cutoff": cutoff_date.isoformat()},
        ).fetchall()
    except Exception as exc:
        logger.warning("Failed to query intraday_market_snapshots: %s", exc)
        return []

    results: List[Dict[str, Any]] = []
    for row in rows_raw:
        try:
            change_pct = _safe_float(row[8])

            # Compute derived labels
            if change_pct is not None:
                if change_pct > 0.5:
                    trend_label = 'rising'
                elif change_pct < -0.5:
                    trend_label = 'falling'
                else:
                    trend_label = 'flat'
                strength_label = 'strong' if abs(change_pct) > 2 else 'neutral'
            else:
                trend_label = 'insufficient'
                strength_label = 'insufficient'

            snapshot_id = str(row[6]) if row[6] else ''
            # Normalize NULL snapshot_id for idempotent upsert
            if not snapshot_id:
                snapshot_id = ''

            results.append({
                'query_date': str(row[0]) if row[0] else '',
                'market': str(row[1]) if row[1] else '',
                'index_code': str(row[2]) if row[2] else '',
                'index_name': str(row[3]) if row[3] else None,
                'timestamp': str(row[4]) if row[4] else None,
                'market_local_timestamp': str(row[5]) if row[5] else '',
                'snapshot_id': snapshot_id,
                'current_price': _safe_float(row[7]),
                'change_pct': change_pct,
                'open': _safe_float(row[9]),
                'high': _safe_float(row[10]),
                'low': _safe_float(row[11]),
                'pre_close': _safe_float(row[12]),
                'volume': _safe_float(row[13]),
                'amount': _safe_float(row[14]),
                'trend_label': trend_label,
                'strength_label': strength_label,
                'breadth_label': None,
                'status': str(row[15]) if row[15] else 'valid',
            })
        except Exception as exc:
            logger.warning("Failed to parse market index row: %s", exc)

    return results


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

def extract_phase2_technical_history(
    main_db,             # DatabaseManager instance
    historical_db,       # HistoricalDatabaseManager instance
    cutoff_date: date,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run all Phase 2 extractors and (if not dry_run) write to historical DB.

    Returns a dict with per-extractor stats: {read_count, written_count}
    """
    from src.storage import DatabaseManager

    stats: Dict[str, Any] = {
        'stock_daily': {'read': 0, 'written': 0},
        'intraday_quote_points': {'read': 0, 'written': 0},
        'postmarket_technical_summary': {'read': 0, 'written': 0},
        'market_light_daily': {'read': 0, 'written': 0},
        'market_index_points': {'read': 0, 'written': 0},
        'dry_run': dry_run,
    }

    cutoff_str = cutoff_date.isoformat()
    logger.info("Phase 2 extraction starting: cutoff=%s dry_run=%s", cutoff_str, dry_run)

    # 1. Extract stock_daily
    try:
        with main_db.session_scope() as session:
            rows = extract_stock_daily(session, cutoff_date)
        stats['stock_daily']['read'] = len(rows)
        if rows and not dry_run:
            stats['stock_daily']['written'] = historical_db.upsert_stock_daily_batch(rows)
        logger.info("extract_stock_daily: read=%s written=%s", len(rows),
                    stats['stock_daily']['written'])
    except Exception as exc:
        logger.error("extract_stock_daily failed: %s", exc)
        raise

    # 2. Extract intraday_quote_points
    try:
        with main_db.session_scope() as session:
            conn = session.connection()
            rows = extract_intraday_quote_points(conn, cutoff_date)
        stats['intraday_quote_points']['read'] = len(rows)
        if rows and not dry_run:
            stats['intraday_quote_points']['written'] = historical_db.upsert_intraday_quote_points_batch(rows)
        logger.info("extract_intraday_quote_points: read=%s written=%s", len(rows),
                    stats['intraday_quote_points']['written'])
    except Exception as exc:
        logger.error("extract_intraday_quote_points failed: %s", exc)
        raise

    # 3. Extract postmarket_technical_summaries
    try:
        with main_db.session_scope() as session:
            rows = extract_postmarket_technical_summaries(session, cutoff_date)
        stats['postmarket_technical_summary']['read'] = len(rows)
        if rows and not dry_run:
            stats['postmarket_technical_summary']['written'] = historical_db.upsert_postmarket_summaries_batch(rows)
        logger.info("extract_postmarket_technical_summaries: read=%s written=%s", len(rows),
                    stats['postmarket_technical_summary']['written'])
    except Exception as exc:
        logger.error("extract_postmarket_technical_summaries failed: %s", exc)
        raise

    # 4. Extract market_light_daily
    try:
        with main_db.session_scope() as session:
            rows = extract_market_light_daily(session, cutoff_date)
        stats['market_light_daily']['read'] = len(rows)
        if rows and not dry_run:
            stats['market_light_daily']['written'] = historical_db.upsert_market_light_daily_batch(rows)
        logger.info("extract_market_light_daily: read=%s written=%s", len(rows),
                    stats['market_light_daily']['written'])
    except Exception as exc:
        logger.error("extract_market_light_daily failed: %s", exc)
        raise

    # 5. Extract market_index_points (Phase 3)
    try:
        with main_db.session_scope() as session:
            conn = session.connection()
            rows = extract_market_index_points(conn, cutoff_date)
        stats['market_index_points']['read'] = len(rows)
        if rows and not dry_run:
            stats['market_index_points']['written'] = historical_db.upsert_market_index_points_batch(rows)
        logger.info("extract_market_index_points: read=%s written=%s", len(rows),
                    stats['market_index_points']['written'])
    except Exception as exc:
        logger.error("extract_market_index_points failed: %s", exc)
        raise

    total_read = sum(s['read'] for s in stats.values() if isinstance(s, dict) and 'read' in s)
    total_written = sum(s['written'] for s in stats.values() if isinstance(s, dict) and 'written' in s)
    logger.info("Phase 2 extraction complete: total_read=%s total_written=%s dry_run=%s",
                total_read, total_written, dry_run)

    return stats
