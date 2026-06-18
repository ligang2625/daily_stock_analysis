# -*- coding: utf-8 -*-
"""Tests for Phase 2 archive extractors: seed main tables and verify extraction output."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

import pytest


class TestTechnicalArchiveExtractors:
    """Test each extractor independently with seeded main DB data."""

    @pytest.fixture
    def db_env(self, tmp_path, monkeypatch):
        """Setup main + historical databases with temp SQLite files."""
        main_db_path = str(tmp_path / "test_extractors_main.db")
        historical_db_path = str(tmp_path / "test_extractors_hist.db")
        monkeypatch.setenv('DATABASE_PATH', main_db_path)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db_path)

        from src.config import Config
        Config.reset_instance()

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{main_db_path}")

        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=historical_db_path)

        yield {
            'db': db,
            'hist_db': hist_db,
            'main_path': main_db_path,
            'hist_path': historical_db_path,
            'tmp_path': tmp_path,
        }

        DatabaseManager.reset_instance()

    # ------------------------------------------------------------------
    # Stock Daily extractor
    # ------------------------------------------------------------------

    def test_extract_stock_daily_basic(self, db_env):
        """Basic stock_daily extraction with pre_close and bias computation."""
        db = db_env['db']
        hist_db = db_env['hist_db']

        from sqlalchemy import text as sa_text

        # Seed stock_daily data
        with db.session_scope() as session:
            for i, (code, trade_date) in enumerate([
                    ('600519', '2026-05-01'),
                    ('600519', '2026-05-02'),
                    ('600519', '2026-05-03'),
                    ('HK00700', '2026-06-01'),
            ]):
                session.execute(
                    sa_text(
                        "INSERT INTO stock_daily (code, code_norm, date, open, high, low, close, "
                        "pct_chg, volume, amount, volume_ratio, data_source, ma5, ma10, ma20) "
                        "VALUES (:code, :cn, :date, :open, :high, :low, :close, :pct, :vol, :amt, "
                        ":vr, :ds, :ma5, :ma10, :ma20)"
                    ),
                    {
                        "code": code, "cn": code,
                        "date": trade_date,
                        "open": 100.0 + i, "high": 102.0 + i, "low": 99.0 + i,
                        "close": 101.0 + i, "pct": 1.0, "vol": 1000000,
                        "amt": 100000000.0, "vr": 1.2,
                        "ds": "akshare",
                        "ma5": 100.0 + i * 0.5,
                        "ma10": 99.0 + i * 0.3,
                        "ma20": 98.0 + i * 0.2,
                    },
                )

        from src.maintenance.archive_extractors import extract_stock_daily
        cutoff = date.today()

        with db.session_scope() as session:
            rows = extract_stock_daily(session, cutoff)

        assert len(rows) == 4

        # First row for 600519 should have pre_close=None
        code_519_rows = [r for r in rows if r['code_norm'] == '600519']
        assert len(code_519_rows) == 3
        assert code_519_rows[0]['pre_close'] is None  # first day
        assert code_519_rows[1]['pre_close'] == 101.0  # previous close
        assert code_519_rows[2]['pre_close'] == 102.0

        # bias should be computed
        for r in code_519_rows:
            close = r['close']
            ma5 = r['ma5']
            if close and ma5:
                assert r['bias5'] is not None
                assert r['bias5'] == pytest.approx((close - ma5) / ma5 * 100, rel=0.01)

    def test_extract_stock_daily_dry_run(self, db_env):
        """extract_stock_daily function itself does NOT write anything."""
        db = db_env['db']

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES ('600519', '600519', '2026-01-01', 100.0, 'test')"
                ),
            )

        from src.maintenance.archive_extractors import extract_stock_daily
        with db.session_scope() as session:
            rows = extract_stock_daily(session, date.today())

        assert len(rows) == 1
        # The extractor only reads, never writes
        assert rows[0]['close'] == 100.0

    # ------------------------------------------------------------------
    # Intraday quote points extractor
    # ------------------------------------------------------------------

    def test_extract_intraday_quote_points(self, db_env):
        """Extract intraday events with raw_quote JSON parsing."""
        db = db_env['db']
        hist_db = db_env['hist_db']

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            conn = session.connection()
            # Create intraday_events table
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
                "raw_quote TEXT, "
                "snapshot_id TEXT, "
                "run_id TEXT, "
                "snapshot_status TEXT"
                ")"
            ))

            # Insert test data
            raw_quote = json.dumps({
                "turnover_rate": 3.5,
                "amount": 50000000.0,
                "volume": 2000000.0,
                "technical_position": "above_ma5",
                "price_position": "near_resistance",
            })
            conn.execute(sa_text(
                "INSERT INTO intraday_events "
                "(query_date, market, timestamp, market_local_timestamp, stock_code, stock_name, "
                "current_price, volume_ratio, change_pct, event_type, description, raw_quote, "
                "snapshot_id, run_id, snapshot_status) "
                "VALUES (:qd, :mkt, :ts, :mlt, :sc, :sn, :cp, :vr, :cpct, :et, :desc, :rq, "
                ":sid, :rid, :ss)"
            ), {
                "qd": "2026-06-10", "mkt": "cn", "ts": "2026-06-10T10:30:00",
                "mlt": "2026-06-10T10:30:00", "sc": "600519", "sn": u"贵州茅台",
                "cp": 1750.0, "vr": 1.1, "cpct": 2.5,
                "et": "enter_ideal_buy", "desc": "Hit ideal buy zone",
                "rq": raw_quote, "sid": "snap_001", "rid": "run_001", "ss": "valid",
            })

        from src.maintenance.archive_extractors import extract_intraday_quote_points
        with db.session_scope() as session:
            conn = session.connection()
            rows = extract_intraday_quote_points(conn, date.today())

        assert len(rows) == 1
        r = rows[0]
        assert r['stock_code'] == '600519'
        assert r['stock_code_norm'] == '600519'
        assert r['current_price'] == 1750.0
        assert r['turnover'] == 3.5  # from raw_quote
        assert r['amount'] == 50000000.0
        assert r['current_volume'] == 2000000.0
        assert r['event_type'] == 'enter_ideal_buy'
        assert r['technical_position'] == 'above_ma5'
        assert r['price_position'] == 'near_resistance'

    def test_extract_intraday_malformed_json(self, db_env):
        """Malformed raw_quote JSON should log warning but not fail the batch."""
        db = db_env['db']

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            conn = session.connection()
            conn.execute(sa_text(
                "CREATE TABLE IF NOT EXISTS intraday_events ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "query_date TEXT NOT NULL, market TEXT, timestamp TEXT NOT NULL, "
                "stock_code TEXT NOT NULL, stock_name TEXT, current_price REAL, "
                "volume_ratio REAL, change_pct REAL, event_type TEXT NOT NULL, "
                "description TEXT, raw_quote TEXT, snapshot_id TEXT"
                ")"
            ))
            conn.execute(sa_text(
                "INSERT INTO intraday_events (query_date, market, timestamp, stock_code, "
                "stock_name, current_price, event_type, raw_quote) "
                "VALUES ('2026-01-01', 'cn', '2026-01-01T10:00:00', '600519', 'test', "
                "100.0, 'price_only', 'not valid json{{{')"
            ))
            conn.execute(sa_text(
                "INSERT INTO intraday_events (query_date, market, timestamp, stock_code, "
                "stock_name, current_price, event_type, raw_quote) "
                "VALUES ('2026-01-01', 'cn', '2026-01-01T10:30:00', '000001', 'test2', "
                "50.0, 'price_only', '{\"amount\": 1000}')"
            ))

        from src.maintenance.archive_extractors import extract_intraday_quote_points
        with db.session_scope() as session:
            conn = session.connection()
            rows = extract_intraday_quote_points(conn, date.today())

        # Both rows should be returned, malformed JSON just has null extractions
        assert len(rows) == 2

    # ------------------------------------------------------------------
    # Postmarket technical summary extractor
    # ------------------------------------------------------------------

    def test_extract_postmarket_summaries(self, db_env):
        """Extract postmarket summaries from analysis_history hot fields."""
        db = db_env['db']

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            # Create a postmarket analysis_history row
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, market, name, report_type, analysis_phase, "
                    "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    "ideal_buy, secondary_buy, stop_loss, take_profit, "
                    "current_price, change_pct, volume_ratio, "
                    "created_at, effective_trading_date) "
                    "VALUES (:qid, :code, :cn, :mkt, :name, :rtype, :phase, :score, :advice, "
                    ":trend, :summary, :ib, :sb, :sl, :tp, :cp, :chpct, :vr, :ca, :etd)"
                ),
                {
                    "qid": "test_q1", "code": "600519", "cn": "600519", "mkt": "cn",
                    "name": u"贵州茅台", "rtype": "daily", "phase": "postmarket",
                    "score": 75, "advice": "hold", "trend": "bullish",
                    "summary": "Strong buy signal", "ib": 95.0, "sb": 92.0,
                    "sl": 90.0, "tp": 110.0, "cp": 101.0, "chpct": 2.5, "vr": 1.2,
                    "ca": datetime(2026, 5, 1), "etd": date(2026, 5, 1),
                },
            )

            # Create a non-postmarket row (should be excluded)
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, name, report_type, analysis_phase, "
                    "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    "created_at) "
                    "VALUES (:qid, :code, :cn, :name, :rtype, :phase, :score, :advice, :trend, "
                    ":summary, :ca)"
                ),
                {
                    "qid": "test_q2", "code": "000001", "cn": "000001", "name": "test",
                    "rtype": None, "phase": "premarket", "score": 50, "advice": "hold",
                    "trend": "neutral", "summary": "nothing", "ca": datetime(2026, 5, 1),
                },
            )

            # Create a market_review row (should be excluded)
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, name, report_type, analysis_phase, "
                    "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    "created_at) "
                    "VALUES (:qid, :code, :cn, :name, :rtype, :phase, :score, :advice, :trend, "
                    ":summary, :ca)"
                ),
                {
                    "qid": "test_q3", "code": "MR", "cn": "MR", "name": "Market Review",
                    "rtype": "market_review", "phase": "postmarket", "score": 50,
                    "advice": "review", "trend": "review", "summary": "overview",
                    "ca": datetime(2026, 5, 1),
                },
            )

        from src.maintenance.archive_extractors import extract_postmarket_technical_summaries
        with db.session_scope() as session:
            rows = extract_postmarket_technical_summaries(session, date.today())

        # Only the postmarket non-market_review row should be extracted
        assert len(rows) == 1
        r = rows[0]
        assert r['code_norm'] == '600519'
        assert r['close'] == 101.0
        assert r['ideal_buy'] == 95.0
        assert r['operation_advice'] == 'hold'
        assert r['risk_level'] == 75

    # ------------------------------------------------------------------
    # Market light daily extractor
    # ------------------------------------------------------------------

    def test_extract_market_light_daily(self, db_env):
        """Extract market_light_snapshots from market_review context_snapshot."""
        db = db_env['db']

        import json as _json
        from sqlalchemy import text as sa_text

        snapshots = {
            "cn": {
                "score": 80, "status": "green", "label": "Normal",
                "temperature_label": u"温和", "data_quality": "ok",
                "reasons": ["breadth good", "index stable"],
                "guidance": "Continue to hold",
            },
            "hk": {
                "score": 65, "status": "yellow", "label": "Caution",
                "temperature_label": u"偏热", "data_quality": "partial",
                "reasons": ["high turnover"],
                "guidance": "Monitor closely",
            },
        }
        context = _json.dumps({"market_light_snapshots": snapshots})

        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, name, report_type, analysis_phase, "
                    "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    "context_snapshot, created_at, effective_trading_date) "
                    "VALUES (:qid, :code, :cn, :name, :rtype, :phase, :score, :advice, "
                    ":trend, :summary, :ctx, :ca, :etd)"
                ),
                {
                    "qid": "mr_1", "code": "MR", "cn": "MR", "name": "Market Review",
                    "rtype": "market_review", "phase": "postmarket", "score": 50,
                    "advice": "review", "trend": "review", "summary": "Overview",
                    "ctx": context,
                    "ca": datetime(2026, 5, 1), "etd": date(2026, 5, 1),
                },
            )

        from src.maintenance.archive_extractors import extract_market_light_daily
        with db.session_scope() as session:
            rows = extract_market_light_daily(session, date.today())

        assert len(rows) == 2  # cn + hk

        cn_row = [r for r in rows if r['region'] == 'cn'][0]
        assert cn_row['score'] == 80
        assert cn_row['status'] == 'green'
        assert cn_row['data_quality'] == 'ok'
        assert cn_row['risk_notes'] is not None
        risk = _json.loads(cn_row['risk_notes'])
        assert 'reasons' in risk
        assert 'guidance' in risk

        hk_row = [r for r in rows if r['region'] == 'hk'][0]
        assert hk_row['score'] == 65
        assert hk_row['status'] == 'yellow'

    def test_extract_market_light_empty_snapshots(self, db_env):
        """Missing or empty market_light_snapshots should not crash."""
        db = db_env['db']

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            # No context_snapshot
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, name, report_type, analysis_phase, "
                    "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    "created_at) "
                    "VALUES (:qid, :code, :cn, :name, :rtype, :phase, :score, :advice, :trend, "
                    ":summary, :ca)"
                ),
                {
                    "qid": "mr_nil", "code": "MR", "cn": "MR", "name": "Market Review",
                    "rtype": "market_review", "phase": "postmarket", "score": 50,
                    "advice": "review", "trend": "review", "summary": "overview",
                    "ca": datetime(2026, 5, 1),
                },
            )

        from src.maintenance.archive_extractors import extract_market_light_daily
        with db.session_scope() as session:
            rows = extract_market_light_daily(session, date.today())

        assert len(rows) == 0  # No snapshots, so nothing to extract

    # ------------------------------------------------------------------
    # Unified entry point
    # ------------------------------------------------------------------

    def test_extract_phase2_dry_run(self, db_env):
        """Dry run should count rows but not write to historical DB."""
        db = db_env['db']
        hist_db = db_env['hist_db']

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            # Seed minimal data
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES ('600519', '600519', '2026-01-01', 100.0, 'test')"
                ),
            )

        from src.maintenance.archive_extractors import extract_phase2_technical_history
        stats = extract_phase2_technical_history(
            main_db=db, historical_db=hist_db,
            cutoff_date=date.today(), dry_run=True,
        )

        assert stats['dry_run'] is True
        assert stats['stock_daily']['read'] == 1
        assert stats['stock_daily']['written'] == 0  # dry run does not write
        # Other extractors should have read 0 (no data)
        assert stats['intraday_quote_points']['read'] == 0
        assert stats['postmarket_technical_summary']['read'] == 0
        assert stats['market_light_daily']['read'] == 0

    def test_extract_phase2_non_dry_run(self, db_env):
        """Non-dry-run should write to historical DB."""
        db = db_env['db']
        hist_db = db_env['hist_db']

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES ('600519', '600519', '2026-01-01', 100.0, 'test')"
                ),
            )

        from src.maintenance.archive_extractors import extract_phase2_technical_history
        stats = extract_phase2_technical_history(
            main_db=db, historical_db=hist_db,
            cutoff_date=date.today(), dry_run=False,
        )

        assert stats['dry_run'] is False
        assert stats['stock_daily']['read'] == 1
        assert stats['stock_daily']['written'] == 1

        # Verify data landed in historical DB
        with hist_db.get_session() as session:
            from sqlalchemy import text as sa_text_hist
            result = session.execute(
                sa_text_hist(
                    "SELECT COUNT(*) FROM historical_stock_daily "
                    "WHERE code_norm = '600519'"
                )
            ).fetchone()
        assert result[0] == 1
