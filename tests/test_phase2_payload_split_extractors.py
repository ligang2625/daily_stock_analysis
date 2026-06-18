# -*- coding: utf-8 -*-
"""Tests for Phase 2 payload-split compatibility: extractors, repository, cutoff, idempotency."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

import pytest


class TestPayloadSplitExtractors:
    """Verify extractors read from analysis_history_payload when legacy columns are NULL."""

    @pytest.fixture
    def env(self, tmp_path, monkeypatch):
        """Setup main + historical databases with payload table."""
        main_db_path = str(tmp_path / "payload_test_main.db")
        historical_db_path = str(tmp_path / "payload_test_hist.db")
        monkeypatch.setenv('DATABASE_PATH', main_db_path)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db_path)

        from src.config import Config
        Config.reset_instance()

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{main_db_path}")

        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=historical_db_path)

        # Ensure the payload table exists
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            conn.execute(sa_text(
                "CREATE TABLE IF NOT EXISTS analysis_history_payload ("
                "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "  history_id INTEGER NOT NULL UNIQUE,"
                "  raw_result TEXT,"
                "  news_content TEXT,"
                "  context_snapshot TEXT,"
                "  payload_size INTEGER,"
                "  compressed BOOLEAN NOT NULL DEFAULT 0,"
                "  created_at DATETIME,"
                "  FOREIGN KEY (history_id) REFERENCES analysis_history(id)"
                ")"
            ))

        yield {
            'db': db, 'hist_db': hist_db,
            'main_path': main_db_path, 'hist_path': historical_db_path,
        }

        DatabaseManager.reset_instance()

    # ------------------------------------------------------------------
    # Test 1: Postmarket archive reads payload table
    # ------------------------------------------------------------------

    def test_postmarket_extractor_reads_payload_when_legacy_null(self, env):
        """When analysis_history legacy columns are NULL, extractor should use payload table."""
        db = env['db']
        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            # Insert analysis_history with NULL context_snapshot and raw_result
            result = session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, market, name, report_type, analysis_phase, "
                    "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    "raw_result, context_snapshot, news_content, "
                    "ideal_buy, secondary_buy, stop_loss, take_profit, "
                    "current_price, change_pct, volume_ratio, turnover_rate, "
                    "created_at, effective_trading_date) "
                    "VALUES (:qid, :code, :cn, :mkt, :name, :rtype, :phase, :score, "
                    ":advice, :trend, :summary, "
                    "NULL, NULL, NULL, "
                    ":ib, :sb, :sl, :tp, :cp, :chpct, :vr, :tr, :ca, :etd)"
                ),
                {
                    "qid": "test_payload", "code": "600519", "cn": "600519", "mkt": "cn",
                    "name": "test", "rtype": None, "phase": "postmarket",
                    "score": 75, "advice": "hold", "trend": "bullish",
                    "summary": "test", "ib": 95.0, "sb": 92.0, "sl": 90.0, "tp": 110.0,
                    "cp": 101.0, "chpct": 2.5, "vr": 1.2, "tr": 3.0,
                    "ca": datetime(2026, 5, 1), "etd": date(2026, 5, 1),
                },
            )
            history_id = result.lastrowid

            # Insert payload with technical data
            payload_raw = json.dumps({
                "enhanced_context": {
                    "technical": {
                        "ma5": 100.5, "ma10": 99.0, "ma20": 98.0,
                        "support": 95.0, "resistance": 108.0,
                    }
                }
            })
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history_payload "
                    "(history_id, raw_result, context_snapshot, news_content, payload_size, compressed, created_at) "
                    "VALUES (:hid, :rr, :cs, :nc, :ps, 0, datetime('now'))"
                ),
                {
                    "hid": history_id,
                    "rr": payload_raw,
                    "cs": json.dumps({"enhanced_context": {"technical": {"ma60": 90.0}}}),
                    "nc": None,
                    "ps": len(payload_raw),
                },
            )

        from src.maintenance.archive_extractors import extract_postmarket_technical_summaries

        with db.session_scope() as session:
            rows = extract_postmarket_technical_summaries(session, date.today())

        assert len(rows) == 1
        r = rows[0]
        # MA should come from payload (via raw_result fallback) even though legacy columns are NULL
        assert r['ma5'] == 100.5
        assert r['ma10'] == 99.0
        assert r['ma20'] == 98.0
        assert r['support_level'] == 95.0
        assert r['resistance_level'] == 108.0
        # Hot fields still come from main table
        assert r['close'] == 101.0
        assert r['ideal_buy'] == 95.0

    # ------------------------------------------------------------------
    # Test 2: Market Light archive reads payload table
    # ------------------------------------------------------------------

    def test_market_light_extractor_reads_payload_when_legacy_null(self, env):
        """When context_snapshot is NULL in analysis_history, extractor should use payload table."""
        db = env['db']
        from sqlalchemy import text as sa_text

        snapshots = {
            "cn": {"score": 80, "status": "green", "label": "Normal",
                   "temperature_label": "mild", "data_quality": "ok",
                   "reasons": ["good"], "guidance": "hold"},
        }
        payload_json = json.dumps({"market_light_snapshots": snapshots})

        with db.session_scope() as session:
            # Insert analysis_history with NULL context_snapshot
            result = session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, name, report_type, analysis_phase, "
                    "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    "context_snapshot, created_at, effective_trading_date) "
                    "VALUES (:qid, :code, :cn, :name, :rtype, :phase, :score, :advice, "
                    ":trend, :summary, NULL, :ca, :etd)"
                ),
                {
                    "qid": "mr_payload", "code": "MR", "cn": "MR", "name": "Market Review",
                    "rtype": "market_review", "phase": "postmarket", "score": 50,
                    "advice": "review", "trend": "review", "summary": "Overview",
                    "ca": datetime(2026, 5, 1), "etd": date(2026, 5, 1),
                },
            )
            history_id = result.lastrowid

            # Put context_snapshot in payload table
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history_payload "
                    "(history_id, context_snapshot, raw_result, news_content, payload_size, compressed, created_at) "
                    "VALUES (:hid, :cs, NULL, NULL, :ps, 0, datetime('now'))"
                ),
                {"hid": history_id, "cs": payload_json, "ps": len(payload_json)},
            )

        from src.maintenance.archive_extractors import extract_market_light_daily

        with db.session_scope() as session:
            rows = extract_market_light_daily(session, date.today())

        assert len(rows) == 1
        r = rows[0]
        assert r['region'] == 'cn'
        assert r['score'] == 80
        assert r['status'] == 'green'

    # ------------------------------------------------------------------
    # Test 3: Repository recent Market Light reads payload table
    # ------------------------------------------------------------------

    def test_repository_market_light_reads_payload_when_legacy_null(self, env):
        """Repository get_market_light_daily should read from payload table when legacy column is NULL."""
        db = env['db']
        hist_db = env['hist_db']
        from sqlalchemy import text as sa_text

        snapshots = {
            "cn": {"score": 90, "status": "green", "label": "Strong",
                   "temperature_label": "mild", "data_quality": "ok",
                   "reasons": [], "guidance": ""},
        }
        payload_json = json.dumps({"market_light_snapshots": snapshots})

        with db.session_scope() as session:
            result = session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, name, report_type, analysis_phase, "
                    "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    "context_snapshot, created_at, effective_trading_date) "
                    "VALUES (:qid, :code, :cn, :name, :rtype, :phase, :score, :advice, "
                    ":trend, :summary, NULL, :ca, :etd)"
                ),
                {
                    "qid": "mr_repo", "code": "MR", "cn": "MR", "name": "Market Review",
                    "rtype": "market_review", "phase": "postmarket", "score": 50,
                    "advice": "review", "trend": "review", "summary": "Overview",
                    "ca": datetime.now() - timedelta(days=1),
                    "etd": date.today() - timedelta(days=1),
                },
            )
            history_id = result.lastrowid

            session.execute(
                sa_text(
                    "INSERT INTO analysis_history_payload "
                    "(history_id, context_snapshot, raw_result, news_content, payload_size, compressed, created_at) "
                    "VALUES (:hid, :cs, NULL, NULL, :ps, 0, datetime('now'))"
                ),
                {"hid": history_id, "cs": payload_json, "ps": len(payload_json)},
            )

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        results = repo.get_market_light_daily('cn', days_back=10)
        assert len(results) >= 1
        assert results[0]['score'] == 90

    # ------------------------------------------------------------------
    # Test 4: Cutoff boundary row not extracted when record_date == cutoff_date
    # ------------------------------------------------------------------

    def test_cutoff_boundary_not_extracted(self, env):
        """Rows with trade_date == cutoff_date should NOT be extracted (only < cutoff)."""
        db = env['db']
        hist_db = env['hist_db']
        from sqlalchemy import text as sa_text

        cutoff = date.today() - timedelta(days=5)

        with db.session_scope() as session:
            # Insert a row exactly on the cutoff date
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES (:code, :cn, :date, :close, :src)"
                ),
                {"code": "600519", "cn": "600519",
                 "date": cutoff.isoformat(), "close": 100.0, "src": "test"},
            )
            # Insert a row before the cutoff date
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES (:code, :cn, :date, :close, :src)"
                ),
                {"code": "600519", "cn": "600519",
                 "date": (cutoff - timedelta(days=1)).isoformat(),
                 "close": 99.0, "src": "test"},
            )
            # Insert a row after the cutoff date
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES (:code, :cn, :date, :close, :src)"
                ),
                {"code": "600519", "cn": "600519",
                 "date": (cutoff + timedelta(days=1)).isoformat(),
                 "close": 101.0, "src": "test"},
            )

        from src.maintenance.archive_extractors import extract_stock_daily

        with db.session_scope() as session:
            rows = extract_stock_daily(session, cutoff)

        # Only the row before cutoff should be extracted (not on or after cutoff)
        assert len(rows) == 1
        assert rows[0]['close'] == 99.0

    # ------------------------------------------------------------------
    # Test 5: Intraday archive idempotency with NULL source snapshot_id
    # ------------------------------------------------------------------

    def test_intraday_idempotency_null_snapshot_id(self, env):
        """Re-running archive should not duplicate intraday rows with missing source snapshot IDs."""
        db = env['db']
        hist_db = env['hist_db']
        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            conn = session.connection()
            conn.execute(sa_text(
                "CREATE TABLE IF NOT EXISTS intraday_events ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "query_date TEXT NOT NULL, market TEXT, timestamp TEXT NOT NULL, "
                "market_local_timestamp TEXT, "
                "stock_code TEXT NOT NULL, stock_name TEXT, current_price REAL, "
                "volume_ratio REAL, change_pct REAL, event_type TEXT NOT NULL, "
                "description TEXT, raw_quote TEXT, snapshot_id TEXT"
                ")"
            ))
            # Insert row with NULL snapshot_id
            conn.execute(sa_text(
                "INSERT INTO intraday_events "
                "(query_date, market, timestamp, stock_code, stock_name, "
                "current_price, volume_ratio, change_pct, event_type, "
                "snapshot_id) "
                "VALUES (:qd, :mkt, :ts, :sc, :sn, :cp, :vr, :cpct, :et, NULL)"
            ), {
                "qd": "2026-06-01", "mkt": "cn", "ts": "2026-06-01T10:30:00",
                "sc": "600519", "sn": "test", "cp": 1750.0, "vr": 1.1,
                "cpct": 2.5, "et": "price_only",
            })

        from src.maintenance.archive_extractors import (
            extract_intraday_quote_points,
            extract_phase2_technical_history,
        )

        # First run: extract and write
        stats1 = extract_phase2_technical_history(
            main_db=db, historical_db=hist_db,
            cutoff_date=date.today(), dry_run=False,
        )
        first_written = stats1['intraday_quote_points']['written']
        assert first_written >= 1

        # Second run: should not duplicate
        stats2 = extract_phase2_technical_history(
            main_db=db, historical_db=hist_db,
            cutoff_date=date.today(), dry_run=False,
        )
        second_written = stats2['intraday_quote_points']['written']
        assert second_written >= 1

        # Count in historical DB should be 1 (not 2)
        with hist_db.get_session() as session:
            result = session.execute(
                sa_text("SELECT COUNT(*) FROM historical_intraday_quote_points")
            ).fetchone()
        assert result[0] == 1

    # ------------------------------------------------------------------
    # Test 6: Repository cross-boundary dedupe still works after cutoff semantics
    # ------------------------------------------------------------------

    def test_repository_cross_boundary_dedupe(self, env):
        """Repository should deduplicate correctly with cutoff < semantics."""
        db = env['db']
        hist_db = env['hist_db']
        from sqlalchemy import text as sa_text

        cutoff_date = date.today() - timedelta(days=5)
        # A date right at the boundary that should be in one DB only
        boundary_date = cutoff_date

        # Seed main DB with a row on the boundary
        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES (:code, :cn, :date, :close, :src)"
                ),
                {"code": "600519", "cn": "600519",
                 "date": boundary_date.isoformat(), "close": 100.0, "src": "test"},
            )

        # Also seed main DB with a more recent row
        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES (:code, :cn, :date, :close, :src)"
                ),
                {"code": "600519", "cn": "600519",
                 "date": (date.today() - timedelta(days=2)).isoformat(),
                 "close": 102.0, "src": "test"},
            )

        # Seed historical DB with a row older than boundary
        old_row = {
            'code': '600519', 'code_norm': '600519', 'market': 'cn',
            'trade_date': (boundary_date - timedelta(days=1)).isoformat(),
            'close': 95.0, 'data_source': 'main.stock_daily',
        }
        hist_db.upsert_stock_daily_batch([old_row])

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        results = repo.get_stock_daily('600519', days_back=30)

        # No duplicate dates
        trade_dates = [r['trade_date'] for r in results]
        assert len(trade_dates) == len(set(trade_dates)), f"Duplicate dates found: {trade_dates}"

        # Verify both source DBs are represented
        sources = {r['source_db'] for r in results}
        # The boundary row should be in main (since it's >= boundary in repository logic)
        # The old row should be in historical
        assert 'main' in sources
        if len(trade_dates) > 1:
            assert 'historical' in sources
