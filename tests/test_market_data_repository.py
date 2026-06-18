# -*- coding: utf-8 -*-
"""Tests for MarketDataRepository: main/historical routing, dedup, HK code, cross-boundary."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

import pytest


class TestMarketDataRepository:
    """Test MarketDataRepository query routing between main and historical DB."""

    @pytest.fixture
    def repo_env(self, tmp_path, monkeypatch):
        """Setup main + historical databases with seed data."""
        main_db_path = str(tmp_path / "repo_main.db")
        historical_db_path = str(tmp_path / "repo_hist.db")
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
            'db': db, 'hist_db': hist_db,
            'main_path': main_db_path, 'hist_path': historical_db_path,
            'tmp_path': tmp_path,
        }

        DatabaseManager.reset_instance()

    # ------------------------------------------------------------------
    # get_stock_daily
    # ------------------------------------------------------------------

    def test_get_stock_daily_main_only(self, repo_env):
        """Recent data should come from main DB only."""
        db = repo_env['db']
        hist_db = repo_env['hist_db']

        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES (:code, :cn, :date, :close, :src)"
                ),
                {"code": "600519", "cn": "600519",
                 "date": (date.today() - timedelta(days=2)).isoformat(),
                 "close": 101.0, "src": "test"},
            )

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        results = repo.get_stock_daily('600519', days_back=10)
        assert len(results) == 1
        assert results[0]['close'] == 101.0
        assert results[0]['source_db'] == 'main'

    def test_get_stock_daily_historical_only(self, repo_env):
        """Old data should come from historical DB only."""
        db = repo_env['db']
        hist_db = repo_env['hist_db']

        # Seed historical DB with old data
        from src.maintenance.archive_extractors import extract_stock_daily

        with db.session_scope() as session:
            from sqlalchemy import text as sa_text
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES (:code, :cn, :date, :close, :src)"
                ),
                {"code": "600519", "cn": "600519",
                 "date": "2024-01-15", "close": 80.0, "src": "test"},
            )
            old_rows = extract_stock_daily(session, date.today())

        if old_rows:
            hist_db.upsert_stock_daily_batch(old_rows)

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        results = repo.get_stock_daily('600519', days_back=500)
        assert len(results) >= 1
        hist_rows = [r for r in results if r['source_db'] == 'historical']
        assert len(hist_rows) >= 1
        assert hist_rows[0]['close'] == 80.0

    def test_get_stock_daily_cross_boundary(self, repo_env):
        """Data spanning retention boundary should merge and dedup from both DBs."""
        db = repo_env['db']
        hist_db = repo_env['hist_db']

        today = date.today()

        from sqlalchemy import text as sa_text

        # Main DB: recent data
        with db.session_scope() as session:
            for days_back, close_val in [(1, 102.0), (3, 101.0)]:
                d = today - timedelta(days=days_back)
                session.execute(
                    sa_text(
                        "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                        "VALUES (:code, :cn, :date, :close, :src)"
                    ),
                    {"code": "600519", "cn": "600519",
                     "date": d.isoformat(), "close": close_val, "src": "test"},
                )

        # Historical DB: old data
        old_row = {
            'code': '600519', 'code_norm': '600519', 'market': 'cn',
            'trade_date': (today - timedelta(days=10)).isoformat(),
            'close': 95.0, 'data_source': 'main.stock_daily',
        }
        hist_db.upsert_stock_daily_batch([old_row])

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        results = repo.get_stock_daily('600519', days_back=30)
        # Should have 3 rows (2 from main, 1 from historical)
        assert len(results) == 3
        sources = {r['source_db'] for r in results}
        assert 'main' in sources
        assert 'historical' in sources

        # No duplicate dates
        trade_dates = [r['trade_date'] for r in results]
        assert len(trade_dates) == len(set(trade_dates))

    # ------------------------------------------------------------------
    # HK code support
    # ------------------------------------------------------------------

    def test_hk_code_normalization(self, repo_env):
        """Repository should normalize HK codes for query."""
        db = repo_env['db']
        hist_db = repo_env['hist_db']

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES (:code, :cn, :date, :close, :src)"
                ),
                {"code": "00700", "cn": "HK00700",
                 "date": (date.today() - timedelta(days=1)).isoformat(),
                 "close": 380.0, "src": "test"},
            )

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        # Query with different HK code formats
        results1 = repo.get_stock_daily('HK00700', days_back=10)
        assert len(results1) >= 1
        assert results1[0]['code_norm'] == 'HK00700'

        results2 = repo.get_stock_daily('00700', days_back=10)
        assert len(results2) >= 1
        assert results2[0]['code_norm'] == 'HK00700'

    # ------------------------------------------------------------------
    # get_intraday_trend
    # ------------------------------------------------------------------

    def test_get_intraday_trend_main(self, repo_env):
        """Intraday trend from main DB should return events."""
        db = repo_env['db']
        hist_db = repo_env['hist_db']

        from sqlalchemy import text as sa_text

        today_str = date.today().isoformat()

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
            conn.execute(sa_text(
                "INSERT INTO intraday_events (query_date, market, timestamp, stock_code, "
                "stock_name, current_price, volume_ratio, change_pct, event_type, "
                "snapshot_id) "
                "VALUES (:qd, :mkt, :ts, :sc, :sn, :cp, :vr, :cpct, :et, :sid)"
            ), {
                "qd": today_str, "mkt": "cn", "ts": f"{today_str}T10:30:00",
                "sc": "600519", "sn": u"茅台", "cp": 1750.0, "vr": 1.1,
                "cpct": 2.0, "et": "price_only", "sid": "snap_001",
            })

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        results = repo.get_intraday_trend('600519', query_date=today_str)
        assert len(results) == 1
        assert results[0]['stock_code'] == '600519'
        assert results[0]['source_db'] == 'main'

    # ------------------------------------------------------------------
    # get_postmarket_technical_summary
    # ------------------------------------------------------------------

    def test_get_postmarket_technical_summary(self, repo_env):
        """Postmarket summaries from main DB hot fields."""
        db = repo_env['db']
        hist_db = repo_env['hist_db']

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
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
                    "qid": "test_1", "code": "600519", "cn": "600519", "mkt": "cn",
                    "name": u"贵州茅台", "rtype": "daily", "phase": "postmarket",
                    "score": 80, "advice": "buy", "trend": "bullish",
                    "summary": "Good entry", "ib": 100.0, "sb": 97.0,
                    "sl": 95.0, "tp": 110.0, "cp": 102.0, "chpct": 2.0, "vr": 1.3,
                    "ca": datetime.now() - timedelta(days=1),
                    "etd": date.today() - timedelta(days=1),
                },
            )

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        results = repo.get_postmarket_technical_summary('600519', days_back=10)
        assert len(results) == 1
        assert results[0]['close'] == 102.0
        assert results[0]['ideal_buy'] == 100.0
        assert results[0]['operation_advice'] == 'buy'
        assert results[0]['source_db'] == 'main'

    # ------------------------------------------------------------------
    # get_market_light_daily
    # ------------------------------------------------------------------

    def test_get_market_light_daily_from_main(self, repo_env):
        """Market light from main DB (parsed from market_review context_snapshot)."""
        db = repo_env['db']
        hist_db = repo_env['hist_db']

        import json as _json
        from sqlalchemy import text as sa_text

        snapshots = {
            "cn": {
                "score": 75, "status": "green", "label": "Normal",
                "temperature_label": u"温和", "data_quality": "ok",
                "reasons": [], "guidance": "",
            },
            "hk": {
                "score": 60, "status": "yellow", "label": "Caution",
                "temperature_label": u"偏热", "data_quality": "partial",
                "reasons": [], "guidance": "",
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
                    "ca": datetime.now() - timedelta(days=1),
                    "etd": date.today() - timedelta(days=1),
                },
            )

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        results = repo.get_market_light_daily('cn', days_back=10)
        assert len(results) >= 1
        cn_result = results[0]
        assert cn_result['region'] == 'cn'
        assert cn_result['score'] == 75
        assert cn_result['source'] == 'main.analysis_history'

        results_hk = repo.get_market_light_daily('hk', days_back=10)
        assert len(results_hk) >= 1
        assert results_hk[0]['score'] == 60

    # ------------------------------------------------------------------
    # Historical DB fallback
    # ------------------------------------------------------------------

    def test_repository_handles_missing_historical_db(self, repo_env):
        """Repository should gracefully handle historical DB with no data."""
        db = repo_env['db']
        hist_db = repo_env['hist_db']

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO stock_daily (code, code_norm, date, close, data_source) "
                    "VALUES (:code, :cn, :date, :close, :src)"
                ),
                {"code": "600519", "cn": "600519",
                 "date": (date.today() - timedelta(days=2)).isoformat(),
                 "close": 101.0, "src": "test"},
            )

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        # Should still return main DB results even if historical is empty
        results = repo.get_stock_daily('600519', days_back=30)
        assert len(results) == 1
        assert results[0]['source_db'] == 'main'

    def test_repository_empty_results(self, repo_env):
        """Non-existent code should return empty list."""
        db = repo_env['db']
        hist_db = repo_env['hist_db']

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        results = repo.get_stock_daily('NOTACODE', days_back=30)
        assert results == []
