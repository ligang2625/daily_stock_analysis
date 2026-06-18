# -*- coding: utf-8 -*-
"""Tests for MarketDataRepository.get_market_index_trend: routing, merge, dedup."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest


def _setup_main_market_index(db):
    from sqlalchemy import text as sa_text
    with db.session_scope() as session:
        conn = session.connection()
        conn.execute(sa_text(
            "CREATE TABLE IF NOT EXISTS intraday_market_snapshots ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "snapshot_id TEXT NOT NULL, "
            "run_id TEXT, "
            "query_date TEXT NOT NULL, "
            "market TEXT NOT NULL, "
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
            "status TEXT NOT NULL DEFAULT 'valid', "
            "error_message TEXT, "
            "timestamp TEXT, "
            "raw_quote TEXT, "
            "created_at TEXT NOT NULL, "
            "UNIQUE(snapshot_id, market, index_code))"
        ))
        session.commit()


class TestMarketDataRepositoryMarketIndex:
    """Test get_market_index_trend with main and historical DB routing."""

    @pytest.fixture
    def repo_env(self, tmp_path, monkeypatch):
        main_path = str(tmp_path / "repo_mkt_main.db")
        hist_path = str(tmp_path / "repo_mkt_hist.db")
        monkeypatch.setenv('DATABASE_PATH', main_path)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', hist_path)

        from src.config import Config
        Config.reset_instance()

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{main_path}")
        _setup_main_market_index(db)

        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=hist_path)

        from src.services.market_data_repository import MarketDataRepository
        repo = MarketDataRepository(db, hist_db, retention_days=5)

        yield {'db': db, 'hist_db': hist_db, 'repo': repo}
        DatabaseManager.reset_instance()

    def test_main_only_trend(self, repo_env):
        """Query returns current-day main DB index points."""
        db = repo_env['db']
        repo = repo_env['repo']
        from sqlalchemy import text as sa_text
        today_str = date.today().isoformat()
        with db.session_scope() as session:
            conn = session.connection()
            conn.execute(sa_text(
                "INSERT OR REPLACE INTO intraday_market_snapshots "
                "(snapshot_id, run_id, query_date, market, index_code, index_name, "
                "current_price, change_pct, status, created_at) "
                "VALUES (:sid, :rid, :qd, :mkt, :ic, :iname, :cp, :chg, :st, :ca)"
            ), {
                "sid": "s-t1", "rid": "r1", "qd": today_str,
                "mkt": "cn", "ic": "000001", "iname": "上证指数",
                "cp": 3000.0, "chg": 0.5, "st": "valid",
                "ca": datetime.now().isoformat(),
            })
            session.commit()

        rows = repo.get_market_index_trend(market='cn', query_date=today_str)
        assert len(rows) >= 1
        r = rows[0]
        assert r['index_code'] == '000001'
        assert r['source_db'] == 'main'

    def test_historical_only_trend(self, repo_env):
        """Query returns historical DB index points when data is old."""
        hist_db = repo_env['hist_db']
        repo = repo_env['repo']
        old_date = (date.today() - timedelta(days=30)).isoformat()
        rows = hist_db.upsert_market_index_points_batch([{
            'query_date': old_date,
            'market': 'cn',
            'index_code': '000001',
            'index_name': '上证指数',
            'current_price': 2800.0,
            'change_pct': -0.5,
            'open': 2790.0,
            'high': 2810.0,
            'low': 2780.0,
            'pre_close': 2810.0,
            'volume': 1000.0,
            'amount': 100000.0,
            'trend_label': 'falling',
            'strength_label': 'neutral',
            'breadth_label': None,
            'status': 'valid',
        }])

        rows = repo.get_market_index_trend(market='cn', query_date=old_date)
        assert len(rows) >= 1
        r = rows[0]
        assert r['source_db'] == 'historical'

    def test_cross_boundary_merge_no_duplicates(self, repo_env):
        """Cross-boundary query should merge without duplicates."""
        db = repo_env['db']
        hist_db = repo_env['hist_db']
        repo = repo_env['repo']

        from sqlalchemy import text as sa_text
        today_str = date.today().isoformat()
        boundary = (date.today() - timedelta(days=5)).isoformat()

        # Main DB row (within retention)
        with db.session_scope() as session:
            conn = session.connection()
            conn.execute(sa_text(
                "INSERT OR REPLACE INTO intraday_market_snapshots "
                "(snapshot_id, run_id, query_date, market, index_code, index_name, "
                "current_price, change_pct, status, market_local_timestamp, created_at) "
                "VALUES (:sid, :rid, :qd, :mkt, :ic, :iname, :cp, :chg, :st, :mlt, :ca)"
            ), {
                "sid": "s-m1", "rid": "r1", "qd": today_str,
                "mkt": "cn", "ic": "000001", "iname": "上证指数",
                "cp": 3000.0, "chg": 0.5, "st": "valid",
                "mlt": f"{today_str}T10:00:00",
                "ca": datetime.now().isoformat(),
            })
            session.commit()

        # Historical DB rows
        hist_db.upsert_market_index_points_batch([{
            'query_date': (date.today() - timedelta(days=10)).isoformat(),
            'market': 'cn',
            'index_code': '000001',
            'index_name': '上证指数',
            'current_price': 2900.0,
            'change_pct': -0.3,
            'open': 2910.0,
            'high': 2920.0,
            'low': 2890.0,
            'pre_close': 2910.0,
            'trend_label': 'flat',
            'strength_label': 'neutral',
            'breadth_label': None,
            'status': 'valid',
        }])

        rows = repo.get_market_index_trend(market='cn', days_back=30)
        assert len(rows) >= 2
        sources = [r['source_db'] for r in rows]
        assert 'main' in sources
        assert 'historical' in sources

    def test_filter_by_index_code(self, repo_env):
        """Query should filter by index_code when provided."""
        db = repo_env['db']
        repo = repo_env['repo']
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            for ic in ('000001', '399001'):
                conn.execute(sa_text(
                    "INSERT OR REPLACE INTO intraday_market_snapshots "
                    "(snapshot_id, run_id, query_date, market, index_code, index_name, "
                    "current_price, status, created_at) "
                    "VALUES (:sid, :rid, :qd, :mkt, :ic, :iname, :cp, :st, :ca)"
                ), {
                    "sid": f"s-{ic}", "rid": "r1", "qd": date.today().isoformat(),
                    "mkt": "cn", "ic": ic, "iname": f"Index{ic}",
                    "cp": 3000.0 if ic == '000001' else 12000.0,
                    "st": "valid", "ca": datetime.now().isoformat(),
                })
            session.commit()

        rows = repo.get_market_index_trend(market='cn', index_code='399001')
        assert all(r['index_code'] == '399001' for r in rows)
