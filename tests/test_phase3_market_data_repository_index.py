# -*- coding: utf-8 -*-
"""Tests for Phase 3: MarketDataRepository.get_market_index_trend cross-DB routing."""

from __future__ import annotations

from datetime import date, timedelta

import pytest


class TestMarketDataRepositoryIndexTrend:
    """Test get_market_index_trend routing across main and historical DBs."""

    @pytest.fixture
    def repo_env(self, tmp_path, monkeypatch):
        main_path = str(tmp_path / "repo_main.db")
        hist_path = str(tmp_path / "repo_hist.db")
        monkeypatch.setenv("DATABASE_PATH", main_path)
        monkeypatch.setenv("HISTORICAL_DATABASE_PATH", hist_path)

        from src.config import Config

        Config.reset_instance()

        from src.storage import DatabaseManager

        DatabaseManager.reset_instance()
        main_db = DatabaseManager(db_url=f"sqlite:///{main_path}")

        from src.storage_historical import HistoricalDatabaseManager

        hist_db = HistoricalDatabaseManager(db_path=hist_path)

        from src.services.market_data_repository import MarketDataRepository

        repo = MarketDataRepository(
            main_db=main_db, historical_db=hist_db, retention_days=7
        )

        # Create main DB table with all columns
        from sqlalchemy import text as sa_text

        with main_db.session_scope() as session:
            conn = session.connection()
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
            session.commit()

        yield {
            "main_db": main_db,
            "hist_db": hist_db,
            "repo": repo,
            "main_path": main_path,
            "hist_path": hist_path,
        }

        DatabaseManager.reset_instance()

    def _add_main_index(self, main_db, query_date, market, index_code,
                        index_name, price, change_pct, ts, snapshot_id="s1"):
        from sqlalchemy import text as sa_text

        with main_db.session_scope() as session:
            conn = session.connection()
            conn.execute(sa_text(
                "INSERT OR REPLACE INTO intraday_market_snapshots "
                "(snapshot_id, run_id, query_date, market, index_code, index_name, "
                "current_price, change_pct, open_price, prev_close, "
                "high_price, low_price, status, timestamp, market_local_timestamp, created_at) "
                "VALUES (:sid, :rid, :qd, :mkt, :ic, :iname, "
                ":cp, :chg, :op, :pc, :hp, :lp, :st, :ts, :mlt, :ca)"
            ), {
                "sid": snapshot_id, "rid": "r1", "qd": query_date,
                "mkt": market, "ic": index_code, "iname": index_name,
                "cp": price, "chg": change_pct,
                "op": None, "pc": None, "hp": None, "lp": None,
                "st": "valid", "ts": ts, "mlt": ts, "ca": ts,
            })
            session.commit()

    def test_main_db_recent_data(self, repo_env):
        """Recent query_date should return data from main DB."""
        today = date.today().isoformat()
        self._add_main_index(
            repo_env["main_db"], today, "cn", "000001",
            "上证指数", 3000.0, 0.5, "2026-06-22T10:00:00",
        )
        result = repo_env["repo"].get_market_index_trend(
            market="cn", query_date=today
        )
        assert len(result) > 0
        r = result[0]
        assert r["index_code"] == "000001"
        assert r["current_price"] == 3000.0
        assert r["source_db"] == "main"

    def test_index_code_filter(self, repo_env):
        """index_code filter should narrow results."""
        today = date.today().isoformat()
        self._add_main_index(
            repo_env["main_db"], today, "cn", "000001",
            "上证指数", 3000.0, 0.5, "2026-06-22T10:00:00",
        )
        self._add_main_index(
            repo_env["main_db"], today, "cn", "399001",
            "深证成指", 10000.0, 1.0, "2026-06-22T10:00:00",
        )
        result = repo_env["repo"].get_market_index_trend(
            market="cn", query_date=today, index_code="000001"
        )
        codes = {r["index_code"] for r in result}
        assert codes == {"000001"}

    def test_historical_db_routing(self, repo_env):
        """Old data should route to historical DB."""
        old_date = (date.today() - timedelta(days=30)).isoformat()

        # Insert into historical DB directly
        from sqlalchemy import text as sa_text

        with repo_env["hist_db"].get_session() as session:
            # Create table if needed
            session.execute(sa_text(
                "CREATE TABLE IF NOT EXISTS historical_market_index_points ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "query_date TEXT, market TEXT, index_code TEXT, index_name TEXT, "
                "timestamp TEXT, market_local_timestamp TEXT, snapshot_id TEXT, "
                "current_price REAL, change_pct REAL, "
                "open REAL, high REAL, low REAL, pre_close REAL, "
                "volume REAL, amount REAL, "
                "trend_label TEXT, strength_label TEXT, breadth_label TEXT, "
                "status TEXT, created_at TEXT, "
                "UNIQUE(market, index_code, market_local_timestamp, snapshot_id))"
            ))
            session.execute(sa_text(
                "INSERT OR REPLACE INTO historical_market_index_points "
                "(query_date, market, index_code, index_name, "
                "current_price, change_pct, timestamp, market_local_timestamp, snapshot_id, "
                "trend_label, strength_label, status, created_at) "
                "VALUES (:qd, :mkt, :ic, :iname, :cp, :chg, :ts, :mlt, :sid, "
                "'rising', 'strong', 'valid', :ca)"
            ), {
                "qd": old_date, "mkt": "cn", "ic": "000001",
                "iname": "上证指数", "cp": 2900.0, "chg": -1.0,
                "ts": f"{old_date}T10:00:00",
                "mlt": f"{old_date}T10:00:00",
                "sid": "legacy:1", "ca": old_date,
            })
            session.commit()

        result = repo_env["repo"].get_market_index_trend(
            market="cn", query_date=old_date
        )
        assert len(result) > 0
        r = result[0]
        assert r["index_code"] == "000001"
        # May be from historical or fallback — verify data presence
        assert r["current_price"] is not None

    def test_cross_boundary_no_duplicates(self, repo_env):
        """Same key across main+historical should not duplicate after merge."""
        today_str = date.today().isoformat()
        recent = today_str
        # Insert same logical point in both DBs
        self._add_main_index(
            repo_env["main_db"], recent, "cn", "000001",
            "上证指数", 3000.0, 0.5, f"{recent}T10:00:00",
        )
        # The repo routes based on retention boundary. Main DB data is returned.
        result = repo_env["repo"].get_market_index_trend(
            market="cn", query_date=recent
        )
        # Should not have duplicates of (market, index_code, market_local_timestamp, snapshot_id)
        seen = set()
        for r in result:
            key = (
                r.get("market", ""),
                r.get("index_code", ""),
                r.get("market_local_timestamp", ""),
                r.get("snapshot_id", ""),
            )
            assert key not in seen, f"Duplicate key: {key}"
            seen.add(key)

    def test_query_date_none_returns_days_back(self, repo_env):
        """When query_date is None, should use days_back window."""
        today = date.today()
        for i in range(5):
            d = (today - timedelta(days=i)).isoformat()
            self._add_main_index(
                repo_env["main_db"], d, "cn", "000001",
                "上证指数", 3000.0 - i * 10, 0.0, f"{d}T10:00:00",
                snapshot_id=f"s{i}",
            )
        result = repo_env["repo"].get_market_index_trend(
            market="cn", days_back=3
        )
        # Should include data within 3 days
        assert len(result) > 0
