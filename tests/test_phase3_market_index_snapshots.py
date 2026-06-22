# -*- coding: utf-8 -*-
"""Tests for Phase 3: intraday_market_snapshots DDL and persistence."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, PropertyMock, patch

import pytest


class TestMarketIndexSnapshotsSchema:
    """Test fresh DB table creation includes all required columns."""

    @pytest.fixture
    def db_env(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / "test_main.db")
        monkeypatch.setenv("DATABASE_PATH", db_path)

        from src.config import Config

        Config.reset_instance()

        from src.storage import DatabaseManager

        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{db_path}")

        yield {"db": db, "db_path": db_path}

        DatabaseManager.reset_instance()

    def test_fresh_db_has_timestamp_column(self, db_env):
        """Fresh DB table should include timestamp column."""
        from src.core.intraday_monitor import IntradayMonitor

        config = MagicMock()
        config.intraday_market_snapshot_enabled = True
        config.logger_level = "WARNING"
        monitor = IntradayMonitor(config=config, fetcher_manager=MagicMock(), db_manager=db_env["db"])

        with db_env["db"].session_scope() as session:
            conn = session.connection()
            from sqlalchemy import text as sa_text

            # Drop and recreate to test fresh DDL
            conn.execute(sa_text("DROP TABLE IF EXISTS intraday_market_snapshots"))
            session.commit()

        monitor._ensure_intraday_market_snapshots_table()

        with db_env["db"].session_scope() as session:
            conn = session.connection()
            from sqlalchemy import text as sa_text

            info = conn.execute(
                sa_text("PRAGMA table_info('intraday_market_snapshots')")
            ).fetchall()
            cols = {row[1] for row in info}

        assert "timestamp" in cols, "Fresh DB must have timestamp column"
        assert "error_message" in cols, "Fresh DB must have error_message column"
        assert "market_local_timestamp" in cols
        assert "status" in cols
        assert "snapshot_id" in cols

    def test_persist_valid_quote_succeeds(self, db_env):
        """_persist_market_snapshots with valid quote should not raise."""
        from src.core.intraday_monitor import IntradayMonitor

        config = MagicMock()
        config.intraday_market_snapshot_enabled = True
        config.logger_level = "WARNING"
        monitor = IntradayMonitor(config=config, fetcher_manager=MagicMock(), db_manager=db_env["db"])

        monitor._ensure_intraday_market_snapshots_table()

        quote = {
            "index_code": "000001",
            "index_name": "上证指数",
            "current_price": 3000.0,
            "change_pct": 0.5,
            "open_price": 2990.0,
            "prev_close": 2995.0,
            "high_price": 3010.0,
            "low_price": 2980.0,
            "volume": 1000000.0,
            "amount": 100000000.0,
            "source": "test",
            "status": "valid",
            "timestamp": "2026-06-22T10:00:00",
        }
        # Should not raise
        monitor._persist_market_snapshots(
            snapshot_id="s1", run_id="r1", market="cn", index_quotes=[quote]
        )

        # Verify it was written
        with db_env["db"].session_scope() as session:
            from sqlalchemy import text as sa_text

            row = session.execute(
                sa_text(
                    "SELECT index_code, current_price, timestamp FROM intraday_market_snapshots"
                )
            ).fetchone()
            assert row is not None
            assert row[0] == "000001"
            assert row[1] == 3000.0
            assert row[2] == "2026-06-22T10:00:00"

    def test_persist_fetch_failed_retains_error_message(self, db_env):
        """fetch_failed quote should preserve error_message."""
        from src.core.intraday_monitor import IntradayMonitor

        config = MagicMock()
        config.intraday_market_snapshot_enabled = True
        config.logger_level = "WARNING"
        monitor = IntradayMonitor(config=config, fetcher_manager=MagicMock(), db_manager=db_env["db"])

        monitor._ensure_intraday_market_snapshots_table()

        quote = {
            "index_code": "000001",
            "index_name": "上证指数",
            "status": "fetch_failed",
            "error_message": "API timeout",
            "market": "cn",
        }
        monitor._persist_market_snapshots(
            snapshot_id="s1", run_id="r1", market="cn", index_quotes=[quote]
        )

        with db_env["db"].session_scope() as session:
            from sqlalchemy import text as sa_text

            row = session.execute(
                sa_text(
                    "SELECT status, error_message FROM intraday_market_snapshots "
                    "WHERE index_code = '000001'"
                )
            ).fetchone()
            assert row is not None
            assert row[0] == "fetch_failed"
            assert row[1] == "API timeout"

    def test_persist_data_unavailable_succeeds(self, db_env):
        """data_unavailable quote should insert without error."""
        from src.core.intraday_monitor import IntradayMonitor

        config = MagicMock()
        config.intraday_market_snapshot_enabled = True
        config.logger_level = "WARNING"
        monitor = IntradayMonitor(config=config, fetcher_manager=MagicMock(), db_manager=db_env["db"])

        monitor._ensure_intraday_market_snapshots_table()

        quote = {
            "index_code": "000001",
            "status": "data_unavailable",
            "error_message": "no quote data returned",
            "market": "cn",
        }
        # Should not raise
        monitor._persist_market_snapshots(
            snapshot_id="s1", run_id="r1", market="cn", index_quotes=[quote]
        )

        with db_env["db"].session_scope() as session:
            from sqlalchemy import text as sa_text

            row = session.execute(
                sa_text(
                    "SELECT status, error_message FROM intraday_market_snapshots "
                    "WHERE index_code = '000001'"
                )
            ).fetchone()
            assert row is not None
            assert row[0] == "data_unavailable"

    def test_unique_constraint_no_duplicate(self, db_env):
        """Same snapshot_id + market + index_code should not duplicate."""
        from src.core.intraday_monitor import IntradayMonitor

        config = MagicMock()
        config.intraday_market_snapshot_enabled = True
        config.logger_level = "WARNING"
        monitor = IntradayMonitor(config=config, fetcher_manager=MagicMock(), db_manager=db_env["db"])

        monitor._ensure_intraday_market_snapshots_table()

        from sqlalchemy import text as sa_text

        # Insert same row twice using raw SQL (UNIQUE constraint should dedupe)
        for _ in range(2):
            with db_env["db"].session_scope() as session:
                conn = session.connection()
                conn.execute(sa_text(
                    "INSERT OR REPLACE INTO intraday_market_snapshots "
                    "(snapshot_id, run_id, query_date, market, market_local_timestamp, "
                    "index_code, index_name, current_price, status, created_at) "
                    "VALUES (:sid, :rid, :qd, :mkt, :mlt, :ic, :iname, :cp, :st, :ca)"
                ), {
                    "sid": "s1", "rid": "r1", "qd": "2026-06-22",
                    "mkt": "cn", "mlt": "2026-06-22T10:00:00",
                    "ic": "000001", "iname": "上证指数",
                    "cp": 3000.0, "st": "valid", "ca": "2026-06-22T10:00:00",
                })
                session.commit()

        with db_env["db"].session_scope() as session:
            count = session.execute(sa_text(
                "SELECT COUNT(*) FROM intraday_market_snapshots "
                "WHERE snapshot_id='s1' AND index_code='000001'"
            )).scalar()
        assert count == 1, f"Expected 1 row, got {count}"

    def test_fail_open_does_not_raise(self, db_env):
        """_persist_market_snapshots should not raise on invalid input."""
        from src.core.intraday_monitor import IntradayMonitor

        config = MagicMock()
        config.intraday_market_snapshot_enabled = True
        config.logger_level = "WARNING"
        monitor = IntradayMonitor(config=config, fetcher_manager=MagicMock(), db_manager=db_env["db"])

        # Empty quote list should be silently ignored
        monitor._persist_market_snapshots(
            snapshot_id="s1", run_id="r1", market="cn", index_quotes=[]
        )

        # Missing fields should not raise (fail-open with warning)
        quote_bad = {"status": "fetch_failed", "error_message": "timeout", "market": "cn"}
        monitor._persist_market_snapshots(
            snapshot_id="s1", run_id="r1", market="cn", index_quotes=[quote_bad]
        )
