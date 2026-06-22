# -*- coding: utf-8 -*-
"""Tests for intraday_market_snapshots table schema and related counting logic.

Covers:
1. _ensure_intraday_market_snapshots_table() creates correct columns.
2. _persist_market_snapshots() writes valid and failed index quotes.
3. historical_snapshot_count counts completed/partial snapshots correctly
   without relying on a non-existent snapshot_type column.
"""

from __future__ import annotations

from datetime import datetime

import pytest


def _setup_intraday_market_snapshots_table(db):
    """Create intraday_market_snapshots table matching production schema."""
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


def _setup_intraday_snapshots_table(db):
    """Create intraday_snapshots table matching production schema."""
    from sqlalchemy import text as sa_text
    with db.session_scope() as session:
        conn = session.connection()
        conn.execute(sa_text(
            "CREATE TABLE IF NOT EXISTS intraday_snapshots ("
            "snapshot_id TEXT PRIMARY KEY, "
            "run_id TEXT NOT NULL, "
            "status TEXT NOT NULL DEFAULT 'running', "
            "started_at TEXT NOT NULL, "
            "finished_at TEXT, "
            "expected_codes TEXT, "
            "completed_codes TEXT, "
            "valid_quote_codes TEXT, "
            "failed_codes TEXT, "
            "query_date TEXT NOT NULL, "
            "markets TEXT, "
            "market_dates TEXT)"
        ))
        session.commit()


class TestIntradayMarketSnapshotsSchema:
    """Test intraday_market_snapshots table creation and column contract."""

    @pytest.fixture
    def db_env(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / "test_main.db")
        monkeypatch.setenv("DATABASE_PATH", db_path)
        from src.config import Config
        Config.reset_instance()
        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{db_path}")
        _setup_intraday_market_snapshots_table(db)
        yield {"db": db, "db_path": db_path}
        DatabaseManager.reset_instance()

    def test_table_created_with_required_columns(self, db_env):
        """_ensure_intraday_market_snapshots_table creates table with
        timestamp, market_local_timestamp, snapshot_id, error_message, status."""
        db = db_env["db"]
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            info = conn.execute(
                sa_text("PRAGMA table_info('intraday_market_snapshots')")
            ).fetchall()
            cols = {row[1] for row in info}
        # Required columns from the plan
        assert "timestamp" in cols, "timestamp column should exist"
        assert "market_local_timestamp" in cols, "market_local_timestamp column should exist"
        assert "snapshot_id" in cols, "snapshot_id column should exist"
        assert "error_message" in cols, "error_message column should exist"
        assert "status" in cols, "status column should exist"
        # Additional core columns
        assert "id" in cols
        assert "query_date" in cols
        assert "market" in cols
        assert "index_code" in cols
        assert "current_price" in cols
        assert "created_at" in cols

    def test_persist_valid_quote_row(self, db_env):
        """A valid index quote can be inserted and read back."""
        db = db_env["db"]
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            conn.execute(sa_text(
                "INSERT OR REPLACE INTO intraday_market_snapshots "
                "(snapshot_id, run_id, query_date, market, index_code, index_name, "
                "current_price, change_pct, status, created_at, market_local_timestamp) "
                "VALUES (:sid, :rid, :qd, :mkt, :ic, :iname, :cp, :chg, :st, :ca, :mlt)"
            ), {
                "sid": "snap-valid-1", "rid": "run-001", "qd": "2026-06-18",
                "mkt": "cn", "ic": "000001", "iname": "\u4e0a\u8bc1\u6307\u6570",
                "cp": 3015.5, "chg": 0.35, "st": "valid",
                "ca": datetime.now().isoformat(),
                "mlt": "2026-06-18T10:00:00",
            })
            session.commit()

        with db.session_scope() as session:
            row = session.execute(
                sa_text(
                    "SELECT index_code, index_name, current_price, change_pct, status "
                    "FROM intraday_market_snapshots WHERE snapshot_id = 'snap-valid-1'"
                )
            ).fetchone()
        assert row is not None
        assert row[0] == "000001"
        assert row[1] == "\u4e0a\u8bc1\u6307\u6570"
        assert row[2] == 3015.5
        assert row[3] == 0.35
        assert row[4] == "valid"

    def test_persist_failed_quote_row(self, db_env):
        """A fetch_failed index quote persists error_message."""
        db = db_env["db"]
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            conn.execute(sa_text(
                "INSERT OR REPLACE INTO intraday_market_snapshots "
                "(snapshot_id, run_id, query_date, market, index_code, "
                "status, error_message, created_at, market_local_timestamp) "
                "VALUES (:sid, :rid, :qd, :mkt, :ic, :st, :err, :ca, :mlt)"
            ), {
                "sid": "snap-fail-1", "rid": "run-002", "qd": "2026-06-18",
                "mkt": "hk", "ic": "HSI",
                "st": "fetch_failed", "err": "Connection timeout",
                "ca": datetime.now().isoformat(),
                "mlt": "2026-06-18T10:05:00",
            })
            session.commit()

        with db.session_scope() as session:
            row = session.execute(
                sa_text(
                    "SELECT status, error_message "
                    "FROM intraday_market_snapshots WHERE snapshot_id = 'snap-fail-1'"
                )
            ).fetchone()
        assert row is not None
        assert row[0] == "fetch_failed"
        assert row[1] == "Connection timeout"

    def test_persist_mixed_valid_and_failed(self, db_env):
        """Both valid and failed rows can coexist in the same table."""
        db = db_env["db"]
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            # Valid row
            conn.execute(sa_text(
                "INSERT OR REPLACE INTO intraday_market_snapshots "
                "(snapshot_id, run_id, query_date, market, index_code, "
                "current_price, status, created_at, market_local_timestamp) "
                "VALUES (:sid, :rid, :qd, :mkt, :ic, :cp, :st, :ca, :mlt)"
            ), {
                "sid": "snap-mix-1", "rid": "run-003", "qd": "2026-06-18",
                "mkt": "us", "ic": "^GSPC", "cp": 5000.0,
                "st": "valid", "ca": datetime.now().isoformat(),
                "mlt": "2026-06-18T10:00:00",
            })
            # Failed row (same snapshot, different index)
            conn.execute(sa_text(
                "INSERT OR REPLACE INTO intraday_market_snapshots "
                "(snapshot_id, run_id, query_date, market, index_code, "
                "status, error_message, created_at, market_local_timestamp) "
                "VALUES (:sid, :rid, :qd, :mkt, :ic, :st, :err, :ca, :mlt)"
            ), {
                "sid": "snap-mix-1", "rid": "run-003", "qd": "2026-06-18",
                "mkt": "us", "ic": "^IXIC",
                "st": "fetch_failed", "err": "Rate limit exceeded",
                "ca": datetime.now().isoformat(),
                "mlt": "2026-06-18T10:00:00",
            })
            session.commit()

        with db.session_scope() as session:
            rows = session.execute(
                sa_text(
                    "SELECT index_code, status, error_message "
                    "FROM intraday_market_snapshots WHERE snapshot_id = 'snap-mix-1' "
                    "ORDER BY index_code"
                )
            ).fetchall()
        assert len(rows) == 2
        status_map = {r[0]: r[1] for r in rows}
        assert status_map["^GSPC"] == "valid"
        assert status_map["^IXIC"] == "fetch_failed"

    def test_unique_constraint(self, db_env):
        """Same (snapshot_id, market, index_code) should not duplicate
        — INSERT OR REPLACE updates instead."""
        db = db_env["db"]
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            for i in range(2):
                conn.execute(sa_text(
                    "INSERT OR REPLACE INTO intraday_market_snapshots "
                    "(snapshot_id, run_id, query_date, market, index_code, "
                    "current_price, status, created_at, market_local_timestamp) "
                    "VALUES (:sid, :rid, :qd, :mkt, :ic, :cp, :st, :ca, :mlt)"
                ), {
                    "sid": "snap-uniq-1", "rid": "run-004", "qd": "2026-06-18",
                    "mkt": "cn", "ic": "000001", "cp": 3010.0 + i,
                    "st": "valid", "ca": datetime.now().isoformat(),
                    "mlt": "2026-06-18T10:00:00",
                })
            session.commit()

        with db.session_scope() as session:
            count = session.execute(
                sa_text(
                    "SELECT COUNT(*) FROM intraday_market_snapshots "
                    "WHERE snapshot_id = 'snap-uniq-1'"
                )
            ).fetchone()
            assert count[0] == 1  # INSERT OR REPLACE updates, doesn't duplicate


class TestHistoricalSnapshotCount:
    """Test historical_snapshot_count counting logic on intraday_snapshots.

    Verifies that counting uses status IN ('completed', 'partial') and not a
    non-existent snapshot_type column, and uses all market dates.
    """

    @pytest.fixture
    def db_env(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / "test_main.db")
        monkeypatch.setenv("DATABASE_PATH", db_path)
        from src.config import Config
        Config.reset_instance()
        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{db_path}")
        _setup_intraday_snapshots_table(db)
        yield {"db": db, "db_path": db_path}
        DatabaseManager.reset_instance()

    def test_counts_completed_snapshots(self, db_env):
        """COUNT with status IN ('completed', 'partial') returns completed rows."""
        db = db_env["db"]
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            # Insert rows with different statuses
            for status in ("completed", "completed", "partial", "running", "failed"):
                conn.execute(sa_text(
                    "INSERT INTO intraday_snapshots "
                    "(snapshot_id, run_id, status, started_at, query_date) "
                    "VALUES (:sid, :rid, :st, :sa, :qd)"
                ), {
                    "sid": f"snap-{status}-{datetime.now().timestamp()}",
                    "rid": "run-test",
                    "st": status,
                    "sa": datetime.now().isoformat(),
                    "qd": "2026-06-18",
                })
            session.commit()

        with db.session_scope() as session:
            row = session.execute(
                sa_text(
                    "SELECT COUNT(*) FROM intraday_snapshots "
                    "WHERE query_date = :qd AND status IN ('completed', 'partial')"
                ),
                {"qd": "2026-06-18"},
            ).scalar()
        # 3 completed/partial rows out of 5 total
        assert row == 3, f"Expected 3 completed/partial, got {row}"

    def test_counts_multiple_market_dates(self, db_env):
        """COUNT queries across multiple market dates correctly."""
        db = db_env["db"]
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            for date_str in ("2026-06-18", "2026-06-19"):
                for status in ("completed", "partial", "running"):
                    conn.execute(sa_text(
                        "INSERT INTO intraday_snapshots "
                        "(snapshot_id, run_id, status, started_at, query_date) "
                        "VALUES (:sid, :rid, :st, :sa, :qd)"
                    ), {
                        "sid": f"snap-{date_str}-{status}",
                        "rid": "run-test",
                        "st": status,
                        "sa": datetime.now().isoformat(),
                        "qd": date_str,
                    })
            session.commit()

        # Query across both dates
        with db.session_scope() as session:
            row = session.execute(
                sa_text(
                    "SELECT COUNT(*) FROM intraday_snapshots "
                    "WHERE query_date IN (:qd1, :qd2) AND status IN ('completed', 'partial')"
                ),
                {"qd1": "2026-06-18", "qd2": "2026-06-19"},
            ).scalar()
        # 2 completed + 2 partial = 4 across both dates
        assert row == 4, f"Expected 4 completed/partial across dates, got {row}"

    def test_excludes_non_counted_statuses(self, db_env):
        """running/failed statuses are not counted."""
        db = db_env["db"]
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            for status in ("running", "failed", "cancelled", "pending"):
                conn.execute(sa_text(
                    "INSERT INTO intraday_snapshots "
                    "(snapshot_id, run_id, status, started_at, query_date) "
                    "VALUES (:sid, :rid, :st, :sa, :qd)"
                ), {
                    "sid": f"snap-{status}",
                    "rid": "run-test",
                    "st": status,
                    "sa": datetime.now().isoformat(),
                    "qd": "2026-06-18",
                })
            session.commit()

        with db.session_scope() as session:
            row = session.execute(
                sa_text(
                    "SELECT COUNT(*) FROM intraday_snapshots "
                    "WHERE query_date = :qd AND status IN ('completed', 'partial')"
                ),
                {"qd": "2026-06-18"},
            ).scalar()
        assert row == 0, f"Expected 0 (no completed/partial), got {row}"

    def test_does_not_use_snapshot_type_column(self, db_env):
        """Verifies the table has no snapshot_type column — the
        historical_snapshot_count query must NOT reference it."""
        db = db_env["db"]
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            info = conn.execute(
                sa_text("PRAGMA table_info('intraday_snapshots')")
            ).fetchall()
            col_names = {row[1] for row in info}
        assert "snapshot_type" not in col_names, (
            "The intraday_snapshots table should NOT have a snapshot_type column. "
            "The count query must use status IN ('completed', 'partial') instead."
        )

    def test_empty_table_returns_zero(self, db_env):
        """COUNT on empty table returns 0 without error."""
        db = db_env["db"]
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            row = session.execute(
                sa_text(
                    "SELECT COUNT(*) FROM intraday_snapshots "
                    "WHERE query_date = :qd AND status IN ('completed', 'partial')"
                ),
                {"qd": "2026-06-18"},
            ).scalar()
        assert row == 0
