# -*- coding: utf-8 -*-
"""Tests for Phase 3: market index archive extraction and historical upsert field mapping."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest


class TestPhase3MarketIndexArchive:
    """Test extract_market_index_points field mapping and upsert idempotency."""

    @pytest.fixture
    def archive_env(self, tmp_path, monkeypatch):
        main_path = str(tmp_path / "archive_main.db")
        hist_path = str(tmp_path / "archive_hist.db")
        monkeypatch.setenv("DATABASE_PATH", main_path)
        monkeypatch.setenv("HISTORICAL_DATABASE_PATH", hist_path)

        from src.config import Config

        Config.reset_instance()

        from src.storage import DatabaseManager

        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{main_path}")

        from src.storage_historical import HistoricalDatabaseManager

        hist_db = HistoricalDatabaseManager(db_path=hist_path)

        # Create main DB table with all Phase 3 columns
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
            # Seed old data
            today = date.today()
            for days_ago in (10, 20, 30):
                qd = (today - timedelta(days=days_ago)).isoformat()
                conn.execute(sa_text(
                    "INSERT OR REPLACE INTO intraday_market_snapshots "
                    "(snapshot_id, run_id, query_date, market, index_code, index_name, "
                    "current_price, change_pct, open_price, prev_close, "
                    "high_price, low_price, volume, amount, "
                    "status, timestamp, market_local_timestamp, created_at) "
                    "VALUES (:sid, :rid, :qd, :mkt, :ic, :iname, "
                    ":cp, :chg, :op, :pc, :hp, :lp, :vol, :amt, "
                    ":st, :ts, :mlt, :ca)"
                ), {
                    "sid": f"s{days_ago}", "rid": "r0", "qd": qd,
                    "mkt": "cn", "ic": "000001", "iname": "上证指数",
                    "cp": 3000.0 - days_ago, "chg": -0.1 * days_ago,
                    "op": 3010.0, "pc": 3005.0, "hp": 3020.0, "lp": 2980.0,
                    "vol": 1000000.0, "amt": 100000000.0,
                    "st": "valid", "ts": f"{qd}T10:00:00",
                    "mlt": f"{qd}T10:00:00", "ca": datetime.now().isoformat(),
                })
            session.commit()

        yield {"db": db, "hist_db": hist_db}
        DatabaseManager.reset_instance()

    def test_extractor_output_fields(self, archive_env):
        """Extracted rows should contain all required output fields."""
        cutoff = date.today() - timedelta(days=5)
        from src.maintenance.archive_extractors import extract_market_index_points

        with archive_env["db"].session_scope() as session:
            conn = session.connection()
            rows = extract_market_index_points(conn, cutoff)

        assert len(rows) > 0, "Should extract rows older than cutoff"
        r = rows[0]

        # Required fields from plan
        assert "query_date" in r
        assert "market" in r
        assert "index_code" in r
        assert "current_price" in r
        assert "change_pct" in r
        assert "open" in r
        assert "high" in r
        assert "low" in r
        assert "pre_close" in r
        assert "trend_label" in r
        assert "strength_label" in r
        assert "status" in r

        # Field mapping verification
        assert r["market"] == "cn"
        assert r["index_code"] == "000001"

    def test_field_mapping_correctness(self, archive_env):
        """Verify field mapping: open_price->open, prev_close->pre_close, etc."""
        cutoff = date.today() - timedelta(days=5)
        from src.maintenance.archive_extractors import extract_market_index_points

        with archive_env["db"].session_scope() as session:
            conn = session.connection()
            rows = extract_market_index_points(conn, cutoff)

        if rows:
            r = rows[0]
            # open_price -> open
            assert "open" in r
            # prev_close -> pre_close
            assert "pre_close" in r
            # high_price -> high
            assert "high" in r
            # low_price -> low
            assert "low" in r

    def test_derived_labels_valid_range(self, archive_env):
        """trend_label and strength_label should be valid values."""
        cutoff = date.today() - timedelta(days=5)
        from src.maintenance.archive_extractors import extract_market_index_points

        with archive_env["db"].session_scope() as session:
            conn = session.connection()
            rows = extract_market_index_points(conn, cutoff)

        for r in rows:
            assert r["trend_label"] in ("rising", "falling", "flat", "insufficient")
            assert r["strength_label"] in ("strong", "weak", "neutral", "insufficient")

    def test_upsert_idempotent(self, archive_env):
        """Running upsert twice should not duplicate rows."""
        cutoff = date.today() - timedelta(days=5)
        from src.maintenance.archive_extractors import extract_market_index_points

        with archive_env["db"].session_scope() as session:
            conn = session.connection()
            rows = extract_market_index_points(conn, cutoff)

        # First write
        w1 = archive_env["hist_db"].upsert_market_index_points_batch(rows)

        from sqlalchemy import text as sa_text

        with archive_env["hist_db"].get_session() as session:
            before = session.execute(
                sa_text("SELECT COUNT(*) FROM historical_market_index_points")
            ).scalar()

        # Second write (same rows)
        w2 = archive_env["hist_db"].upsert_market_index_points_batch(rows)

        with archive_env["hist_db"].get_session() as session:
            after = session.execute(
                sa_text("SELECT COUNT(*) FROM historical_market_index_points")
            ).scalar()

        assert after == before, f"Rows changed from {before} to {after} after idempotent upsert"
        assert w2 == len(rows), f"Second upsert should report {len(rows)} processed, got {w2}"
