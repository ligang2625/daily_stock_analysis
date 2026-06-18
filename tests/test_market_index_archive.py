# -*- coding: utf-8 -*-
"""Tests for market index archive extraction and historical DB upsert."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest


class TestMarketIndexArchive:
    """Test market index archive extraction and historical DB upsert."""

    @pytest.fixture
    def archive_env(self, tmp_path, monkeypatch):
        main_path = str(tmp_path / "archive_main.db")
        hist_path = str(tmp_path / "archive_hist.db")
        monkeypatch.setenv('DATABASE_PATH', main_path)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', hist_path)

        from src.config import Config
        Config.reset_instance()

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{main_path}")

        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=hist_path)

        # Create intraday_market_snapshots table with all Phase 3 columns
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
            # Seed data
            today = date.today()
            for days_ago in (7, 10, 15, 30):
                qd = (today - timedelta(days=days_ago)).isoformat()
                snap_id = days_ago * 100
                conn.execute(sa_text(
                    "INSERT OR REPLACE INTO intraday_market_snapshots "
                    "(snapshot_id, run_id, query_date, market, index_code, index_name, "
                    "current_price, change_pct, open_price, high_price, low_price, prev_close, "
                    "volume, amount, status, created_at) "
                    "VALUES (:sid, :rid, :qd, :mkt, :ic, :iname, :cp, :chg, :op, :hp, :lp, :pc, "
                    ":vol, :amt, :st, :ca)"
                ), {
                    "sid": f"s{snap_id}", "rid": "r0", "qd": qd,
                    "mkt": "cn", "ic": "000001", "iname": "上证指数",
                    "cp": 3000.0 + days_ago, "chg": 0.1 * days_ago,
                    "op": 2990.0, "hp": 3010.0, "lp": 2980.0, "pc": 2995.0,
                    "vol": 1000000.0, "amt": 100000000.0, "st": "valid",
                    "ca": datetime.now().isoformat(),
                })
            session.commit()

        yield {'db': db, 'hist_db': hist_db}
        DatabaseManager.reset_instance()

    def test_extractor_reads_with_cutoff(self, archive_env):
        """Extractor should only read rows with query_date < cutoff."""
        db = archive_env['db']
        cutoff = date.today() - timedelta(days=9)
        from src.maintenance.archive_extractors import extract_market_index_points
        with db.session_scope() as session:
            conn = session.connection()
            rows = extract_market_index_points(conn, cutoff)
        # 4 rows total (7,10,15,30 days ago), cutoff=9: 3 rows (10,15,30) < cutoff
        assert len(rows) == 3, f"Expected 3 rows older than {cutoff}, got {len(rows)}"
        for r in rows:
            assert r['query_date'] < cutoff.isoformat(), f"Row {r['query_date']} not older than {cutoff}"

    def test_extractor_field_mapping(self, archive_env):
        """Extractor should map fields correctly: open_price→open, prev_close→pre_close, etc."""
        db = archive_env['db']
        cutoff = date.today() - timedelta(days=5)
        from src.maintenance.archive_extractors import extract_market_index_points
        with db.session_scope() as session:
            conn = session.connection()
            rows = extract_market_index_points(conn, cutoff)
        if rows:
            r = rows[0]
            assert 'open' in r
            assert 'pre_close' in r
            assert 'high' in r
            assert 'low' in r
            assert r.get('market') == 'cn'
            assert r.get('index_code') == '000001'

    def test_derived_labels(self, archive_env):
        """Extractor should compute trend_label and strength_label."""
        db = archive_env['db']
        cutoff = date.today() - timedelta(days=5)
        from src.maintenance.archive_extractors import extract_market_index_points
        with db.session_scope() as session:
            conn = session.connection()
            rows = extract_market_index_points(conn, cutoff)
        if rows:
            for r in rows:
                assert 'trend_label' in r
                assert 'strength_label' in r
                assert r['trend_label'] in ('rising', 'falling', 'flat', 'insufficient')

    def test_upsert_idempotent(self, archive_env):
        """Running upsert twice should not duplicate rows."""
        db = archive_env['db']
        hist_db = archive_env['hist_db']
        cutoff = date.today() - timedelta(days=5)
        from src.maintenance.archive_extractors import extract_market_index_points
        with db.session_scope() as session:
            conn = session.connection()
            rows = extract_market_index_points(conn, cutoff)

        w1 = hist_db.upsert_market_index_points_batch(rows)
        # Count using raw SQL
        from sqlalchemy import text as sa_text
        with hist_db.get_session() as session:
            before = session.execute(sa_text("SELECT COUNT(*) FROM historical_market_index_points")).scalar()

        w2 = hist_db.upsert_market_index_points_batch(rows)

        with hist_db.get_session() as session:
            after = session.execute(sa_text("SELECT COUNT(*) FROM historical_market_index_points")).scalar()
        # Same rows, idempotent upsert: count should not change after second write
        assert after == before, f"Rows changed from {before} to {after} after idempotent upsert"
        assert w2 == len(rows), f"Second upsert should report {len(rows)} processed, got {w2}"

    def test_historical_table_create(self, archive_env):
        """Historical DB should have historical_market_index_points table."""
        hist_db = archive_env['hist_db']
        from sqlalchemy import text as sa_text
        with hist_db.get_session() as session:
            info = session.execute(
                sa_text("PRAGMA table_info('historical_market_index_points')")
            ).fetchall()
            cols = {row[1] for row in info}
        assert 'query_date' in cols
        assert 'index_code' in cols
        assert 'trend_label' in cols
        assert 'strength_label' in cols
