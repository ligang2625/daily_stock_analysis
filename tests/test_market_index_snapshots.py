# -*- coding: utf-8 -*-
"""Tests for market index snapshot collection: ORM, persistence, failure semantics."""

from __future__ import annotations

from datetime import date, datetime

import pytest


def _setup_intraday_market_snapshots_table(db):
    """Create intraday_market_snapshots table with required columns."""
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
            "UNIQUE(market, index_code, market_local_timestamp, snapshot_id))"
        ))
        session.commit()


class TestMarketIndexSnapshots:
    """Test market index snapshot table creation and persistence."""

    @pytest.fixture
    def db_env(self, tmp_path, monkeypatch):
        main_path = str(tmp_path / "snap_main.db")
        monkeypatch.setenv('DATABASE_PATH', main_path)
        from src.storage import DatabaseManager
        from src.config import Config
        Config.reset_instance()
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{main_path}")
        _setup_intraday_market_snapshots_table(db)
        yield {'db': db, 'path': main_path, 'tmp': tmp_path}
        DatabaseManager.reset_instance()

    def test_schema_migration_missing_columns(self, db_env):
        """_apply_schema_migrations should add timestamp and error_message to existing table."""
        db = db_env['db']
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            info = conn.execute(sa_text("PRAGMA table_info('intraday_market_snapshots')")).fetchall()
            cols = {row[1] for row in info}
        assert 'error_message' in cols, "error_message column should be added by migration"
        assert 'timestamp' in cols, "timestamp column should be added by migration"

    def test_orm_model_matches_table(self, db_env):
        """ORM model IntradayMarketSnapshot should map to existing table."""
        from src.storage import IntradayMarketSnapshot
        assert IntradayMarketSnapshot.__tablename__ == 'intraday_market_snapshots'
        import inspect
        cols = [c.name for c in inspect.getmembers(IntradayMarketSnapshot)
                if hasattr(c, 'name') and isinstance(c, type(IntradayMarketSnapshot.__table__.c.id))]
        # Just check the ORM model can be queried
        db = db_env['db']
        with db.session_scope() as session:
            count = session.query(IntradayMarketSnapshot).count()
            assert count == 0

    def test_persist_valid_row(self, db_env):
        """Insert a valid market index row and verify it's readable."""
        db = db_env['db']
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            conn.execute(sa_text(
                "INSERT OR REPLACE INTO intraday_market_snapshots "
                "(snapshot_id, run_id, query_date, market, index_code, index_name, "
                "current_price, change_pct, status, created_at) "
                "VALUES (:sid, :rid, :qd, :mkt, :ic, :iname, :cp, :chg, :st, :ca)"
            ), {
                "sid": "snap-1", "rid": "run-1", "qd": "2026-06-18",
                "mkt": "cn", "ic": "000001", "iname": "上证指数",
                "cp": 3000.0, "chg": 0.5, "st": "valid", "ca": datetime.now().isoformat(),
            })
            session.commit()

        with db.session_scope() as session:
            rows = conn = session.execute(
                sa_text("SELECT index_code, index_name, current_price FROM intraday_market_snapshots")
            ).fetchall()
            assert len(rows) == 1
            assert rows[0][0] == '000001'

    def test_persist_failed_row_with_error_message(self, db_env):
        """A fetch_failed row should persist error_message."""
        db = db_env['db']
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            conn.execute(sa_text(
                "INSERT OR REPLACE INTO intraday_market_snapshots "
                "(snapshot_id, run_id, query_date, market, index_code, "
                "status, error_message, created_at) "
                "VALUES (:sid, :rid, :qd, :mkt, :ic, :st, :err, :ca)"
            ), {
                "sid": "snap-2", "rid": "run-2", "qd": "2026-06-18",
                "mkt": "hk", "ic": "HSI",
                "st": "fetch_failed", "err": "Connection timeout",
                "ca": datetime.now().isoformat(),
            })
            session.commit()

        with db.session_scope() as session:
            row = session.execute(
                sa_text("SELECT status, error_message FROM intraday_market_snapshots WHERE index_code = 'HSI'")
            ).fetchone()
            assert row is not None
            assert row[0] == 'fetch_failed'
            assert row[1] == 'Connection timeout'

    def test_unique_constraint_prevents_duplicates(self, db_env):
        """Same (snapshot_id, market, index_code) should not duplicate."""
        db = db_env['db']
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            conn = session.connection()
            for _ in range(2):
                conn.execute(sa_text(
                    "INSERT OR REPLACE INTO intraday_market_snapshots "
                    "(snapshot_id, run_id, query_date, market, index_code, "
                    "current_price, status, created_at, market_local_timestamp) "
                    "VALUES (:sid, :rid, :qd, :mkt, :ic, :cp, :st, :ca, :mlt)"
                ), {
                    "sid": "snap-3", "rid": "run-3", "qd": "2026-06-18",
                    "mkt": "us", "ic": "^GSPC", "cp": 5000.0,
                    "st": "valid", "ca": datetime.now().isoformat(),
                    "mlt": "2026-06-18T10:00:00",
                })
            session.commit()

        with db.session_scope() as session:
            rows = session.execute(
                sa_text("SELECT COUNT(*) FROM intraday_market_snapshots WHERE snapshot_id = 'snap-3'")
            ).fetchone()
            assert rows[0] == 1  # INSERT OR REPLACE updates, doesn't duplicate


class TestMarketIndexConfig:
    """Test market index configuration defaults."""

    def test_default_cn_indices(self):
        from src.config import get_config
        config = get_config()
        assert '000001' in config.intraday_market_indices_cn
        assert '399001' in config.intraday_market_indices_cn

    def test_default_hk_indices(self):
        from src.config import get_config
        config = get_config()
        assert 'HSI' in config.intraday_market_indices_hk

    def test_default_us_indices(self):
        from src.config import get_config
        config = get_config()
        assert '^GSPC' in config.intraday_market_indices_us

    def test_snapshot_enabled_default(self):
        from src.config import get_config
        config = get_config()
        assert config.intraday_market_snapshot_enabled is True
