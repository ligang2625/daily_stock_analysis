# -*- coding: utf-8 -*-
"""Integration tests for archive Phase 2: dry-run, technical-only, full archive, failure protection."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

import pytest


class TestArchivePhase2Integration:
    """Test archive command integration with Phase 2 extraction."""

    @pytest.fixture
    def archive_env(self, tmp_path, monkeypatch):
        """Setup main + historical databases with seed data for integration tests."""
        main_db = str(tmp_path / "archive_int_main.db")
        historical_db = str(tmp_path / "archive_int_hist.db")
        monkeypatch.setenv('DATABASE_PATH', main_db)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db)

        from src.config import Config
        Config.reset_instance()
        # Also set class-level defaults so new instances pick up the right paths
        monkeypatch.setattr(Config, 'database_path', main_db)
        monkeypatch.setattr(Config, 'historical_database_path', historical_db)

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{main_db}")

        # Seed data
        from sqlalchemy import text as sa_text
        old_date = datetime.now() - timedelta(days=30)
        recent_date = datetime.now()

        with db.session_scope() as session:
            # stock_daily
            for i in range(3):
                session.execute(
                    sa_text(
                        "INSERT INTO stock_daily (code, code_norm, date, close, data_source, created_at) "
                        "VALUES (:code, :cn, :date, :close, :src, :ca)"
                    ),
                    {"code": "600519", "cn": "600519",
                     "date": (date.today() - timedelta(days=10 + i)).isoformat(),
                     "close": 100.0 + i, "src": "test",
                     "ca": old_date - timedelta(days=i)},
                )

            # analysis_history payload for cleanup
            for i in range(2):
                session.execute(
                    sa_text(
                        "INSERT INTO analysis_history (code, name, report_type, "
                        "sentiment_score, operation_advice, trend_prediction, "
                        "analysis_summary, raw_result, created_at) "
                        "VALUES (:code, :name, :rtype, :score, :advice, :trend, "
                        ":summary, :raw, :ca)"
                    ),
                    {"code": "000001", "name": "test",
                     "rtype": None, "score": 50, "advice": "hold",
                     "trend": "neutral", "summary": "test",
                     "raw": "{}", "ca": old_date},
                )
                session.execute(
                    sa_text(
                        "INSERT INTO analysis_history_payload (history_id, raw_result, compressed, created_at) "
                        "VALUES (:hid, :raw, 0, :ca)"
                    ),
                    {"hid": i + 1, "raw": "{}", "ca": old_date},
                )

            # news_intel for cleanup
            session.execute(
                sa_text(
                    "INSERT INTO news_intel (code, title, url, fetched_at) "
                    "VALUES (:code, :title, :url, :fetched_at)"
                ),
                {"code": "600519", "title": "Old news", "url": "http://x.com/old",
                 "fetched_at": old_date},
            )

        yield {
            'db': db, 'main_path': main_db, 'hist_path': historical_db,
            'tmp_path': tmp_path,
        }

        DatabaseManager.reset_instance()

    # ------------------------------------------------------------------
    # dry-run
    # ------------------------------------------------------------------

    def test_archive_dry_run_no_writes_no_deletes(self, archive_env):
        """--dry-run should count but not write or delete."""
        from src.maintenance.archive import run_archive

        rc = run_archive(days=1, dry_run=True)
        assert rc == 0

        # Verify no data was deleted
        db = archive_env['db']
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            # stock_daily should still have 3 rows
            count = session.execute(
                sa_text("SELECT COUNT(*) FROM stock_daily")
            ).fetchone()
            assert count[0] == 3

            # analysis_history should still have rows
            ah_count = session.execute(
                sa_text("SELECT COUNT(*) FROM analysis_history")
            ).fetchone()
            assert ah_count[0] >= 2

            # payload should still have rows
            ap_count = session.execute(
                sa_text("SELECT COUNT(*) FROM analysis_history_payload")
            ).fetchone()
            assert ap_count[0] >= 2

            # news_intel should still have rows
            ni_count = session.execute(
                sa_text("SELECT COUNT(*) FROM news_intel")
            ).fetchone()
            assert ni_count[0] >= 1

        # Historical DB should have an archive run logged
        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=archive_env['hist_path'])
        with hist_db.get_session() as hsession:
            from sqlalchemy import text, desc
            result = hsession.execute(
                text("SELECT status FROM archive_runs ORDER BY id DESC LIMIT 1")
            ).fetchone()
        assert result is not None
        assert result[0] == 'success'

    # ------------------------------------------------------------------
    # technical-only
    # ------------------------------------------------------------------

    def test_archive_technical_only_writes_hist_not_delete_main(self, archive_env):
        """--technical-only should extract to historical DB but not clean main DB."""
        from src.maintenance.archive import run_archive

        rc = run_archive(days=1, technical_only=True)
        assert rc == 0

        # Check historical DB has stock_daily data
        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=archive_env['hist_path'])

        with hist_db.get_session() as session:
            from sqlalchemy import text
            count = session.execute(
                text("SELECT COUNT(*) FROM historical_stock_daily")
            ).fetchone()
        assert count[0] >= 1  # stock_daily should have been extracted

        # Main DB should still have all data
        db = archive_env['db']
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            sd_count = session.execute(
                sa_text("SELECT COUNT(*) FROM stock_daily")
            ).fetchone()
            assert sd_count[0] == 3  # not deleted

            ap_count = session.execute(
                sa_text("SELECT COUNT(*) FROM analysis_history_payload")
            ).fetchone()
            assert ap_count[0] == 2  # not deleted

    # ------------------------------------------------------------------
    # validate-only
    # ------------------------------------------------------------------

    def test_archive_validate_only_reads_not_writes(self, archive_env):
        """--validate-only should run Phase 2 extraction in dry-run mode and stop."""
        from src.maintenance.archive import run_archive

        rc = run_archive(days=1, validate_only=True)
        assert rc == 0

        # Historical DB should have NO data written
        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=archive_env['hist_path'])
        with hist_db.get_session() as session:
            from sqlalchemy import text
            count = session.execute(
                text("SELECT COUNT(*) FROM historical_stock_daily")
            ).fetchone()
        assert count[0] == 0  # validate-only does not write

        # Main DB should be untouched
        db = archive_env['db']
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            sd_count = session.execute(
                sa_text("SELECT COUNT(*) FROM stock_daily")
            ).fetchone()
            assert sd_count[0] == 3

    # ------------------------------------------------------------------
    # skip-technical-archive
    # ------------------------------------------------------------------

    def test_archive_skip_technical_archive(self, archive_env):
        """--skip-technical-archive should skip Phase 2 extraction entirely."""
        from src.maintenance.archive import run_archive

        rc = run_archive(days=1, skip_technical_archive=True)
        assert rc == 0

        # Historical DB should have NO technical data written
        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=archive_env['hist_path'])
        with hist_db.get_session() as session:
            from sqlalchemy import text
            count = session.execute(
                text("SELECT COUNT(*) FROM historical_stock_daily")
            ).fetchone()
        assert count[0] == 0

    # ------------------------------------------------------------------
    # full archive
    # ------------------------------------------------------------------

    def test_full_archive_writes_technical_and_cleans_main(self, archive_env):
        """Normal archive should extract technical history then clean main DB."""
        from src.maintenance.archive import run_archive

        rc = run_archive(days=1, skip_vacuum=True)
        assert rc == 0

        # Historical DB should have stock_daily data
        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=archive_env['hist_path'])
        with hist_db.get_session() as session:
            from sqlalchemy import text
            hsd_count = session.execute(
                text("SELECT COUNT(*) FROM historical_stock_daily")
            ).fetchone()
        assert hsd_count[0] >= 1

        # Main DB should have expired rows deleted
        db = archive_env['db']
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            # stock_daily is PROTECTED - should NOT be deleted
            sd_count = session.execute(
                sa_text("SELECT COUNT(*) FROM stock_daily")
            ).fetchone()
            assert sd_count[0] == 3

            # expired payload rows should be deleted
            ap_count = session.execute(
                sa_text("SELECT COUNT(*) FROM analysis_history_payload")
            ).fetchone()
            assert ap_count[0] == 0  # old rows deleted

            # expired news_intel should be deleted
            ni_count = session.execute(
                sa_text("SELECT COUNT(*) FROM news_intel")
            ).fetchone()
            assert ni_count[0] == 0  # old rows deleted

        # Archive run ledger should be successful with rows written
        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=archive_env['hist_path'])
        with hist_db.get_session() as hsession:
            from sqlalchemy import text
            result = hsession.execute(
                text("SELECT status, rows_written FROM archive_runs ORDER BY id DESC LIMIT 1")
            ).fetchone()
        assert result is not None
        assert result[0] == 'success'
        assert result[1] > 0

    # ------------------------------------------------------------------
    # failure protection
    # ------------------------------------------------------------------

    def test_extractor_failure_protects_main_db(self, archive_env, monkeypatch):
        """When Phase 2 extraction fails, main DB should not be modified."""
        # Monkey-patch extract_stock_daily to raise an error
        import src.maintenance.archive_extractors as ext_mod

        original_func = ext_mod.extract_stock_daily

        def failing_extract(session, cutoff_date):
            raise RuntimeError("Simulated extraction failure")

        monkeypatch.setattr(ext_mod, 'extract_stock_daily', failing_extract)

        from src.maintenance.archive import run_archive

        rc = run_archive(days=1)
        assert rc == 1  # Should return failure

        # Verify main DB is untouched
        db = archive_env['db']
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            sd_count = session.execute(
                sa_text("SELECT COUNT(*) FROM stock_daily")
            ).fetchone()
            assert sd_count[0] == 3  # not deleted

            ap_count = session.execute(
                sa_text("SELECT COUNT(*) FROM analysis_history_payload")
            ).fetchone()
            assert ap_count[0] == 2  # not deleted

        # Archive run should be marked failed
        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=archive_env['hist_path'])
        from sqlalchemy import desc, select
        with hist_db.get_session() as session:
            from src.storage_historical import ArchiveRun
            last_run = session.execute(
                select(ArchiveRun).order_by(desc(ArchiveRun.id)).limit(1)
            ).scalars().first()
        assert last_run is not None
        assert last_run.status == 'failed'
        assert 'Simulated extraction failure' in (last_run.error_message or '')
