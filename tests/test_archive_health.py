# -*- coding: utf-8 -*-
"""Tests for archive health module (Phase 4)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

import pytest

from src.config import Config


class TestArchiveHealth:
    """Test archive_health module."""

    @pytest.fixture
    def health_env(self, tmp_path, monkeypatch):
        """Setup clean temp databases."""
        main_db = str(tmp_path / "health_main.db")
        historical_db = str(tmp_path / "health_historical.db")

        monkeypatch.setenv('DATABASE_PATH', main_db)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db)

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        Config.reset_instance()

        yield {
            'main_db': main_db,
            'historical_db': historical_db,
            'tmp_path': tmp_path,
        }

        DatabaseManager.reset_instance()
        Config.reset_instance()

    def test_no_runs_fresh_db(self, health_env):
        """Health check on a database with no archive runs."""
        from src.maintenance.archive_health import check_archive_health

        report = check_archive_health(max_stale_hours=48)

        assert 'status' in report
        # Should be warning since no runs exist
        assert report['status'] == 'warning'
        assert report['last_successful_run'] is None
        assert len(report['warnings']) >= 1

    def test_recent_success_status(self, health_env):
        """Health check after a recent successful run."""
        from src.maintenance.archive_health import check_archive_health
        from src.storage_historical import HistoricalDatabaseManager

        historical_db = HistoricalDatabaseManager(
            db_path=health_env['historical_db']
        )
        run_id = historical_db.create_archive_run(
            cutoff_date='2026-06-15',
            main_db_path=health_env['main_db'],
            historical_db_path=health_env['historical_db'],
        )
        historical_db.finish_archive_run(
            run_id=run_id,
            success=True,
            rows_read=10,
            rows_written=5,
            rows_deleted=3,
        )

        report = check_archive_health(max_stale_hours=48)
        assert report['status'] == 'ok'
        assert report['last_successful_run'] is not None
        assert report['last_successful_run']['status'] == 'success' or \
               report['last_successful_run'].get('rows_read') == 10

    def test_latest_run_failed(self, health_env):
        """Health check when latest run failed."""
        from src.maintenance.archive_health import check_archive_health
        from src.storage_historical import HistoricalDatabaseManager

        historical_db = HistoricalDatabaseManager(
            db_path=health_env['historical_db']
        )
        run_id = historical_db.create_archive_run(
            cutoff_date='2026-06-15',
            main_db_path=health_env['main_db'],
            historical_db_path=health_env['historical_db'],
        )
        historical_db.finish_archive_run(
            run_id=run_id,
            success=False,
            rows_read=5,
            rows_written=0,
            rows_deleted=0,
            error_message='Test failure',
        )

        report = check_archive_health(max_stale_hours=48)
        assert report['status'] == 'error'
        assert len(report['errors']) >= 1

    def test_stale_success_with_rows(self, health_env):
        """Health check when last success is stale and pending rows exist."""
        from src.maintenance.archive_health import check_archive_health
        from src.storage_historical import HistoricalDatabaseManager
        from src.storage import DatabaseManager

        historical_db = HistoricalDatabaseManager(
            db_path=health_env['historical_db']
        )
        # Create a successful run but set finished_at to 100 hours ago
        run_id = historical_db.create_archive_run(
            cutoff_date='2026-06-14',
            main_db_path=health_env['main_db'],
            historical_db_path=health_env['historical_db'],
        )

        # Manually update the run to simulate an old successful run
        session = historical_db.get_session()
        try:
            from sqlalchemy import update
            from src.storage_historical import ArchiveRun
            old_time = datetime.now() - timedelta(hours=100)
            session.execute(
                update(ArchiveRun)
                .where(ArchiveRun.id == run_id)
                .values(
                    finished_at=old_time,
                    status='success',
                    rows_read=100,
                    rows_written=50,
                    rows_deleted=30,
                )
            )
            session.commit()
        finally:
            session.close()

        # Seed main DB with old rows to create pending archive candidates
        from sqlalchemy import text as sa_text
        db = DatabaseManager.get_instance()
        with db.session_scope() as session:
            old_date = (datetime.now() - timedelta(days=10)).isoformat()
            session.execute(sa_text(
                f"INSERT INTO news_intel (code, title, url, fetched_at) "
                f"VALUES ('600519', 'Test', 'http://example.com/test', '{old_date}')"
            ))

        report = check_archive_health(max_stale_hours=48)
        assert report['status'] in ('error', 'warning')
        assert report['pending_archive_rows'] >= 1

    def test_json_output(self, health_env, capsys):
        """CLI produces valid JSON."""
        from src.maintenance.archive_health import main as health_main

        exit_code = health_main(['--max-stale-hours', '48', '--format', 'json', '--soft-fail'])
        captured = capsys.readouterr()
        assert exit_code == 0
        parsed = json.loads(captured.out)
        assert 'status' in parsed

    def test_exit_code_nonzero_without_soft_fail(self, health_env):
        """Exit code is non-zero on warning without --soft-fail."""
        from src.maintenance.archive_health import main as health_main

        # Fresh DB with no runs should return non-zero exit
        exit_code = health_main(['--max-stale-hours', '48', '--format', 'json'])
        assert exit_code != 0
