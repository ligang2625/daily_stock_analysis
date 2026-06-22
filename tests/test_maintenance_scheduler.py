# -*- coding: utf-8 -*-
"""Tests for maintenance scheduler integration (Phase 4)."""

from __future__ import annotations

import os
import time
from unittest.mock import ANY, MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# build_maintenance_background_tasks tests
# ---------------------------------------------------------------------------

class TestBuildMaintenanceBackgroundTasks:
    """Test that build_maintenance_background_tasks produces correct task dicts."""

    @pytest.fixture
    def config_class(self, monkeypatch):
        """Get a fresh config class for task-building tests."""
        from src.config import Config
        Config.reset_instance()
        yield Config
        Config.reset_instance()

    def _make_config(self, config_class, **overrides):
        """Build a Config with specific Phase 4 settings."""
        defaults = {
            'database_path': '/tmp/test_main.db',
            'historical_database_path': '/tmp/test_hist.db',
            'archive_enabled': False,
            'archive_interval_hours': 24,
            'archive_run_on_start': False,
            'archive_skip_vacuum': False,
            'archive_validate_before_cleanup': True,
            'archive_retention_days': 5,
            'db_diagnostics_enabled': False,
            'db_diagnostics_interval_hours': 24,
            'db_diagnostics_output_dir': './logs/db_diagnostics',
            'historical_backup_enabled': False,
            'historical_backup_dir': './data/backups',
            'historical_backup_retention_days': 14,
            'historical_backup_run_before_archive': True,
        }
        defaults.update(overrides)
        return config_class(**defaults)

    def _build_tasks(self, config):
        from main import build_maintenance_background_tasks
        return build_maintenance_background_tasks(config, lambda: config)

    # -- archive_enabled tests --

    def test_archive_disabled_no_task(self, config_class):
        config = self._make_config(config_class, archive_enabled=False)
        tasks = self._build_tasks(config)
        names = [t['name'] for t in tasks]
        assert 'archive_maintenance' not in names

    def test_archive_enabled_creates_task(self, config_class):
        config = self._make_config(config_class, archive_enabled=True)
        tasks = self._build_tasks(config)
        names = [t['name'] for t in tasks]
        assert 'archive_maintenance' in names

    def test_archive_task_correct_interval(self, config_class):
        config = self._make_config(config_class, archive_enabled=True,
                                   archive_interval_hours=12)
        tasks = self._build_tasks(config)
        archive_task = [t for t in tasks if t['name'] == 'archive_maintenance'][0]
        assert archive_task['interval_seconds'] == 12 * 3600

    def test_archive_task_minimum_interval_1_hour(self, config_class):
        config = self._make_config(config_class, archive_enabled=True,
                                   archive_interval_hours=0)
        tasks = self._build_tasks(config)
        archive_task = [t for t in tasks if t['name'] == 'archive_maintenance'][0]
        assert archive_task['interval_seconds'] == 1 * 3600

    def test_archive_task_respects_run_on_start(self, config_class):
        config = self._make_config(config_class, archive_enabled=True,
                                   archive_run_on_start=True)
        tasks = self._build_tasks(config)
        archive_task = [t for t in tasks if t['name'] == 'archive_maintenance'][0]
        assert archive_task['run_immediately'] is True

    def test_archive_task_run_on_start_false(self, config_class):
        config = self._make_config(config_class, archive_enabled=True,
                                   archive_run_on_start=False)
        tasks = self._build_tasks(config)
        archive_task = [t for t in tasks if t['name'] == 'archive_maintenance'][0]
        assert archive_task['run_immediately'] is False

    # -- diagnostics tests --

    def test_diagnostics_disabled_no_task(self, config_class):
        config = self._make_config(config_class, db_diagnostics_enabled=False)
        tasks = self._build_tasks(config)
        names = [t['name'] for t in tasks]
        assert 'db_diagnostics' not in names

    def test_diagnostics_enabled_creates_task(self, config_class):
        config = self._make_config(config_class, db_diagnostics_enabled=True)
        tasks = self._build_tasks(config)
        names = [t['name'] for t in tasks]
        assert 'db_diagnostics' in names

    def test_diagnostics_task_correct_interval(self, config_class):
        config = self._make_config(config_class, db_diagnostics_enabled=True,
                                   db_diagnostics_interval_hours=6)
        tasks = self._build_tasks(config)
        diag_task = [t for t in tasks if t['name'] == 'db_diagnostics'][0]
        assert diag_task['interval_seconds'] == 6 * 3600

    def test_diagnostics_not_run_immediately(self, config_class):
        config = self._make_config(config_class, db_diagnostics_enabled=True)
        tasks = self._build_tasks(config)
        diag_task = [t for t in tasks if t['name'] == 'db_diagnostics'][0]
        assert diag_task['run_immediately'] is False

    # -- combined scenarios --

    def test_both_archive_and_diagnostics_enabled(self, config_class):
        config = self._make_config(config_class, archive_enabled=True,
                                   db_diagnostics_enabled=True)
        tasks = self._build_tasks(config)
        names = [t['name'] for t in tasks]
        assert 'archive_maintenance' in names
        assert 'db_diagnostics' in names
        assert len(tasks) == 2

    def test_all_disabled_returns_empty(self, config_class):
        config = self._make_config(config_class, archive_enabled=False,
                                   db_diagnostics_enabled=False)
        tasks = self._build_tasks(config)
        assert len(tasks) == 0

    # -- backup-before-archive: no separate backup task --

    def test_backup_before_archive_no_separate_task(self, config_class):
        """When backup_before_archive=True AND archive_enabled=True,
        there is only one archive_maintenance task (backup runs inside it)."""
        config = self._make_config(config_class, archive_enabled=True,
                                   historical_backup_enabled=True,
                                   historical_backup_run_before_archive=True)
        tasks = self._build_tasks(config)
        names = [t['name'] for t in tasks]
        assert names.count('archive_maintenance') == 1
        assert 'historical_backup' not in names

    def test_backup_disabled_no_archive_change(self, config_class):
        config = self._make_config(config_class, archive_enabled=True,
                                   historical_backup_enabled=False,
                                   historical_backup_run_before_archive=True)
        tasks = self._build_tasks(config)
        names = [t['name'] for t in tasks]
        assert 'archive_maintenance' in names
        assert 'historical_backup' not in names


# ---------------------------------------------------------------------------
# Archive job: validation preflight + actual archive tests
# ---------------------------------------------------------------------------

class TestArchiveMaintenanceJob:
    """Test that the archive maintenance job correctly validates then archives."""

    @pytest.fixture
    def config_class(self, monkeypatch):
        """Fresh Config for each test."""
        from src.config import Config
        Config.reset_instance()
        yield Config
        Config.reset_instance()

    def _make_config(self, config_class, **overrides):
        defaults = {
            'database_path': '/tmp/test_main.db',
            'historical_database_path': '/tmp/test_hist.db',
            'archive_enabled': True,
            'archive_interval_hours': 24,
            'archive_run_on_start': False,
            'archive_skip_vacuum': False,
            'archive_validate_before_cleanup': True,
            'archive_retention_days': 5,
            'historical_backup_enabled': False,
            'historical_backup_dir': './data/backups',
            'historical_backup_retention_days': 14,
            'historical_backup_run_before_archive': False,
            'historical_backup_required_before_archive': True,
        }
        defaults.update(overrides)
        return config_class(**defaults)

    def _build_and_get_archive_job(self, config):
        from main import build_maintenance_background_tasks
        tasks = build_maintenance_background_tasks(config, lambda: config)
        for t in tasks:
            if t['name'] == 'archive_maintenance':
                return t['task']
        return None

    def test_validation_then_actual_with_validate_enabled(self, config_class):
        """When validate_before_cleanup=True, validation is called first,
        then actual archive."""
        config = self._make_config(config_class,
                                   archive_validate_before_cleanup=True)
        job = self._build_and_get_archive_job(config)
        assert job is not None

        call_order = []

        def fake_run_archive(*, days, dry_run, skip_vacuum, validate_only, **kw):
            call_order.append({
                'days': days,
                'dry_run': dry_run,
                'skip_vacuum': skip_vacuum,
                'validate_only': validate_only,
            })
            return 0

        with patch('src.maintenance.archive.run_archive', side_effect=fake_run_archive):
            job()

        assert len(call_order) == 2
        # First call: validation
        assert call_order[0]['validate_only'] is True
        assert call_order[0]['skip_vacuum'] is True
        assert call_order[0]['dry_run'] is False
        # Second call: actual archive
        assert call_order[1]['validate_only'] is False
        assert call_order[1]['skip_vacuum'] is False  # from config default
        assert call_order[1]['dry_run'] is False

    def test_validation_failure_blocks_actual(self, config_class):
        """When validation fails (returns != 0), actual archive is skipped."""
        config = self._make_config(config_class,
                                   archive_validate_before_cleanup=True)
        job = self._build_and_get_archive_job(config)
        assert job is not None

        call_order = []

        def fake_run_archive(*, days, dry_run, skip_vacuum, validate_only, **kw):
            call_order.append(validate_only)
            return 1  # Simulate validation failure

        with patch('src.maintenance.archive.run_archive', side_effect=fake_run_archive):
            job()

        # Only validation was called, not actual archive
        assert call_order == [True]

    def test_no_validation_when_disabled(self, config_class):
        """When validate_before_cleanup=False, validation is skipped entirely."""
        config = self._make_config(config_class,
                                   archive_validate_before_cleanup=False)
        job = self._build_and_get_archive_job(config)
        assert job is not None

        call_order = []

        def fake_run_archive(*, days, dry_run, skip_vacuum, validate_only, **kw):
            call_order.append(validate_only)
            return 0

        with patch('src.maintenance.archive.run_archive', side_effect=fake_run_archive):
            job()

        # Only actual archive (validate_only=False)
        assert call_order == [False]

    def test_backup_before_archive_when_enabled(self, config_class):
        """When backup_before_archive=True, backup runs before archive."""
        config = self._make_config(
            config_class,
            archive_validate_before_cleanup=False,
            historical_backup_enabled=True,
            historical_backup_run_before_archive=True,
        )
        job = self._build_and_get_archive_job(config)
        assert job is not None

        events = []

        def fake_backup(**kw):
            events.append('backup')
            return 0

        def fake_run_archive(**kw):
            events.append('archive')
            return 0

        with patch('src.maintenance.backup.run_backup', side_effect=fake_backup), \
             patch('src.maintenance.archive.run_archive', side_effect=fake_run_archive):
            job()

        assert events == ['backup', 'archive']

    def test_backup_failure_does_not_block_archive_when_not_required(self, config_class):
        """Backup failure is logged but archive proceeds when not required."""
        config = self._make_config(
            config_class,
            archive_validate_before_cleanup=False,
            historical_backup_enabled=True,
            historical_backup_run_before_archive=True,
            historical_backup_required_before_archive=False,
        )
        job = self._build_and_get_archive_job(config)
        assert job is not None

        events = []

        def fake_backup(**kw):
            events.append('backup')
            raise RuntimeError("Backup failed")

        def fake_run_archive(**kw):
            events.append('archive')
            return 0

        with patch('src.maintenance.backup.run_backup', side_effect=fake_backup), \
             patch('src.maintenance.archive.run_archive', side_effect=fake_run_archive):
            job()

        assert events == ['backup', 'archive']

    def test_backup_failure_blocks_archive_when_required(self, config_class):
        """When backup is required and returns non-zero, archive is skipped."""
        config = self._make_config(
            config_class,
            archive_validate_before_cleanup=False,
            historical_backup_enabled=True,
            historical_backup_run_before_archive=True,
            historical_backup_required_before_archive=True,
        )
        job = self._build_and_get_archive_job(config)
        assert job is not None

        events = []

        def fake_backup(**kw):
            events.append('backup')
            return 1  # Simulate backup failure

        def fake_run_archive(**kw):
            events.append('archive')
            return 0

        with patch('src.maintenance.backup.run_backup', side_effect=fake_backup), \
             patch('src.maintenance.archive.run_archive', side_effect=fake_run_archive):
            job()

        assert events == ['backup']  # archive NOT called

    def test_backup_failure_allows_archive_when_not_required(self, config_class):
        """When backup is not required and returns non-zero, archive proceeds."""
        config = self._make_config(
            config_class,
            archive_validate_before_cleanup=False,
            historical_backup_enabled=True,
            historical_backup_run_before_archive=True,
            historical_backup_required_before_archive=False,
        )
        job = self._build_and_get_archive_job(config)
        assert job is not None

        events = []

        def fake_backup(**kw):
            events.append('backup')
            return 1

        def fake_run_archive(**kw):
            events.append('archive')
            return 0

        with patch('src.maintenance.backup.run_backup', side_effect=fake_backup), \
             patch('src.maintenance.archive.run_archive', side_effect=fake_run_archive):
            job()

        assert events == ['backup', 'archive']

    def test_backup_success_then_archive_with_validation(self, config_class):
        """Successful backup followed by validation then actual archive."""
        config = self._make_config(
            config_class,
            archive_validate_before_cleanup=True,
            historical_backup_enabled=True,
            historical_backup_run_before_archive=True,
            historical_backup_required_before_archive=True,
        )
        job = self._build_and_get_archive_job(config)
        assert job is not None

        events = []

        def fake_backup(**kw):
            events.append('backup')
            return 0

        def fake_run_archive(*, days, dry_run, skip_vacuum, validate_only, **kw):
            events.append(f'archive_validate_only={validate_only}')
            return 0

        with patch('src.maintenance.backup.run_backup', side_effect=fake_backup), \
             patch('src.maintenance.archive.run_archive', side_effect=fake_run_archive):
            job()

        assert len(events) == 3, f"Expected 3 events, got: {events}"
        assert events[0] == 'backup'
        assert events[1] == 'archive_validate_only=True'
        assert events[2] == 'archive_validate_only=False'

    def test_backup_success_no_validation(self, config_class):
        """Successful backup then archive without validation preflight."""
        config = self._make_config(
            config_class,
            archive_validate_before_cleanup=False,
            historical_backup_enabled=True,
            historical_backup_run_before_archive=True,
            historical_backup_required_before_archive=True,
        )
        job = self._build_and_get_archive_job(config)
        assert job is not None

        events = []

        def fake_backup(**kw):
            events.append('backup')
            return 0

        def fake_run_archive(*, days, dry_run, skip_vacuum, validate_only, **kw):
            events.append(f'archive_validate_only={validate_only}')
            return 0

        with patch('src.maintenance.backup.run_backup', side_effect=fake_backup), \
             patch('src.maintenance.archive.run_archive', side_effect=fake_run_archive):
            job()

        assert len(events) == 2, f"Expected 2 events, got: {events}"
        assert events[0] == 'backup'
        assert events[1] == 'archive_validate_only=False'

    def test_no_negative_backup_interval(self, config_class):
        """Archive interval of 1 hour should not create a 0 or 30-second
        backup task interval."""
        config = self._make_config(config_class, archive_interval_hours=1)
        from main import build_maintenance_background_tasks
        tasks = build_maintenance_background_tasks(config, lambda: config)
        for t in tasks:
            assert t['interval_seconds'] >= 3600, (
                f"Task {t['name']} interval is {t['interval_seconds']}s, "
                f"expected >= 3600s (1 hour)"
            )


# ---------------------------------------------------------------------------
# Scheduler integration tests (existing)
# ---------------------------------------------------------------------------

class TestMaintenanceScheduler:
    """Test that maintenance tasks register correctly in the scheduler."""

    def test_archive_disabled_by_default(self, tmp_path, monkeypatch):
        """Scheduler registers no archive task when archive is disabled."""
        from src.scheduler import Scheduler

        monkeypatch.setenv('SCHEDULE_ENABLED', 'false')
        monkeypatch.setenv('ARCHIVE_ENABLED', 'false')
        monkeypatch.setenv('DATABASE_PATH', str(tmp_path / "main.db"))
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', str(tmp_path / "hist.db"))

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()

        scheduler = Scheduler(schedule_time="23:59")

        def dummy_task():
            pass

        scheduler.set_daily_task(dummy_task, run_immediately=False)

        # No archive task should be registered
        archive_tasks = [
            t for t in scheduler._background_tasks
            if 'archive' in t.get('name', '').lower()
        ]
        assert len(archive_tasks) == 0

        scheduler.stop()
        DatabaseManager.reset_instance()

    def test_diagnostics_disabled_by_default(self, tmp_path, monkeypatch):
        """Scheduler registers no diagnostics task when disabled."""
        from src.scheduler import Scheduler

        monkeypatch.setenv('DATABASE_PATH', str(tmp_path / "main.db"))
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', str(tmp_path / "hist.db"))

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()

        scheduler = Scheduler(schedule_time="23:59")

        def dummy_task():
            pass

        scheduler.set_daily_task(dummy_task, run_immediately=False)

        diagnostics_tasks = [
            t for t in scheduler._background_tasks
            if 'diagnostics' in t.get('name', '').lower()
        ]
        assert len(diagnostics_tasks) == 0

        scheduler.stop()
        DatabaseManager.reset_instance()

    def test_overlapping_background_task_prevented(self, tmp_path, monkeypatch):
        """A running background task cannot overlap with another instance of itself."""
        from src.scheduler import Scheduler

        monkeypatch.setenv('DATABASE_PATH', str(tmp_path / "main.db"))
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', str(tmp_path / "hist.db"))

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()

        scheduler = Scheduler(schedule_time="23:59")

        run_count = [0]
        def slow_task():
            run_count[0] += 1
            time.sleep(0.5)

        scheduler.set_daily_task(lambda: None, run_immediately=False)
        scheduler.add_background_task(
            task=slow_task,
            interval_seconds=0.1,
            run_immediately=True,
            name="slow_task",
        )

        # Give it time to start
        time.sleep(0.2)

        # While the task is still running, try to start it again
        for entry in scheduler._background_tasks:
            if entry.get('name') == 'slow_task':
                result = scheduler._start_background_task(entry)
                assert result is False  # Should refuse to start again

        # Wait for completion
        time.sleep(0.6)
        assert run_count[0] == 1  # Only started once

        scheduler.stop()
        DatabaseManager.reset_instance()

    def test_scheduler_stop_graceful(self, tmp_path, monkeypatch):
        """Scheduler stops gracefully."""
        from src.scheduler import Scheduler

        monkeypatch.setenv('DATABASE_PATH', str(tmp_path / "main.db"))
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', str(tmp_path / "hist.db"))

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()

        scheduler = Scheduler(schedule_time="23:59")

        def dummy_task():
            pass

        scheduler.set_daily_task(dummy_task, run_immediately=False)
        scheduler.stop()

        assert scheduler._running is False

        DatabaseManager.reset_instance()


# ---------------------------------------------------------------------------
# Legacy background task registration tests (existing)
# ---------------------------------------------------------------------------

class TestMaintenanceBackgroundTaskRegistration:
    """Test that maintenance tasks can be added as background tasks."""

    def test_add_archive_background_task(self, tmp_path, monkeypatch):
        """Ensure archive task format is compatible with scheduler."""
        from src.scheduler import Scheduler

        monkeypatch.setenv('DATABASE_PATH', str(tmp_path / "main.db"))
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', str(tmp_path / "hist.db"))

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()

        scheduler = Scheduler(schedule_time="23:59")

        def background_func():
            pass

        scheduler.add_background_task(
            task=background_func,
            interval_seconds=3600 * 24,
            run_immediately=False,
            name="archive_maintenance",
        )

        assert len(scheduler._background_tasks) == 1
        task = scheduler._background_tasks[0]
        assert task['name'] == 'archive_maintenance'
        assert task['interval_seconds'] == 86400
        assert task['task'] is background_func

        scheduler.stop()
        DatabaseManager.reset_instance()
