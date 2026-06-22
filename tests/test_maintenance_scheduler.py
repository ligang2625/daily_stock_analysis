# -*- coding: utf-8 -*-
"""Tests for maintenance scheduler integration (Phase 4)."""

from __future__ import annotations

import os
import threading
import time

import pytest


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
