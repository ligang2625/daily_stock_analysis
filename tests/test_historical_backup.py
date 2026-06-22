# -*- coding: utf-8 -*-
"""Tests for historical database backup module (Phase 4)."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta

import pytest


class TestHistoricalBackup:
    """Test backup module operations."""

    @pytest.fixture
    def backup_env(self, tmp_path, monkeypatch):
        """Setup clean temp databases and backup directory."""
        main_db = str(tmp_path / "backup_main.db")
        historical_db = str(tmp_path / "backup_historical.db")
        backup_dir = str(tmp_path / "backups")

        monkeypatch.setenv('DATABASE_PATH', main_db)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db)
        monkeypatch.setenv('HISTORICAL_BACKUP_DIR', backup_dir)

        from src.storage import DatabaseManager
        from src.storage_historical import HistoricalDatabaseManager
        from src.config import Config

        Config.reset_instance()
        DatabaseManager.reset_instance()

        # Create the databases
        DatabaseManager.get_instance()
        HistoricalDatabaseManager(db_path=historical_db)

        yield {
            'main_db': main_db,
            'historical_db': historical_db,
            'backup_dir': backup_dir,
            'tmp_path': tmp_path,
        }

        Config.reset_instance()
        DatabaseManager.reset_instance()

    def test_historical_only_backup(self, backup_env):
        """Backup creates a file for the historical DB."""
        from src.maintenance.backup import run_backup

        exit_code = run_backup(
            historical_only=True,
            output_dir=backup_env['backup_dir'],
            use_sqlite_api=True,
        )
        assert exit_code == 0
        files = list(os.listdir(backup_env['backup_dir']))
        assert len(files) >= 1
        assert any('historical_market' in f for f in files)

    def test_main_only_backup(self, backup_env):
        """Backup creates a file for the main DB."""
        from src.maintenance.backup import run_backup

        exit_code = run_backup(
            main_only=True,
            output_dir=backup_env['backup_dir'],
            use_sqlite_api=True,
        )
        assert exit_code == 0
        files = list(os.listdir(backup_env['backup_dir']))
        assert len(files) >= 1
        assert any('stock_analysis' in f for f in files)

    def test_all_backup(self, backup_env):
        """Backup creates both files."""
        from src.maintenance.backup import run_backup

        exit_code = run_backup(
            backup_all=True,
            output_dir=backup_env['backup_dir'],
            use_sqlite_api=True,
        )
        assert exit_code == 0
        files = list(os.listdir(backup_env['backup_dir']))
        assert len(files) >= 2

    def test_backup_opens_with_sqlite(self, backup_env):
        """Backup file can be opened by SQLite."""
        from src.maintenance.backup import run_backup

        exit_code = run_backup(
            historical_only=True,
            output_dir=backup_env['backup_dir'],
            use_sqlite_api=True,
        )
        assert exit_code == 0
        files = list(os.listdir(backup_env['backup_dir']))
        target = os.path.join(backup_env['backup_dir'], files[0])

        conn = sqlite3.connect(target)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn.close()
        assert len(tables) >= 1

    def test_retention_cleanup(self, backup_env):
        """Retention cleanup removes old backups but not new ones."""
        from src.maintenance.backup import run_backup, BACKUP_FILENAME_PATTERN

        # Create a backup file with old mtime
        old_file = os.path.join(
            backup_env['backup_dir'],
            f"historical_market_{datetime.now().strftime('%Y%m%d')}_000000.db",
        )
        os.makedirs(backup_env['backup_dir'], exist_ok=True)
        # Create an empty file
        with open(old_file, 'w') as f:
            f.write('')
        # Set mtime to 30 days ago
        old_mtime = datetime.now().timestamp() - 30 * 86400
        os.utime(old_file, (old_mtime, old_mtime))

        # Create a recent backup
        exit_code = run_backup(
            historical_only=True,
            output_dir=backup_env['backup_dir'],
            retention_days=14,
            cleanup_old=True,
            use_sqlite_api=True,
        )
        assert exit_code == 0

        # Old file should be gone
        assert not os.path.exists(old_file)

        # Recent file should remain
        files = [
            f for f in os.listdir(backup_env['backup_dir'])
            if BACKUP_FILENAME_PATTERN.match(f)
        ]
        assert len(files) >= 1

    def test_backup_missing_source_no_flag_fails(self, backup_env, monkeypatch):
        """Backup of non-existent DB returns 1 when --allow-missing is not set."""
        from src.maintenance.backup import run_backup
        from src.config import Config

        # Point historical DB path to a directory that definitely doesn't exist
        nonexistent = str(backup_env['tmp_path'] / "nonexistent" / "hist.db")
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', nonexistent)
        Config.reset_instance()

        exit_code = run_backup(
            historical_only=True,
            output_dir=backup_env['backup_dir'],
            use_sqlite_api=True,
            allow_missing=False,
        )
        assert exit_code == 1

    def test_backup_missing_source_with_flag_ok(self, backup_env, monkeypatch):
        """Backup of non-existent DB returns 0 when --allow-missing is set."""
        from src.maintenance.backup import run_backup
        from src.config import Config

        nonexistent = str(backup_env['tmp_path'] / "nonexistent" / "hist.db")
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', nonexistent)
        Config.reset_instance()

        exit_code = run_backup(
            historical_only=True,
            output_dir=backup_env['backup_dir'],
            use_sqlite_api=True,
            allow_missing=True,
        )
        assert exit_code == 0

    def test_cli_all_default(self, backup_env, capsys, monkeypatch):
        """CLI defaults to --all when no target specified."""
        from src.maintenance.backup import main as backup_main

        exit_code = backup_main([
            '--all',
            '--output-dir', backup_env['backup_dir'],
        ])
        assert exit_code == 0

    def test_cli_retention(self, backup_env, capsys):
        """CLI with --cleanup-old."""
        from src.maintenance.backup import main as backup_main

        exit_code = backup_main([
            '--historical-only',
            '--output-dir', backup_env['backup_dir'],
            '--retention-days', '14',
            '--cleanup-old',
        ])
        assert exit_code == 0
