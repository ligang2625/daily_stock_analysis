# -*- coding: utf-8 -*-
"""Tests for umbrella maintenance CLI (Phase 4)."""

from __future__ import annotations

import os
import sys

import pytest


class TestMaintenanceCLI:
    """Test umbrella maintenance CLI dispatch."""

    @pytest.fixture
    def cli_env(self, tmp_path, monkeypatch):
        """Setup clean environment."""
        main_db = str(tmp_path / "cli_main.db")
        historical_db = str(tmp_path / "cli_hist.db")
        monkeypatch.setenv('DATABASE_PATH', main_db)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db)

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()

        yield {}
        DatabaseManager.reset_instance()

    def test_help_shows_subcommands(self, cli_env, capsys):
        """Running without subcommand shows help."""
        from scripts.maintenance import main as cli_main

        # Simulate no args
        old_argv = sys.argv.copy()
        try:
            sys.argv = ['maintenance.py']
            exit_code = cli_main()
        finally:
            sys.argv = old_argv

        captured = capsys.readouterr()
        assert exit_code == 1
        assert 'archive' in captured.out.lower() or 'backup' in captured.out.lower()

    def test_invalid_subcommand(self, cli_env, capsys):
        """Invalid subcommand returns non-zero."""
        from scripts.maintenance import main as cli_main

        old_argv = sys.argv.copy()
        try:
            sys.argv = ['maintenance.py', 'nonexistent']
            exit_code = cli_main()
        finally:
            sys.argv = old_argv

        assert exit_code == 1

    def test_archive_subcommand_dispatches(self, cli_env, capsys):
        """Archive subcommand dispatches to archive module."""
        from scripts.maintenance import main as cli_main

        old_argv = sys.argv.copy()
        try:
            sys.argv = ['maintenance.py', 'archive', '--dry-run', '--days', '5']
            exit_code = cli_main()
        finally:
            sys.argv = old_argv

        assert exit_code == 0

    def test_diagnostics_subcommand_dispatches(self, cli_env, capsys):
        """Diagnostics subcommand dispatches to diagnostics module."""
        from scripts.maintenance import main as cli_main

        old_argv = sys.argv.copy()
        try:
            sys.argv = ['maintenance.py', 'diagnostics', '--format', 'json']
            exit_code = cli_main()
        finally:
            sys.argv = old_argv
        assert exit_code == 0

    def test_backup_subcommand_dispatches(self, cli_env, tmp_path, capsys):
        """Backup subcommand dispatches to backup module."""
        from scripts.maintenance import main as cli_main

        backup_dir = str(tmp_path / "backups")
        old_argv = sys.argv.copy()
        try:
            sys.argv = [
                'maintenance.py', 'backup',
                '--historical-only',
                '--output-dir', backup_dir,
            ]
            exit_code = cli_main()
        finally:
            sys.argv = old_argv
        assert exit_code == 0

    def test_health_subcommand_dispatches(self, cli_env, capsys):
        """Health subcommand dispatches to health module."""
        from scripts.maintenance import main as cli_main

        old_argv = sys.argv.copy()
        try:
            sys.argv = [
                'maintenance.py', 'health',
                '--max-stale-hours', '48',
                '--format', 'json',
                '--soft-fail',
            ]
            exit_code = cli_main()
        finally:
            sys.argv = old_argv
        assert exit_code == 0
