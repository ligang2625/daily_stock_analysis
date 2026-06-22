# -*- coding: utf-8 -*-
"""Tests for database diagnostics module (Phase 4)."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

import pytest


class TestDBDiagnostics:
    """Test db_diagnostics module with temp SQLite databases."""

    @pytest.fixture
    def diag_env(self, tmp_path, monkeypatch):
        """Setup clean temp databases for diagnostics testing."""
        main_db = str(tmp_path / "diag_main.db")
        historical_db = str(tmp_path / "diag_historical.db")

        monkeypatch.setenv('DATABASE_PATH', main_db)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db)

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()

        yield {
            'main_db': main_db,
            'historical_db': historical_db,
            'tmp_path': tmp_path,
        }

        DatabaseManager.reset_instance()

    def test_fresh_db_diagnostics(self, diag_env):
        """Diagnostics on an empty/fresh database."""
        from src.maintenance.db_diagnostics import generate_diagnostics

        report = generate_diagnostics(retention_days=5)

        assert 'status' in report
        assert 'generated_at' in report
        assert 'main_db' in report
        assert 'historical_db' in report
        assert 'warnings' in report
        assert 'errors' in report
        # Historical DB should exist (created empty) or be None
        assert report['main_db']['file_path'] == diag_env['main_db']

    def test_fresh_db_json_output(self, diag_env):
        """JSON output is stable and parseable."""
        from src.maintenance.db_diagnostics import generate_diagnostics

        report = generate_diagnostics(retention_days=5)
        json_str = json.dumps(report, ensure_ascii=False, indent=2, default=str)
        parsed = json.loads(json_str)

        assert parsed['status'] in ('ok', 'warning', 'error')
        assert 'main_db' in parsed

    def test_main_db_tables_reported(self, diag_env):
        """Diagnostics includes table row counts for main DB."""
        from src.storage import DatabaseManager
        from src.maintenance.db_diagnostics import generate_diagnostics

        db = DatabaseManager.get_instance()
        report = generate_diagnostics(retention_days=5)

        tables = report['main_db'].get('tables', {})
        assert isinstance(tables, dict)
        # At least some known tables should appear
        known_tables = ['analysis_history', 'stock_daily']
        for tbl in known_tables:
            assert tbl in tables, f"Table {tbl} should be in diagnostics"

        # All counts should be 0 for fresh DB
        for tbl_name in known_tables:
            info = tables[tbl_name]
            if isinstance(info, dict):
                assert info.get('row_count') == 0

    def test_diagnostics_with_payload_rows(self, diag_env):
        """Diagnostics with payload data reports correctly."""
        from src.storage import DatabaseManager
        from src.maintenance.db_diagnostics import generate_diagnostics

        db = DatabaseManager.get_instance()

        # Insert some test data
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            session.execute(sa_text(
                "INSERT INTO stock_daily (code, code_norm, date, close, created_at) "
                "VALUES ('600519', '600519', '2026-06-20', 1800.0, '2026-06-20')"
            ))
            session.execute(sa_text(
                "INSERT INTO analysis_history (code, name, created_at) "
                "VALUES ('600519', '茅台', '2026-06-20')"
            ))

        report = generate_diagnostics(retention_days=5)
        tables = report['main_db'].get('tables', {})

        assert tables.get('stock_daily', {}).get('row_count') == 1
        assert tables.get('analysis_history', {}).get('row_count') == 1

    def test_diagnostics_json_schema_stability(self, diag_env):
        """JSON schema is stable across calls."""
        from src.maintenance.db_diagnostics import generate_diagnostics

        report1 = generate_diagnostics(retention_days=5)
        report2 = generate_diagnostics(retention_days=5)

        # Same keys
        assert set(report1.keys()) == set(report2.keys())
        # Status field present
        assert 'status' in report1
        # Main DB keys consistent
        assert 'file_path' in report1['main_db']


class TestDBDiagnosticsOutput:
    """Test CLI output formatting for diagnostics."""

    @pytest.fixture
    def diag_env(self, tmp_path, monkeypatch):
        """Clean environment."""
        main_db = str(tmp_path / "diag_main.db")
        historical_db = str(tmp_path / "diag_historical.db")
        monkeypatch.setenv('DATABASE_PATH', main_db)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db)
        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        yield {}
        DatabaseManager.reset_instance()

    def test_text_format_prints(self, diag_env, capsys):
        """Text format produces readable output."""
        from src.maintenance.db_diagnostics import main as diag_main

        exit_code = diag_main(['--format', 'text'])
        captured = capsys.readouterr()
        assert exit_code == 0
        assert 'Database Diagnostics Report' in captured.out

    def test_json_format_prints(self, diag_env, capsys):
        """JSON format produces JSON output."""
        from src.maintenance.db_diagnostics import main as diag_main

        exit_code = diag_main(['--format', 'json'])
        captured = capsys.readouterr()
        assert exit_code == 0
        parsed = json.loads(captured.out)
        assert 'status' in parsed

    def test_output_to_file(self, diag_env, tmp_path):
        """Output to file writes correctly."""
        from src.maintenance.db_diagnostics import main as diag_main

        out_path = tmp_path / "diagnostics.json"
        exit_code = diag_main(['--format', 'json', '--output', str(out_path)])
        assert exit_code == 0
        assert out_path.is_file()
        content = json.loads(out_path.read_text(encoding='utf-8'))
        assert 'status' in content
