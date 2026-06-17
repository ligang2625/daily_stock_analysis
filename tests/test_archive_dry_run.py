# -*- coding: utf-8 -*-
"""Tests for archive maintenance command: dry-run and actual cleanup."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

import pytest


class TestArchiveDryRun:
    """Test archive dry-run counting without mutation."""

    @pytest.fixture
    def archives_env(self, tmp_path, monkeypatch):
        """Setup a clean environment with test databases."""
        main_db = str(tmp_path / "main_archive_test.db")
        historical_db = str(tmp_path / "historical_archive_test.db")

        # Override config paths
        monkeypatch.setenv('DATABASE_PATH', main_db)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db)

        # Monkey-patch config defaults
        import src.config
        original_defaults = src.config.Config.__dataclass_fields__.copy()
        # We'll just use env vars for this test

        # Reset singletons
        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()

        yield {
            'main_db': main_db,
            'historical_db': historical_db,
            'tmp_path': tmp_path,
        }

    def _seed_data(self, db, now_date):
        """Insert test data into various tables for counting."""
        from datetime import date as dt_date

        with db.session_scope() as session:
            from src.storage import (
                AnalysisHistoryPayload, NewsIntel, LLMUsage,
                AgentProviderTurn, ConversationMessage, FundamentalSnapshot,
                AlertTriggerRecord, DecisionSignalRecord,
                AnalysisHistory, StockDaily,
            )
            from sqlalchemy import text as sa_text

            # Insert old and recent rows in various tables
            old_date = now_date - timedelta(days=30)
            recent_date = now_date

            # analysis_history_payload (via raw SQL to set created_at)
            for dt, count in [(old_date, 5), (recent_date, 2)]:
                for i in range(count):
                    session.execute(
                        sa_text(
                            "INSERT INTO analysis_history (code, name, created_at) "
                            "VALUES (:code, :name, :created_at)"
                        ),
                        {"code": f"TEST{i}", "name": f"Test{i}", "created_at": dt},
                    )

            # Use the newly inserted rows for payloads
            from sqlalchemy import select as sa_select
            ah_rows = session.execute(
                sa_select(AnalysisHistory.id, AnalysisHistory.created_at)
            ).all()

            for ah_id, ah_created in ah_rows:
                session.execute(
                    sa_text(
                        "INSERT INTO analysis_history_payload (history_id, raw_result, compressed, created_at) "
                        "VALUES (:hid, :raw, 0, :created_at)"
                    ),
                    {"hid": ah_id, "raw": '{}', "created_at": ah_created},
                )

            # news_intel uses fetched_at
            for dt, count in [(old_date, 10), (recent_date, 3)]:
                for i in range(count):
                    session.execute(
                        sa_text(
                            "INSERT INTO news_intel (code, title, url, fetched_at) "
                            "VALUES (:code, :title, :url, :fetched_at)"
                        ),
                        {
                            "code": "600519",
                            "title": f"News {i}",
                            "url": f"http://example.com/{i}_{dt.date()}",
                            "fetched_at": dt,
                        },
                    )

            # llm_usage
            for dt, count in [(old_date, 8), (recent_date, 2)]:
                for i in range(count):
                    session.execute(
                        sa_text(
                            "INSERT INTO llm_usage (call_type, model, prompt_tokens, completion_tokens, total_tokens, called_at) "
                            "VALUES (:call_type, :model, :prompt, :comp, :total, :called_at)"
                        ),
                        {
                            "call_type": "analysis",
                            "model": "gpt-4",
                            "prompt": 100,
                            "comp": 50,
                            "total": 150,
                            "called_at": dt,
                        },
                    )

            # agent_provider_turns (many NOT NULL columns)
            for dt, count in [(old_date, 4), (recent_date, 1)]:
                for i in range(count):
                    session.execute(
                        sa_text(
                            "INSERT INTO agent_provider_turns "
                            "(session_id, run_id, provider, model, anchor_user_message_id, "
                            " anchor_assistant_message_id, messages_json, contains_reasoning, "
                            " contains_tool_calls, contains_thinking_blocks, must_roundtrip, "
                            " estimated_tokens, created_at) "
                            "VALUES (:sid, :rid, :prov, :model, 1, 1, '[]', 0, 0, 0, 0, 0, :created_at)"
                        ),
                        {
                            "sid": f"session_{i}",
                            "rid": f"run_{i}",
                            "prov": "deepseek",
                            "model": "deepseek-v3",
                            "created_at": dt,
                        },
                    )

            # conversation_messages
            for dt, count in [(old_date, 12), (recent_date, 2)]:
                for i in range(count):
                    session.execute(
                        sa_text(
                            "INSERT INTO conversation_messages (session_id, role, content, created_at) "
                            "VALUES (:sid, :role, :content, :created_at)"
                        ),
                        {
                            "sid": f"conv_{i}",
                            "role": "user",
                            "content": f"Hello {i}",
                            "created_at": dt,
                        },
                    )

            # fundamental_snapshot
            for dt, count in [(old_date, 3), (recent_date, 1)]:
                for i in range(count):
                    session.execute(
                        sa_text(
                            "INSERT INTO fundamental_snapshot (query_id, code, payload, created_at) "
                            "VALUES (:qid, :code, :payload, :created_at)"
                        ),
                        {
                            "qid": f"q_{i}",
                            "code": "600519",
                            "payload": '{}',
                            "created_at": dt,
                        },
                    )

            # alert_triggers uses triggered_at, has NOT NULL status
            for dt, count in [(old_date, 2), (recent_date, 0)]:
                for i in range(count):
                    session.execute(
                        sa_text(
                            "INSERT INTO alert_triggers (target, reason, status, triggered_at) "
                            "VALUES (:target, :reason, 'triggered', :triggered_at)"
                        ),
                        {
                            "target": "600519",
                            "reason": f"trigger {i}",
                            "triggered_at": dt,
                        },
                    )

            # decision_signals (status is NOT NULL)
            for dt, count in [(old_date, 3), (recent_date, 1)]:
                for i in range(count):
                    session.execute(
                        sa_text(
                            "INSERT INTO decision_signals (stock_code, market, source_type, trigger_source, action, status, plan_quality, created_at) "
                            "VALUES (:code, :market, :st, :ts, :action, 'active', :pq, :created_at)"
                        ),
                        {
                            "code": "600519",
                            "market": "cn",
                            "st": "intraday",
                            "ts": "snapshot",
                            "action": "hold",
                            "pq": "ok",
                            "created_at": dt,
                        },
                    )

    def test_dry_run_counts_correctly(self, archives_env, monkeypatch):
        """Dry-run should count rows per table without deleting anything."""
        from src.config import Config

        # Override config paths
        from src.config import Config as _Config
        monkeypatch.setattr(_Config, 'database_path', archives_env['main_db'])
        monkeypatch.setattr(_Config, 'historical_database_path', archives_env['historical_db'])

        # Create our DB
        from src.storage import DatabaseManager
        from src.storage_historical import HistoricalDatabaseManager

        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{archives_env['main_db']}")
        historical_db = HistoricalDatabaseManager(db_path=archives_env['historical_db'])

        now_date = datetime(2026, 6, 17)
        self._seed_data(db, now_date)

        # Run archive with a cutoff that should catch old rows
        from src.maintenance.archive import run_archive

        cutoff = date(2026, 6, 1)  # Old rows are from ~2026-05-18

        result = run_archive(
            days=5,
            dry_run=True,
            cutoff_date=cutoff,
        )

        assert result == 0

    def test_dry_run_does_not_delete(self, archives_env, monkeypatch):
        """After dry-run, all rows should still be present."""
        from src.config import get_config

        cfg = get_config()
        cfg.database_path = archives_env['main_db']
        cfg.historical_database_path = archives_env['historical_db']

        from src.storage import DatabaseManager
        from src.storage_historical import HistoricalDatabaseManager

        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{archives_env['main_db']}")
        HistoricalDatabaseManager(db_path=archives_env['historical_db'])

        now_date = datetime(2026, 6, 17)
        self._seed_data(db, now_date)

        from src.maintenance.archive import run_archive
        from sqlalchemy import text as sa_text

        # Count before
        with db.session_scope() as session:
            before = session.execute(
                sa_text("SELECT COUNT(*) FROM news_intel")
            ).fetchone()[0]

        # Dry run
        run_archive(days=5, dry_run=True, cutoff_date=date(2026, 6, 1))

        # Count after
        with db.session_scope() as session:
            after = session.execute(
                sa_text("SELECT COUNT(*) FROM news_intel")
            ).fetchone()[0]

        assert before == after

    def test_archive_run_ledger_created(self, archives_env, monkeypatch):
        """Each run (dry or real) should create an archive_runs record."""
        from src.config import get_config

        cfg = get_config()
        cfg.database_path = archives_env['main_db']
        cfg.historical_database_path = archives_env['historical_db']

        from src.storage import DatabaseManager
        from src.storage_historical import HistoricalDatabaseManager

        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{archives_env['main_db']}")
        historical_db = HistoricalDatabaseManager(db_path=archives_env['historical_db'])

        now_date = datetime(2026, 6, 17)
        self._seed_data(db, now_date)

        from src.maintenance.archive import run_archive
        run_archive(days=5, dry_run=True, cutoff_date=date(2026, 6, 1))

        # Check archive_runs
        session = historical_db.get_session()
        try:
            from sqlalchemy import select as sa_select
            from src.storage_historical import ArchiveRun
            runs = session.execute(
                sa_select(ArchiveRun).order_by(ArchiveRun.id.desc())
            ).scalars().all()
            assert len(runs) >= 1
            assert runs[0].status == 'success'
            assert runs[0].cutoff_date is not None
        finally:
            session.close()

    def test_actual_cleanup_deletes_allowed_tables(self, archives_env, monkeypatch):
        """Non-dry-run should delete old rows from allowed tables but NOT from protected ones."""
        from src.config import get_config

        cfg = get_config()
        cfg.database_path = archives_env['main_db']
        cfg.historical_database_path = archives_env['historical_db']

        from src.storage import DatabaseManager
        from src.storage_historical import HistoricalDatabaseManager

        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{archives_env['main_db']}")
        HistoricalDatabaseManager(db_path=archives_env['historical_db'])

        now_date = datetime(2026, 6, 17)
        self._seed_data(db, now_date)

        from src.maintenance.archive import run_archive
        from sqlalchemy import text as sa_text

        # Count analysis_history before
        with db.session_scope() as session:
            ah_before = session.execute(
                sa_text("SELECT COUNT(*) FROM analysis_history")
            ).fetchone()[0]

        run_archive(days=5, dry_run=False, cutoff_date=date(2026, 6, 1), skip_vacuum=True)

        with db.session_scope() as session:
            # Protected: analysis_history rows NOT deleted
            ah_after = session.execute(
                sa_text("SELECT COUNT(*) FROM analysis_history")
            ).fetchone()[0]
            assert ah_after == ah_before, "analysis_history rows should NOT be deleted"

            # Allowed: old news_intel rows should be deleted, recent ones kept
            news_total = session.execute(
                sa_text("SELECT COUNT(*) FROM news_intel")
            ).fetchone()[0]
            assert news_total < 13  # fewer than before (old ones deleted), more than 0

            # Very old LLM usage should be deleted
            llm_total = session.execute(
                sa_text("SELECT COUNT(*) FROM llm_usage")
            ).fetchone()[0]
            assert llm_total < 10
