# -*- coding: utf-8 -*-
"""Tests for Phase 1 archive sequencing and backfill operations."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest


class TestArchiveSequencing:
    """Verify archive cleanup correct ordering: null legacy -> delete payload -> keep summary."""

    @pytest.fixture
    def archives_env(self, tmp_path):
        """Setup a clean test environment."""
        main_db = str(tmp_path / "main_archive_seq_test.db")
        historical_db = str(tmp_path / "historical_archive_seq_test.db")

        import src.config
        original_path = src.config.Config.database_path
        original_hist_path = src.config.Config.historical_database_path

        from src.config import Config as _Config
        _Config.database_path = main_db
        _Config.historical_database_path = historical_db

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()

        yield {'main_db': main_db, 'historical_db': historical_db}

        # Restore
        _Config.database_path = original_path
        _Config.historical_database_path = original_hist_path
        DatabaseManager.reset_instance()

    def _seed_old_payload_data(self, db, cutoff_date, recent=False):
        """Seed old records with both legacy columns and payload rows."""
        from sqlalchemy import text as sa_text

        created = cutoff_date - timedelta(days=10) if not recent else cutoff_date + timedelta(days=1)

        with db.session_scope() as session:
            # Create analysis_history row with legacy payload
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, name, report_type, "
                    " sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    " raw_result, news_content, context_snapshot, model_used, "
                    " current_price, change_pct, analysis_phase, created_at, updated_at) "
                    "VALUES (:qid, :code, :cn, :name, :rt, "
                    " :ss, :oa, :tp, :as_, :rr, :nc, :cs, :mu, "
                    " :cp, :chg, :ap, :ca, :upd)"
                ),
                {
                    "qid": f"q-seq-{'recent' if recent else 'old'}-001",
                    "code": "600519",
                    "cn": "600519",
                    "name": "Maotai",
                    "rt": "single_stock",
                    "ss": 70,
                    "oa": "hold",
                    "tp": "neutral",
                    "as_": "Summary",
                    "rr": '{"model_used": "gpt-4", "code": "600519"}',
                    "nc": "Old news content",
                    "cs": '{"realtime_quote": {"price": 1800}}',
                    "mu": "gpt-4",
                    "cp": 1800.0,
                    "chg": 1.0,
                    "ap": "postmarket",
                    "ca": created,
                    "upd": created,
                },
            )

            # Get the inserted row ID
            from src.storage import AnalysisHistory
            from sqlalchemy import select as sa_select
            rec = session.execute(
                sa_select(AnalysisHistory).where(
                    AnalysisHistory.query_id == f"q-seq-{'recent' if recent else 'old'}-001"
                )
            ).scalars().first()

            # Create payload row
            if rec:
                session.execute(
                    sa_text(
                        "INSERT INTO analysis_history_payload "
                        "(history_id, raw_result, news_content, context_snapshot, payload_size, compressed, created_at) "
                        "VALUES (:hid, :rr, :nc, :cs, :ps, 0, :ca)"
                    ),
                    {
                        "hid": rec.id,
                        "rr": '{"model_used": "gpt-4", "code": "600519"}',
                        "nc": "Payload news",
                        "cs": '{"realtime_quote": {"price": 1800}}',
                        "ps": 300,
                        "ca": created,
                    },
                )
            return rec.id if rec else None

    def test_archive_legacy_nulls_then_deletes_payload(self, archives_env):
        """After archive, legacy columns should be NULL, payload row deleted, summary kept."""
        from src.storage import DatabaseManager
        from src.storage_historical import HistoricalDatabaseManager

        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{archives_env['main_db']}")
        HistoricalDatabaseManager(db_path=archives_env['historical_db'])

        cutoff = date.today() - timedelta(days=5)
        self._seed_old_payload_data(db, cutoff, recent=False)
        recent_id = self._seed_old_payload_data(db, cutoff, recent=True)

        from src.maintenance.archive import run_archive

        result = run_archive(days=5, dry_run=False, cutoff_date=cutoff, skip_vacuum=True)
        assert result == 0

        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            # Old record: legacy columns should be NULL
            old_row = session.execute(
                sa_text("SELECT raw_result, news_content, context_snapshot FROM analysis_history WHERE query_id = 'q-seq-old-001'")
            ).fetchone()
            assert old_row is not None
            assert old_row[0] is None, "Legacy raw_result should be NULL after archive"
            assert old_row[1] is None, "Legacy news_content should be NULL after archive"
            assert old_row[2] is None, "Legacy context_snapshot should be NULL after archive"

            # Old payload row should be deleted
            payload_rows = session.execute(
                sa_text("SELECT COUNT(*) FROM analysis_history_payload WHERE history_id IN "
                        "(SELECT id FROM analysis_history WHERE query_id = 'q-seq-old-001')")
            ).fetchone()[0]
            assert payload_rows == 0, "Old payload rows should be deleted"

            # Summary row should still exist
            summary_count = session.execute(
                sa_text("SELECT COUNT(*) FROM analysis_history WHERE query_id = 'q-seq-old-001'")
            ).fetchone()[0]
            assert summary_count == 1, "Summary row should NOT be deleted"

            # Recent record should be untouched
            recent_row = session.execute(
                sa_text("SELECT raw_result, news_content FROM analysis_history WHERE query_id = 'q-seq-recent-001'")
            ).fetchone()
            assert recent_row is not None
            assert recent_row[0] is not None, "Recent legacy raw_result should NOT be nulled"

    def test_archive_dry_run_reports_correct_counts(self, archives_env):
        """Dry-run should report separate counts for legacy nulls, payload deletes, runtime deletes."""
        from src.storage import DatabaseManager
        from src.storage_historical import HistoricalDatabaseManager

        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{archives_env['main_db']}")
        HistoricalDatabaseManager(db_path=archives_env['historical_db'])

        # Use an old cutoff so our old data is caught
        cutoff = date.today() - timedelta(days=3)
        self._seed_old_payload_data(db, cutoff, recent=False)

        # Add some news_intel that will be counted
        from sqlalchemy import text as sa_text
        old_fetched = cutoff - timedelta(days=30)
        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO news_intel (code, title, url, fetched_at) "
                    "VALUES (:code, :title, :url, :fetched_at)"
                ),
                {
                    "code": "600519",
                    "title": "Old news",
                    "url": "http://example.com/old",
                    "fetched_at": old_fetched,
                },
            )

        from src.maintenance.archive import run_archive, _dry_run
        counts = _dry_run(db, cutoff)
        assert 'analysis_history' in counts
        assert 'analysis_history_payload' in counts
        assert 'analysis_history_legacy_payload_cols' in counts
        assert 'news_intel' in counts


class TestPhase1Backfill:
    """Verify idempotent Phase 1 backfill operations."""

    @pytest.fixture
    def db_with_legacy_data(self, tmp_path):
        """Create a DB with old-style rows that need backfill."""
        db_path = str(tmp_path / "test_backfill.db")
        db_url = f"sqlite:///{db_path}"

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=db_url)

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            # Row 1: missing code_norm, market, hot fields, payload row
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, name, report_type, "
                    " sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    " raw_result, news_content, context_snapshot, "
                    " analysis_phase, created_at) "
                    "VALUES (:qid, :code, :name, :rt, "
                    " :ss, :oa, :tp, :as_, :rr, :nc, :cs, "
                    " :ap, :ca)"
                ),
                {
                    "qid": "q-backfill-001",
                    "code": "000001",
                    "name": "PingAn",
                    "rt": "single_stock",
                    "ss": 65,
                    "oa": "buy",
                    "tp": "up",
                    "as_": "Summary",
                    "rr": '{"model_used": "gpt-4", "code": "000001", "name": "PingAn"}',
                    "nc": "News",
                    "cs": '{"realtime_quote": {"price": 12.5, "change_pct": 1.0, "volume_ratio": 0.8, "turnover_rate": 1.2}}',
                    "ap": "premarket",
                    "ca": datetime.now(),
                },
            )
            # Row 2: HK code needing code_norm
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, name, report_type, "
                    " sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    " raw_result, news_content, context_snapshot, "
                    " analysis_phase, created_at) "
                    "VALUES (:qid, :code, :name, :rt, "
                    " :ss, :oa, :tp, :as_, :rr, :nc, :cs, "
                    " :ap, :ca)"
                ),
                {
                    "qid": "q-backfill-hk-001",
                    "code": "HK00700",
                    "name": "Tencent",
                    "rt": "single_stock",
                    "ss": 70,
                    "oa": "hold",
                    "tp": "neutral",
                    "as_": "HK Summary",
                    "rr": '{"model_used": "deepseek-v4", "code": "HK00700"}',
                    "nc": "HK News",
                    "cs": '{"realtime_quote": {"price": 380.0, "change_pct": 1.5}}',
                    "ap": "postmarket",
                    "ca": datetime.now(),
                },
            )

        return db

    def test_backfill_populates_code_norm_and_market(self, db_with_legacy_data):
        """Backfill should populate code_norm and market on legacy rows."""
        counts = db_with_legacy_data.backfill_phase1(batch_size=10)
        assert counts['analysis_code_norm'] >= 2
        assert counts['analysis_market'] >= 2
        assert counts['analysis_payload_rows'] >= 2

        from sqlalchemy import text as sa_text
        with db_with_legacy_data.session_scope() as session:
            # CN code
            row = session.execute(
                sa_text("SELECT code_norm, market FROM analysis_history WHERE query_id = 'q-backfill-001'")
            ).fetchone()
            assert row is not None
            assert row[0] == "000001", f"Expected code_norm=000001, got {row[0]}"
            assert row[1] == "cn", f"Expected market=cn, got {row[1]}"

            # HK code
            row_hk = session.execute(
                sa_text("SELECT code_norm, market FROM analysis_history WHERE query_id = 'q-backfill-hk-001'")
            ).fetchone()
            assert row_hk is not None
            assert row_hk[0] == "HK00700", f"Expected code_norm=HK00700, got {row_hk[0]}"
            assert row_hk[1] == "hk", f"Expected market=hk, got {row_hk[1]}"

    def test_backfill_is_idempotent(self, db_with_legacy_data):
        """Running backfill twice should not duplicate payload rows."""
        counts1 = db_with_legacy_data.backfill_phase1(batch_size=10)
        counts2 = db_with_legacy_data.backfill_phase1(batch_size=10)

        # Second run should produce 0 additional payload rows (INSERT OR IGNORE)
        assert counts2['analysis_payload_rows'] == 0

        from sqlalchemy import text as sa_text
        with db_with_legacy_data.session_scope() as session:
            count = session.execute(
                sa_text("SELECT COUNT(*) FROM analysis_history_payload")
            ).fetchone()[0]
            assert count == counts1['analysis_payload_rows']

    def test_backfill_handles_malformed_json(self, tmp_path):
        """Backfill should skip rows with malformed JSON without crashing."""
        db_path = str(tmp_path / "test_backfill_malformed.db")
        db_url = f"sqlite:///{db_path}"

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=db_url)

        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, name, report_type, "
                    " sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    " raw_result, "
                    " analysis_phase, created_at) "
                    "VALUES (:qid, :code, :name, :rt, "
                    " :ss, :oa, :tp, :as_, :rr, "
                    " :ap, :ca)"
                ),
                {
                    "qid": "q-malformed-001",
                    "code": "600519",
                    "name": "Test",
                    "rt": "single_stock",
                    "ss": 50,
                    "oa": "hold",
                    "tp": "neutral",
                    "as_": "Bad JSON",
                    "rr": "NOT VALID JSON {{{[[[",
                    "ap": "postmarket",
                    "ca": datetime.now(),
                },
            )

        # Should not raise
        counts = db.backfill_phase1(batch_size=10)
        assert 'analysis_code_norm' in counts
        # Should still backfill code_norm at minimum
        assert counts['analysis_code_norm'] >= 1
