# -*- coding: utf-8 -*-
"""Tests for market_light_daily extraction edge cases and idempotency."""

from __future__ import annotations

import json
import os
from datetime import date, datetime

import pytest


class TestMarketLightDailyArchive:
    """Test market_light extraction edge cases."""

    @pytest.fixture
    def light_env(self, tmp_path, monkeypatch):
        """Setup main + historical DB with a clean environment."""
        main_db_path = str(tmp_path / "test_light_main.db")
        historical_db_path = str(tmp_path / "test_light_hist.db")
        monkeypatch.setenv('DATABASE_PATH', main_db_path)
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db_path)

        from src.config import Config
        Config.reset_instance()

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{main_db_path}")

        from src.storage_historical import HistoricalDatabaseManager
        hist_db = HistoricalDatabaseManager(db_path=historical_db_path)

        yield {'db': db, 'hist_db': hist_db, 'tmp_path': tmp_path}

        DatabaseManager.reset_instance()

    def _insert_market_review(self, db, trade_date: str, snapshots: dict):
        """Helper to insert a market_review analysis_history row."""
        from sqlalchemy import text as sa_text

        context = json.dumps({"market_light_snapshots": snapshots})
        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, name, report_type, analysis_phase, "
                    "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    "context_snapshot, created_at, effective_trading_date) "
                    "VALUES (:qid, :code, :cn, :name, :rtype, :phase, :score, :advice, "
                    ":trend, :summary, :ctx, :ca, :etd)"
                ),
                {
                    "qid": f"mr_{trade_date}", "code": "MR", "cn": "MR",
                    "name": "Market Review", "rtype": "market_review", "phase": "postmarket",
                    "score": 50, "advice": "review", "trend": "review",
                    "summary": "Overview", "ctx": context,
                    "ca": datetime.fromisoformat(trade_date) if '-' in trade_date else datetime.now(),
                    "etd": trade_date,
                },
            )

    def test_idempotent_upsert(self, light_env):
        """Same market_light row upserted twice should not duplicate."""
        db = light_env['db']
        hist_db = light_env['hist_db']

        self._insert_market_review(db, '2026-06-01', {
            "cn": {
                "score": 80, "status": "green", "label": "Normal",
                "temperature_label": u"温和", "data_quality": "ok",
                "reasons": ["good"], "guidance": "hold",
            }
        })

        from src.maintenance.archive_extractors import extract_market_light_daily, extract_phase2_technical_history

        # First upsert
        stats1 = extract_phase2_technical_history(
            main_db=db, historical_db=hist_db,
            cutoff_date=date.today(), dry_run=False,
        )
        assert stats1['market_light_daily']['written'] == 1

        # Second upsert
        stats2 = extract_phase2_technical_history(
            main_db=db, historical_db=hist_db,
            cutoff_date=date.today(), dry_run=False,
        )
        # Should still be 1 written (not duplicated)
        assert stats2['market_light_daily']['written'] == 1

        with hist_db.get_session() as session:
            from sqlalchemy import text
            result = session.execute(
                text("SELECT COUNT(*) FROM historical_market_light_daily")
            ).fetchone()
        assert result[0] == 1

    def test_missing_fields_graceful(self, light_env):
        """Snapshot with missing fields should not fail extraction."""
        db = light_env['db']
        hist_db = light_env['hist_db']

        # Insert with minimal fields
        self._insert_market_review(db, '2026-06-01', {
            "us": {
                "score": 50,
                # No status, label, temperature_label, reasons, guidance, data_quality
            }
        })

        from src.maintenance.archive_extractors import extract_market_light_daily

        with db.session_scope() as session:
            rows = extract_market_light_daily(session, date.today())

        assert len(rows) == 1
        r = rows[0]
        assert r['region'] == 'us'
        assert r['score'] == 50
        assert r['status'] == ''  # empty string for missing
        assert r['data_quality'] == ''

    def test_non_dict_snapshot_skipped(self, light_env):
        """A snapshot that is not a dict within market_light_snapshots should be skipped."""
        db = light_env['db']
        hist_db = light_env['hist_db']

        self._insert_market_review(db, '2026-06-01', {
            "cn": "not_a_dict",  # String instead of dict
        })

        from src.maintenance.archive_extractors import extract_market_light_daily

        with db.session_scope() as session:
            rows = extract_market_light_daily(session, date.today())

        # String snapshots should be skipped
        assert len(rows) == 0

    def test_cn_hk_us_regions(self, light_env):
        """Multiple regions in one market_light_snapshots should produce one row per region."""
        db = light_env['db']
        hist_db = light_env['hist_db']

        self._insert_market_review(db, '2026-06-01', {
            "cn": {"score": 80, "status": "green", "label": "Normal", "data_quality": "ok"},
            "hk": {"score": 65, "status": "yellow", "label": "Caution", "data_quality": "partial"},
            "us": {"score": 55, "status": "yellow", "label": "Monitor", "data_quality": "ok"},
        })

        from src.maintenance.archive_extractors import extract_market_light_daily

        with db.session_scope() as session:
            rows = extract_market_light_daily(session, date.today())

        assert len(rows) == 3
        regions = {r['region'] for r in rows}
        assert regions == {'cn', 'hk', 'us'}
