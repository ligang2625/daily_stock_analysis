# -*- coding: utf-8 -*-
"""Tests for Phase 1 payload split and DB migration compatibility."""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from src.storage import (
    DatabaseManager,
    AnalysisHistory,
    AnalysisHistoryPayload,
    StockDaily,
    CURRENT_SCHEMA_VERSION,
)


class TestAdditiveMigration:
    """Test that schema migration works idempotently on a fresh database."""

    def test_fresh_db_creates_all_tables(self, tmp_path):
        """A fresh SQLite database should have all required tables and columns."""
        db_path = str(tmp_path / "test_migration.db")
        db_url = f"sqlite:///{db_path}"

        # Reset singleton for test
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=db_url)

        with db.session_scope() as session:
            # analysis_history_payload should exist
            from sqlalchemy import text as sa_text
            tables = session.execute(
                sa_text("SELECT name FROM sqlite_master WHERE type='table'")
            ).fetchall()
            table_names = {t[0] for t in tables}
            assert 'analysis_history_payload' in table_names
            assert 'analysis_history' in table_names
            assert 'stock_daily' in table_names

            # Check new columns on analysis_history
            info = session.execute(
                sa_text("PRAGMA table_info('analysis_history')")
            ).fetchall()
            cols = {row[1] for row in info}
            for col in ('code_norm', 'market', 'model_used', 'current_price',
                        'change_pct', 'volume_ratio', 'turnover_rate',
                        'market_phase_summary', 'updated_at'):
                assert col in cols, f"Missing column: {col}"

            # Check code_norm on stock_daily
            sd_info = session.execute(
                sa_text("PRAGMA table_info('stock_daily')")
            ).fetchall()
            sd_cols = {row[1] for row in sd_info}
            assert 'code_norm' in sd_cols

    def test_repeat_migration_is_safe(self, tmp_path):
        """Re-running migration on the same DB should not fail."""
        db_path = str(tmp_path / "test_repeat_migration.db")
        db_url = f"sqlite:///{db_path}"

        # First init
        DatabaseManager.reset_instance()
        db1 = DatabaseManager(db_url=db_url)
        db1._engine.dispose()

        # Second init (simulates restart)
        DatabaseManager.reset_instance()
        db2 = DatabaseManager(db_url=db_url)
        assert db2._initialized

    def test_indexes_exist(self, tmp_path):
        """Compound indexes should be created."""
        db_path = str(tmp_path / "test_indexes.db")
        db_url = f"sqlite:///{db_path}"

        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=db_url)

        with db.session_scope() as session:
            from sqlalchemy import text as sa_text
            indexes = session.execute(
                sa_text("SELECT name FROM sqlite_master WHERE type='index'")
            ).fetchall()
            index_names = {i[0] for i in indexes}
            assert 'ix_analysis_yesterday_lookup_norm' in index_names
            assert 'ix_analysis_market_review_date' in index_names
            assert 'ix_analysis_created_id' in index_names
            assert 'ix_stock_daily_norm_date' in index_names


class TestAnalysisHistoryPayloadSplit:
    """Test that new writes split hot fields from payloads."""

    @pytest.fixture
    def db(self, tmp_path):
        """Provide a fresh isolated DatabaseManager."""
        db_path = str(tmp_path / "test_payload_split.db")
        db_url = f"sqlite:///{db_path}"
        DatabaseManager.reset_instance()
        return DatabaseManager(db_url=db_url)

    class _MockResult:
        """A simple mock result class that can be instantiated."""
        def __init__(self, code="600519", name="茅台", score=70):
            self.code = code
            self.name = name
            self.sentiment_score = score
            self.operation_advice = "hold"
            self.trend_prediction = "震荡"
            self.analysis_summary = "测试摘要"
            self.model_used = "gpt-4"
            self.current_price = 1820.0
            self.change_pct = 1.5

    def test_save_creates_hot_fields_and_payload(self, db):
        """New save should populate hot columns AND create a payload row."""
        result = self._MockResult()
        query_id = "test-query-001"
        context = {"realtime_quote": {"volume_ratio": 1.2, "turnover_rate": 2.5}}

        db.save_analysis_history(
            result=result,
            query_id=query_id,
            report_type="single_stock",
            news_content="Some news",
            context_snapshot=context,
            analysis_phase="postmarket",
            effective_trading_date=date.today(),
        )

        with db.session_scope() as session:
            from sqlalchemy import select as sa_select
            history = session.execute(
                sa_select(AnalysisHistory).where(
                    AnalysisHistory.query_id == query_id
                )
            ).scalars().first()
            assert history is not None
            assert history.code_norm == "600519"
            assert history.market == "cn"
            assert history.model_used == "gpt-4"
            assert history.current_price == 1820.0
            assert history.change_pct == 1.5
            assert history.volume_ratio == 1.2
            assert history.turnover_rate == 2.5
            assert history.updated_at is not None

            payload = session.execute(
                sa_select(AnalysisHistoryPayload).where(
                    AnalysisHistoryPayload.history_id == history.id
                )
            ).scalars().first()
            assert payload is not None
            assert payload.payload_size is not None
            assert payload.payload_size > 0
            assert payload.raw_result is not None

            # Phase 1: legacy columns should be NULL — payload lives in payload table only
            assert history.raw_result is None, "Legacy raw_result should be NULL (payload in payload table)"
            assert history.news_content is None, "Legacy news_content should be NULL (payload in payload table)"
            assert history.context_snapshot is None, "Legacy context_snapshot should be NULL (payload in payload table)"
            # But payload row should have the content
            assert payload.raw_result is not None, "Payload table should have raw_result"
            assert payload.news_content is not None, "Payload table should have news_content"

    def test_get_analysis_history_payload(self, db):
        """Test the payload helper method."""
        result = self._MockResult()
        query_id = "test-query-002"

        db.save_analysis_history(
            result=result,
            query_id=query_id,
            report_type="single_stock",
            news_content="News",
            analysis_phase="premarket",
        )

        history_id = None
        with db.session_scope() as session:
            from sqlalchemy import select as sa_select
            history = session.execute(
                sa_select(AnalysisHistory).where(
                    AnalysisHistory.query_id == query_id
                )
            ).scalars().first()
            history_id = history.id

        payload = db.get_analysis_history_payload(history_id)
        assert payload is not None
        assert payload.history_id == history_id
        assert payload.raw_result is not None

    def test_get_analysis_history_with_payload(self, db):
        """Test the merge helper method."""
        result = self._MockResult()
        query_id = "test-query-003"

        db.save_analysis_history(
            result=result,
            query_id=query_id,
            report_type="single_stock",
            news_content="News content",
            analysis_phase="intraday",
        )

        history_id = None
        with db.session_scope() as session:
            from sqlalchemy import select as sa_select
            history = session.execute(
                sa_select(AnalysisHistory).where(
                    AnalysisHistory.query_id == query_id
                )
            ).scalars().first()
            history_id = history.id

        merged = db.get_analysis_history_with_payload(history_id)
        assert merged is not None
        assert merged['id'] == history_id
        assert merged['code'] == '600519'
        assert merged['code_norm'] == '600519'
        assert 'payload_raw_result' in merged
        assert merged['payload_news_content'] == 'News content'

    def test_delete_also_removes_payload(self, db):
        """Deleting an analysis history should also delete its payload row."""
        result = self._MockResult()
        query_id = "test-query-004"

        db.save_analysis_history(
            result=result,
            query_id=query_id,
            report_type="single_stock",
            news_content="News",
            analysis_phase="postmarket",
        )

        history_id = None
        with db.session_scope() as session:
            from sqlalchemy import select as sa_select
            history = session.execute(
                sa_select(AnalysisHistory).where(
                    AnalysisHistory.query_id == query_id
                )
            ).scalars().first()
            history_id = history.id

        db.delete_analysis_history_records([history_id])

        with db.session_scope() as session:
            from sqlalchemy import select as sa_select
            payload = session.execute(
                sa_select(AnalysisHistoryPayload).where(
                    AnalysisHistoryPayload.history_id == history_id
                )
            ).scalars().first()
            assert payload is None

    def test_hk_code_normalization_on_save(self, db):
        """HK stock codes should get normalized code_norm."""
        result = self._MockResult(code="HK00700", name="腾讯")
        query_id = "test-query-hk-001"

        db.save_analysis_history(
            result=result,
            query_id=query_id,
            report_type="single_stock",
            news_content="News",
            analysis_phase="postmarket",
        )

        with db.session_scope() as session:
            from sqlalchemy import select as sa_select
            history = session.execute(
                sa_select(AnalysisHistory).where(
                    AnalysisHistory.query_id == query_id
                )
            ).scalars().first()
            assert history.code_norm == "HK00700"
            assert history.market == "hk"
            assert history.code == "HK00700"


class TestStockDailyCodeNorm:
    """Test that save_daily_data populates code_norm."""

    def test_cn_code_norm(self, tmp_path):
        import pandas as pd

        db_path = str(tmp_path / "test_sd_norm.db")
        db_url = f"sqlite:///{db_path}"
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=db_url)

        df = pd.DataFrame({
            'date': [date.today()],
            'open': [100.0],
            'high': [105.0],
            'low': [99.0],
            'close': [102.0],
            'volume': [1000000],
            'amount': [102000000],
            'pct_chg': [1.0],
        })

        db.save_daily_data(df, '600519', data_source='test')

        with db.session_scope() as session:
            from sqlalchemy import select as sa_select
            row = session.execute(
                sa_select(StockDaily).where(StockDaily.code == '600519')
            ).scalars().first()
            assert row is not None
            assert row.code_norm == "600519"
