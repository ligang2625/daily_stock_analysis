# -*- coding: utf-8 -*-
"""Tests for Phase 2 historical database schema: table creation, constraints, upsert idempotency."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

import pytest


class TestHistoricalStorageSchema:
    """Verify Phase 2 tables exist and constraints work."""

    @pytest.fixture
    def historical_env(self, tmp_path, monkeypatch):
        """Setup temporary historical database with Phase 2 tables."""
        historical_db = str(tmp_path / "test_historical_schema.db")
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db)

        from src.storage_historical import HistoricalDatabaseManager
        mgr = HistoricalDatabaseManager(db_path=historical_db)
        yield mgr
        # Cleanup
        mgr._engine.dispose()

    def test_tables_created_on_init(self, historical_env):
        """All Phase 2 tables + archive_runs should exist after init."""
        mgr = historical_env
        with mgr.get_session() as session:
            from sqlalchemy import text as sa_text
            tables = session.execute(
                sa_text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            ).fetchall()
            table_names = {row[0] for row in tables}

        assert 'archive_runs' in table_names
        assert 'historical_stock_daily' in table_names
        assert 'historical_intraday_quote_points' in table_names
        assert 'historical_postmarket_technical_summary' in table_names
        assert 'historical_market_light_daily' in table_names

    def test_repeated_init_is_idempotent(self, tmp_path, monkeypatch):
        """Repeated HistoricalDatabaseManager init should not error."""
        historical_db = str(tmp_path / "test_idempotent.db")
        monkeypatch.setenv('HISTORICAL_DATABASE_PATH', historical_db)

        from src.storage_historical import HistoricalDatabaseManager
        mgr1 = HistoricalDatabaseManager(db_path=historical_db)
        mgr2 = HistoricalDatabaseManager(db_path=historical_db)

        with mgr2.get_session() as session:
            from sqlalchemy import text as sa_text
            tables = session.execute(
                sa_text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            ).fetchall()
            table_names = {row[0] for row in tables}
        mgr1._engine.dispose()
        mgr2._engine.dispose()

        assert 'historical_stock_daily' in table_names
        assert 'historical_intraday_quote_points' in table_names

    def test_upsert_stock_daily_idempotent(self, historical_env):
        """Duplicate upsert should not create duplicate rows."""
        mgr = historical_env

        rows = [
            {
                'code': '600519', 'code_norm': '600519', 'market': 'cn',
                'trade_date': '2026-05-01', 'open': 100.0, 'high': 102.0,
                'low': 99.0, 'close': 101.0, 'pre_close': 100.5,
                'pct_chg': 1.0, 'volume': 1000000, 'amount': 100000000.0,
                'volume_ratio': 1.2, 'ma5': 100.5, 'ma10': 99.8, 'ma20': 98.5,
                'bias5': 0.5, 'bias10': 1.2, 'bias20': 2.5,
                'data_source': 'main.stock_daily',
            },
        ]

        # First upsert
        written1 = mgr.upsert_stock_daily_batch(rows)
        assert written1 == 1

        # Second upsert with same key
        written2 = mgr.upsert_stock_daily_batch(rows)
        # Should still be 1 (upsert, not insert)
        assert written2 == 1

        # Verify only one row exists
        with mgr.get_session() as session:
            from sqlalchemy import text, select, func
            from src.storage_historical import HistoricalStockDaily
            count = session.execute(
                select(func.count()).select_from(HistoricalStockDaily)
            ).scalar_one()
        assert count == 1

    def test_upsert_intraday_quote_points_idempotent(self, historical_env):
        """Duplicate intraday upsert should not create duplicates."""
        mgr = historical_env

        rows = [
            {
                'query_date': '2026-05-01', 'market': 'cn',
                'stock_code': '600519', 'stock_code_norm': '600519',
                'stock_name': u'贵州茅台', 'timestamp': '2026-05-01T10:30:00',
                'snapshot_id': 'snap_001', 'current_price': 101.5,
                'change_pct': 0.5, 'volume_ratio': 1.1,
                'event_type': 'price_only',
            },
        ]

        written1 = mgr.upsert_intraday_quote_points_batch(rows)
        assert written1 == 1

        written2 = mgr.upsert_intraday_quote_points_batch(rows)
        assert written2 == 1

        with mgr.get_session() as session:
            from sqlalchemy import select, func
            from src.storage_historical import HistoricalIntradayQuotePoint
            count = session.execute(
                select(func.count()).select_from(HistoricalIntradayQuotePoint)
            ).scalar_one()
        assert count == 1

    def test_upsert_postmarket_summaries_idempotent(self, historical_env):
        """Duplicate postmarket summary upsert should not create duplicates."""
        mgr = historical_env

        rows = [
            {
                'source_history_id': 1, 'code': '600519', 'code_norm': '600519',
                'market': 'cn', 'trade_date': '2026-05-01',
                'analysis_phase': 'postmarket', 'report_type': 'daily',
                'close': 101.0, 'pct_chg': 1.0, 'volume_ratio': 1.2,
                'ideal_buy': 95.0, 'secondary_buy': 92.0, 'stop_loss': 90.0,
                'take_profit': 110.0, 'trend_prediction': 'bullish',
                'operation_advice': 'hold', 'risk_level': 60,
                'technical_summary': 'Strong uptrend',
            },
        ]

        written1 = mgr.upsert_postmarket_summaries_batch(rows)
        assert written1 == 1

        written2 = mgr.upsert_postmarket_summaries_batch(rows)
        assert written2 == 1

        with mgr.get_session() as session:
            from sqlalchemy import select, func
            from src.storage_historical import HistoricalPostmarketTechnicalSummary
            count = session.execute(
                select(func.count()).select_from(HistoricalPostmarketTechnicalSummary)
            ).scalar_one()
        assert count == 1

    def test_upsert_market_light_daily_idempotent(self, historical_env):
        """Duplicate market_light upsert should not create duplicates."""
        mgr = historical_env

        rows = [
            {
                'region': 'cn', 'trade_date': '2026-05-01',
                'score': 75, 'status': 'green', 'label': 'Normal',
                'temperature_label': u'正常', 'data_quality': 'ok',
                'risk_notes': '{"reasons": [], "guidance": ""}',
            },
        ]

        written1 = mgr.upsert_market_light_daily_batch(rows)
        assert written1 == 1

        written2 = mgr.upsert_market_light_daily_batch(rows)
        assert written2 == 1

        with mgr.get_session() as session:
            from sqlalchemy import select, func
            from src.storage_historical import HistoricalMarketLightDaily
            count = session.execute(
                select(func.count()).select_from(HistoricalMarketLightDaily)
            ).scalar_one()
        assert count == 1

    def test_historical_stock_daily_nullable_fields(self, historical_env):
        """Minimal valid row with many nulls should be accepted."""
        mgr = historical_env

        rows = [
            {
                'code': 'AAPL', 'code_norm': 'AAPL', 'market': 'us',
                'trade_date': '2026-05-01', 'close': 195.0,
                'data_source': 'main.stock_daily',
            },
        ]

        written = mgr.upsert_stock_daily_batch(rows)
        assert written == 1

        with mgr.get_session() as session:
            from src.storage_historical import HistoricalStockDaily
            result = session.query(HistoricalStockDaily).filter(
                HistoricalStockDaily.code_norm == 'AAPL'
            ).first()
        assert result is not None
        assert result.close == 195.0
        assert result.ma60 is None  # nullable
        assert result.turnover_rate is None  # nullable
