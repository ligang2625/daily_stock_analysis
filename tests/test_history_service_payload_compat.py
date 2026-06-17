# -*- coding: utf-8 -*-
"""Tests for HistoryService compatibility with Phase 1 hot/payload split."""

from __future__ import annotations

from datetime import date, datetime

import pytest


class TestHistoryServicePayloadCompat:
    """Test that HistoryService works with both new and old records."""

    @pytest.fixture
    def db_with_data(self, tmp_path):
        """Create a DB with both new-style and old-style records."""
        db_path = str(tmp_path / "test_hs_compat.db")
        db_url = f"sqlite:///{db_path}"

        from src.storage import DatabaseManager

        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=db_url)

        # Create a new-style record (with hot columns + payload)
        class MockResult:
            code = "600519"
            name = "茅台"
            sentiment_score = 70
            operation_advice = "hold"
            trend_prediction = "震荡"
            analysis_summary = "测试摘要"
            model_used = "deepseek-v4"
            current_price = 1820.0
            change_pct = 1.5

        db.save_analysis_history(
            result=MockResult(),
            query_id="q-new-001",
            report_type="single_stock",
            news_content="News for new record",
            context_snapshot={"realtime_quote": {"volume_ratio": 1.2, "turnover_rate": 2.5}},
            analysis_phase="postmarket",
            effective_trading_date=date.today(),
        )

        # Create an old-style record (no hot columns, legacy payload only)
        from sqlalchemy import text as sa_text
        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history (query_id, code, name, report_type, "
                    "sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    "raw_result, news_content, context_snapshot, analysis_phase, "
                    "created_at) "
                    "VALUES (:qid, :code, :name, :rt, :ss, :oa, :tp, :as_, :rr, :nc, :cs, :ap, :ca)"
                ),
                {
                    "qid": "q-old-001",
                    "code": "000001",
                    "name": "平安银行",
                    "rt": "single_stock",
                    "ss": 65,
                    "oa": "buy",
                    "tp": "上涨",
                    "as_": "旧记录摘要",
                    "rr": '{"model_used": "gpt-4", "code": "000001", "name": "平安银行"}',
                    "nc": "News for old record",
                    "cs": '{"realtime_quote": {"volume_ratio": 0.8, "price": 12.5, "change_pct": -0.5, "turnover_rate": 1.2}}',
                    "ap": "premarket",
                    "ca": datetime(2026, 6, 10),
                },
            )

        return db

    def test_list_uses_hot_columns_for_new_records(self, db_with_data):
        """New records should use hot columns instead of parsing payload."""
        from src.services.history_service import HistoryService
        hs = HistoryService(db_manager=db_with_data)

        result = hs.get_history_list(
            stock_code="600519",
            page=1,
            limit=10,
        )

        assert result["total"] >= 1
        items = result["items"]
        new_item = next((i for i in items if i["stock_code"] == "600519"), None)
        assert new_item is not None
        assert new_item["model_used"] == "deepseek-v4"
        assert new_item["current_price"] == 1820.0
        assert new_item["change_pct"] == 1.5
        assert new_item["volume_ratio"] == 1.2

    def test_list_falls_back_to_payload_for_old_records(self, db_with_data):
        """Old records without hot columns should fall back to payload parsing."""
        from src.services.history_service import HistoryService
        hs = HistoryService(db_manager=db_with_data)

        result = hs.get_history_list(
            stock_code="000001",
            page=1,
            limit=10,
        )

        items = result["items"]
        old_item = next((i for i in items if i["stock_code"] == "000001"), None)
        assert old_item is not None
        assert old_item["model_used"] == "gpt-4"
        assert old_item["current_price"] == 12.5
        assert old_item["change_pct"] == -0.5
        assert old_item["volume_ratio"] == 0.8

    def test_detail_uses_hot_columns(self, db_with_data):
        """Detail view should prefer hot model_used column."""
        from src.services.history_service import HistoryService
        hs = HistoryService(db_manager=db_with_data)

        # Find the new record
        with db_with_data.session_scope() as session:
            from sqlalchemy import select as sa_select
            from src.storage import AnalysisHistory
            rec = session.execute(
                sa_select(AnalysisHistory).where(
                    AnalysisHistory.query_id == "q-new-001"
                )
            ).scalars().first()
            rec_id = rec.id

        detail = hs.get_history_detail_by_id(rec_id)
        assert detail is not None
        assert detail["model_used"] == "deepseek-v4"
        assert detail["stock_code"] == "600519"

    def test_deletion_removes_payload_row(self, db_with_data):
        """Deletion should remove both history and payload rows."""
        with db_with_data.session_scope() as session:
            from sqlalchemy import select as sa_select
            from src.storage import AnalysisHistory
            rec = session.execute(
                sa_select(AnalysisHistory).where(
                    AnalysisHistory.query_id == "q-new-001"
                )
            ).scalars().first()
            rec_id = rec.id

        from src.services.history_service import HistoryService
        hs = HistoryService(db_manager=db_with_data)
        hs.delete_history_records([rec_id])

        # Verify deletion
        with db_with_data.session_scope() as session:
            from sqlalchemy import select as sa_select
            from src.storage import AnalysisHistory, AnalysisHistoryPayload
            rec = session.execute(
                sa_select(AnalysisHistory).where(
                    AnalysisHistory.id == rec_id
                )
            ).scalars().first()
            assert rec is None

            payload = session.execute(
                sa_select(AnalysisHistoryPayload).where(
                    AnalysisHistoryPayload.history_id == rec_id
                )
            ).scalars().first()
            assert payload is None


class TestHistoryServiceResponseShape:
    """Verify response shape compatibility with Phase 1 changes."""

    def test_response_fields_present(self, tmp_path):
        """All expected fields should be in list and detail responses."""
        db_path = str(tmp_path / "test_hs_shape.db")
        db_url = f"sqlite:///{db_path}"

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=db_url)

        class MockResult:
            code = "600519"
            name = "茅台"
            sentiment_score = 70
            operation_advice = "hold"
            trend_prediction = "震荡"
            analysis_summary = "摘要"
            model_used = "deepseek-v4"
            current_price = 1820.0
            change_pct = 1.5

        db.save_analysis_history(
            result=MockResult(),
            query_id="q-shape-001",
            report_type="single_stock",
            news_content="News",
            context_snapshot={"realtime_quote": {"volume_ratio": 1.5, "turnover_rate": 2.0, "current_price": 1820, "change_pct": 1.5}},
            analysis_phase="postmarket",
            effective_trading_date=date.today(),
        )

        from src.services.history_service import HistoryService
        hs = HistoryService(db_manager=db)

        # Test list response shape
        result = hs.get_history_list(stock_code="600519", page=1, limit=10)
        item = result["items"][0]

        required_list_fields = [
            "id", "query_id", "stock_code", "stock_name", "report_type",
            "trend_prediction", "analysis_summary", "sentiment_score",
            "operation_advice", "action", "action_label", "model_used",
            "created_at", "market_phase_summary", "current_price",
            "change_pct", "volume_ratio", "turnover_rate",
        ]
        for field in required_list_fields:
            assert field in item, f"Missing field '{field}' in list item"

        # Test detail response shape
        detail = hs.get_history_detail_by_id(item["id"])
        required_detail_fields = [
            "id", "query_id", "stock_code", "stock_name", "report_type",
            "created_at", "model_used", "analysis_summary", "operation_advice",
            "action", "action_label", "trend_prediction", "sentiment_score",
            "sentiment_label", "ideal_buy", "secondary_buy", "stop_loss",
            "take_profit", "news_content", "raw_result", "context_snapshot",
        ]
        for field in required_detail_fields:
            assert field in detail, f"Missing field '{field}' in detail"
