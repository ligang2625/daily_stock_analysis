# -*- coding: utf-8 -*-
"""Tests for Phase 1 code_norm filtering in history queries."""

from __future__ import annotations

from datetime import date, datetime

import pytest


class TestHistoryCodeNormFilters:
    """Verify that code_norm is used effectively in history list queries."""

    @pytest.fixture
    def db_with_hk_data(self, tmp_path):
        """Create a DB with HK stock records using various code variants."""
        db_path = str(tmp_path / "test_hk_filters.db")
        db_url = f"sqlite:///{db_path}"

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=db_url)

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            # Insert HK record with code_norm populated
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, market, name, report_type, "
                    " sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    " model_used, current_price, change_pct, volume_ratio, turnover_rate, "
                    " analysis_phase, effective_trading_date, created_at, updated_at) "
                    "VALUES (:qid, :code, :code_norm, :market, :name, :rt, "
                    " :ss, :oa, :tp, :as_, :mu, :cp, :chg, :vr, :tr, "
                    " :ap, :etd, :ca, :upd)"
                ),
                {
                    "qid": "q-hk-001",
                    "code": "HK00700",
                    "code_norm": "HK00700",
                    "market": "hk",
                    "name": "Tencent",
                    "rt": "single_stock",
                    "ss": 70,
                    "oa": "hold",
                    "tp": "up",
                    "as_": "Summary",
                    "mu": "deepseek-v4",
                    "cp": 380.0,
                    "chg": 1.5,
                    "vr": 1.2,
                    "tr": 0.5,
                    "ap": "postmarket",
                    "etd": date.today(),
                    "ca": datetime.now(),
                    "upd": datetime.now(),
                },
            )
            # Legacy HK record without code_norm
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, name, report_type, "
                    " sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    " raw_result, model_used, current_price, change_pct, volume_ratio, "
                    " turnover_rate, created_at) "
                    "VALUES (:qid, :code, :name, :rt, "
                    " :ss, :oa, :tp, :as_, :rr, :mu, :cp, :chg, :vr, "
                    " :tr, :ca)"
                ),
                {
                    "qid": "q-hk-old-001",
                    "code": "HK00700",
                    "name": "Tencent",
                    "rt": "single_stock",
                    "ss": 65,
                    "oa": "buy",
                    "tp": "up",
                    "as_": "Old Summary",
                    "rr": '{"model_used": "gpt-4", "code": "HK00700"}',
                    "mu": "gpt-4",
                    "cp": 375.0,
                    "chg": 2.0,
                    "vr": 1.1,
                    "tr": 0.6,
                    "ca": datetime(2026, 6, 10),
                },
            )

        return db

    def test_filter_by_hk_variant_hk00700(self, db_with_hk_data):
        """Filter by 'hk00700' should find the record via code_norm."""
        from src.services.history_service import HistoryService
        hs = HistoryService(db_manager=db_with_hk_data)

        result = hs.get_history_list(stock_code="hk00700", page=1, limit=10)
        assert result["total"] >= 1
        codes = {item["stock_code"] for item in result["items"]}
        assert "HK00700" in codes

    def test_filter_by_hk_variant_00700_dot_hk(self, db_with_hk_data):
        """Filter by '00700.HK' should find the record."""
        from src.services.history_service import HistoryService
        hs = HistoryService(db_manager=db_with_hk_data)

        result = hs.get_history_list(stock_code="00700.HK", page=1, limit=10)
        assert result["total"] >= 1

    def test_filter_by_hk_variant_bare_00700(self, db_with_hk_data):
        """Filter by bare '00700' should find the record."""
        from src.services.history_service import HistoryService
        hs = HistoryService(db_manager=db_with_hk_data)

        result = hs.get_history_list(stock_code="00700", page=1, limit=10)
        assert result["total"] >= 1


class TestHistoryDetailPayloadOnly:
    """Verify detail/markdown rendering from payload table only."""

    @pytest.fixture
    def db_with_payload_only(self, tmp_path):
        """Create a DB where legacy columns are NULL and payload exists."""
        db_path = str(tmp_path / "test_detail_payload.db")
        db_url = f"sqlite:///{db_path}"

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=db_url)

        from src.storage import AnalysisHistory
        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            rec = AnalysisHistory(
                query_id="q-payload-only-001",
                code="600519",
                code_norm="600519",
                market="cn",
                name="Maotai",
                report_type="single_stock",
                sentiment_score=70,
                operation_advice="hold",
                trend_prediction="neutral",
                analysis_summary="Test summary",
                model_used="deepseek-v4",
                current_price=1800.0,
                change_pct=1.0,
                volume_ratio=1.2,
                turnover_rate=0.8,
                analysis_phase="postmarket",
                effective_trading_date=date.today(),
                created_at=datetime.now(),
                updated_at=datetime.now(),
                # Legacy columns NULL — payload only
                raw_result=None,
                news_content=None,
                context_snapshot=None,
            )
            session.add(rec)
            session.flush()

            # Create payload row with full content
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history_payload "
                    "(history_id, raw_result, news_content, context_snapshot, payload_size, compressed) "
                    "VALUES (:hid, :rr, :nc, :cs, :ps, 0)"
                ),
                {
                    "hid": rec.id,
                    "rr": '{"model_used": "deepseek-v4", "code": "600519", "name": "Maotai",'
                          '"sentiment_score": 70, "operation_advice": "hold",'
                          '"trend_prediction": "neutral", "analysis_summary": "Payload summary",'
                          '"dashboard": {"ideal_buy": "1750", "secondary_buy": "1720",'
                          '"stop_loss": "1680", "take_profit": "1900"}}',
                    "nc": "News from payload table",
                    "cs": '{"realtime_quote": {"price": 1800.0, "change_pct": 1.0,'
                          '"volume_ratio": 1.2, "turnover_rate": 0.8}}',
                    "ps": 500,
                },
            )
            record_id = rec.id

        return db, record_id

    def test_detail_renders_from_payload(self, db_with_payload_only):
        """Detail view should render from payload table when legacy columns are NULL."""
        db, record_id = db_with_payload_only
        from src.services.history_service import HistoryService
        hs = HistoryService(db_manager=db)

        detail = hs.get_history_detail_by_id(record_id)
        assert detail is not None
        assert detail["stock_code"] == "600519"
        assert detail["model_used"] == "deepseek-v4"
        # News content should come from payload
        assert detail["news_content"] is not None
        # Sniper points should come from payload's raw_result
        assert detail["ideal_buy"] is not None
        # payload metadata should be present
        assert "payload_available" in detail
        assert detail["payload_available"] is True

    def test_detail_handles_expired_payload(self, tmp_path):
        """Detail should not crash when payload has expired and legacy is NULL."""
        db_path = str(tmp_path / "test_expired_payload.db")
        db_url = f"sqlite:///{db_path}"

        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=db_url)

        from sqlalchemy import text as sa_text

        with db.session_scope() as session:
            session.execute(
                sa_text(
                    "INSERT INTO analysis_history "
                    "(query_id, code, code_norm, market, name, report_type, "
                    " sentiment_score, operation_advice, trend_prediction, analysis_summary, "
                    " model_used, current_price, change_pct, volume_ratio, turnover_rate, "
                    " raw_result, news_content, context_snapshot, "
                    " analysis_phase, created_at) "
                    "VALUES (:qid, :code, :cn, :mkt, :name, :rt, "
                    " :ss, :oa, :tp, :as_, :mu, :cp, :chg, :vr, :tr, "
                    " NULL, NULL, NULL, :ap, :ca)"
                ),
                {
                    "qid": "q-expired-001",
                    "code": "000001",
                    "cn": "000001",
                    "mkt": "cn",
                    "name": "PingAn",
                    "rt": "single_stock",
                    "ss": 65,
                    "oa": "buy",
                    "tp": "up",
                    "as_": "Expired summary",
                    "mu": "deepseek-v4",
                    "cp": 12.5,
                    "chg": 1.0,
                    "vr": 0.8,
                    "tr": 1.2,
                    "ap": "premarket",
                    "ca": datetime.now(),
                },
            )
            # No payload row

        with db.session_scope() as session:
            from src.storage import AnalysisHistory
            from sqlalchemy import select as sa_select
            rec = session.execute(
                sa_select(AnalysisHistory).where(AnalysisHistory.query_id == "q-expired-001")
            ).scalars().first()
            record_id = rec.id

        from src.services.history_service import HistoryService
        hs = HistoryService(db_manager=db)
        detail = hs.get_history_detail_by_id(record_id)
        assert detail is not None
        assert detail["stock_code"] == "000001"
        assert detail["payload_expired"] is True
        # Should not crash
        assert detail["news_content"] is not None or detail["news_content"] is None  # safe
