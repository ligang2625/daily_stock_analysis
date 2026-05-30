# -*- coding: utf-8 -*-
"""
Tests for intraday_monitor.py — _compare_with_thresholds (pure function),
load_yesterday_analysis, monitor_snapshot, and final_decision.
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock

from src.core.intraday_monitor import (
    IntradayEvent,
    IntradayMonitor,
    _compare_with_thresholds,
    _find_sniper_points_in_dict,
    _safe_float,
)


# ============================================================
# _safe_float
# ============================================================

class TestSafeFloat:
    def test_returns_float(self):
        assert _safe_float(3.14) == 3.14

    def test_returns_int_as_float(self):
        assert _safe_float(42) == 42.0

    def test_returns_none_for_none(self):
        assert _safe_float(None) is None

    def test_returns_none_for_empty_string(self):
        assert _safe_float("") is None

    def test_returns_none_for_garbage(self):
        assert _safe_float("abc") is None


# ============================================================
# _find_sniper_points_in_dict
# ============================================================

class TestFindSniperPointsInDict:
    def test_direct_keys(self):
        d = {"ideal_buy": 10.0, "secondary_buy": 9.0, "stop_loss": 8.0, "take_profit": 12.0}
        result = _find_sniper_points_in_dict(d)
        assert result["ideal_buy"] == 10.0
        assert result["take_profit"] == 12.0

    def test_sniper_points_subdict(self):
        d = {"sniper_points": {"ideal_buy": 10.0}}
        result = _find_sniper_points_in_dict(d)
        assert result["ideal_buy"] == 10.0

    def test_battle_plan_nested(self):
        d = {"battle_plan": {"ideal_buy": 15.0}}
        result = _find_sniper_points_in_dict(d)
        assert result["ideal_buy"] == 15.0

    def test_double_nested_dashboard(self):
        d = {"dashboard": {"battle_plan": {"ideal_buy": 20.0}}}
        result = _find_sniper_points_in_dict(d)
        assert result["ideal_buy"] == 20.0

    def test_empty_dict(self):
        assert _find_sniper_points_in_dict({}) == {}

    def test_non_dict(self):
        assert _find_sniper_points_in_dict("not_a_dict") == {}


# ============================================================
# _compare_with_thresholds (pure function)
# ============================================================

class TestCompareWithThresholds:
    _NOW = datetime(2026, 5, 30, 10, 30, 0)
    _YESTERDAY = {
        "ideal_buy": 100.0,
        "secondary_buy": 95.0,
        "stop_loss": 90.0,
        "take_profit": 115.0,
    }

    def test_break_stop_loss(self):
        events = _compare_with_thresholds("000001", "测试", 88.0, 1.0, -2.0, self._YESTERDAY, self._NOW)
        assert events[0].event_type == "break_stop_loss"

    def test_enter_secondary_buy(self):
        # stop_loss=90 < secondary_buy=95 < ideal_buy=100, price=93 → secondary_buy zone
        events = _compare_with_thresholds("000001", "测试", 93.0, 1.0, -1.0, self._YESTERDAY, self._NOW)
        assert events[0].event_type == "enter_secondary_buy"

    def test_enter_ideal_buy(self):
        events = _compare_with_thresholds("000001", "测试", 100.0, 1.0, 0.0, self._YESTERDAY, self._NOW)
        assert events[0].event_type == "enter_ideal_buy"

    def test_near_buy_zone(self):
        events = _compare_with_thresholds("000001", "测试", 101.5, 0.5, 0.5, self._YESTERDAY, self._NOW)
        assert events[0].event_type == "near_buy_zone"

    def test_enter_take_profit(self):
        events = _compare_with_thresholds("000001", "测试", 116.0, 1.0, 5.0, self._YESTERDAY, self._NOW)
        assert events[0].event_type == "enter_take_profit"

    def test_price_only(self):
        events = _compare_with_thresholds("000001", "测试", 108.0, 1.0, 1.0, self._YESTERDAY, self._NOW)
        assert events[0].event_type == "price_only"

    def test_unusual_volume_simultaneous(self):
        events = _compare_with_thresholds("000001", "测试", 88.0, 5.0, -5.0, self._YESTERDAY, self._NOW)
        types = [e.event_type for e in events]
        assert "break_stop_loss" in types
        assert "unusual_volume" in types

    def test_no_thresholds(self):
        """When no thresholds available, only price_only event."""
        events = _compare_with_thresholds("000001", "测试", 100.0, 1.0, 0.0, {}, self._NOW)
        assert events[0].event_type == "price_only"

    def test_single_most_severe_event(self):
        """Only one price event per snapshot — the most severe one."""
        events = _compare_with_thresholds("000001", "测试", 80.0, 1.0, -10.0, self._YESTERDAY, self._NOW)
        price_events = [e for e in events if e.event_type != "unusual_volume"]
        assert len(price_events) == 1
        assert price_events[0].event_type == "break_stop_loss"

    def test_volume_ratio_less_than_3_no_volume_event(self):
        events = _compare_with_thresholds("000001", "测试", 88.0, 2.9, -5.0, self._YESTERDAY, self._NOW)
        types = [e.event_type for e in events]
        assert "unusual_volume" not in types


# ============================================================
# IntradayMonitor — load_yesterday_analysis
# ============================================================

class TestLoadYesterdayAnalysis:
    @pytest.fixture(autouse=True)
    def _check_deps(self):
        try:
            import pandas  # noqa: F401
        except ImportError:
            pytest.skip("pandas not available")

    def test_loads_sniper_columns(self):
        from src.storage import AnalysisHistory
        mock_session = MagicMock()
        mock_row = MagicMock(spec=AnalysisHistory)
        mock_row.code = "000001"
        mock_row.ideal_buy = 100.0
        mock_row.secondary_buy = 95.0
        mock_row.stop_loss = 90.0
        mock_row.take_profit = 115.0
        mock_row.raw_result = None
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [mock_row]

        monitor = _make_monitor()
        monitor._db.session_scope = MagicMock()
        monitor._db.session_scope.return_value = mock_session

        result = monitor.load_yesterday_analysis(["000001"])
        assert "000001" in result
        assert result["000001"]["ideal_buy"] == 100.0
        assert result["000001"]["stop_loss"] == 90.0

    def test_no_analysis_for_stock(self):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []

        monitor = _make_monitor()
        monitor._db.session_scope = MagicMock()
        monitor._db.session_scope.return_value = mock_session

        result = monitor.load_yesterday_analysis(["999999"])
        assert result == {}

    def test_falls_back_to_raw_result(self):
        import json
        from src.storage import AnalysisHistory
        mock_session = MagicMock()
        mock_row = MagicMock(spec=AnalysisHistory)
        mock_row.code = "000001"
        mock_row.ideal_buy = None
        mock_row.secondary_buy = None
        mock_row.stop_loss = None
        mock_row.take_profit = None
        mock_row.raw_result = json.dumps({"ideal_buy": 50.0, "stop_loss": 45.0})
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [mock_row]

        monitor = _make_monitor()
        monitor._db.session_scope = MagicMock()
        monitor._db.session_scope.return_value = mock_session

        result = monitor.load_yesterday_analysis(["000001"])
        assert result["000001"]["ideal_buy"] == 50.0
        assert result["000001"]["stop_loss"] == 45.0

    def test_db_error_graceful(self):
        monitor = _make_monitor()
        monitor._db.session_scope = MagicMock(side_effect=RuntimeError("DB down"))

        result = monitor.load_yesterday_analysis(["000001"])
        assert result == {}


# ============================================================
# IntradayMonitor — monitor_snapshot
# ============================================================

class TestMonitorSnapshot:
    def test_non_trading_day_skips(self):
        monitor = _make_monitor(intraday_stocks="000001")
        with patch.object(monitor, '_is_trading_day', return_value=False):
            with patch.object(monitor, 'load_yesterday_analysis') as mock_load:
                monitor._monitor_snapshot_locked()
                mock_load.assert_not_called()

    def test_lazy_init_on_first_call(self):
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._initialized = False
        with patch.object(monitor, '_is_trading_day', return_value=True):
            with patch.object(monitor, 'load_yesterday_analysis') as mock_load:
                with patch.object(monitor, 'clear_today_events') as mock_clear:
                    with patch.object(monitor, '_snapshot_one_stock'):
                        monitor._monitor_snapshot_locked()
                        mock_load.assert_called_once()
                        mock_clear.assert_called_once()
                        assert monitor._initialized is True

    def test_already_initialized_skips_init(self):
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._initialized = True
        with patch.object(monitor, '_is_trading_day', return_value=True):
            with patch.object(monitor, 'load_yesterday_analysis') as mock_load:
                with patch.object(monitor, '_snapshot_one_stock'):
                    monitor._monitor_snapshot_locked()
                    mock_load.assert_not_called()

    def test_suspended_stock_skipped(self):
        """When realtime quote has no valid price, suspended event saved."""
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._initialized = True
        monitor._yesterday_analysis = {"000001": {"ideal_buy": 100.0, "secondary_buy": 95.0, "stop_loss": 90.0, "take_profit": 115.0}}

        fake_quote = MagicMock()
        fake_quote.price = None
        fake_quote.name = "测试"

        with patch.object(monitor, '_is_trading_day', return_value=True):
            with patch.object(monitor, '_get_realtime_with_timeout', return_value=fake_quote):
                with patch.object(monitor, '_save_event') as mock_save:
                    monitor._monitor_snapshot_locked()
                    saved_event = mock_save.call_args[0][0]
                    assert saved_event.event_type == "suspended"


# ============================================================
# IntradayMonitor — final_decision
# ============================================================

class TestFinalDecision:
    def test_non_trading_day_skips(self):
        monitor = _make_monitor()
        with patch.object(monitor, '_is_trading_day', return_value=False):
            with patch.object(monitor, '_load_today_events') as mock_load:
                monitor._final_decision_locked()
                mock_load.assert_not_called()

    def test_no_events_empty_email(self):
        monitor = _make_monitor()
        with patch.object(monitor, '_is_trading_day', return_value=True):
            with patch.object(monitor, '_load_today_events', return_value=[]):
                with patch.object(monitor, '_send_decision_email') as mock_send:
                    monitor._final_decision_locked()
                    mock_send.assert_not_called()


# ============================================================
# Helpers
# ============================================================

def _make_monitor(intraday_stocks: str = ""):
    """Create an IntradayMonitor with mocked dependencies."""
    config = MagicMock()
    config.intraday_monitor_stocks = intraday_stocks
    config.llm_model = "test-model"
    config.llm_max_tokens = 500
    config.llm_temperature = 0.3

    fetcher = MagicMock()
    db = MagicMock()
    email = MagicMock()

    return IntradayMonitor(config=config, fetcher_manager=fetcher, db_manager=db, email_sender=email)
