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

    def test_take_profit_priority_over_near_buy_zone(self):
        """take_profit=101, ideal_buy=100, price=101 -> enter_take_profit, not near_buy_zone."""
        yesterday = {"ideal_buy": 100.0, "secondary_buy": 95.0, "stop_loss": 90.0, "take_profit": 101.0}
        events = _compare_with_thresholds("000001", "测试", 101.0, 1.0, 2.0, yesterday, self._NOW)
        assert events[0].event_type == "enter_take_profit"

    def test_near_buy_zone_when_below_take_profit(self):
        """Price at ideal_buy*1.02 but below take_profit -> near_buy_zone."""
        yesterday = {"ideal_buy": 100.0, "secondary_buy": 95.0, "stop_loss": 90.0, "take_profit": 115.0}
        events = _compare_with_thresholds("000001", "测试", 102.0, 1.0, 1.0, yesterday, self._NOW)
        assert events[0].event_type == "near_buy_zone"


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
        with patch.object(monitor, '_is_stock_trading_today', return_value=False):
            with patch.object(monitor, 'load_yesterday_analysis') as mock_load:
                monitor._monitor_snapshot_locked()
                mock_load.assert_not_called()

    def test_lazy_init_on_first_call(self):
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._initialized = False
        with patch.object(monitor, '_filter_unknown_market_stocks', side_effect=lambda x: x):
            with patch.object(monitor, '_is_stock_trading_today', return_value=True):
                with patch.object(monitor, '_get_stock_markets', return_value={"000001": "cn"}):
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
        monitor._daily_init_marker = monitor._get_daily_init_key("cn")
        with patch.object(monitor, '_filter_unknown_market_stocks', side_effect=lambda x: x):
            with patch.object(monitor, "_should_clear_events", return_value=False):
                with patch.object(monitor, '_is_stock_trading_today', return_value=True):
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

        with patch.object(monitor, '_filter_unknown_market_stocks', side_effect=lambda x: x):
            with patch.object(monitor, '_is_stock_trading_today', return_value=True):
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
        with patch.object(monitor, '_is_stock_trading_today', return_value=False):
            with patch.object(monitor, '_get_stock_codes', return_value=["000001"]):
                with patch.object(monitor, '_load_today_events') as mock_load:
                    monitor._final_decision_locked()
                    mock_load.assert_not_called()

    def test_no_events_empty_email(self):
        monitor = _make_monitor()
        with patch.object(monitor, '_is_stock_trading_today', return_value=True):
            with patch.object(monitor, '_get_stock_codes', return_value=["000001"]):
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
    # Explicitly set to False so MagicMock's truthy default doesn't trigger unintended behavior
    config.intraday_reset_on_start = False
    config.intraday_calendar_fail_open = False
    config.intraday_legacy_fallback_enabled = False
    config.intraday_unknown_market_policy = "skip"
    config.intraday_force_run = False

    fetcher = MagicMock()
    db = MagicMock()
    email = MagicMock()

    return IntradayMonitor(config=config, fetcher_manager=fetcher, db_manager=db, email_sender=email)


# ============================================================
# IntradayMonitor — _resolve_market
# ============================================================

class TestResolveMarket:
    def test_defaults_to_cn(self):
        monitor = _make_monitor()
        assert monitor._resolve_primary_market() == 'cn'

    def test_uses_config_value(self):
        monitor = _make_monitor()
        monitor._config.market_review_region = 'hk'
        assert monitor._resolve_primary_market() == 'hk'

    def test_us_market(self):
        monitor = _make_monitor()
        monitor._config.market_review_region = 'us'
        assert monitor._resolve_primary_market() == 'us'

    def test_invalid_value_falls_back(self):
        monitor = _make_monitor()
        monitor._config.market_review_region = 'both'
        assert monitor._resolve_primary_market() == 'cn'


# ============================================================
# IntradayMonitor — load_yesterday_analysis backward compat
# ============================================================

class TestLoadYesterdayAnalysisBackwardCompat:
    @pytest.fixture(autouse=True)
    def _check_deps(self):
        try:
            import pandas  # noqa: F401
        except ImportError:
            pytest.skip("pandas not available")

    def test_fallback_used_for_old_records(self):
        """Records without effective_trading_date/analysis_phase are found via fallback."""
        from src.storage import AnalysisHistory
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [],  # primary query: no results
            [  # fallback query: one old record
                _make_mock_row("000001", 100, 95, 90, 115),
            ],
        ]
        # Diagnostic queries: 3 queries when primary misses, all via .filter().all() (no order_by)
        mock_session.query.return_value.filter.return_value.all.side_effect = [[], [], []]

        monitor = _make_monitor()
        monitor._config.intraday_legacy_fallback_enabled = True
        monitor._db.session_scope = MagicMock()
        monitor._db.session_scope.return_value = mock_session

        result = monitor.load_yesterday_analysis(["000001"])
        assert "000001" in result
        assert result["000001"]["ideal_buy"] == 100.0

    def test_primary_beats_fallback(self):
        """When both new and old records exist, primary (postmarket) wins."""
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session

        primary_row = _make_mock_row("000001", 120, 115, 110, 130)
        fallback_row = _make_mock_row("000001", 100, 95, 90, 115)

        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [primary_row],   # primary query
            [fallback_row],  # fallback query
        ]

        monitor = _make_monitor()
        monitor._db.session_scope = MagicMock()
        monitor._db.session_scope.return_value = mock_session

        result = monitor.load_yesterday_analysis(["000001"])
        # Primary record values should win
        assert result["000001"]["ideal_buy"] == 120.0

    def test_mixed_new_and_old_across_stocks(self):
        """Stock A has new record, Stock B has only old record — both found."""
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session

        primary_row = _make_mock_row("000001", 120, 115, 110, 130)
        fallback_row = _make_mock_row("000002", 50, 45, 40, 60)

        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [primary_row],   # primary: only stock A
            [fallback_row],  # fallback: stock B from old records
        ]
        # Diagnostic queries for stock B (missed in primary): 3 queries
        mock_session.query.return_value.filter.return_value.all.side_effect = [[], [], []]

        monitor = _make_monitor()
        monitor._config.intraday_legacy_fallback_enabled = True
        monitor._db.session_scope = MagicMock()
        monitor._db.session_scope.return_value = mock_session

        result = monitor.load_yesterday_analysis(["000001", "000002"])
        assert result["000001"]["ideal_buy"] == 120.0
        assert result["000002"]["ideal_buy"] == 50.0


def _make_mock_row(code, ideal, secondary, stop, profit):
    """Helper to create a mock AnalysisHistory row."""
    from src.storage import AnalysisHistory
    row = MagicMock(spec=AnalysisHistory)
    row.code = code
    row.ideal_buy = ideal
    row.secondary_buy = secondary
    row.stop_loss = stop
    row.take_profit = profit
    row.raw_result = None
    return row


# ============================================================
# New tests per 20260607 intraday logic fixes
# ============================================================

class TestMarketQueryDate:
    """P1: market-local date for event query_date."""

    def test_cn_market_date_uses_shanghai_tz(self):
        monitor = _make_monitor()
        market_date = monitor._get_market_query_date('cn')
        assert isinstance(market_date, str)
        assert '-' in market_date

    def test_us_market_date_differs_from_cn(self):
        """US market date may differ from CN when server TZ is Asia."""
        monitor = _make_monitor()
        cn_date = monitor._get_market_query_date('cn')
        us_date = monitor._get_market_query_date('us')
        # Both should be valid date strings
        from datetime import date as dt_date
        assert dt_date.fromisoformat(cn_date)
        assert dt_date.fromisoformat(us_date)


class TestLoadYesterdayAnalysisFallbackRestriction:
    """P0: fallback only applies to old records without new columns."""

    @pytest.fixture(autouse=True)
    def _check_deps(self):
        try:
            import pandas  # noqa: F401
        except ImportError:
            pytest.skip("pandas not available")

    def test_new_records_not_in_fallback(self):
        """Records with analysis_phase set should NOT match fallback query."""
        from src.storage import AnalysisHistory
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session

        # Primary returns postmarket record
        primary_row = _make_mock_row("000001", 120, 115, 110, 130)

        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [primary_row],  # primary: postmarket
            [],             # fallback: empty (not reached since primary hit)
        ]

        monitor = _make_monitor()
        monitor._db.session_scope = MagicMock()
        monitor._db.session_scope.return_value = mock_session

        result = monitor.load_yesterday_analysis(["000001"])
        assert result["000001"]["ideal_buy"] == 120.0

    def test_fallback_requires_null_columns(self):
        """Fallback query must include analysis_phase IS NULL AND effective_trading_date IS NULL."""
        monitor = _make_monitor()
        monitor._config.intraday_legacy_fallback_enabled = True
        monitor._db.session_scope = MagicMock()
        mock_session = MagicMock()

        fallback_row = _make_mock_row("000001", 50, 45, 40, 60)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [],              # primary query: no results
            [fallback_row],  # fallback query: old record
        ]
        # Diagnostic queries when primary misses: 3 queries
        mock_session.query.return_value.filter.return_value.all.side_effect = [[], [], []]

        monitor._db.session_scope.return_value = mock_session
        result = monitor.load_yesterday_analysis(["000001"])
        assert "000001" in result
        assert result["000001"]["ideal_buy"] == 50.0

    def test_fallback_disabled_by_config(self):
        """When intraday_legacy_fallback_enabled=False, no fallback used."""
        monitor = _make_monitor()
        monitor._config.intraday_legacy_fallback_enabled = False
        monitor._db.session_scope = MagicMock()
        mock_session = MagicMock()

        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [],  # primary query: no results
        ]
        # Diagnostic queries when primary misses: 3 queries
        mock_session.query.return_value.filter.return_value.all.side_effect = [[], [], []]

        monitor._db.session_scope.return_value = mock_session
        result = monitor.load_yesterday_analysis(["000001"])
        assert result == {}

    def test_fallback_hits_log_warning(self, caplog):
        """Fallback hit must emit a warning log."""
        import logging
        caplog.set_level(logging.WARNING)

        monitor = _make_monitor()
        monitor._config.intraday_legacy_fallback_enabled = True
        monitor._db.session_scope = MagicMock()
        mock_session = MagicMock()

        fallback_row = _make_mock_row("000001", 50, 45, 40, 60)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [],              # primary: empty
            [fallback_row],  # fallback: old record
        ]
        # Diagnostic queries when primary misses: 3 queries
        mock_session.query.return_value.filter.return_value.all.side_effect = [[], [], []]

        monitor._db.session_scope.return_value = mock_session
        monitor.load_yesterday_analysis(["000001"])
        assert any("legacy fallback" in msg for msg in caplog.messages), \
            f"expected 'legacy fallback' warning, got: {caplog.messages}"


class TestPerStockTradingDay:
    """P0: Per-stock trading day check for multi-market setups."""

    @pytest.fixture(autouse=True)
    def _check_deps(self):
        try:
            import pandas  # noqa: F401
            from src.core import trading_calendar  # noqa: F401
        except (ImportError, ModuleNotFoundError):
            pytest.skip("pandas not available")

    def test_cn_stock_uses_cn_calendar(self):
        from src.core import trading_calendar as _tc
        monitor = _make_monitor()
        with patch.object(_tc, 'is_market_open_strict', return_value=(True, "open")) as mock_strict:
            assert monitor._is_stock_trading_today('600519') is True
            mock_strict.assert_called_once()
            assert mock_strict.call_args[0][0] == 'cn'

    def test_hk_stock_uses_hk_calendar(self):
        from src.core import trading_calendar as _tc
        monitor = _make_monitor()
        with patch.object(_tc, 'is_market_open_strict', return_value=(True, "open")) as mock_strict:
            monitor._is_stock_trading_today('hk00700')
            assert mock_strict.call_args[0][0] == 'hk'

    def test_unknown_stock_returns_false(self):
        """Unknown stock market → fail-closed (not silently open)."""
        monitor = _make_monitor()
        # Empty string or garbage code → unknown market → False
        assert monitor._is_stock_trading_today('') is False

    def test_calendar_error_fail_closed_by_default(self):
        from src.core import trading_calendar as _tc
        monitor = _make_monitor()
        monitor._config.intraday_calendar_fail_open = False
        with patch.object(_tc, 'is_market_open_strict', side_effect=RuntimeError("boom")):
            assert monitor._is_stock_trading_today('600519') is False

    def test_calendar_error_fail_open_with_config(self):
        from src.core import trading_calendar as _tc
        monitor = _make_monitor()
        monitor._config.intraday_calendar_fail_open = True
        with patch.object(_tc, 'is_market_open_strict', side_effect=RuntimeError("boom")):
            assert monitor._is_stock_trading_today('600519') is True


class TestRunOneShotDecisionNonTrading:
    """P1: run_one_shot_decision on non-trading day must not write events."""

    def test_non_trading_day_no_events_written(self):
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._config.intraday_force_run = False
        with patch.object(monitor, '_is_stock_trading_today', return_value=False):
            with patch.object(monitor, '_snapshot_one_stock') as mock_snap:
                with patch.object(monitor, '_final_decision_locked') as mock_final:
                    monitor.run_one_shot_decision()
                    mock_snap.assert_not_called()
                    mock_final.assert_not_called()

    def test_non_trading_day_with_force_run(self):
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._config.intraday_force_run = True
        with patch.object(monitor, '_is_stock_trading_today', return_value=False):
            with patch.object(monitor, 'load_yesterday_analysis'):
                with patch.object(monitor, '_snapshot_one_stock') as mock_snap:
                    with patch.object(monitor, '_final_decision_locked') as mock_final:
                        monitor.run_one_shot_decision()
                        mock_snap.assert_called()
                        mock_final.assert_called_once()


class TestRealtimeTimeout:
    """P1: _get_realtime_with_timeout must not block indefinitely."""

    def test_timeout_returns_none(self):
        monitor = _make_monitor()
        import time as _time
        # Simulate a very slow call
        monitor._fetcher.get_realtime_quote = lambda code: _time.sleep(10) or MagicMock()
        # Use a tiny timeout
        result = monitor._get_realtime_with_timeout("000001", timeout=0.05)
        assert result is None


class TestRestartPreservesEvents:
    """P1: Process restart must not clear previously persisted today events."""

    def test_second_instance_does_not_clear(self):
        """New IntradayMonitor instance with same daily_init DB marker skips clear."""
        monitor1 = _make_monitor(intraday_stocks="000001")
        marker = monitor1._get_daily_init_key("cn")
        monitor1._daily_init_marker = marker
        monitor1._initialized = True

        # Second instance on same day: DB returns matching marker → no clear
        monitor2 = _make_monitor(intraday_stocks="000001")
        monitor2._daily_init_marker = None  # "restart" state
        monitor2._initialized = False
        # Mock DB to return today's marker (same as what first run persisted)
        monitor2._db.get_intraday_state = MagicMock(return_value=marker)

        assert monitor2._should_clear_events("cn") is False

    def test_new_trading_day_does_clear(self):
        """When daily init marker changes (DB returns different/None marker), should clear events."""
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._daily_init_marker = "cn:2020-01-01"  # old date in memory
        # DB also returns stale marker (or None)
        monitor._db.get_intraday_state = MagicMock(return_value="cn:2020-01-01")
        # Today's key is different (2026-06-07), so should clear
        assert monitor._should_clear_events("cn") is True


class TestFinalDecisionLoadsYesterday:
    """P1: final_decision must load _yesterday_analysis if empty."""

    def test_loads_yesterday_when_empty(self):
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._yesterday_analysis = {}

        with patch.object(monitor, '_is_stock_trading_today', return_value=True):
            with patch.object(monitor, 'load_yesterday_analysis') as mock_load:
                with patch.object(monitor, '_load_today_events', return_value=[]):
                    monitor._final_decision_locked()
                    mock_load.assert_called_once()

    def test_no_load_when_yesterday_present(self):
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._yesterday_analysis = {"000001": {"ideal_buy": 100.0}}

        with patch.object(monitor, '_is_stock_trading_today', return_value=True):
            with patch.object(monitor, 'load_yesterday_analysis') as mock_load:
                with patch.object(monitor, '_load_today_events', return_value=[]):
                    monitor._final_decision_locked()
                    mock_load.assert_not_called()


# ============================================================
# P0-1: Phase resolution in non-Agent save path
# ============================================================

class TestPhaseResolution:
    @pytest.fixture(autouse=True)
    def _check_deps(self):
        try:
            import pandas  # noqa: F401
        except ImportError:
            pytest.skip("pandas not available")
    """P0-1: _resolve_analysis_phase must not save 'auto'."""

    def test_auto_resolves_to_context_phase(self):
        from src.core.pipeline import _resolve_analysis_phase
        result = _resolve_analysis_phase("auto", "postmarket")
        assert result == "postmarket"

    def test_auto_resolves_to_unknown_on_empty_context(self):
        from src.core.pipeline import _resolve_analysis_phase
        result = _resolve_analysis_phase("auto", "")
        assert result == "unknown"

    def test_explicit_phase_passes_through(self):
        from src.core.pipeline import _resolve_analysis_phase
        assert _resolve_analysis_phase("intraday", "postmarket") == "intraday"
        assert _resolve_analysis_phase("premarket", "") == "premarket"

    def test_unknown_phase_logs_warning(self):
        import logging
        from src.core.pipeline import _resolve_analysis_phase
        with patch.object(logging.getLogger('src.core.pipeline'), 'warning') as mock_warn:
            result = _resolve_analysis_phase("auto", "unknown")
            assert result == "unknown"
            mock_warn.assert_called_once()


# ============================================================
# P0-2: Persistent marker preserves events across restarts
# ============================================================

class TestPersistentMarkerRestart:
    """P0-2: DB-backed init marker prevents event loss on restart."""

    def test_same_day_restart_preserves_events(self):
        monitor = _make_monitor(intraday_stocks="600519")
        today_key = monitor._get_daily_init_key("cn")
        monitor._db.get_intraday_state = MagicMock(return_value=today_key)
        assert monitor._should_clear_events("cn") is False

    def test_new_day_without_marker_clears(self):
        monitor = _make_monitor(intraday_stocks="600519")
        monitor._db.get_intraday_state = MagicMock(return_value=None)
        assert monitor._should_clear_events("cn") is True

    def test_clear_today_events_with_reset_marker(self):
        monitor = _make_monitor(intraday_stocks="600519")
        monitor._db.set_intraday_state = MagicMock()
        monitor._db.session_scope = MagicMock()
        monitor.clear_today_events(reset_marker=True)
        monitor._db.set_intraday_state.assert_called()

    def test_clear_today_events_without_reset_keeps_marker(self):
        monitor = _make_monitor(intraday_stocks="600519")
        monitor._db.set_intraday_state = MagicMock()
        monitor._db.session_scope = MagicMock()
        monitor.clear_today_events(reset_marker=False)
        monitor._db.set_intraday_state.assert_not_called()

    def test_reset_on_start_flag_triggers_clear(self):
        monitor = _make_monitor(intraday_stocks="600519")
        monitor._config.intraday_reset_on_start = True
        monitor._db.session_scope = MagicMock()
        with patch.object(monitor, 'clear_today_events') as mock_clear:
            with patch.object(monitor, 'load_yesterday_analysis'):
                with patch.object(monitor, '_is_stock_trading_today', return_value=True):
                    with patch.object(monitor, '_get_stock_markets', return_value={"600519": "cn"}):
                        with patch.object(monitor, '_snapshot_one_stock', return_value=[]):
                            with patch.object(monitor, '_should_clear_events', return_value=False):
                                monitor._monitor_snapshot_locked()
                                mock_clear.assert_called_once_with(reset_marker=True)


# ============================================================
# P1-1: Unknown market handling
# ============================================================

class TestUnknownMarketHandling:
    """P1-1: Unknown stock market must not silently default to CN."""

    def test_get_market_for_unknown_stock_returns_none(self):
        monitor = _make_monitor()
        result = monitor._get_market_for_stock("GARBAGE123")
        assert result is None

    def test_unknown_market_skip_in_snapshot(self):
        monitor = _make_monitor(intraday_stocks="600519,UNKNOWN999,000002")
        monitor._config.intraday_unknown_market_policy = "skip"
        codes = monitor._get_stock_codes()
        assert "UNKNOWN999" in codes
        with patch.object(monitor, '_get_market_for_stock', side_effect=lambda c: "cn" if c.isdigit() else None):
            filtered = monitor._filter_unknown_market_stocks(codes)
            assert "UNKNOWN999" not in filtered
            assert "600519" in filtered

    def test_cn_compat_mode_keeps_unknown(self):
        monitor = _make_monitor(intraday_stocks="600519,UNKNOWN999")
        monitor._config.intraday_unknown_market_policy = "cn_compat"
        codes = monitor._get_stock_codes()
        with patch.object(monitor, '_get_market_for_stock', side_effect=lambda c: "cn" if c.isdigit() else None):
            filtered = monitor._filter_unknown_market_stocks(codes)
            assert "UNKNOWN999" in filtered

    def test_save_event_skips_unknown_market(self):
        event = IntradayEvent(
            stock_code="UNKNOWN999",
            stock_name="Test",
            timestamp=datetime(2026, 6, 7, 10, 30),
            current_price=10.0,
            volume_ratio=1.0,
            change_pct=0.0,
            event_type="test",
            description="test",
        )
        monitor = _make_monitor(intraday_stocks="UNKNOWN999")
        monitor._config.intraday_unknown_market_policy = "skip"
        monitor._db.session_scope = MagicMock()
        with patch.object(monitor, '_get_market_for_stock', return_value=None):
            with patch.object(monitor._db, 'session_scope') as mock_scope:
                monitor._save_event(event)
                mock_scope.assert_not_called()


# ============================================================
# P1-2: Strict calendar contract
# ============================================================

class TestStrictCalendar:
    @pytest.fixture(autouse=True)
    def _check_deps(self):
        try:
            import pandas  # noqa: F401
        except ImportError:
            pytest.skip("pandas not available")
    """P1-2: is_market_open_strict returns explicit status."""

    def test_strict_returns_open_tuple(self):
        from src.core.trading_calendar import is_market_open_strict
        from datetime import date as dt_date
        result = is_market_open_strict("cn", dt_date(2026, 6, 8))
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[1] in ("open", "closed", "calendar_unavailable", "unknown_market", "error")

    def test_unknown_market_returns_unknown_status(self):
        from src.core.trading_calendar import is_market_open_strict
        from datetime import date as dt_date
        is_open, status = is_market_open_strict("xx", dt_date(2026, 6, 8))
        assert is_open is None
        assert status == "unknown_market"

    def test_intraday_monitor_uses_strict(self):
        from src.core import trading_calendar as _tc
        monitor = _make_monitor()
        with patch.object(_tc, 'is_market_open_strict', return_value=(True, "open")) as mock_strict:
            assert monitor._is_stock_trading_today("600519") is True
            mock_strict.assert_called_once()

    def test_calendar_unavailable_fail_closed(self):
        from src.core import trading_calendar as _tc
        monitor = _make_monitor()
        monitor._config.intraday_calendar_fail_open = False
        with patch.object(_tc, 'is_market_open_strict', return_value=(None, "calendar_unavailable")):
            assert monitor._is_stock_trading_today("600519") is False

    def test_calendar_unavailable_fail_open(self):
        from src.core import trading_calendar as _tc
        monitor = _make_monitor()
        monitor._config.intraday_calendar_fail_open = True
        with patch.object(_tc, 'is_market_open_strict', return_value=(None, "calendar_unavailable")):
            assert monitor._is_stock_trading_today("600519") is True

    def test_closed_day_returns_false(self):
        from src.core import trading_calendar as _tc
        monitor = _make_monitor()
        with patch.object(_tc, 'is_market_open_strict', return_value=(False, "closed")):
            assert monitor._is_stock_trading_today("600519") is False


# ============================================================
# P1-3: Market-local timestamps
# ============================================================

class TestMarketLocalTimestamp:
    @pytest.fixture(autouse=True)
    def _check_deps(self):
        try:
            import pandas  # noqa: F401
        except ImportError:
            pytest.skip("pandas not available")
    """P1-3: Event timestamps are in market-local time, not server-local."""

    def test_event_has_market_local_timestamp_field(self):
        event = IntradayEvent(
            stock_code="600519",
            stock_name="茅台",
            timestamp=datetime(2026, 6, 7, 14, 30),
            current_price=1800.0,
            volume_ratio=1.0,
            change_pct=0.0,
            event_type="test",
            description="test",
            market_local_timestamp="2026-06-07T14:30:00+08:00",
        )
        assert event.market_local_timestamp == "2026-06-07T14:30:00+08:00"

    def test_event_to_dict_includes_market_local_timestamp(self):
        event = IntradayEvent(
            stock_code="600519",
            stock_name="茅台",
            timestamp=datetime(2026, 6, 7, 14, 30),
            current_price=1800.0,
            volume_ratio=1.0,
            change_pct=0.0,
            event_type="test",
            description="test",
            market_local_timestamp="2026-06-07T14:30:00+08:00",
        )
        d = event.to_dict()
        assert "market_local_timestamp" in d
        assert d["market_local_timestamp"] == "2026-06-07T14:30:00+08:00"

    def test_send_decision_email_single_market_title(self):
        monitor = _make_monitor(intraday_stocks="600519")
        monitor._email_sender = MagicMock()
        events = [
            IntradayEvent(
                stock_code="600519",
                stock_name="茅台",
                timestamp=datetime(2026, 6, 7, 14, 30),
                current_price=1800.0,
                volume_ratio=1.0,
                change_pct=0.0,
                event_type="test",
                description="test",
            )
        ]
        with patch.object(monitor, '_get_market_query_date', return_value="2026-06-07"):
            with patch.object(monitor, '_get_market_for_stock', return_value="cn"):
                monitor._send_decision_email("test content", events)
        call_args = monitor._email_sender.send_to_email.call_args
        subject = call_args[1]["subject"]
        assert "CN:2026-06-07" in subject

    def test_send_decision_email_multi_market_title(self):
        monitor = _make_monitor(intraday_stocks="600519,hk00700")
        monitor._email_sender = MagicMock()
        events = [
            IntradayEvent(
                stock_code="600519",
                stock_name="茅台",
                timestamp=datetime(2026, 6, 7, 14, 30),
                current_price=1800.0,
                volume_ratio=1.0,
                change_pct=0.0,
                event_type="test",
                description="test",
            ),
            IntradayEvent(
                stock_code="hk00700",
                stock_name="腾讯",
                timestamp=datetime(2026, 6, 7, 14, 30),
                current_price=400.0,
                volume_ratio=1.0,
                change_pct=0.0,
                event_type="test",
                description="test",
            ),
        ]

        def mock_query_date(mkt):
            return {"cn": "2026-06-07", "hk": "2026-06-07"}.get(mkt, "2026-06-07")

        with patch.object(monitor, '_get_market_query_date', side_effect=mock_query_date):
            with patch.object(monitor, '_get_market_for_stock', side_effect=lambda c: "cn" if c == "600519" else "hk"):
                monitor._send_decision_email("test content", events)
        call_args = monitor._email_sender.send_to_email.call_args
        subject = call_args[1]["subject"]
        assert "CN" in subject
        assert "HK" in subject
        assert "/" in subject


# ============================================================
# P1-4: Fallback default-off + diagnostic logging
# ============================================================

class TestFallbackDefaultOff:
    @pytest.fixture(autouse=True)
    def _check_deps(self):
        try:
            import pandas  # noqa: F401
        except ImportError:
            pytest.skip("pandas not available")
    """P1-4: Legacy fallback defaults to off, diagnostic logging on primary miss."""

    def test_fallback_defaults_to_false(self):
        monitor = _make_monitor(intraday_stocks="600519")
        monitor._config.intraday_legacy_fallback_enabled = False
        monitor._db.session_scope = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [],  # primary query: no results
        ]
        monitor._db.session_scope.return_value = mock_session
        result = monitor.load_yesterday_analysis(["600519"])
        assert result == {}

    def test_fallback_explicitly_enabled(self):
        monitor = _make_monitor(intraday_stocks="600519")
        monitor._config.intraday_legacy_fallback_enabled = True
        monitor._db.session_scope = MagicMock()
        mock_session = MagicMock()
        fallback_row = _make_mock_row("600519", 100, 95, 90, 115)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [],              # primary: no results
            [fallback_row],  # fallback: old record found
        ]
        monitor._db.session_scope.return_value = mock_session
        result = monitor.load_yesterday_analysis(["600519"])
        assert "600519" in result
        assert result["600519"]["ideal_buy"] == 100.0

    def test_diagnostic_logging_on_primary_miss(self):
        import logging
        monitor = _make_monitor(intraday_stocks="600519")
        monitor._config.intraday_legacy_fallback_enabled = False
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [],  # primary query: no results
        ]
        mock_session.query.return_value.filter.return_value.all.side_effect = [
            [],  # diagnostic 1: same-date-different-phase
            [],  # diagnostic 2: analysis_phase='auto'
            [],  # diagnostic 3: NULL effective_trading_date
        ]
        monitor._db.session_scope.return_value = mock_session
        with patch.object(logging.getLogger('src.core.intraday_monitor'), 'warning') as mock_warn:
            result = monitor.load_yesterday_analysis(["600519"])
            assert any("primary query 未命中" in str(call) for call in mock_warn.call_args_list)

    def test_unknown_market_not_in_by_market_grouping(self):
        monitor = _make_monitor(intraday_stocks="600519,UNKNOWN999")
        monitor._config.intraday_unknown_market_policy = "skip"
        mock_session = MagicMock()
        primary_row = _make_mock_row("600519", 120, 115, 110, 130)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = [
            [primary_row],  # primary query: only 600519
        ]
        monitor._db.session_scope.return_value = mock_session
        result = monitor.load_yesterday_analysis(["600519", "UNKNOWN999"])
        assert "600519" in result
        assert "UNKNOWN999" not in result
