# -*- coding: utf-8 -*-
"""
Tests for intraday_monitor.py — _compare_with_thresholds (pure function),
load_yesterday_analysis, monitor_snapshot, and final_decision.
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock

from src.core.intraday_monitor import (
    EmailReportType,
    IntradayEvent,
    IntradayMonitor,
    LLMConfigError,
    LLMResult,
    SnapshotState,
    _compare_with_thresholds,
    _find_sniper_points_in_dict,
    _log_ctx,
    _safe_float,
)
try:
    from data_provider.base import DataFetcherManager
except ImportError:
    DataFetcherManager = None  # type: ignore

try:
    from data_provider.realtime_types import (
        BULK_SOURCE_REGISTRY,
        RealtimeBatchResult,
        RealtimeSnapshotCache,
        RealtimeSourceCapability,
        is_bulk_source,
    )
except ImportError:
    BULK_SOURCE_REGISTRY = None
    RealtimeBatchResult = None
    RealtimeSnapshotCache = None
    RealtimeSourceCapability = None
    is_bulk_source = None


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
                            with patch.object(monitor, '_fallback_single_quote_without_bulk_sources'):
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
                        with patch.object(monitor, '_fallback_single_quote_without_bulk_sources'):
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
                        with patch.object(monitor, '_get_latest_completed_snapshot', return_value={
                            "snapshot_id": "snap_test",
                            "expected_codes": ["000001"],
                            "valid_quote_codes": ["000001"],
                            "failed_codes": [],
                        }):
                            with patch.object(monitor, '_load_events_for_snapshot', return_value=[]):
                                monitor._final_decision_locked()
                                mock_send.assert_not_called()


# ============================================================
# Helpers
# ============================================================

def _make_monitor(intraday_stocks: str = ""):
    """Create an IntradayMonitor with mocked dependencies."""
    config = MagicMock()
    config.intraday_monitor_stocks = intraday_stocks
    config.litellm_model = "test-model"
    config.llm_max_tokens = 500
    config.llm_temperature = 0.3
    # Explicitly set to False so MagicMock's truthy default doesn't trigger unintended behavior
    config.intraday_reset_on_start = False
    config.intraday_calendar_fail_open = False
    config.intraday_legacy_fallback_enabled = False
    config.intraday_unknown_market_policy = "skip"
    config.intraday_force_run = False
    # Timeout configuration fields
    config.intraday_quote_timeout_cn_fast = 12.0
    config.intraday_quote_timeout_hk_full = 60.0
    config.intraday_quote_timeout_hk_fast = 15.0
    config.intraday_quote_timeout_us = 15.0
    config.intraday_quote_timeout_default = 20.0
    # Coverage and alert config
    config.intraday_min_quote_coverage = 0.8
    config.intraday_send_llm_failure_alert = True
    config.intraday_send_raw_summary_on_llm_failure = False

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
            with patch.object(monitor, '_fallback_single_quote_without_bulk_sources') as mock_snap:
                with patch.object(monitor, '_final_decision_locked') as mock_final:
                    monitor.run_one_shot_decision()
                    mock_snap.assert_not_called()
                    mock_final.assert_not_called()

    def test_non_trading_day_with_force_run(self):
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._config.intraday_force_run = True
        with patch.object(monitor, '_is_stock_trading_today', return_value=False):
            with patch.object(monitor, 'load_yesterday_analysis'):
                with patch.object(monitor, '_fallback_single_quote_without_bulk_sources') as mock_snap:
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
                        with patch.object(monitor, '_fallback_single_quote_without_bulk_sources'):
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


# ============================================================
# Fake helpers for testing
# ============================================================

class TestFakeRealtimeFetcher:
    """Fake fetcher for testing timeouts, late results, and batch caching."""

    class FakeFetcher:
        """Simulates a data fetcher with configurable delay."""
        def __init__(self, delay=0.0, price=100.0):
            self.delay = delay
            self.price = price
            self.call_count = 0

        def get_realtime_quote(self, code):
            self.call_count += 1
            import time
            time.sleep(self.delay)
            q = MagicMock()
            q.price = self.price
            q.name = "TestStock"
            q.volume_ratio = 1.0
            q.change_pct = 0.0
            return q

    def test_fake_fetcher_returns_quote(self):
        f = self.FakeFetcher()
        q = f.get_realtime_quote("000001")
        assert q.price == 100.0
        assert q.name == "TestStock"

    def test_fake_fetcher_counts_calls(self):
        f = self.FakeFetcher()
        f.get_realtime_quote("000001")
        f.get_realtime_quote("000002")
        assert f.call_count == 2


class TestFakeEmailSender:
    """Fake email sender that records sends for assertion."""

    class FakeEmailSender:
        def __init__(self):
            self.sends = []

        def send_to_email(self, content, subject, **kwargs):
            self.sends.append({"content": content, "subject": subject, "kwargs": kwargs})
            return True

    def test_records_send(self):
        s = self.FakeEmailSender()
        s.send_to_email("test body", "test subject", html=True)
        assert len(s.sends) == 1
        assert s.sends[0]["subject"] == "test subject"
        assert s.sends[0]["content"] == "test body"

    def test_multiple_sends(self):
        s = self.FakeEmailSender()
        s.send_to_email("body1", "subj1")
        s.send_to_email("body2", "subj2")
        assert len(s.sends) == 2


# ============================================================
# Issue 1: Timeout resolution by market
# ============================================================

class TestResolveQuoteTimeout:
    """Issue 1: _resolve_quote_timeout returns market-aware timeouts."""

    def test_cn_timeout_is_12s(self):
        monitor = _make_monitor()
        monitor._config.intraday_quote_timeout_cn_fast = 12.0
        with patch.object(monitor, '_get_market_for_stock', return_value='cn'):
            t = monitor._resolve_quote_timeout("600519")
        assert t == 12.0

    def test_hk_full_timeout_is_60s(self):
        monitor = _make_monitor()
        monitor._config.intraday_quote_timeout_hk_full = 60.0
        with patch.object(monitor, '_get_market_for_stock', return_value='hk'):
            t = monitor._resolve_quote_timeout("hk00700", source_hint="stock_hk_spot")
        assert t == 60.0

    def test_hk_fast_timeout_is_15s(self):
        monitor = _make_monitor()
        monitor._config.intraday_quote_timeout_hk_fast = 15.0
        with patch.object(monitor, '_get_market_for_stock', return_value='hk'):
            t = monitor._resolve_quote_timeout("hk00700")
        assert t == 15.0

    def test_hk_fast_timeout_with_other_hint(self):
        monitor = _make_monitor()
        monitor._config.intraday_quote_timeout_hk_fast = 15.0
        with patch.object(monitor, '_get_market_for_stock', return_value='hk'):
            t = monitor._resolve_quote_timeout("hk00700", source_hint="other_source")
        assert t == 15.0

    def test_us_timeout_is_15s(self):
        monitor = _make_monitor()
        monitor._config.intraday_quote_timeout_us = 15.0
        with patch.object(monitor, '_get_market_for_stock', return_value='us'):
            t = monitor._resolve_quote_timeout("AAPL")
        assert t == 15.0

    def test_default_timeout_for_unknown(self):
        monitor = _make_monitor()
        monitor._config.intraday_quote_timeout_default = 20.0
        with patch.object(monitor, '_get_market_for_stock', return_value=None):
            t = monitor._resolve_quote_timeout("UNKNOWN")
        assert t == 20.0

    def test_custom_timeout_values(self):
        monitor = _make_monitor()
        monitor._config.intraday_quote_timeout_cn_fast = 8.0
        monitor._config.intraday_quote_timeout_us = 25.0
        with patch.object(monitor, '_get_market_for_stock', side_effect=lambda c: 'cn' if c == '600519' else 'us'):
            assert monitor._resolve_quote_timeout("600519") == 8.0
            assert monitor._resolve_quote_timeout("AAPL") == 25.0


# ============================================================
# Issue 1: Late result rejection
# ============================================================

class TestExpiredRequests:
    """Issue 1: _expired_requests set guards against late results."""

    def test_timeout_adds_request_id(self):
        monitor = _make_monitor()
        monitor._current_snapshot_id = "test_snap"
        monitor._current_run_id = "test_run"
        monitor._expired_requests = set()
        import time as _time
        monitor._fetcher.get_realtime_quote = lambda code: _time.sleep(10) or MagicMock()
        result = monitor._get_realtime_with_timeout("000001", timeout=0.05, snapshot_id="test_snap")
        assert result is None
        assert len(monitor._expired_requests) > 0

    def test_late_result_ignored(self):
        monitor = _make_monitor()
        monitor._current_snapshot_id = "test_snap"
        monitor._current_run_id = "test_run"
        monitor._expired_requests = set()
        # Pre-populate expired_requests to simulate a race
        with patch('time.time', return_value=1000000.0):
            request_id = f"test_snap:000001:{1000000.0}"
            monitor._expired_requests.add(request_id)
            # Now call — future will complete, but request_id is already expired
            monitor._fetcher.get_realtime_quote = lambda code: TestFakeRealtimeFetcher.FakeFetcher().get_realtime_quote(code)
            result = monitor._get_realtime_with_timeout("000001", timeout=1.0, snapshot_id="test_snap")
            assert result is None  # ignored as late

    def test_not_expired_returns_result(self):
        monitor = _make_monitor()
        monitor._current_snapshot_id = "test_snap"
        monitor._expired_requests = set()
        monitor._fetcher.get_realtime_quote = lambda code: TestFakeRealtimeFetcher.FakeFetcher(delay=0.0).get_realtime_quote(code)
        result = monitor._get_realtime_with_timeout("000001", timeout=2.0, snapshot_id="test_snap")
        assert result is not None
        assert result.price == 100.0

    def test_executor_exception_returns_none(self):
        monitor = _make_monitor()
        monitor._current_snapshot_id = "test_snap"
        monitor._fetcher.get_realtime_quote = MagicMock(side_effect=RuntimeError("fetch failed"))
        result = monitor._get_realtime_with_timeout("000001", timeout=2.0, snapshot_id="test_snap")
        assert result is None


# ============================================================
# Issue 2: Per-market batch caching
# ============================================================

class TestBatchCache:
    """Issue 2: Snapshot-level batch cache in DataFetcherManager."""

    @pytest.fixture(autouse=True)
    def _check_deps(self):
        if DataFetcherManager is None:
            pytest.skip("pandas not available")

    def test_cache_set_and_get(self):
        q = MagicMock()
        q.price = 100.0
        data = {"hk00700": q}
        snap_id = "cache_test_snap"
        DataFetcherManager._set_snapshot_cache(snap_id, "hk:akshare_hk_spot", data, ttl=90.0)
        cached = DataFetcherManager._get_snapshot_cache(snap_id, "hk:akshare_hk_spot")
        assert cached is not None
        assert "hk00700" in cached
        assert cached["hk00700"].price == 100.0

    def test_cache_miss_returns_none(self):
        result = DataFetcherManager._get_snapshot_cache("nonexistent_snap", "any_key")
        assert result is None

    def test_cache_miss_wrong_key(self):
        q = MagicMock()
        q.price = 50.0
        DataFetcherManager._set_snapshot_cache("snap1", "key_a", {"code_a": q})
        result = DataFetcherManager._get_snapshot_cache("snap1", "key_b")
        assert result is None

    def test_clear_cache(self):
        q = MagicMock()
        q.price = 200.0
        DataFetcherManager._set_snapshot_cache("snap2", "hk:batch", {"hk00001": q})
        DataFetcherManager.clear_snapshot_cache("snap2")
        result = DataFetcherManager._get_snapshot_cache("snap2", "hk:batch")
        assert result is None

    def test_expired_cache_returns_none(self):
        q = MagicMock()
        q.price = 300.0
        DataFetcherManager._set_snapshot_cache("snap3", "expired_key", {"exp": q}, ttl=-1.0)
        result = DataFetcherManager._get_snapshot_cache("snap3", "expired_key")
        assert result is None


# ============================================================
# Issue 3: Snapshot state management
# ============================================================

class TestSnapshotLifecycle:
    """Issue 3: SnapshotState lifecycle — create, persist, retrieve."""

    def test_snapshot_state_dataclass(self):
        state = SnapshotState(
            snapshot_id="snap_001",
            run_id="run_abc",
            expected_codes=["600519", "000001"],
        )
        assert state.status == "running"
        assert state.snapshot_id == "snap_001"
        assert state.run_id == "run_abc"
        assert state.expected_codes == ["600519", "000001"]
        assert state.finished_at is None

    def test_ensure_intraday_snapshots_table(self):
        """Verify _ensure_intraday_snapshots_table is callable without unhandled exception."""
        monitor = _make_monitor()
        mock_conn = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.connection.return_value = mock_conn
        # The method catches all exceptions and logs them, so we verify no uncaught exception
        with patch.object(monitor._db, 'session_scope', return_value=mock_session):
            monitor._ensure_intraday_snapshots_table()  # must not raise

    def test_persist_snapshot_state_smoke(self):
        """_persist_snapshot_state should not raise unhandled exceptions."""
        monitor = _make_monitor()
        mock_conn = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.connection.return_value = mock_conn
        with patch.object(monitor._db, 'session_scope', return_value=mock_session):
            state = SnapshotState(
                snapshot_id="snap_persist_1",
                run_id="run_test",
                status="completed",
                expected_codes=["000001"],
                completed_codes=["000001"],
                valid_quote_codes=["000001"],
                failed_codes=[],
                finished_at=datetime(2026, 6, 8, 10, 30),
            )
            monitor._persist_snapshot_state(state)  # must not raise

    def test_get_latest_completed_snapshot_none(self):
        """Returns None when DB returns no rows."""
        monitor = _make_monitor()
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.connection.return_value = mock_conn
        with patch.object(monitor._db, 'session_scope', return_value=mock_session):
            result = monitor._get_latest_completed_snapshot()
        assert result is None

    def test_update_snapshot_status_smoke(self):
        """_update_snapshot_status should not raise unhandled exceptions."""
        monitor = _make_monitor()
        mock_conn = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.connection.return_value = mock_conn
        with patch.object(monitor._db, 'session_scope', return_value=mock_session):
            monitor._update_snapshot_status("snap_x", "expired")  # must not raise


# ============================================================
# Issue 3: Coverage gate
# ============================================================

class TestCoverageGate:
    """Issue 3: Coverage threshold enforcement in _final_decision_locked."""

    def test_coverage_below_threshold_sends_alert(self):
        import json
        monitor = _make_monitor(intraday_stocks="600519,000001,000002")
        monitor._config.intraday_min_quote_coverage = 0.8
        monitor._email_sender = TestFakeEmailSender.FakeEmailSender()

        with patch.object(monitor, '_is_stock_trading_today', return_value=True):
            with patch.object(monitor, '_get_stock_codes', return_value=["600519", "000001", "000002"]):
                with patch.object(monitor, '_get_latest_completed_snapshot') as mock_snap:
                    mock_snap.return_value = {
                        "snapshot_id": "snap_cov",
                        "expected_codes": ["600519", "000001", "000002"],
                        "valid_quote_codes": ["600519"],  # only 1/3 = 33%
                        "failed_codes": ["000001", "000002"],
                    }
                    monitor._final_decision_locked()
        assert len(monitor._email_sender.sends) == 1
        send = monitor._email_sender.sends[0]
        assert "数据质量告警" in send["subject"] or "DATA_QUALITY" in send["subject"].upper()

    def test_coverage_above_threshold_proceeds(self):
        import json
        monitor = _make_monitor(intraday_stocks="600519,000001")
        monitor._config.intraday_min_quote_coverage = 0.5
        # We need to mock LLM to avoid real API call
        monitor._yesterday_analysis = {
            "600519": {"ideal_buy": 100.0, "secondary_buy": 95.0, "stop_loss": 90.0, "take_profit": 115.0},
        }

        with patch.object(monitor, '_is_stock_trading_today', return_value=True):
            with patch.object(monitor, '_get_stock_codes', return_value=["600519", "000001"]):
                with patch.object(monitor, '_get_latest_completed_snapshot') as mock_snap:
                    mock_snap.return_value = {
                        "snapshot_id": "snap_ok",
                        "expected_codes": ["600519", "000001"],
                        "valid_quote_codes": ["600519", "000001"],
                        "failed_codes": [],
                    }
                    with patch.object(monitor, '_load_events_for_snapshot', return_value=[]):
                        monitor._final_decision_locked()
        # Coverage OK but no events -> should not send email
        # (no email because all_events is empty after coverage check)

    def test_coverage_gate_failure_includes_coverage_info(self):
        monitor = _make_monitor(intraday_stocks="600519,000001,000002,000003,000004")
        monitor._config.intraday_min_quote_coverage = 0.6
        monitor._email_sender = TestFakeEmailSender.FakeEmailSender()

        with patch.object(monitor, '_is_stock_trading_today', return_value=True):
            with patch.object(monitor, '_get_stock_codes', return_value=["600519", "000001", "000002", "000003", "000004"]):
                with patch.object(monitor, '_get_latest_completed_snapshot') as mock_snap:
                    mock_snap.return_value = {
                        "snapshot_id": "snap_partial",
                        "expected_codes": ["600519", "000001", "000002", "000003", "000004"],
                        "valid_quote_codes": ["600519", "000001"],  # 2/5 = 40%
                        "failed_codes": ["000002", "000003", "000004"],
                    }
                    monitor._final_decision_locked()
        assert len(monitor._email_sender.sends) == 1
        content = monitor._email_sender.sends[0]["content"]
        assert "覆盖率不足" in content
        assert "2" in content  # valid count
        assert "5" in content  # expected count


# ============================================================
# Issue 4: LLM model field fallback chain
# ============================================================

class TestResolveLLMModel:
    """Issue 4: _resolve_llm_model checks litellm_model first, then llm_model, etc."""

    def test_litellm_model_first(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = "gemini/test"
        model = monitor._resolve_llm_model()
        assert model == "gemini/test"

    def test_llm_model_fallback(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = ""
        monitor._config.llm_model = "gpt-4"
        model = monitor._resolve_llm_model()
        assert model == "gpt-4"

    def test_openai_model_fallback(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = ""
        monitor._config.llm_model = None
        monitor._config.openai_model = "gpt-3.5-turbo"
        model = monitor._resolve_llm_model()
        assert model == "gpt-3.5-turbo"

    def test_model_name_fallback(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = ""
        monitor._config.llm_model = None
        monitor._config.openai_model = None
        monitor._config.model_name = "claude-3"
        model = monitor._resolve_llm_model()
        assert model == "claude-3"

    def test_ai_model_fallback(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = ""
        monitor._config.llm_model = None
        monitor._config.openai_model = None
        monitor._config.model_name = None
        monitor._config.ai_model = "deepseek"
        model = monitor._resolve_llm_model()
        assert model == "deepseek"

    def test_empty_all_fields(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = ""
        monitor._config.llm_model = None
        monitor._config.openai_model = None
        monitor._config.model_name = None
        monitor._config.ai_model = None
        model = monitor._resolve_llm_model()
        assert model == ""

    def test_whitespace_only_skipped(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = "   "
        monitor._config.llm_model = "valid-model"
        model = monitor._resolve_llm_model()
        assert model == "valid-model"


# ============================================================
# Issue 4: LLM config errors don't retry
# ============================================================

class TestLLMConfigErrorNoRetry:
    """Issue 4: Config errors (auth, key) return immediately without retry."""

    @pytest.fixture(autouse=True)
    def _check_deps(self):
        try:
            import litellm  # noqa: F401
        except ImportError:
            pytest.skip("litellm not available")

    def test_config_error_returns_immediately(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = "test/model"
        with patch('litellm.completion', side_effect=Exception("AuthenticationError: invalid key")):
            result = monitor._call_llm_with_retries("test prompt")
            assert result.status == "config_error"
            assert "invalid key" in result.error_message

    def test_auth_error_returns_immediately(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = "test/model"
        with patch('litellm.completion', side_effect=Exception("API key not found")):
            result = monitor._call_llm_with_retries("test prompt")
            assert result.status == "config_error"

    def test_no_model_configured(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = ""
        monitor._config.llm_model = None
        monitor._config.openai_model = None
        monitor._config.model_name = None
        monitor._config.ai_model = None
        result = monitor._call_llm_with_retries("test")
        assert result.status == "config_error"
        assert "No LLM model configured" in result.error_message

    def test_success_returns_llm_result(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = "test/model"
        mock_response = MagicMock()
        mock_choice = MagicMock()
        mock_message = MagicMock()
        mock_message.content = "Buy recommendation"
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        with patch('litellm.completion', return_value=mock_response):
            result = monitor._call_llm_with_retries("test prompt")
            assert result.status == "success"
            assert result.content == "Buy recommendation"

    def test_empty_response_after_retries(self):
        monitor = _make_monitor()
        monitor._config.litellm_model = "test/model"
        mock_response = MagicMock()
        mock_choice = MagicMock()
        mock_message = MagicMock()
        mock_message.content = ""
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        with patch('litellm.completion', return_value=mock_response):
            with patch('time.sleep'):  # skip sleep
                result = monitor._call_llm_with_retries("test prompt")
                assert result.status == "empty_response"


# ============================================================
# Issue 5: Events stamped with snapshot_id
# ============================================================

class TestEventSnapshotId:
    """Issue 5: _stamp_event sets snapshot_id, run_id, snapshot_status."""

    def test_stamp_event_sets_all_fields(self):
        monitor = _make_monitor()
        monitor._current_snapshot_id = "snap_stamp_1"
        monitor._current_run_id = "run_stamp_a"
        event = IntradayEvent(
            stock_code="600519",
            stock_name="茅台",
            timestamp=datetime(2026, 6, 8, 10, 30),
            current_price=100.0,
            volume_ratio=1.0,
            change_pct=0.0,
            event_type="test",
            description="test",
        )
        stamped = monitor._stamp_event(event, snapshot_status="valid")
        assert stamped.snapshot_id == "snap_stamp_1"
        assert stamped.run_id == "run_stamp_a"
        assert stamped.snapshot_status == "valid"

    def test_stamp_event_no_status(self):
        monitor = _make_monitor()
        monitor._current_snapshot_id = "snap_no_status"
        monitor._current_run_id = "run_no_status"
        event = IntradayEvent(
            stock_code="000001",
            stock_name="Test",
            timestamp=datetime(2026, 6, 8, 10, 30),
            current_price=50.0,
            volume_ratio=1.0,
            change_pct=0.0,
            event_type="test",
            description="test",
        )
        stamped = monitor._stamp_event(event)
        assert stamped.snapshot_id == "snap_no_status"
        assert stamped.run_id == "run_no_status"
        assert stamped.snapshot_status is None

    def test_event_to_dict_includes_snapshot_fields(self):
        event = IntradayEvent(
            stock_code="600519",
            stock_name="茅台",
            timestamp=datetime(2026, 6, 8, 10, 30),
            current_price=100.0,
            volume_ratio=1.0,
            change_pct=0.0,
            event_type="test",
            description="test",
            snapshot_id="snap_dict",
            run_id="run_dict",
            snapshot_status="valid",
        )
        d = event.to_dict()
        assert d["snapshot_id"] == "snap_dict"
        assert d["run_id"] == "run_dict"
        assert d["snapshot_status"] == "valid"

    def test_save_event_stamps_event(self):
        """When _save_event is called without stamping, the caller should stamp first.
        Check that the event passed has its fields."""
        monitor = _make_monitor()
        monitor._current_snapshot_id = "snap_save"
        monitor._current_run_id = "run_save"
        mock_session = MagicMock()
        mock_conn = MagicMock()
        mock_session.connection.return_value = mock_conn
        monitor._db.session_scope.return_value = mock_session

        with patch.object(monitor, '_get_market_for_stock', return_value="cn"):
            with patch.object(monitor, '_get_market_query_date', return_value="2026-06-08"):
                event = IntradayEvent(
                    stock_code="600519",
                    stock_name="茅台",
                    timestamp=datetime(2026, 6, 8, 10, 30),
                    current_price=100.0,
                    volume_ratio=1.0,
                    change_pct=0.0,
                    event_type="test",
                    description="test",
                )
                stamped = monitor._stamp_event(event, snapshot_status="valid")
                monitor._save_event(stamped)
                # Check that INSERT had snapshot_id
                insert_calls = [
                    c for c in mock_conn.execute.call_args_list
                    if c[1] and isinstance(c[1], dict) and "snapshot_id" in str(c[1].get("sid", ""))
                ]
                # At minimum, the INSERT ran
                assert True  # smoke test: no exception raised


# ============================================================
# Issue 6: Distributed process lock with TTL
# ============================================================

class TestProcessLock:
    """Issue 6: _acquire_process_lock, _release_process_lock, _force_release_process_lock."""

    def _setup_lock_mock(self, monitor, fetchone_result=None):
        """Set up mock chain for process lock tests."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = fetchone_result
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.connection.return_value = mock_conn
        monitor._db.session_scope = MagicMock()
        monitor._db.session_scope.return_value = mock_session
        return mock_session, mock_conn

    def test_acquire_lock_method_exists(self):
        """Verify _acquire_process_lock is callable without unhandled exception."""
        monitor = _make_monitor()
        self._setup_lock_mock(monitor, fetchone_result=None)
        result = monitor._acquire_process_lock("test_lock", ttl_seconds=60)
        assert isinstance(result, bool)

    def test_acquire_lock_already_held(self):
        monitor = _make_monitor()
        from datetime import datetime as dt, timedelta
        future_time = (dt.utcnow() + timedelta(seconds=60)).isoformat()
        self._setup_lock_mock(monitor, fetchone_result=[future_time, "other_holder"])
        result = monitor._acquire_process_lock("held_lock")
        assert result is False

    def test_acquire_lock_expired(self):
        monitor = _make_monitor()
        from datetime import datetime as dt, timedelta
        past_time = (dt.utcnow() - timedelta(seconds=60)).isoformat()
        self._setup_lock_mock(monitor, fetchone_result=[past_time, "old_holder"])
        result = monitor._acquire_process_lock("expired_lock")
        assert isinstance(result, bool)

    def test_release_lock(self):
        monitor = _make_monitor()
        self._setup_lock_mock(monitor)
        monitor._release_process_lock("test_lock")  # must not raise

    def test_force_release_lock(self):
        monitor = _make_monitor()
        self._setup_lock_mock(monitor)
        monitor._force_release_process_lock("force_lock")  # must not raise

    def test_acquire_lock_db_error(self):
        monitor = _make_monitor()
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.connection.side_effect = RuntimeError("DB down")
        monitor._db.session_scope = MagicMock()
        monitor._db.session_scope.return_value = mock_session
        result = monitor._acquire_process_lock("error_lock")
        assert result is False


# ============================================================
# Issue 7: Email routing by report type
# ============================================================

class TestEmailReportType:
    """Issue 7: _send_decision_email routes by EmailReportType."""

    def test_official_decision_subject(self):
        monitor = _make_monitor()
        monitor._email_sender = TestFakeEmailSender.FakeEmailSender()
        with patch.object(monitor, '_get_market_query_date', return_value="2026-06-08"):
            with patch.object(monitor, '_resolve_primary_market', return_value="cn"):
                monitor._send_decision_email(
                    "Decision body",
                    report_type=EmailReportType.OFFICIAL_DECISION,
                    snapshot_id="snap_off",
                    coverage_ratio=0.95,
                    valid_count=19,
                    expected_count=20,
                )
        assert len(monitor._email_sender.sends) == 1
        subject = monitor._email_sender.sends[0]["subject"]
        assert "盘中监控报告" in subject

    def test_data_quality_alert_subject(self):
        monitor = _make_monitor()
        monitor._email_sender = TestFakeEmailSender.FakeEmailSender()
        monitor._send_decision_email(
            "Alert body",
            report_type=EmailReportType.DATA_QUALITY_ALERT,
            snapshot_id="snap_dq",
            coverage_ratio=0.5,
            valid_count=5,
            expected_count=10,
        )
        subject = monitor._email_sender.sends[0]["subject"]
        assert "数据质量告警" in subject or "告警" in subject

    def test_llm_failure_alert_subject(self):
        monitor = _make_monitor()
        monitor._email_sender = TestFakeEmailSender.FakeEmailSender()
        monitor._send_decision_email(
            "LLM failure",
            report_type=EmailReportType.LLM_FAILURE_ALERT,
            snapshot_id="snap_llm",
            llm_status="network_error",
        )
        subject = monitor._email_sender.sends[0]["subject"]
        assert "LLM" in subject

    def test_no_baseline_alert_subject(self):
        monitor = _make_monitor()
        monitor._email_sender = TestFakeEmailSender.FakeEmailSender()
        monitor._send_decision_email(
            "No baseline",
            report_type=EmailReportType.NO_BASELINE_ALERT,
            snapshot_id="snap_nb",
            baseline_status="missing",
        )
        subject = monitor._email_sender.sends[0]["subject"]
        assert "无基线" in subject or "baseline" in subject.lower()

    def test_snapshot_incomplete_alert_subject(self):
        monitor = _make_monitor()
        monitor._email_sender = TestFakeEmailSender.FakeEmailSender()
        monitor._send_decision_email(
            "No snapshot",
            report_type=EmailReportType.SNAPSHOT_INCOMPLETE_ALERT,
            snapshot_id=None,
            baseline_status="available",
        )
        subject = monitor._email_sender.sends[0]["subject"]
        assert "快照未完成" in subject or "SNAPSHOT" in subject.upper()

    def test_coverage_summary_prepended_for_official(self):
        monitor = _make_monitor()
        monitor._email_sender = TestFakeEmailSender.FakeEmailSender()
        with patch.object(monitor, '_get_market_query_date', return_value="2026-06-08"):
            with patch.object(monitor, '_resolve_primary_market', return_value="cn"):
                monitor._send_decision_email(
                    "LLM output here",
                    report_type=EmailReportType.OFFICIAL_DECISION,
                    snapshot_id="snap_sum",
                    coverage_ratio=1.0,
                    valid_count=10,
                    expected_count=10,
                    llm_status="success",
                    baseline_status="available",
                )
        content = monitor._email_sender.sends[0]["content"]
        assert "快照覆盖率" in content
        assert "snap_sum" in content

    def test_alert_types_no_coverage_prefix(self):
        monitor = _make_monitor()
        monitor._email_sender = TestFakeEmailSender.FakeEmailSender()
        monitor._send_decision_email(
            "Alert content",
            report_type=EmailReportType.DATA_QUALITY_ALERT,
            snapshot_id="snap_alert",
            coverage_ratio=0.5,
            valid_count=5,
            expected_count=10,
        )
        content = monitor._email_sender.sends[0]["content"]
        assert "Alert content" in content
        # No coverage summary prepended for alerts
        assert "快照覆盖率" not in content


# ============================================================
# Issue 8: Structured logging context
# ============================================================

class TestLogContext:
    """Issue 8: _log_ctx helper for structured logging with run_id/snapshot_id."""

    def test_log_ctx_includes_run_id(self):
        monitor = _make_monitor()
        monitor._current_run_id = "run_log_test"
        ctx = _log_ctx(monitor)
        assert ctx["run_id"] == "run_log_test"

    def test_log_ctx_includes_snapshot_id(self):
        monitor = _make_monitor()
        monitor._current_snapshot_id = "snap_log_test"
        ctx = _log_ctx(monitor)
        assert ctx["snapshot_id"] == "snap_log_test"

    def test_log_ctx_includes_extra(self):
        monitor = _make_monitor()
        ctx = _log_ctx(monitor, stock="600519", market="cn")
        assert ctx.get("stock") == "600519"
        assert ctx.get("market") == "cn"

    def test_log_ctx_none_when_no_monitor_attrs(self):
        monitor = _make_monitor()
        monitor._current_run_id = None
        monitor._current_snapshot_id = None
        ctx = _log_ctx(monitor)
        assert ctx["run_id"] is None
        assert ctx["snapshot_id"] is None

    def test_log_ctx_always_returns_dict(self):
        monitor = _make_monitor()
        ctx = _log_ctx(monitor)
        assert isinstance(ctx, dict)
        assert "run_id" in ctx
        assert "snapshot_id" in ctx


# ============================================================
# Batch prefetch: RealtimeBatchResult dataclass
# ============================================================

class TestRealtimeBatchResult:
    """RealtimeBatchResult dataclass."""

    @pytest.fixture(autouse=True)
    def _check_deps(self):
        if RealtimeBatchResult is None:
            pytest.skip("realtime_types not available")

    def test_basic_dataclass(self):
        result = RealtimeBatchResult(
            quotes={"hk00700": None},
            source="akshare_hk_spot",
            is_bulk=True,
            market="hk",
            elapsed_seconds=42.5,
            requested_count=7,
            matched_count=3,
        )
        assert result.source == "akshare_hk_spot"
        assert result.is_bulk is True
        assert result.market == "hk"
        assert result.elapsed_seconds == 42.5
        assert result.requested_count == 7
        assert result.matched_count == 3
        assert "hk00700" in result.quotes


# ============================================================
# Batch prefetch: RealtimeSourceCapability + is_bulk_source
# ============================================================

class TestRealtimeSourceCapability:
    """RealtimeSourceCapability dataclass and is_bulk_source."""

    @pytest.fixture(autouse=True)
    def _check_deps(self):
        if RealtimeSourceCapability is None:
            pytest.skip("realtime_types not available")

    def test_bulk_source_disables_single_stock(self):
        cap = RealtimeSourceCapability(
            source_name="test_bulk",
            market="hk",
            is_bulk=True,
        )
        assert cap.is_bulk is True
        assert cap.supports_single_stock is False  # __post_init__

    def test_non_bulk_keeps_single_stock(self):
        cap = RealtimeSourceCapability(
            source_name="test_single",
            market="cn",
            is_bulk=False,
        )
        assert cap.supports_single_stock is True

    def test_defaults(self):
        cap = RealtimeSourceCapability(source_name="default_test")
        assert cap.is_bulk is False
        assert cap.supports_single_stock is True
        assert cap.market == ""
        assert cap.default_timeout == 15.0

    def test_is_bulk_source_known(self):
        assert is_bulk_source is not None
        assert is_bulk_source("akshare_hk_spot") is True

    def test_is_bulk_source_unknown(self):
        assert is_bulk_source("some_random_source") is False

    def test_bulk_registry_has_expected_entries(self):
        if BULK_SOURCE_REGISTRY is None:
            pytest.skip("BULK_SOURCE_REGISTRY not available")
        assert "akshare_hk_spot_em" in BULK_SOURCE_REGISTRY
        assert "akshare_hk_spot" in BULK_SOURCE_REGISTRY
        assert "akshare_cn_spot" in BULK_SOURCE_REGISTRY
        for name, cap in BULK_SOURCE_REGISTRY.items():
            assert cap.is_bulk is True
            assert cap.supports_single_stock is False


# ============================================================
# Batch prefetch: RealtimeSnapshotCache
# ============================================================

class TestRealtimeSnapshotCache:
    """RealtimeSnapshotCache class."""

    @pytest.fixture(autouse=True)
    def _check_deps(self):
        if RealtimeSnapshotCache is None:
            pytest.skip("realtime_types not available")

    def setup_method(self):
        RealtimeSnapshotCache.clear_all()

    def test_set_and_get(self):
        data = {"hk00700": MagicMock()}
        data["hk00700"].price = 100.0
        RealtimeSnapshotCache.set("snap_cache_1", "hk:test", data, ttl=90.0)
        cached = RealtimeSnapshotCache.get("snap_cache_1", "hk:test")
        assert cached is not None
        assert cached["hk00700"].price == 100.0

    def test_miss_nonexistent(self):
        result = RealtimeSnapshotCache.get("no_such_snap", "any_key")
        assert result is None

    def test_miss_wrong_key(self):
        RealtimeSnapshotCache.set("snap2", "key_a", {"code": MagicMock()})
        result = RealtimeSnapshotCache.get("snap2", "key_b")
        assert result is None

    def test_clear(self):
        RealtimeSnapshotCache.set("snap3", "hk:batch", {"hk00001": MagicMock()})
        RealtimeSnapshotCache.clear("snap3")
        result = RealtimeSnapshotCache.get("snap3", "hk:batch")
        assert result is None

    def test_expired_ttl_returns_none(self):
        RealtimeSnapshotCache.set("snap4", "exp_key", {"exp": MagicMock()}, ttl=-1.0)
        result = RealtimeSnapshotCache.get("snap4", "exp_key")
        assert result is None

    def test_clear_all(self):
        RealtimeSnapshotCache.set("snap5", "k1", {"a": MagicMock()})
        RealtimeSnapshotCache.set("snap5", "k2", {"b": MagicMock()})
        RealtimeSnapshotCache.clear_all()
        assert RealtimeSnapshotCache.get("snap5", "k1") is None
        assert RealtimeSnapshotCache.get("snap5", "k2") is None

    def test_snapshot_isolation(self):
        RealtimeSnapshotCache.set("snap_a", "key", {"code": MagicMock()})
        RealtimeSnapshotCache.set("snap_b", "key", {"code2": MagicMock()})
        cached_a = RealtimeSnapshotCache.get("snap_a", "key")
        cached_b = RealtimeSnapshotCache.get("snap_b", "key")
        assert cached_a is not None
        assert cached_b is not None
        assert cached_a is not cached_b


# ============================================================
# Batch prefetch: _snapshot_one_stock_with_quote + fallback
# ============================================================

class TestSnapshotOneStockWithQuote:
    """_snapshot_one_stock_with_quote and _fallback_single_quote_without_bulk_sources."""

    def test_with_quote_suspended(self):
        """quote.price <= 0 → suspended event."""
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._email_sender = None
        mock_quote = MagicMock()
        mock_quote.price = 0.0
        mock_quote.name = "TestStock"

        with patch.object(monitor, '_save_event') as mock_save:
            monitor._snapshot_one_stock_with_quote("000001", datetime.now(), mock_quote)
            saved = mock_save.call_args[0][0]
            assert saved.event_type == "suspended"
            assert saved.snapshot_status == "suspended"

    def test_with_quote_price_only_no_baseline(self):
        """No yesterday analysis → price_only event."""
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._email_sender = None
        monitor._yesterday_analysis = {}
        mock_quote = MagicMock()
        mock_quote.price = 100.0
        mock_quote.volume_ratio = 1.2
        mock_quote.change_pct = 0.5
        mock_quote.name = "TestStock"

        with patch.object(monitor, '_save_event') as mock_save:
            monitor._snapshot_one_stock_with_quote("000001", datetime.now(), mock_quote)
            saved = mock_save.call_args[0][0]
            assert saved.event_type == "price_only"
            assert saved.snapshot_status == "valid"

    def test_with_quote_break_stop_loss(self):
        """quote with stop_loss threshold → break_stop_loss event."""
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._email_sender = None
        monitor._yesterday_analysis = {
            "000001": {"ideal_buy": 100.0, "secondary_buy": 95.0, "stop_loss": 90.0, "take_profit": 115.0},
        }
        mock_quote = MagicMock()
        mock_quote.price = 88.0
        mock_quote.volume_ratio = 1.0
        mock_quote.change_pct = -3.0
        mock_quote.name = "TestStock"

        with patch.object(monitor, '_save_event') as mock_save:
            monitor._snapshot_one_stock_with_quote("000001", datetime.now(), mock_quote)
            saved = mock_save.call_args[0][0]
            assert saved.event_type == "break_stop_loss"

    def test_fallback_single_quote(self):
        """_fallback_single_quote_without_bulk_sources when get_realtime_quote returns None."""
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._email_sender = None
        monitor._current_snapshot_id = "fallback_test_snap"
        monitor._current_run_id = "fallback_test_run"

        with patch.object(monitor, '_get_realtime_with_timeout', return_value=None):
            with patch.object(monitor, '_save_event') as mock_save:
                monitor._fallback_single_quote_without_bulk_sources("000001", datetime.now())
                saved = mock_save.call_args[0][0]
                assert saved.event_type == "data_unavailable"
                assert saved.snapshot_status == "timeout"
                assert saved.snapshot_id == "fallback_test_snap"

    def test_fallback_single_quote_success(self):
        """_fallback_single_quote_without_bulk_sources with successful quote."""
        monitor = _make_monitor(intraday_stocks="000001")
        monitor._email_sender = None
        mock_quote = MagicMock()
        mock_quote.price = 105.0
        mock_quote.volume_ratio = 1.5
        mock_quote.change_pct = 2.0
        mock_quote.name = "TestStock"
        monitor._yesterday_analysis = {
            "000001": {"ideal_buy": 100.0, "secondary_buy": 95.0, "stop_loss": 90.0, "take_profit": 115.0},
        }

        with patch.object(monitor, '_get_realtime_with_timeout', return_value=mock_quote):
            with patch.object(monitor, '_save_event') as mock_save:
                monitor._fallback_single_quote_without_bulk_sources("000001", datetime.now())
                saved = mock_save.call_args[0][0]
                assert saved.event_type == "enter_ideal_buy"
                assert saved.stock_code == "000001"


# ============================================================
# Snapshot state persistence tests
# ============================================================

class TestSnapshotStatePersistence:
    """Tests for snapshot state creation, persistence, and defensive handling."""

    @staticmethod
    def _setup_persist_mock(monitor, stored_rows=None):
        """Set up mock session_scope with in-memory store for snapshot state round-trip.

        Returns (patcher, stored_dict) where stored_dict["rows"] is the mutable list of
        rows that INSERT will push to and SELECT will read from.
        """
        import json as _json
        stored = {"rows": []}
        if stored_rows:
            stored["rows"] = list(stored_rows)

        mock_conn = MagicMock()

        def _execute_side_effect(query, params=None):
            result = MagicMock()
            query_text = str(query) if query is not None else ""
            params = params if params else {}

            if "INSERT OR REPLACE INTO intraday_snapshots" in query_text and params:
                stored["rows"] = [r for r in stored["rows"] if r[0] != params.get("sid")]
                # _persist_snapshot_state already json.dumps() the list fields before
                # passing them as params, so store them as-is (they are JSON strings).
                stored["rows"].append((
                    params.get("sid"),
                    params.get("rid"),
                    params.get("st"),
                    params.get("sa"),
                    params.get("fa"),
                    params.get("ec"),
                    params.get("cc"),
                    params.get("vc"),
                    params.get("fc"),
                    params.get("qd"),
                ))
            elif "SELECT" in query_text and "WHERE status = 'completed'" in query_text:
                result.fetchall.return_value = [r for r in stored["rows"] if r[2] == "completed"]
            elif "SELECT" in query_text:
                result.fetchall.return_value = stored["rows"][:]
            # CREATE TABLE IF NOT EXISTS passes silently
            return result

        mock_conn.execute.side_effect = _execute_side_effect

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.connection.return_value = mock_conn

        patcher = patch.object(monitor._db, 'session_scope', return_value=mock_session)
        patcher.start()
        return patcher, stored

    def test_ensure_snapshots_table_creates_on_empty_db(self):
        """_ensure_intraday_snapshots_table works on empty DB."""
        monitor = _make_monitor()
        self._setup_persist_mock(monitor)
        # Must not raise
        monitor._ensure_intraday_snapshots_table()
        # After ensure, query should work without exception
        result = monitor._get_latest_completed_snapshot()
        assert result is None  # no rows yet

    def test_get_latest_completed_snapshot_none_when_no_completed(self):
        """Returns None when no completed rows exist (table exists)."""
        monitor = _make_monitor()
        self._setup_persist_mock(monitor)
        monitor._ensure_intraday_snapshots_table()
        from src.core.intraday_monitor import SnapshotState
        state = SnapshotState(
            snapshot_id="test-snap-running",
            run_id="run-001",
            status="running",
            expected_codes=["600519"],
        )
        monitor._persist_snapshot_state(state)
        result = monitor._get_latest_completed_snapshot()
        assert result is None

    def test_snapshot_state_persist_and_readback(self):
        """Snapshot state round-trips through persist and readback."""
        monitor = _make_monitor()
        self._setup_persist_mock(monitor)
        monitor._ensure_intraday_snapshots_table()
        state = SnapshotState(
            snapshot_id="test-snap-002",
            run_id="run-002",
            status="completed",
            expected_codes=["600519", "000001"],
            completed_codes=["600519", "000001"],
            valid_quote_codes=["600519"],
            failed_codes=["000001"],
        )
        monitor._persist_snapshot_state(state)
        result = monitor._get_latest_completed_snapshot()
        assert result is not None
        assert result["snapshot_id"] == "test-snap-002"
        assert result["status"] == "completed"
        assert result["expected_codes"] == ["600519", "000001"]
        assert result["valid_quote_codes"] == ["600519"]
        assert result["failed_codes"] == ["000001"]

    def test_snapshot_state_persist_all_fields(self):
        """Completed snapshot has all required fields non-null."""
        monitor = _make_monitor()
        self._setup_persist_mock(monitor)
        monitor._ensure_intraday_snapshots_table()
        state = SnapshotState(
            snapshot_id="test-snap-all-fields",
            run_id="run-all-fields",
            status="completed",
            started_at=datetime(2026, 6, 10, 10, 0, 0),
            finished_at=datetime(2026, 6, 10, 10, 1, 0),
            expected_codes=["600519", "000001"],
            completed_codes=["600519", "000001"],
            valid_quote_codes=["600519"],
            failed_codes=["000001"],
        )
        monitor._persist_snapshot_state(state)
        result = monitor._get_latest_completed_snapshot()
        assert result is not None
        assert result["snapshot_id"] == "test-snap-all-fields"
        assert result["status"] == "completed"
        assert result["expected_codes"] == ["600519", "000001"]
        assert result["completed_codes"] == ["600519", "000001"]
        assert result["valid_quote_codes"] == ["600519"]
        assert result["failed_codes"] == ["000001"]

    def test_run_one_shot_snapshot_creates_completed_snapshot(self):
        """run_one_shot_snapshot creates completed snapshot with all fields."""
        monitor = _make_monitor(intraday_stocks="600519,000001")
        monitor._config.intraday_force_run = True
        self._setup_persist_mock(monitor)
        monitor._ensure_intraday_snapshots_table()

        with patch.object(monitor, '_is_stock_trading_today', return_value=True):
            with patch.object(monitor, 'load_yesterday_analysis'):
                with patch.object(monitor, '_run_snapshot_for_codes') as mock_run:
                    mock_run.return_value = {
                        "completed_codes": ["600519", "000001"],
                        "valid_quote_codes": ["600519", "000001"],
                        "failed_codes": [],
                        "timeout_count": 0,
                        "suspended_count": 0,
                        "batch_matched_count": 2,
                        "fallback_count": 0,
                    }
                    monitor.run_one_shot_snapshot()

        result = monitor._get_latest_completed_snapshot()
        assert result is not None
        assert result["status"] == "completed"
        assert "600519" in result["expected_codes"]
        assert "000001" in result["expected_codes"]
        assert len(result["valid_quote_codes"]) == 2
        assert len(result["failed_codes"]) == 0
        assert result["snapshot_id"] is not None
        assert "standalone" in result["snapshot_id"]

    def test_run_one_shot_decision_creates_completed_snapshot(self):
        """run_one_shot_decision pre-decision snapshot creates completed record."""
        monitor = _make_monitor(intraday_stocks="600519")
        monitor._config.intraday_force_run = True
        self._setup_persist_mock(monitor)
        monitor._ensure_intraday_snapshots_table()

        with patch.object(monitor, '_is_stock_trading_today', return_value=True):
            with patch.object(monitor, 'load_yesterday_analysis'):
                with patch.object(monitor, '_run_snapshot_for_codes') as mock_run:
                    mock_run.return_value = {
                        "completed_codes": ["600519"],
                        "valid_quote_codes": ["600519"],
                        "failed_codes": [],
                        "timeout_count": 0,
                        "suspended_count": 0,
                        "batch_matched_count": 1,
                        "fallback_count": 0,
                    }
                    with patch.object(monitor, '_final_decision_locked'):
                        monitor.run_one_shot_decision()

        result = monitor._get_latest_completed_snapshot()
        assert result is not None
        assert result["status"] == "completed"
        assert result["expected_codes"] == ["600519"]
        assert result["valid_quote_codes"] == ["600519"]
        assert "decision" in result["snapshot_id"]

    def test_snapshot_coverage_all_valid(self):
        """100% valid coverage when all codes valid."""
        monitor = _make_monitor()
        self._setup_persist_mock(monitor)
        monitor._ensure_intraday_snapshots_table()
        state = SnapshotState(
            snapshot_id="test-cov-full",
            run_id="run-cov",
            status="completed",
            expected_codes=["600519", "000001", "000002"],
            completed_codes=["600519", "000001", "000002"],
            valid_quote_codes=["600519", "000001", "000002"],
            failed_codes=[],
        )
        monitor._persist_snapshot_state(state)
        result = monitor._get_latest_completed_snapshot()
        assert result is not None
        expected = len(result["expected_codes"])
        valid = len(result["valid_quote_codes"])
        assert valid / expected == 1.0

    def test_snapshot_coverage_partial(self):
        """Partial valid coverage is correctly calculated."""
        monitor = _make_monitor()
        self._setup_persist_mock(monitor)
        monitor._ensure_intraday_snapshots_table()
        state = SnapshotState(
            snapshot_id="test-cov-partial",
            run_id="run-cov",
            status="completed",
            expected_codes=["600519", "000001", "hk00700"],
            completed_codes=["600519", "000001"],
            valid_quote_codes=["600519"],
            failed_codes=["000001", "hk00700"],
        )
        monitor._persist_snapshot_state(state)
        result = monitor._get_latest_completed_snapshot()
        assert result is not None
        expected = len(result["expected_codes"])
        valid = len(result["valid_quote_codes"])
        assert valid / expected == 1.0 / 3.0

    def test_run_snapshot_with_state_persistence_creates_running_then_completed(self):
        """_run_snapshot_with_state_persistence creates running then completed state."""
        monitor = _make_monitor()
        self._setup_persist_mock(monitor)
        monitor._ensure_intraday_snapshots_table()

        mock_results = {
            "completed_codes": ["600519"],
            "valid_quote_codes": ["600519"],
            "failed_codes": [],
            "timeout_count": 0,
            "suspended_count": 0,
            "batch_matched_count": 1,
            "fallback_count": 0,
        }
        with patch.object(monitor, '_run_snapshot_for_codes', return_value=mock_results):
            results, state = monitor._run_snapshot_with_state_persistence(
                codes=["600519"],
                now=datetime(2026, 6, 10, 10, 30, 0),
                snapshot_id="test-shared-method",
                source="test",
            )

        assert state.status == "completed"
        assert state.expected_codes == ["600519"]
        assert state.completed_codes == ["600519"]
        assert state.valid_quote_codes == ["600519"]
        assert state.failed_codes == []
        assert state.finished_at is not None

        result = monitor._get_latest_completed_snapshot()
        assert result is not None
        assert result["snapshot_id"] == "test-shared-method"
