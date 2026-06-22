# -*- coding: utf-8 -*-
"""Tests for intraday monitor LLM fallback logic."""
import pytest
from unittest.mock import MagicMock, patch
from src.core.intraday_monitor import LLMResult, IntradayMonitor


class TestLLMResultExtended:
    """Test that LLMResult extended dataclass works correctly."""

    def test_default_values(self):
        result = LLMResult(status="success", content="test")
        assert result.status == "success"
        assert result.content == "test"
        assert result.provider is None
        assert result.model is None
        assert result.base_url_host is None
        assert result.attempts == 1
        assert result.primary_error_message is None
        assert result.fallback_error_message is None

    def test_provider_field(self):
        result = LLMResult(status="success", content="test", provider="primary")
        assert result.provider == "primary"

    def test_attempts_field(self):
        result = LLMResult(status="success", content="test", attempts=2)
        assert result.attempts == 2

    def test_error_fields(self):
        result = LLMResult(
            status="network_error",
            error_message="both failed",
            provider="fallback",
            primary_error_message="timeout",
            fallback_error_message="bad gateway",
        )
        assert result.primary_error_message == "timeout"
        assert result.fallback_error_message == "bad gateway"


class TestShouldTryFallback:
    """Test _should_try_fallback_llm logic."""

    def test_disabled_returns_false(self):
        config = MagicMock()
        config.intraday_llm_fallback_enabled = False
        config.intraday_llm_fallback_model = "openai/gpt-4"
        config.intraday_llm_fallback_api_key = "sk-test"
        config.intraday_llm_fallback_retry_on = "config_error,rate_limited,network_error,empty_response"

        monitor = MagicMock(spec=IntradayMonitor)
        monitor._config = config

        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False

    def test_disabled_no_model_returns_false(self):
        config = MagicMock()
        config.intraday_llm_fallback_enabled = True
        config.intraday_llm_fallback_model = None
        config.intraday_llm_fallback_api_key = "sk-test"
        config.intraday_llm_fallback_retry_on = "config_error,rate_limited,network_error,empty_response"

        monitor = MagicMock(spec=IntradayMonitor)
        monitor._config = config

        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False

    def test_disabled_no_key_returns_false(self):
        config = MagicMock()
        config.intraday_llm_fallback_enabled = True
        config.intraday_llm_fallback_model = "openai/gpt-4"
        config.intraday_llm_fallback_api_key = None
        config.intraday_llm_fallback_retry_on = "config_error,rate_limited,network_error,empty_response"

        monitor = MagicMock(spec=IntradayMonitor)
        monitor._config = config

        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False

    def test_enabled_with_config_error_returns_true(self):
        config = MagicMock()
        config.intraday_llm_fallback_enabled = True
        config.intraday_llm_fallback_model = "openai/gpt-4"
        config.intraday_llm_fallback_api_key = "sk-test"
        config.intraday_llm_fallback_retry_on = "config_error,rate_limited,network_error,empty_response"

        monitor = MagicMock(spec=IntradayMonitor)
        monitor._config = config

        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is True

    def test_enabled_with_rate_limited_returns_true(self):
        config = MagicMock()
        config.intraday_llm_fallback_enabled = True
        config.intraday_llm_fallback_model = "openai/gpt-4"
        config.intraday_llm_fallback_api_key = "sk-test"
        config.intraday_llm_fallback_retry_on = "config_error,rate_limited,network_error,empty_response"

        monitor = MagicMock(spec=IntradayMonitor)
        monitor._config = config

        result = IntradayMonitor._should_try_fallback_llm(monitor, "rate_limited")
        assert result is True

    def test_enabled_with_not_in_retry_on_returns_false(self):
        """Status not in retry_on list should not trigger fallback."""
        config = MagicMock()
        config.intraday_llm_fallback_enabled = True
        config.intraday_llm_fallback_model = "openai/gpt-4"
        config.intraday_llm_fallback_api_key = "sk-test"
        config.intraday_llm_fallback_retry_on = "rate_limited,network_error"  # no config_error

        monitor = MagicMock(spec=IntradayMonitor)
        monitor._config = config

        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False


class TestCallLlmWithFallback:
    """Test _call_llm_with_fallback orchestrator logic."""

    def test_primary_success_no_fallback(self):
        """When primary succeeds, fallback should not be called."""
        monitor = MagicMock(spec=IntradayMonitor)
        primary = LLMResult(status="success", content="test", provider="primary")
        monitor._call_llm = MagicMock(return_value=primary)
        monitor._should_try_fallback_llm = MagicMock(return_value=False)
        monitor._call_fallback_llm = MagicMock()

        result = IntradayMonitor._call_llm_with_fallback(monitor, "prompt")

        assert result.status == "success"
        assert result.content == "test"
        assert result.provider == "primary"
        monitor._call_fallback_llm.assert_not_called()

    def test_primary_fails_fallback_disabled(self):
        """When primary fails and fallback disabled, should return primary result."""
        monitor = MagicMock(spec=IntradayMonitor)
        primary = LLMResult(status="rate_limited", error_message="429 too many requests", provider="primary")
        monitor._call_llm = MagicMock(return_value=primary)
        monitor._should_try_fallback_llm = MagicMock(return_value=False)
        monitor._call_fallback_llm = MagicMock()

        result = IntradayMonitor._call_llm_with_fallback(monitor, "prompt")

        assert result.status == "rate_limited"
        assert result.provider == "primary"
        monitor._call_fallback_llm.assert_not_called()

    def test_primary_fails_fallback_succeeds(self):
        """When primary fails but fallback succeeds, return fallback result."""
        monitor = MagicMock(spec=IntradayMonitor)
        primary = LLMResult(status="rate_limited", error_message="429", provider="primary", attempts=1)
        fallback = LLMResult(status="success", content="fallback response", provider="fallback", model="openai/gpt-4", attempts=1)
        monitor._call_llm = MagicMock(return_value=primary)
        monitor._should_try_fallback_llm = MagicMock(return_value=True)
        monitor._call_fallback_llm = MagicMock(return_value=fallback)

        result = IntradayMonitor._call_llm_with_fallback(monitor, "prompt")

        assert result.status == "success"
        assert result.content == "fallback response"
        assert result.provider == "fallback"
        assert result.primary_error_message == "429"
        assert result.attempts == 2  # primary.attempts + fallback.attempts

    def test_both_fail(self):
        """When both primary and fallback fail, return merged failure."""
        monitor = MagicMock(spec=IntradayMonitor)
        primary = LLMResult(status="rate_limited", error_message="429", provider="primary", attempts=1)
        fallback = LLMResult(status="network_error", error_message="timeout", provider="fallback", model="openai/gpt-4", attempts=1)
        monitor._call_llm = MagicMock(return_value=primary)
        monitor._should_try_fallback_llm = MagicMock(return_value=True)
        monitor._call_fallback_llm = MagicMock(return_value=fallback)

        result = IntradayMonitor._call_llm_with_fallback(monitor, "prompt")

        assert result.status == "rate_limited"
        assert "主LLM: 429" in result.error_message
        assert "备用LLM: timeout" in result.error_message
        assert result.primary_error_message == "429"
        assert result.fallback_error_message == "timeout"
        assert result.attempts == 2
