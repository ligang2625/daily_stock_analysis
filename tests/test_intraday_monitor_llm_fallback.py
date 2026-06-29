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
        # New fields
        assert result.primary_status is None
        assert result.fallback_status is None
        assert result.fallback_attempted is False
        assert result.fallback_skipped_reason is None

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

    def test_dual_failure_fields(self):
        """New fields for dual-failure reporting."""
        result = LLMResult(
            status="rate_limited",
            error_message="both failed",
            provider="fallback",
            primary_status="rate_limited",
            fallback_status="network_error",
            fallback_attempted=True,
            fallback_skipped_reason="base_url missing or empty",
        )
        assert result.primary_status == "rate_limited"
        assert result.fallback_status == "network_error"
        assert result.fallback_attempted is True
        assert result.fallback_skipped_reason == "base_url missing or empty"

    def test_dual_failure_fields_defaults(self):
        """Verify defaults when only status is provided."""
        result = LLMResult(status="fail")
        assert result.status == "fail"
        assert result.primary_status is None
        assert result.fallback_status is None
        assert result.fallback_attempted is False
        assert result.fallback_skipped_reason is None

    def test_dual_failure_fields_set(self):
        """Verify all dual-failure fields can be set explicitly."""
        result = LLMResult(
            status="rate_limited",
            primary_status="rate_limited",
            fallback_status="network_error",
            fallback_attempted=True,
            fallback_skipped_reason=None,
        )
        assert result.primary_status == "rate_limited"
        assert result.fallback_status == "network_error"
        assert result.fallback_attempted is True
        assert result.fallback_skipped_reason is None


class TestShouldTryFallback:
    """Test _should_try_fallback_llm logic."""

    def _make_config(self, **kwargs):
        """Helper to create a mock config with defaults for all required fields."""
        config = MagicMock()
        config.intraday_llm_fallback_enabled = kwargs.get("enabled", False)
        config.intraday_llm_fallback_model = kwargs.get("model", None)
        config.intraday_llm_fallback_api_key = kwargs.get("api_key", None)
        config.intraday_llm_fallback_base_url = kwargs.get("base_url", None)
        config.intraday_llm_fallback_protocol = kwargs.get("protocol", "openai")
        config.intraday_llm_fallback_retry_on = kwargs.get(
            "retry_on", "config_error,rate_limited,network_error,empty_response"
        )
        return config

    def _make_monitor(self, config):
        monitor = MagicMock(spec=IntradayMonitor)
        monitor._config = config
        return monitor

    def test_disabled_returns_false(self):
        config = self._make_config(
            enabled=False,
            model="openai/gpt-4",
            api_key="sk-test",
            base_url="https://test.api.com/v1",
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False
        assert monitor._fallback_skipped_reason is not None

    def test_disabled_no_model_returns_false(self):
        config = self._make_config(
            enabled=True,
            model=None,
            api_key="sk-test",
            base_url="https://test.api.com/v1",
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False

    def test_disabled_no_key_returns_false(self):
        config = self._make_config(
            enabled=True,
            model="openai/gpt-4",
            api_key=None,
            base_url="https://test.api.com/v1",
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False

    def test_enabled_with_config_error_returns_true(self):
        config = self._make_config(
            enabled=True,
            model="openai/gpt-4",
            api_key="sk-test",
            base_url="https://test.api.com/v1",
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is True

    def test_enabled_with_rate_limited_returns_true(self):
        config = self._make_config(
            enabled=True,
            model="openai/gpt-4",
            api_key="sk-test",
            base_url="https://test.api.com/v1",
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "rate_limited")
        assert result is True

    def test_enabled_with_not_in_retry_on_returns_false(self):
        """Status not in retry_on list should not trigger fallback."""
        config = self._make_config(
            enabled=True,
            model="openai/gpt-4",
            api_key="sk-test",
            base_url="https://test.api.com/v1",
            retry_on="rate_limited,network_error",  # no config_error
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False

    # ---- New tests for base_url validation ----

    def test_missing_base_url_returns_false(self):
        """_should_try_fallback_llm rejects missing base_url."""
        config = self._make_config(
            enabled=True,
            model="openai/gpt-4",
            api_key="sk-test",
            base_url=None,
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False
        assert monitor._fallback_skipped_reason == "base_url missing or empty"

    def test_empty_base_url_returns_false(self):
        """_should_try_fallback_llm rejects empty base_url."""
        config = self._make_config(
            enabled=True,
            model="openai/gpt-4",
            api_key="sk-test",
            base_url="",
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False
        assert monitor._fallback_skipped_reason == "base_url missing or empty"

    def test_base_url_invalid_scheme_returns_false(self):
        """_should_try_fallback_llm rejects base_url without http:// or https://."""
        config = self._make_config(
            enabled=True,
            model="openai/gpt-4",
            api_key="sk-test",
            base_url="ftp://invalid.scheme.com/v1",
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False
        assert "invalid scheme" in monitor._fallback_skipped_reason.lower()

    def test_base_url_no_scheme_returns_false(self):
        """_should_try_fallback_llm rejects base_url with no scheme."""
        config = self._make_config(
            enabled=True,
            model="openai/gpt-4",
            api_key="sk-test",
            base_url="api.example.com/v1",
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False
        assert "invalid scheme" in monitor._fallback_skipped_reason.lower()

    def test_unsupported_protocol_returns_false(self):
        """_should_try_fallback_llm rejects unsupported protocol."""
        config = self._make_config(
            enabled=True,
            model="openai/gpt-4",
            api_key="sk-test",
            base_url="https://test.api.com/v1",
            protocol="gemini",
        )
        monitor = self._make_monitor(config)
        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False
        assert "unsupported protocol" in monitor._fallback_skipped_reason.lower()


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
        """When both primary and fallback fail, return merged failure with separate statuses."""
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

    # ---- New tests for dual failure and skipped reason ----

    def test_both_fail_preserves_separate_statuses(self):
        """Dual failure preserves primary_status and fallback_status separately."""
        monitor = MagicMock(spec=IntradayMonitor)
        primary = LLMResult(status="rate_limited", error_message="429", provider="primary", attempts=1)
        fallback = LLMResult(status="network_error", error_message="timeout", provider="fallback", model="openai/gpt-4", attempts=1)
        monitor._call_llm = MagicMock(return_value=primary)
        monitor._should_try_fallback_llm = MagicMock(return_value=True)
        monitor._call_fallback_llm = MagicMock(return_value=fallback)

        result = IntradayMonitor._call_llm_with_fallback(monitor, "prompt")

        # Separate statuses preserved
        assert result.primary_status == "rate_limited"
        assert result.fallback_status == "network_error"
        assert result.fallback_attempted is True

    def test_config_missing_base_url_returns_false_with_reason(self):
        """Config missing base_url returns False and sets fallback_skipped_reason."""
        config = MagicMock()
        config.intraday_llm_fallback_enabled = True
        config.intraday_llm_fallback_model = "openai/gpt-4"
        config.intraday_llm_fallback_api_key = "sk-test"
        config.intraday_llm_fallback_base_url = None
        config.intraday_llm_fallback_protocol = "openai"
        config.intraday_llm_fallback_retry_on = "config_error,rate_limited,network_error,empty_response"

        monitor = MagicMock(spec=IntradayMonitor)
        monitor._config = config

        result = IntradayMonitor._should_try_fallback_llm(monitor, "config_error")
        assert result is False
        assert monitor._fallback_skipped_reason == "base_url missing or empty"
