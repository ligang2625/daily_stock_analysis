# -*- coding: utf-8 -*-
"""Tests for intraday_monitor batching logic with mocked LLMResult returns.

Tests exercise _generate_single_batch_decision and _generate_batched_decision
by mocking _generate_single_batch_llm to return controlled LLMResult objects.
"""

from unittest.mock import MagicMock, patch

from src.core.intraday_monitor import (
    IntradayMonitor,
    LLMResult,
    IntradayReportResult,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

STOCK_BEGIN = "<!-- STOCK_BEGIN:{code} -->"
STOCK_END = "<!-- STOCK_END:{code} -->"
BATCH_END = "<!-- INTRADAY_BATCH_COMPLETE -->"


def _make_block(code: str, body: str = "") -> str:
    return f"{STOCK_BEGIN.format(code=code)}\n{body}\n{STOCK_END.format(code=code)}"


def _make_batch_content(codes: list[str], extra: str = "") -> str:
    """Build LLM-style batch content with blocks for every code and batch end marker."""
    parts = [_make_block(c, f"{c} 分析内容") for c in codes]
    if extra:
        parts.append(extra)
    parts.append(BATCH_END)
    return "\n\n".join(parts)


def _make_llm_result(codes: list[str], finish_reason: str = "stop", status: str = "success", extra: str = "") -> LLMResult:
    """Build an LLMResult with proper stock blocks for the given codes."""
    return LLMResult(
        status=status,
        content=_make_batch_content(codes, extra=extra),
        finish_reason=finish_reason,
        model="test-model",
    )


def _make_monitor(**config_overrides):
    """Create an IntradayMonitor with all dependencies mocked.

    Uses a plain object (not MagicMock) for config so that hasattr/getattr
    behave normally. MagicMock auto-creates attributes, breaking default-value
    fallback in _generate_batched_decision.
    """
    # Start with safe defaults, then apply overrides
    defaults = {
        'intraday_llm_batch_size': 0,
        'intraday_llm_compensation_max_rounds': 2,
    }
    defaults.update(config_overrides)

    config = MagicMock()
    for k, v in defaults.items():
        setattr(config, k, v)

    monitor = IntradayMonitor(
        config=config,
        fetcher_manager=MagicMock(),
        db_manager=MagicMock(),
        email_sender=None,
        llm_analyzer=None,
    )
    return monitor


def _empty_params(codes: list[str]):
    """Minimal parameter dict for decision generation methods."""
    return dict(
        events=[],
        yesterday_analysis={c: {} for c in codes},
        valid_quote_codes=codes,
        merged_times=[],
        markets_set=set(),
        stock_timelines={c: {"stock_name": c, "points": []} for c in codes},
        market_timelines={},
        market_timeline_stats={},
        historical_snapshot_count=0,
        relative_strength_summary=None,
    )


# ---------------------------------------------------------------------------
# Test 1: Single batch with finish_reason="stop" + all codes covered → complete=True
# ---------------------------------------------------------------------------


def test_single_batch_stop_all_covered_complete():
    codes = ["600519", "000001", "hk00700"]
    monitor = _make_monitor()
    params = _empty_params(codes)

    mock_result = _make_llm_result(codes, finish_reason="stop")

    with patch.object(monitor, "_generate_single_batch_llm", return_value=mock_result):
        result = monitor._generate_single_batch_decision(**params)

    assert isinstance(result, IntradayReportResult)
    assert result.complete is True
    assert result.error_type is None
    assert set(result.covered_codes) == set(codes)
    assert result.missing_codes == []
    assert result.finish_reasons == ["stop"]


# ---------------------------------------------------------------------------
# Test 2: finish_reason="length" → complete=False, error_type="truncated_response"
# ---------------------------------------------------------------------------


def test_single_batch_truncated_length():
    codes = ["600519"]
    monitor = _make_monitor()
    params = _empty_params(codes)

    mock_result = _make_llm_result(codes, finish_reason="length")

    with patch.object(monitor, "_generate_single_batch_llm", return_value=mock_result):
        result = monitor._generate_single_batch_decision(**params)

    assert result.complete is False
    assert result.error_type == "truncated_response"


def test_single_batch_truncated_max_tokens():
    codes = ["600519"]
    monitor = _make_monitor()
    params = _empty_params(codes)

    mock_result = _make_llm_result(codes, finish_reason="max_tokens")

    with patch.object(monitor, "_generate_single_batch_llm", return_value=mock_result):
        result = monitor._generate_single_batch_decision(**params)

    assert result.complete is False
    assert result.error_type == "truncated_response"


# ---------------------------------------------------------------------------
# Test 3: Multi-batch: 10 stocks, batch_size=6, two batches both complete → complete=True
# ---------------------------------------------------------------------------


def test_multi_batch_two_batches_both_complete():
    codes = [f"stock{i:03d}" for i in range(10)]  # 10 stocks
    batch1 = codes[:6]
    batch2 = codes[6:]
    monitor = _make_monitor(intraday_llm_batch_size=6)
    params = _empty_params(codes)

    mock1 = _make_llm_result(batch1, finish_reason="stop")
    mock2 = _make_llm_result(batch2, finish_reason="stop")

    with patch.object(monitor, "_generate_single_batch_llm", side_effect=[mock1, mock2]):
        result = monitor._generate_batched_decision(**params)

    assert result.complete is True
    assert set(result.covered_codes) == set(codes)
    assert result.batch_count == 2


# ---------------------------------------------------------------------------
# Test 4: First batch missing a stock, second batch has it → compensation fills gap
# ---------------------------------------------------------------------------


def test_first_batch_missing_compensation_fills():
    codes = ["600519", "000001", "hk00700", "AAPL"]
    monitor = _make_monitor(intraday_llm_batch_size=2, intraday_llm_compensation_max_rounds=2)
    params = _empty_params(codes)

    # Batch 1: 600519, 000001  — missing 000001
    mock1 = _make_llm_result(["600519"], finish_reason="stop")
    # Batch 2: hk00700, AAPL  — both covered
    mock2 = _make_llm_result(["hk00700", "AAPL"], finish_reason="stop")
    # Compensation round 1: 000001 covered
    mock_comp = _make_llm_result(["000001"], finish_reason="stop")

    with patch.object(monitor, "_generate_single_batch_llm", side_effect=[mock1, mock2, mock_comp]):
        result = monitor._generate_batched_decision(**params)

    assert result.complete is True
    assert set(result.covered_codes) == set(codes)
    assert result.batch_count == 3  # 2 regular + 1 compensation


# ---------------------------------------------------------------------------
# Test 5: Compensation retry: first comp still missing → second comp round triggered
# ---------------------------------------------------------------------------


def test_compensation_retry_second_round_succeeds():
    codes = ["600519", "000001", "hk00700"]
    monitor = _make_monitor(intraday_llm_batch_size=2, intraday_llm_compensation_max_rounds=2)
    params = _empty_params(codes)

    # Batch 1: 600519, 000001 — both covered
    mock1 = _make_llm_result(["600519", "000001"], finish_reason="stop")
    # Batch 2: hk00700 — MISSING from response (no block)
    mock2 = _make_llm_result([], finish_reason="stop")  # content has BATCH_END but no stock blocks
    # Comp round 1: still can't cover hk00700
    mock_comp1 = _make_llm_result([], finish_reason="stop")
    # Comp round 2: finally covers hk00700
    mock_comp2 = _make_llm_result(["hk00700"], finish_reason="stop")

    with patch.object(monitor, "_generate_single_batch_llm", side_effect=[mock1, mock2, mock_comp1, mock_comp2]):
        result = monitor._generate_batched_decision(**params)

    assert result.complete is True
    assert "hk00700" in result.covered_codes


# ---------------------------------------------------------------------------
# Test 6: Compensation exhausted: max_rounds=2 still missing → complete=False
# ---------------------------------------------------------------------------


def test_compensation_exhausted_still_missing():
    codes = ["600519", "000001", "hk00700"]
    monitor = _make_monitor(intraday_llm_batch_size=1, intraday_llm_compensation_max_rounds=2)
    params = _empty_params(codes)

    # Batch 1: covers 600519
    mock1 = _make_llm_result(["600519"], finish_reason="stop")
    # Batch 2: empty for 000001
    mock2 = _make_llm_result([], finish_reason="stop")
    # Batch 3: empty for hk00700
    mock3 = _make_llm_result([], finish_reason="stop")
    # Compensation round 1: empty (still missing 000001, hk00700)
    mock_comp1 = _make_llm_result([], finish_reason="stop")
    # Compensation round 2: empty (still missing)
    mock_comp2 = _make_llm_result([], finish_reason="stop")

    with patch.object(
        monitor, "_generate_single_batch_llm",
        side_effect=[mock1, mock2, mock3, mock_comp1, mock_comp2],
    ):
        result = monitor._generate_batched_decision(**params)

    assert result.complete is False
    assert len(result.missing_codes) > 0


# ---------------------------------------------------------------------------
# Additional: compensation aborts early when truncated
# ---------------------------------------------------------------------------


def test_compensation_aborts_on_truncated():
    codes = ["600519", "000001", "hk00700"]
    monitor = _make_monitor(intraday_llm_batch_size=1, intraday_llm_compensation_max_rounds=2)
    params = _empty_params(codes)

    mock1 = _make_llm_result(["600519"], finish_reason="stop")
    mock2 = _make_llm_result([], finish_reason="stop")  # 000001 missing
    mock3 = _make_llm_result([], finish_reason="stop")  # hk00700 missing
    # Compensation round 1 is truncated → aborts, no round 2
    mock_comp1 = _make_llm_result([], finish_reason="length")

    with patch.object(
        monitor, "_generate_single_batch_llm",
        side_effect=[mock1, mock2, mock3, mock_comp1],
    ):
        result = monitor._generate_batched_decision(**params)

    # Should abort after truncated compensation
    assert result.complete is False
    assert any(fr in ("length", "max_tokens") for fr in result.finish_reasons)


# ---------------------------------------------------------------------------
# Additional: single batch with missing codes
# ---------------------------------------------------------------------------


def test_single_batch_missing_codes():
    codes = ["600519", "000001", "hk00700"]
    monitor = _make_monitor()
    params = _empty_params(codes)

    # Only returns block for one code
    mock_result = _make_llm_result(["600519"], finish_reason="stop")

    with patch.object(monitor, "_generate_single_batch_llm", return_value=mock_result):
        result = monitor._generate_single_batch_decision(**params)

    assert result.complete is False
    assert result.error_type == "missing_stocks"
    assert "000001" in result.missing_codes
    assert "hk00700" in result.missing_codes


# ---------------------------------------------------------------------------
# Additional: empty LLM response
# ---------------------------------------------------------------------------


def test_single_batch_empty_response():
    codes = ["600519"]
    monitor = _make_monitor()
    params = _empty_params(codes)

    mock_result = LLMResult(status="empty_response", content="", finish_reason=None)
    with patch.object(monitor, "_generate_single_batch_llm", return_value=mock_result):
        result = monitor._generate_single_batch_decision(**params)

    assert result.complete is False
    # Content goes through _merge_report_batches which adds REPORT_END_MARKER
    assert not result.content.strip() or result.content.strip() == "<!-- INTRADAY_REPORT_COMPLETE -->"
    assert len(result.covered_codes) == 0
