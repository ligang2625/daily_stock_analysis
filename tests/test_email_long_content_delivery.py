# -*- coding: utf-8 -*-
"""Tests for EmailSender long-content handling: split, attachment, MIME gating."""

from unittest.mock import MagicMock, patch, PropertyMock

from src.config import Config
from src.notification_sender.email_sender import EmailSender, EmailDeliveryResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

STOCK_BEGIN = "<!-- STOCK_BEGIN:{code} -->"
STOCK_END = "<!-- STOCK_END:{code} -->"


def _make_block(code: str, body: str = "") -> str:
    return f"{STOCK_BEGIN.format(code=code)}\n{body}\n{STOCK_END.format(code=code)}"


def _build_multi_block_content(codes: list[str], preamble: str = "", body_size: int = 200) -> str:
    """Build content with structured stock blocks for each code."""
    padding = "A" * body_size
    blocks = [_make_block(c, f"分析 {c}\n{padding}") for c in codes]
    if preamble:
        return preamble + "\n\n" + "\n\n".join(blocks)
    return "\n\n".join(blocks)


def _make_config(**overrides):
    """Create Config with minimal fields + overrides."""
    # Config needs at least stock_list
    defaults = dict(
        stock_list=[],
        email_sender="test@example.com",
        email_password="test-password",
        email_receivers=["to@example.com"],
        email_max_inline_bytes=0,  # disabled by default
        email_long_content_mode="auto",
        email_attach_full_report=True,
    )
    defaults.update(overrides)
    return Config(**defaults)


def _make_sender(config=None):
    """Create an EmailSender with _send_single_email mocked to return True."""
    if config is None:
        config = _make_config()
    sender = EmailSender(config)
    # Mock the low-level SMTP send to always succeed
    sender._send_single_email = MagicMock(return_value=True)
    # Mock _measure_mime_bytes to return a controlled size
    sender._measure_mime_bytes = MagicMock(return_value=50000)
    return sender


# ---------------------------------------------------------------------------
# Test 1: Short content (under max_inline) → single inline send, mode="inline"
# ---------------------------------------------------------------------------


def test_short_content_single_inline():
    content = _build_multi_block_content(["600519", "000001"])
    config = _make_config(email_max_inline_bytes=200000)
    sender = _make_sender(config)

    # Content MIME is 50000 < 200000
    result = sender.send_to_email(content, subject="测试报告")

    assert isinstance(result, EmailDeliveryResult)
    assert result.success is True
    assert result.mode == "inline"
    assert result.parts_sent == 1
    assert result.parts_failed == 0
    assert result.parts_total == 1
    sender._send_single_email.assert_called_once()


# ---------------------------------------------------------------------------
# Test 2: Long content → splits by stock blocks, mode="split"
# ---------------------------------------------------------------------------


def test_long_content_splits_by_stock_blocks():
    codes = ["600519", "000001", "hk00700"]
    content = _build_multi_block_content(codes)
    config = _make_config(email_max_inline_bytes=10000, email_long_content_mode="split")
    sender = _make_sender(config)

    # Content MIME is 50000 > 10000, but each block MIME is 50000 which > 10000 too...
    # We need blocks to be small enough individually
    sender._measure_mime_bytes = MagicMock(side_effect=[
        50000,   # test_msg full MIME → triggers split
        5000,    # block 1 MIME check
        5000,    # block 2 MIME check
        5000,    # block 3 MIME check
        10000,   # final full attachment MIME check
    ])

    result = sender.send_to_email(content, subject="测试报告", report_id="test123")

    assert result.mode == "split"
    assert result.success is True
    # 3 blocks + 1 final attachment = 4 parts (attach_full=True)
    assert result.parts_total == 4
    assert result.parts_sent == 4


def test_long_content_split_without_final_attachment():
    codes = ["600519", "000001"]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="split",
        email_attach_full_report=False,
    )
    sender = _make_sender(config)

    sender._measure_mime_bytes = MagicMock(side_effect=[
        50000,   # test_msg → triggers split
        5000,    # block 1
        5000,    # block 2
    ])

    result = sender.send_to_email(content, subject="测试")

    assert result.mode == "split"
    assert result.parts_total == 2  # no attachment
    assert result.parts_sent == 2


# ---------------------------------------------------------------------------
# Test 3: Each split part MIME size < max_inline
# ---------------------------------------------------------------------------


def test_each_split_part_under_max_inline():
    codes = [f"c{i:03d}" for i in range(5)]
    content = _build_multi_block_content(codes)
    config = _make_config(email_max_inline_bytes=10000, email_attach_full_report=False)
    sender = _make_sender(config)

    sizes = [50000] + [3000] * 5  # full test_msg then 5 blocks
    sender._measure_mime_bytes = MagicMock(side_effect=sizes)

    result = sender.send_to_email(content, subject="测试")

    assert result.mode == "split"
    assert result.success is True
    assert result.parts_failed == 0
    # All 5 calls to _send_single_email should succeed
    assert sender._send_single_email.call_count == 5


# ---------------------------------------------------------------------------
# Test 4: Single block exceeding max_inline → skipped (not sent oversized)
# ---------------------------------------------------------------------------


def test_single_block_exceeds_max_inline_skipped():
    codes = ["600519", "000001"]
    content = _build_multi_block_content(codes)
    config = _make_config(email_max_inline_bytes=10000, email_attach_full_report=False)
    sender = _make_sender(config)

    # Block 1 MIME is 20000 > 10000 → skipped
    # Block 2 MIME is 5000 → sent
    sender._measure_mime_bytes = MagicMock(side_effect=[
        50000,   # test_msg full
        20000,   # block 1 → too large, skipped
        5000,    # block 2 → OK
    ])

    result = sender.send_to_email(content, subject="测试")

    assert result.mode == "split"
    assert result.success is False
    assert result.parts_failed == 1
    assert result.parts_sent == 1
    assert 0 in result.failed_part_indices  # block index 0 failed
    assert sender._send_single_email.call_count == 1  # only block 2 sent


# ---------------------------------------------------------------------------
# Test 5: Attachment mode → no full HTML in body, only short index
# ---------------------------------------------------------------------------


def test_attachment_mode_short_index_body():
    content = _build_multi_block_content(["600519"])
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="attachment",
    )
    sender = _make_sender(config)

    sender._measure_mime_bytes = MagicMock(side_effect=[
        50000,   # test_msg full → triggers content check
        8000,    # attachment MIME check → under 10000
    ])

    result = sender.send_to_email(content, subject="测试报告", report_id="rpt")

    assert result.mode == "attachment"
    assert result.success is True
    # Verify the sent message is an attachment-style message.
    # The MIME attachment body has text/plain + text/html parts inside a
    # multipart/alternative. The payload walk confirms the structure.
    call_args = sender._send_single_email.call_args
    sent_msg = call_args[0][0]
    # Walk parts to find the text/plain body
    plain_parts = []
    for part in sent_msg.walk():
        if part.get_content_type() == "text/plain":
            plain_parts.append(part.get_payload(decode=True).decode("utf-8"))
    assert any("完整报告见附件" in p for p in plain_parts), f"plain parts: {plain_parts}"


# ---------------------------------------------------------------------------
# Test 6: Attachment MIME size > max_inline → blocked (returns False)
# ---------------------------------------------------------------------------


def test_attachment_mime_exceeds_max_inline_blocked():
    content = _build_multi_block_content(["600519"])
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="attachment",
    )
    sender = _make_sender(config)

    sender._measure_mime_bytes = MagicMock(side_effect=[
        50000,   # test_msg full → triggers content check
        20000,   # attachment MIME → exceeds 10000, blocked
    ])

    result = sender.send_to_email(content, subject="测试")

    assert result.mode == "attachment"
    assert result.success is False
    assert result.parts_sent == 0
    assert result.parts_failed == 1
    # _send_single_email should NOT be called
    sender._send_single_email.assert_not_called()


# ---------------------------------------------------------------------------
# Test 7: Split + final attachment: attach_full=True → sends [i/N] parts + final full report
# ---------------------------------------------------------------------------


def test_split_with_final_attachment():
    codes = ["600519", "000001", "hk00700"]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="split",
        email_attach_full_report=True,
    )
    sender = _make_sender(config)

    sender._measure_mime_bytes = MagicMock(side_effect=[
        50000,   # test_msg full
        4000,    # block 1
        4000,    # block 2
        4000,    # block 3
        8000,    # final attachment MIME
    ])

    result = sender.send_to_email(content, subject="[测试] 报告", report_id="rpt12345")

    assert result.mode == "split"
    assert result.success is True
    assert result.parts_total == 4  # 3 blocks + 1 full attachment
    assert result.parts_sent == 4
    assert result.parts_failed == 0

    # Verify subject format on block emails (contains [i/N])
    subjects = []
    for call_args in sender._send_single_email.call_args_list:
        msg = call_args[0][0]
        subjects.append(str(msg["Subject"]))
    any_part_subject = any("[1/" in s for s in subjects)
    assert any_part_subject, f"Expected [i/N] subject in split parts, got: {subjects}"


# ---------------------------------------------------------------------------
# Test 8: Mixed success: one part fails → result shows which part, success=False
# ---------------------------------------------------------------------------


def test_mixed_success_with_failed_part():
    codes = ["600519", "000001", "hk00700"]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="split",
        email_attach_full_report=False,
    )
    sender = _make_sender(config)

    sender._measure_mime_bytes = MagicMock(side_effect=[
        50000,   # test_msg full
        4000,    # block 1
        4000,    # block 2
        4000,    # block 3
    ])

    # Block 2 (index 1) fails
    sender._send_single_email = MagicMock(side_effect=[True, False, True])

    result = sender.send_to_email(content, subject="测试")

    assert result.mode == "split"
    assert result.success is False
    assert result.parts_total == 3
    assert result.parts_sent == 2
    assert result.parts_failed == 1
    assert result.failed_part_indices == [1]


# ---------------------------------------------------------------------------
# Additional: not configured → success=False
# ---------------------------------------------------------------------------


def test_not_configured_returns_false():
    config = _make_config(email_sender="", email_password="")
    sender = _make_sender(config)
    result = sender.send_to_email("content")
    assert result.success is False
    assert result.error_detail == "email not configured"
    sender._send_single_email.assert_not_called()


# ---------------------------------------------------------------------------
# Additional: EmailDeliveryResult dataclass defaults
# ---------------------------------------------------------------------------


def test_email_delivery_result_defaults():
    result = EmailDeliveryResult()
    assert result.success is True
    assert result.parts_sent == 0
    assert result.parts_failed == 0
    assert result.parts_total == 0
    assert result.failed_part_indices == []
    assert result.mode == ""
    assert result.error_detail is None


def test_email_delivery_result_bool_conversion():
    assert bool(EmailDeliveryResult(success=True)) is True
    assert bool(EmailDeliveryResult(success=False)) is False


# ---------------------------------------------------------------------------
# Additional: _measure_mime_bytes static method
# ---------------------------------------------------------------------------


def test_measure_mime_bytes():
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    msg = MIMEMultipart()
    msg.attach(MIMEText("hello", "plain", "utf-8"))
    size = EmailSender._measure_mime_bytes(msg)
    assert isinstance(size, int)
    assert size > 0


def test_measure_mime_bytes_graceful_fallback():
    result = EmailSender._measure_mime_bytes(MagicMock())
    assert result == 0
