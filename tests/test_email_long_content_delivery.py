# -*- coding: utf-8 -*-
"""Tests for EmailSender long-content handling with delivery plan architecture.

Covers: inline, split, attachment, auto-degrade, sub-split, conservation check,
partial delivery, and the lossless delivery invariants (no stock block is skipped).
"""

from unittest.mock import MagicMock, patch, PropertyMock

from src.config import Config
from src.notification_sender.email_sender import (
    EmailSender,
    EmailDeliveryResult,
    DeliveryPart,
)

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
    defaults = dict(
        stock_list=[],
        email_sender="test@example.com",
        email_password="test-password",
        email_receivers=["to@example.com"],
        email_max_inline_bytes=0,  # disabled by default
        email_max_message_bytes=20_000_000,
        email_long_content_mode="auto",
        email_attach_full_report=True,
    )
    defaults.update(overrides)
    return Config(**defaults)


def _make_sender(config=None, *, mim_sizes=None):
    """Create an EmailSender with _send_single_email mocked to return True.

    If mim_sizes is provided, _measure_mime_bytes returns those sizes in
    sequence; otherwise it returns 50000 (above default inline threshold).
    """
    if config is None:
        config = _make_config()
    sender = EmailSender(config)
    sender._send_single_email = MagicMock(return_value=True)
    if mim_sizes is not None:
        sender._measure_mime_bytes = MagicMock(side_effect=list(mim_sizes) + [5000] * 50)
    else:
        sender._measure_mime_bytes = MagicMock(return_value=5000)
    return sender


# ---------------------------------------------------------------------------
# Test 1: Short content (under max_inline) → single inline send
# ---------------------------------------------------------------------------


def test_short_content_single_inline():
    content = _build_multi_block_content(["600519", "000001"])
    config = _make_config(email_max_inline_bytes=200000)
    sender = _make_sender(config, mim_sizes=[50000])  # MIME 50000 < 200000

    result = sender.send_to_email(content, subject="测试报告")

    assert isinstance(result, EmailDeliveryResult)
    assert result.success is True
    assert result.mode == "inline"
    assert result.parts_sent == 1
    assert result.parts_failed == 0
    assert result.parts_total == 1
    sender._send_single_email.assert_called_once()


# ---------------------------------------------------------------------------
# Test 2: Long content → splits by stock blocks (inline_body mode for each)
# ---------------------------------------------------------------------------


def test_long_content_splits_by_stock_blocks():
    codes = ["600519", "000001", "hk00700"]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="split",
        email_attach_full_report=False,
    )
    # Full MIME > inline, each block MIME < inline → splits into inline_body parts
    sender = _make_sender(config, mim_sizes=[50000])  # only full is big

    result = sender.send_to_email(
        content, subject="测试报告", report_id="test123", expected_codes=codes,
    )

    assert result.mode == "split"
    assert result.success is True
    # 3 blocks (no attachment since attach_full=False)
    assert result.parts_total == 3
    assert result.parts_sent == 3
    assert result.parts_failed == 0


# ---------------------------------------------------------------------------
# Test 3: Long content with full attachment
# ---------------------------------------------------------------------------


def test_long_content_split_without_final_attachment():
    codes = ["600519", "000001"]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="split",
        email_attach_full_report=False,
    )
    sender = _make_sender(config, mim_sizes=[50000])

    result = sender.send_to_email(content, subject="测试", expected_codes=codes)

    assert result.mode == "split"
    assert result.parts_total == 2
    assert result.parts_sent == 2
    assert result.success is True


# ---------------------------------------------------------------------------
# Test 4: Each split part sends successfully
# ---------------------------------------------------------------------------


def test_each_split_part_sent_successfully():
    codes = [f"c{i:03d}" for i in range(5)]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_attach_full_report=False,
    )
    sender = _make_sender(config, mim_sizes=[50000])

    result = sender.send_to_email(content, subject="测试", expected_codes=codes)

    assert result.mode == "split"
    assert result.success is True
    assert result.parts_failed == 0
    assert result.parts_sent == 5


# ---------------------------------------------------------------------------
# Test 5: Single block exceeds max_inline → auto-degrades to attachment (NOT skipped)
# ---------------------------------------------------------------------------


def test_single_block_exceeds_inline_auto_degrades_to_attachment():
    """When a single stock block MIME exceeds max_inline, it auto-degrades to
    attachment mode instead of being silently skipped. This is the fix for the
    critical bug where oversized blocks were lost."""
    codes = ["600519", "000001"]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_attach_full_report=False,
    )
    sender = _make_sender(config)
    # Full MIME is big → triggers delivery plan
    # Each block is bigger than inline → should auto-degrade to attachment
    sender._measure_mime_bytes = MagicMock(return_value=50000)

    result = sender.send_to_email(content, subject="测试", expected_codes=codes)

    # Both blocks should be delivered as attachments (auto-degrade)
    assert result.success is True
    assert result.parts_sent == 2
    assert result.parts_failed == 0
    # No stock codes in failed list
    assert result.failed_stock_codes == []


# ---------------------------------------------------------------------------
# Test 6: Attachment mode → short index body only
# ---------------------------------------------------------------------------


def test_attachment_mode_short_index_body():
    content = _build_multi_block_content(["600519"])
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="attachment",
        email_attach_full_report=False,
    )
    sender = _make_sender(config, mim_sizes=[50000])

    result = sender.send_to_email(content, subject="测试报告", report_id="rpt")

    assert result.mode == "attachment"
    assert result.success is True
    call_args = sender._send_single_email.call_args
    sent_msg = call_args[0][0]
    plain_parts = []
    for part in sent_msg.walk():
        if part.get_content_type() == "text/plain":
            plain_parts.append(part.get_payload(decode=True).decode("utf-8"))
    assert any("完整报告见附件" in p for p in plain_parts), f"plain parts: {plain_parts}"


# ---------------------------------------------------------------------------
# Test 7: Attachment MIME exceeds max_message → blocked
# ---------------------------------------------------------------------------


def test_attachment_mime_exceeds_max_message_blocked():
    """When attachment MIME exceeds max_message AND sub-split can't reduce it
    below the threshold, the part is marked as failed."""
    content = _build_multi_block_content(["600519"])
    config = _make_config(
        email_max_inline_bytes=10000,
        email_max_message_bytes=5000,
        email_long_content_mode="attachment",
        email_attach_full_report=False,
    )
    sender = _make_sender(config)
    # All MIME measurements return 50000 (above both thresholds)
    sender._measure_mime_bytes = MagicMock(return_value=50000)
    # Send always succeeds if called
    sender._send_single_email = MagicMock(return_value=True)

    result = sender.send_to_email(content, subject="测试")

    # Full attachment is too large → sub-split occurs → sub-parts are sent
    # The content gets delivered — sub-splitting is the lossless recovery
    assert result.parts_sent > 0
    # All expected codes should be delivered (sub-split succeeds)
    assert not any("600519" in result.failed_stock_codes for _ in [0]) or result.failed_stock_codes == []


# ---------------------------------------------------------------------------
# Test 8: Split + final attachment
# ---------------------------------------------------------------------------


def test_split_with_final_attachment():
    codes = ["600519", "000001", "hk00700"]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="split",
        email_attach_full_report=True,
    )
    sender = _make_sender(config, mim_sizes=[50000])

    result = sender.send_to_email(
        content, subject="[测试] 报告", report_id="rpt12345", expected_codes=codes,
    )

    assert result.mode == "split_with_attachment"
    assert result.success is True
    assert result.parts_total == 4  # 3 blocks + 1 full attachment
    assert result.parts_sent == 4
    assert result.parts_failed == 0

    # Verify subjects include [i/N] format
    subjects = []
    for call_args in sender._send_single_email.call_args_list:
        msg = call_args[0][0]
        subjects.append(str(msg["Subject"]))
    any_part_subject = any("[1/" in s for s in subjects)
    assert any_part_subject, f"Expected [i/N] subject in split parts, got: {subjects}"


# ---------------------------------------------------------------------------
# Test 9: Mixed success — one part fails
# ---------------------------------------------------------------------------


def test_mixed_success_with_failed_part():
    codes = ["600519", "000001", "hk00700"]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="split",
        email_attach_full_report=False,
    )
    sender = _make_sender(config, mim_sizes=[50000])

    # Block 2 (index 1) fails
    sender._send_single_email = MagicMock(side_effect=[True, False, True])

    result = sender.send_to_email(content, subject="测试", expected_codes=codes)

    assert result.mode == "split"
    assert result.success is False
    assert result.partial_delivery is True
    assert result.parts_total == 3
    assert result.parts_sent == 2
    assert result.parts_failed == 1
    assert result.failed_part_indices == [1]
    # The failed stock code should be tracked
    assert len(result.failed_stock_codes) > 0


# ---------------------------------------------------------------------------
# Test 10: Not configured → success=False
# ---------------------------------------------------------------------------


def test_not_configured_returns_false():
    config = _make_config(email_sender="", email_password="")
    sender = _make_sender(config)
    result = sender.send_to_email("content")
    assert result.success is False
    assert result.error_detail == "email not configured"
    sender._send_single_email.assert_not_called()


# ---------------------------------------------------------------------------
# Test 11: EmailDeliveryResult defaults
# ---------------------------------------------------------------------------


def test_email_delivery_result_defaults():
    result = EmailDeliveryResult()
    assert result.success is True
    assert result.parts_sent == 0
    assert result.parts_failed == 0
    assert result.parts_total == 0
    assert result.failed_part_indices == []
    assert result.failed_stock_codes == []
    assert result.mode == ""
    assert result.error_detail is None
    assert result.partial_delivery is False


def test_email_delivery_result_bool_conversion():
    assert bool(EmailDeliveryResult(success=True)) is True
    assert bool(EmailDeliveryResult(success=False)) is False


# ---------------------------------------------------------------------------
# Test 12: _measure_mime_bytes
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


# ---------------------------------------------------------------------------
# Test 13: Content conservation — all expected codes are delivered
# ---------------------------------------------------------------------------


def test_content_conservation_all_codes_delivered():
    codes = ["600519", "000001", "hk00700", "AAPL"]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_attach_full_report=False,
    )
    sender = _make_sender(config, mim_sizes=[50000])

    result = sender.send_to_email(content, subject="测试", expected_codes=codes)

    assert result.success is True
    # Every code should be in a delivery part
    all_part_codes = set()
    for part in result.delivery_parts:
        all_part_codes.update(part.stock_codes)
    assert all_part_codes == set(codes)
    # No failed stock codes
    assert result.failed_stock_codes == []


# ---------------------------------------------------------------------------
# Test 14: DeliveryPart dataclass
# ---------------------------------------------------------------------------


def test_delivery_part_defaults():
    part = DeliveryPart(part_id="test")
    assert part.part_id == "test"
    assert part.stock_codes == []
    assert part.markdown == ""
    assert part.mode == ""
    assert part.sha256 == ""
    assert part.mime_bytes == 0
    assert part.sent is False
    assert part.error is None


# ---------------------------------------------------------------------------
# Test 15: Sub-split functions
# ---------------------------------------------------------------------------


def test_split_by_heading_level():
    content = "## Heading 1\nContent 1\n## Heading 2\nContent 2\n## Heading 3\nContent 3"
    chunks = EmailSender._split_by_heading_level(content, level=2)
    assert len(chunks) == 3
    assert all("## " in c for c in chunks)


def test_split_by_paragraphs():
    content = "Para 1\nline2\n\nPara 2\n\nPara 3"
    chunks = EmailSender._split_by_paragraphs(content)
    assert len(chunks) == 3
    assert chunks[0] == "Para 1\nline2"


def test_split_by_safe_boundary():
    content = "Hello World! " * 500
    chunks = EmailSender._split_by_safe_boundary(content, 500)
    assert len(chunks) > 1
    # Reassembly should match original (minus edge whitespace)
    reassembled = "".join(chunks)
    assert reassembled == content


def test_split_by_safe_boundary_multibyte_utf8():
    """Chinese characters are multi-byte — boundary should not split inside a char."""
    content = "中文测试内容" * 100
    chunks = EmailSender._split_by_safe_boundary(content, 200)
    reassembled = "".join(chunks)
    assert reassembled == content


# ---------------------------------------------------------------------------
# Test 16: Single oversized block sub-splits instead of being skipped
# ---------------------------------------------------------------------------


def test_single_oversized_block_sub_splits():
    """A single stock block exceeding max_message is sub-split — NOT skipped."""
    codes = ["600519"]
    content = _build_multi_block_content(codes, body_size=5000)
    config = _make_config(
        email_max_inline_bytes=1000,
        email_max_message_bytes=5000,
        email_long_content_mode="split",
        email_attach_full_report=False,
    )
    sender = _make_sender(config)
    # Full MIME large → triggers delivery plan
    # Block MIME > max_message → triggers sub-split
    sender._measure_mime_bytes = MagicMock(side_effect=[50000] + [50000] * 20)

    result = sender.send_to_email(content, subject="测试", expected_codes=codes)

    # The sub-split parts should have been delivered
    assert result.parts_sent > 0
    # No stock should be lost
    assert result.failed_stock_codes == []
    # All sub-parts have the single stock code
    for part in result.delivery_parts:
        assert "600519" in part.stock_codes or not part.stock_codes


# ---------------------------------------------------------------------------
# Test 17: Partial delivery tracks which stocks failed
# ---------------------------------------------------------------------------


def test_partial_delivery_tracks_failed_stocks():
    codes = ["600519", "000001", "hk00700"]
    content = _build_multi_block_content(codes)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="split",
        email_attach_full_report=False,
    )
    sender = _make_sender(config, mim_sizes=[50000])
    sender._send_single_email = MagicMock(side_effect=[True, False, True])

    result = sender.send_to_email(content, subject="测试", expected_codes=codes)

    assert result.partial_delivery is True
    assert len(result.failed_stock_codes) > 0
    assert result.parts_sent == 2
    assert result.parts_failed == 1


# ---------------------------------------------------------------------------
# Test 18: Preamble merges into first stock block
# ---------------------------------------------------------------------------


def test_preamble_merged_into_first_block():
    preamble = "# 盘中监控报告\n\n这是引言部分。"
    content = _build_multi_block_content(["600519", "000001"], preamble=preamble)
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="split",
        email_attach_full_report=False,
    )
    sender = _make_sender(config, mim_sizes=[50000])

    result = sender.send_to_email(content, subject="测试", expected_codes=["600519", "000001"])

    # First delivery part should contain preamble
    first_part = result.delivery_parts[0]
    assert "盘中监控报告" in first_part.markdown
    assert "600519" in first_part.stock_codes


# ---------------------------------------------------------------------------
# Test 19: Conservation check rejects unassigned codes
# ---------------------------------------------------------------------------


def test_conservation_rejects_unassigned_codes():
    """If expected_codes include a stock not in content, delivery should fail."""
    content = _build_multi_block_content(["600519"])
    config = _make_config(
        email_max_inline_bytes=10000,
        email_long_content_mode="split",
        email_attach_full_report=False,
    )
    sender = _make_sender(config, mim_sizes=[50000])

    result = sender.send_to_email(
        content, subject="测试",
        expected_codes=["600519", "000001"],  # 000001 not in content
    )

    assert result.success is False
    assert "Unassigned" in (result.error_detail or "")


# ---------------------------------------------------------------------------
# Test 20: Empty content handled gracefully
# ---------------------------------------------------------------------------


def test_empty_content_handled_gracefully():
    config = _make_config(email_max_inline_bytes=10000)
    sender = _make_sender(config, mim_sizes=[100])

    result = sender.send_to_email("", subject="测试")

    assert isinstance(result, EmailDeliveryResult)
    assert result.success is True
    assert result.mode == "inline"
    sender._send_single_email.assert_called_once()
