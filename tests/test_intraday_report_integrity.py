# -*- coding: utf-8 -*-
"""Tests for parse_stock_blocks() from src.core.report_integrity."""

from src.core.report_integrity import parse_stock_blocks, StockBlockParseResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

STOCK_BEGIN = "<!-- STOCK_BEGIN:{code} -->"
STOCK_END = "<!-- STOCK_END:{code} -->"


def _make_block(code: str, body: str = "") -> str:
    return f"{STOCK_BEGIN.format(code=code)}\n{body}\n{STOCK_END.format(code=code)}"


def _make_content(*blocks: str, preamble: str = "", trailing: str = "") -> str:
    parts = []
    if preamble:
        parts.append(preamble)
    parts.extend(blocks)
    if trailing:
        parts.append(trailing)
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Test 1: Basic parsing — properly formatted blocks with expected codes
# ---------------------------------------------------------------------------


def test_basic_parsing():
    content = _make_content(
        _make_block("600519", "贵州茅台分析内容"),
        _make_block("hk00700", "腾讯控股分析内容"),
        preamble="# 盘中监控报告\n\n这里是引言。",
    )
    result = parse_stock_blocks(content, expected_codes=["600519", "hk00700"])

    assert result.blocks_by_code == {
        "600519": "贵州茅台分析内容",
        "hk00700": "腾讯控股分析内容",
    }
    assert result.missing_codes == []
    assert result.duplicate_codes == []
    assert result.unknown_codes == []
    assert result.malformed_blocks == []
    assert "盘中监控报告" in result.preamble
    assert result.trailing_content == ""


def test_basic_parsing_with_trailing():
    content = _make_content(
        _make_block("600519", "茅台分析"),
        trailing="---\n\n以上为盘后总结。",
    )
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert result.blocks_by_code == {"600519": "茅台分析"}
    assert result.missing_codes == []
    assert result.trailing_content == "---\n\n以上为盘后总结。"


# ---------------------------------------------------------------------------
# Test 2: No blocks found → all codes missing
# ---------------------------------------------------------------------------


def test_no_blocks_found_all_missing():
    content = "这是一段没有任何股票块的普通文本。"
    result = parse_stock_blocks(content, expected_codes=["600519", "000001"])

    assert result.blocks_by_code == {}
    assert set(result.missing_codes) == {"600519", "000001"}
    assert result.preamble == content
    assert result.trailing_content == ""


# ---------------------------------------------------------------------------
# Test 3: Stock code in "本批股票" list text only (not in block) → must be missing
# ---------------------------------------------------------------------------


def test_code_in_instruction_text_not_declared():
    """Stock code appears in surrounding text but not inside a structured block.

    Substring-based matching would produce a false positive; parse_stock_blocks
    must not count it as covered.
    """
    content = _make_content(
        _make_block("600519", "茅台分析"),
        preamble="本批股票: 600519, 000001\n\n请分析以下股票。",
    )
    result = parse_stock_blocks(content, expected_codes=["600519", "000001"])

    assert result.blocks_by_code == {"600519": "茅台分析"}
    assert result.missing_codes == ["000001"]
    # 000001 in preamble text should NOT appear in blocks_by_code
    assert "000001" not in result.blocks_by_code


# ---------------------------------------------------------------------------
# Test 4: Duplicate blocks for same stock → duplicate_codes populated
# ---------------------------------------------------------------------------


def test_duplicate_blocks():
    content = _make_content(
        _make_block("600519", "第一份茅台分析"),
        _make_block("600519", "第二份茅台分析（重复）"),
    )
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert result.blocks_by_code == {"600519": "第一份茅台分析"}
    assert result.missing_codes == []
    assert result.duplicate_codes == ["600519"]


def test_duplicate_only_appears_once():
    """Only the first block is kept; duplicate is recorded but not in blocks_by_code twice."""
    content = _make_content(
        _make_block("hk00700", "第一份"),
        _make_block("hk00700", "第二份"),
        _make_block("hk00700", "第三份"),
    )
    result = parse_stock_blocks(content, expected_codes=["hk00700"])

    assert result.blocks_by_code == {"hk00700": "第一份"}
    assert result.duplicate_codes == ["hk00700"]


# ---------------------------------------------------------------------------
# Test 5: Malformed block (no STOCK_END for a code) → detected
# ---------------------------------------------------------------------------


def test_block_without_stock_end():
    """When STOCK_BEGIN has no matching STOCK_END and is followed by another
    block's STOCK_END, the state machine detects the BEGIN/END mismatch.
    Neither block is trusted; both are recorded as malformed.
    """
    content = (
        f"{STOCK_BEGIN.format(code='600519')}\n"
        f"茅台分析内容，但没有结束标记。\n"
        f"{STOCK_BEGIN.format(code='000001')}\n"
        f"平安分析\n"
        f"{STOCK_END.format(code='000001')}\n"
    )
    result = parse_stock_blocks(content, expected_codes=["600519", "000001"])

    # State machine: BEGIN 600519 → nested BEGIN 000001 → END 000001 mismatches BEGIN 600519
    # Neither block is valid → neither appears in blocks_by_code
    assert "600519" not in result.blocks_by_code
    assert "000001" not in result.blocks_by_code
    assert result.mismatched_pairs == [{"begin_code": "600519", "end_code": "000001"}]
    assert result.nested_codes == ["600519"]
    assert "600519" in result.missing_codes
    assert "000001" in result.missing_codes
    assert not result.is_clean


def test_malformed_block_unrecognized_code():
    """Block code that cannot be normalized ends up in malformed_blocks.

    State machine: unrecognized BEGIN is skipped (not entered), the following
    END is therefore orphaned. Both produce malformed_blocks entries.
    """
    content = _make_content(
        _make_block("", "空的股票代码"),
    )
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert result.blocks_by_code == {}
    assert len(result.malformed_blocks) >= 1
    assert any(m["reason"] == "unrecognized code format" for m in result.malformed_blocks)
    assert any(m["raw_code"] == "" for m in result.malformed_blocks)
    assert not result.is_clean


# ---------------------------------------------------------------------------
# Test 6: Unknown code in block (not in expected) → unknown_codes populated
# ---------------------------------------------------------------------------


def test_unknown_code_in_block():
    content = _make_content(
        _make_block("600519", "茅台分析"),
        _make_block("AAPL", "苹果公司分析"),
    )
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert result.blocks_by_code == {"600519": "茅台分析", "AAPL": "苹果公司分析"}
    assert result.missing_codes == []
    assert result.unknown_codes == ["AAPL"]


def test_unknown_codes_with_duplicates_and_known():
    content = _make_content(
        _make_block("600519", "茅台"),
        _make_block("UNKNOWN1", "未知一"),
        _make_block("UNKNOWN2", "未知二"),
    )
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert result.blocks_by_code == {
        "600519": "茅台",
        "UNKNOWN1": "未知一",
        "UNKNOWN2": "未知二",
    }
    assert set(result.unknown_codes) == {"UNKNOWN1", "UNKNOWN2"}


# ---------------------------------------------------------------------------
# Test 7: Empty content returns empty result
# ---------------------------------------------------------------------------


def test_empty_content():
    result = parse_stock_blocks("", expected_codes=["600519"])
    assert result.blocks_by_code == {}
    assert result.missing_codes == []
    assert result.duplicate_codes == []
    assert result.unknown_codes == []
    assert result.malformed_blocks == []
    assert result.preamble == ""

    result_none = parse_stock_blocks("")
    assert result_none.blocks_by_code == {}


# ---------------------------------------------------------------------------
# Test 8: expected_codes=None parses all blocks without validation
# ---------------------------------------------------------------------------


def test_no_expected_codes_parses_all_blocks():
    content = _make_content(
        _make_block("600519", "茅台分析"),
        _make_block("hk00700", "腾讯分析"),
        _make_block("AAPL", "苹果分析"),
    )
    result = parse_stock_blocks(content, expected_codes=None)

    assert result.blocks_by_code == {
        "600519": "茅台分析",
        "hk00700": "腾讯分析",
        "AAPL": "苹果分析",
    }
    assert result.missing_codes == []
    assert result.duplicate_codes == []
    assert result.unknown_codes == []
    # Validation is skipped when expected_codes=None
    assert result.malformed_blocks == []


def test_none_expected_does_not_flag_duplicates():
    """When expected_codes=None, duplicates are NOT detected because validation is off."""
    content = _make_content(
        _make_block("600519", "第一份"),
        _make_block("600519", "第二份"),
    )
    result = parse_stock_blocks(content, expected_codes=None)

    assert result.blocks_by_code == {"600519": "第一份"}
    assert result.duplicate_codes == []  # validation skipped


def test_empty_expected_list_behaves_like_none():
    content = _make_content(
        _make_block("600519", "茅台"),
        _make_block("hk00700", "腾讯"),
    )
    result = parse_stock_blocks(content, expected_codes=[])

    assert len(result.blocks_by_code) == 2
    assert result.missing_codes == []
    assert result.duplicate_codes == []
    assert result.unknown_codes == []


# ---------------------------------------------------------------------------
# Additional edge case: normalized code matching (HK prefix)
# ---------------------------------------------------------------------------


def test_hk_code_normalization_in_blocks():
    """HK codes are normalized: HK00700 → hk00700."""
    content = _make_content(
        _make_block("HK00700", "腾讯控股"),
    )
    result = parse_stock_blocks(content, expected_codes=["hk00700"])

    assert "hk00700" in result.blocks_by_code
    assert result.missing_codes == []


# ---------------------------------------------------------------------------
# Additional: StockBlockParseResult dataclass defaults
# ---------------------------------------------------------------------------


def test_stock_block_parse_result_defaults():
    result = StockBlockParseResult()
    assert result.blocks_by_code == {}
    assert result.missing_codes == []
    assert result.duplicate_codes == []
    assert result.unknown_codes == []
    assert result.malformed_blocks == []
    assert result.preamble == ""
    assert result.trailing_content == ""
    assert result.unclosed_codes == []
    assert result.orphan_end_codes == []
    assert result.mismatched_pairs == []
    assert result.empty_codes == []
    assert result.nested_codes == []
    assert result.is_clean is True


# ---------------------------------------------------------------------------
# State machine tests — strict validation
# ---------------------------------------------------------------------------


def test_begin_end_code_mismatch():
    """BEGIN code != END code → mismatched_pairs populated, complete → False."""
    content = (
        "<!-- STOCK_BEGIN:600519 -->\n"
        "茅台分析内容\n"
        "<!-- STOCK_END:000001 -->\n"
    )
    result = parse_stock_blocks(content, expected_codes=["600519", "000001"])

    assert result.blocks_by_code == {}
    assert result.mismatched_pairs == [{"begin_code": "600519", "end_code": "000001"}]
    assert not result.is_clean
    assert "600519" in result.missing_codes
    assert "000001" in result.missing_codes


def test_empty_block_body_detected():
    """Block with whitespace-only body → empty_codes populated, not in blocks_by_code.

    The code is counted as seen (code_counts=1) so it's NOT in missing_codes.
    But it's NOT in blocks_by_code (body was empty). Validation layer checks
    structural_anomalies to mark the report incomplete.
    """
    content = (
        "<!-- STOCK_BEGIN:600519 -->\n"
        "   \n"
        "<!-- STOCK_END:600519 -->\n"
    )
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert result.blocks_by_code == {}
    assert result.empty_codes == ["600519"]
    assert not result.is_clean


def test_orphan_begin_unclosed():
    """STOCK_BEGIN without matching STOCK_END → unclosed_codes, not in blocks_by_code."""
    content = "<!-- STOCK_BEGIN:600519 -->\n茅台分析内容\n"
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert result.unclosed_codes == ["600519"]
    assert result.blocks_by_code == {}
    assert not result.is_clean


def test_orphan_end_no_begin():
    """STOCK_END without preceding STOCK_BEGIN → orphan_end_codes."""
    content = "<!-- STOCK_END:600519 -->\n"
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert result.orphan_end_codes == ["600519"]
    assert result.blocks_by_code == {}
    assert not result.is_clean


def test_nested_blocks_detected():
    """STOCK_BEGIN inside another block → nested_codes populated."""
    content = (
        "<!-- STOCK_BEGIN:600519 -->\n"
        "茅台分析\n"
        "<!-- STOCK_BEGIN:000001 -->\n"
        "嵌套的平安分析\n"
        "<!-- STOCK_END:000001 -->\n"
        "<!-- STOCK_END:600519 -->\n"
    )
    result = parse_stock_blocks(content, expected_codes=["600519", "000001"])

    # Outer block (600519) absorbs the inner BEGIN as part of its body mismatched with END
    assert not result.is_clean
    assert len(result.nested_codes) >= 1
    assert len(result.mismatched_pairs) >= 1


def test_duplicate_blocks_with_state_machine():
    """Duplicate blocks for the same code are detected."""
    content = (
        "<!-- STOCK_BEGIN:600519 -->\n第一份\n<!-- STOCK_END:600519 -->\n"
        "<!-- STOCK_BEGIN:600519 -->\n第二份\n<!-- STOCK_END:600519 -->\n"
    )
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert result.duplicate_codes == ["600519"]
    assert len(result.blocks_by_code) == 1  # only first kept
    assert not result.is_clean


def test_unknown_code_with_state_machine():
    """Code not in expected list → unknown_codes."""
    content = (
        "<!-- STOCK_BEGIN:600519 -->\n茅台\n<!-- STOCK_END:600519 -->\n"
        "<!-- STOCK_BEGIN:XYZ999 -->\n未知\n<!-- STOCK_END:XYZ999 -->\n"
    )
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert "XYZ999" in result.unknown_codes
    assert not result.is_clean


def test_is_clean_property_all_clear():
    """is_clean is True when no anomalies detected."""
    content = (
        "<!-- STOCK_BEGIN:600519 -->\n茅台分析\n<!-- STOCK_END:600519 -->\n"
        "<!-- STOCK_BEGIN:000001 -->\n平安分析\n<!-- STOCK_END:000001 -->\n"
    )
    result = parse_stock_blocks(content, expected_codes=["600519", "000001"])

    assert result.is_clean is True
    assert result.unclosed_codes == []
    assert result.orphan_end_codes == []
    assert result.mismatched_pairs == []
    assert result.empty_codes == []
    assert result.nested_codes == []
