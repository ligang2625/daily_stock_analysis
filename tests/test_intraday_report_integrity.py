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
    """When STOCK_BEGIN has no matching STOCK_END, the regex (non-greedy) consumes
    content up to the NEXT STOCK_END, producing a merged block that swallows the
    following STOCK_BEGIN. Both codes end up inside the block for the first code.
    The inner (swallowed) code is therefore missing from blocks_by_code.
    """
    content = (
        f"{STOCK_BEGIN.format(code='600519')}\n"
        f"茅台分析内容，但没有结束标记。\n"
        f"{STOCK_BEGIN.format(code='000001')}\n"
        f"平安分析\n"
        f"{STOCK_END.format(code='000001')}\n"
    )
    result = parse_stock_blocks(content, expected_codes=["600519", "000001"])

    # The regex matched: STOCK_BEGIN:600519 ... up to STOCK_END:000001
    # Everything — including STOCK_BEGIN:000001 — is the body of 600519's block.
    assert "600519" in result.blocks_by_code
    # 000001 was consumed inside 600519's block, so it is missing
    assert "000001" not in result.blocks_by_code
    assert "000001" in result.missing_codes


def test_malformed_block_unrecognized_code():
    """Block code that cannot be normalized ends up in malformed_blocks."""
    content = _make_content(
        _make_block("", "空的股票代码"),
    )
    result = parse_stock_blocks(content, expected_codes=["600519"])

    assert result.blocks_by_code == {}
    assert len(result.malformed_blocks) == 1
    assert result.malformed_blocks[0]["reason"] == "unrecognized code format"
    assert result.malformed_blocks[0]["raw_code"] == ""


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
