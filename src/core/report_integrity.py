# -*- coding: utf-8 -*-
"""Report integrity validation utilities — block parsing, stock coverage checks.

These are shared between intraday_monitor (validation) and email_sender (splitting).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.utils.stock_code import normalize_stock_code_key


@dataclass
class StockBlockParseResult:
    """Result of parsing structured stock analysis blocks from LLM output.

    Blocks are delimited by <!-- STOCK_BEGIN:CODE --> ... <!-- STOCK_END:CODE -->.
    """
    blocks_by_code: Dict[str, str] = field(default_factory=dict)
    missing_codes: List[str] = field(default_factory=list)
    duplicate_codes: List[str] = field(default_factory=list)
    unknown_codes: List[str] = field(default_factory=list)
    malformed_blocks: List[Dict[str, Any]] = field(default_factory=list)
    preamble: str = ""
    trailing_content: str = ""


def parse_stock_blocks(content: str, expected_codes: Optional[List[str]] = None) -> StockBlockParseResult:
    """Parse LLM report content for structured stock analysis blocks.

    Expected format: <!-- STOCK_BEGIN:CODE --> ... analysis ... <!-- STOCK_END:CODE -->

    This replaces substring-based stock-code matching, which produces false
    positives when stock codes appear in the prompt/instruction text rather
    than in actual analysis blocks.

    When expected_codes is None or empty, all blocks are parsed but no
    missing/duplicate/unknown validation is performed. This is useful for
    email splitting where the caller just needs stock-block boundaries.
    """
    result = StockBlockParseResult()

    if not content:
        return result

    if expected_codes is None:
        expected_codes = []

    # Skip validation if no codes to validate against
    do_validate = bool(expected_codes)

    # Build a set of normalized expected codes for fast lookup
    expected_set = {normalize_stock_code_key(c) for c in expected_codes if normalize_stock_code_key(c)}
    # Map normalized -> original for output
    rev_map = {normalize_stock_code_key(c): c for c in expected_codes if normalize_stock_code_key(c)}

    block_pattern = re.compile(
        r'<!--\s*STOCK_BEGIN\s*:\s*(\S*)\s*-->(.*?)<!--\s*STOCK_END\s*:\s*\S*\s*-->',
        re.DOTALL,
    )

    matches = list(block_pattern.finditer(content))

    if not matches:
        result.preamble = content.strip()
        if do_validate:
            result.missing_codes = list(expected_codes)
        return result

    # Preamble: everything before the first STOCK_BEGIN
    first_match_start = matches[0].start()
    result.preamble = content[:first_match_start].strip()

    # Trailing content: everything after the last STOCK_END
    last_match_end = matches[-1].end()
    result.trailing_content = content[last_match_end:].strip()

    # Track codes seen (Counter for duplicate detection)
    code_counts: Dict[str, int] = {}
    for m in matches:
        raw_code = m.group(1).strip()
        block_body = m.group(2).strip()

        # Normalize the parsed code
        normalized = normalize_stock_code_key(raw_code)
        if not normalized:
            result.malformed_blocks.append({
                "raw_code": raw_code,
                "reason": "unrecognized code format",
            })
            continue

        code_counts[normalized] = code_counts.get(normalized, 0) + 1

        # Keep the first block for each code (duplicates discarded)
        if normalized in result.blocks_by_code:
            continue
        result.blocks_by_code[normalized] = block_body

    # Classify each expected code (only when validating)
    if do_validate:
        for code in expected_codes:
            norm = normalize_stock_code_key(code)
            if not norm:
                result.missing_codes.append(code)
                continue
            count = code_counts.get(norm, 0)
            if count == 0:
                result.missing_codes.append(code)
            elif count > 1:
                result.duplicate_codes.append(code)

        # Detect codes in blocks that aren't in expected set
        for norm_code in code_counts:
            if norm_code not in expected_set:
                original = rev_map.get(norm_code, norm_code)
                result.unknown_codes.append(original)

    return result
