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

    # --- Strict state-machine fields ---
    unclosed_codes: List[str] = field(default_factory=list)      # BEGIN without matching END
    orphan_end_codes: List[str] = field(default_factory=list)    # END without preceding BEGIN
    mismatched_pairs: List[Dict[str, str]] = field(default_factory=list)  # BEGIN code != END code
    empty_codes: List[str] = field(default_factory=list)         # block body empty after strip
    nested_codes: List[str] = field(default_factory=list)        # BEGIN inside another block

    @property
    def is_clean(self) -> bool:
        """True when no structural or content anomalies were found in the parse."""
        return not (
            self.unclosed_codes
            or self.orphan_end_codes
            or self.mismatched_pairs
            or self.empty_codes
            or self.nested_codes
            or self.malformed_blocks
            or self.duplicate_codes
            or self.unknown_codes
        )


# ---------------------------------------------------------------------------
# State machine parser
# ---------------------------------------------------------------------------

_BEGIN_RE = re.compile(r'<!--\s*STOCK_BEGIN\s*:\s*(\S+)\s*-->')
_END_RE = re.compile(r'<!--\s*STOCK_END\s*:\s*(\S+)\s*-->')

# Combined marker pattern for scanning the document sequentially
_MARKER_RE = re.compile(
    r'<!--\s*(STOCK_BEGIN|STOCK_END)\s*:\s*(\S*)\s*-->'
)


def parse_stock_blocks(content: str, expected_codes: Optional[List[str]] = None) -> StockBlockParseResult:
    """Parse LLM report content for structured stock analysis blocks using a state machine.

    Expected format: <!-- STOCK_BEGIN:CODE --> ... analysis ... <!-- STOCK_END:CODE -->

    The state machine enforces:
    - BEGIN/END code consistency (mismatch → mismatched_pairs)
    - No empty blocks (empty body after strip → empty_codes)
    - No nested blocks (BEGIN inside another block → nested_codes)
    - No orphan BEGIN (unclosed → unclosed_codes)
    - No orphan END (END outside a block → orphan_end_codes)

    When expected_codes is None or empty, all blocks are parsed but no
    missing/duplicate/unknown validation is performed. This is useful for
    email splitting where the caller just needs stock-block boundaries.
    """
    result = StockBlockParseResult()

    if not content:
        return result

    expected_codes = list(expected_codes) if expected_codes else []
    do_validate = bool(expected_codes)

    # Build normalized lookup sets (only when validating)
    expected_set: set = set()
    rev_map: Dict[str, str] = {}
    if do_validate:
        expected_set = {normalize_stock_code_key(c) for c in expected_codes if normalize_stock_code_key(c)}
        rev_map = {normalize_stock_code_key(c): c for c in expected_codes if normalize_stock_code_key(c)}

    # --- State machine scan ---
    # state: "outside" | name of current block's BEGIN code (str)
    state = "outside"
    current_begin_pos = -1  # position in content where current block's BEGIN starts

    # Temporary accumulators
    code_counts: Dict[str, int] = {}

    for m in _MARKER_RE.finditer(content):
        marker_type = m.group(1)       # "STOCK_BEGIN" or "STOCK_END"
        raw_code = m.group(2).strip()  # the code inside
        marker_pos = m.start()

        if marker_type == "STOCK_BEGIN":
            if state != "outside":
                # Nested BEGIN — record the outer code as having a nested block
                outer_norm = normalize_stock_code_key(state)
                if outer_norm and outer_norm not in result.nested_codes:
                    result.nested_codes.append(rev_map.get(outer_norm, state) if do_validate else state)
                result.malformed_blocks.append({
                    "raw_code": raw_code,
                    "reason": "nested STOCK_BEGIN while inside block",
                    "outer_code": state,
                })
                # Remain in current block (don't switch state) — this BEGIN is malformed
                continue

            norm = normalize_stock_code_key(raw_code)
            if not norm:
                result.malformed_blocks.append({
                    "raw_code": raw_code,
                    "reason": "unrecognized code format",
                })
                # Do NOT enter a block for an unrecognized code — skip this BEGIN
                # and let its content be absorbed as preamble/trailing.
                continue

            state = norm
            current_begin_pos = marker_pos
            code_counts[norm] = code_counts.get(norm, 0) + 1

        elif marker_type == "STOCK_END":
            if state == "outside":
                # Orphan END
                norm = normalize_stock_code_key(raw_code)
                if norm:
                    result.orphan_end_codes.append(rev_map.get(norm, norm) if do_validate else norm)
                else:
                    result.orphan_end_codes.append(raw_code)
                result.malformed_blocks.append({
                    "raw_code": raw_code,
                    "reason": "orphan STOCK_END without preceding BEGIN",
                })
                continue

            # We are inside a block — extract the body
            body = content[current_begin_pos + len(m.group(0)) if current_begin_pos >= 0 else 0:marker_pos]
            # Wait — that's wrong. Let me reconsider. the body is between the BEGIN and the END.
            # Actually, let me extract the body properly.

            # Find the BEGIN marker that opened this block
            begin_marker_end = current_begin_pos
            # Re-scan to find the BEGIN's end position
            begin_match = _BEGIN_RE.search(content, current_begin_pos)
            if begin_match:
                begin_marker_end = begin_match.end()
            body = content[begin_marker_end:marker_pos].strip()

            begin_norm = state
            end_norm = normalize_stock_code_key(raw_code)

            # Check BEGIN/END code consistency
            if end_norm and begin_norm != end_norm:
                mismatch_entry = {
                    "begin_code": rev_map.get(begin_norm, begin_norm) if do_validate else begin_norm,
                    "end_code": rev_map.get(end_norm, raw_code) if do_validate else raw_code,
                }
                result.mismatched_pairs.append(mismatch_entry)
                result.malformed_blocks.append({
                    "begin_code": begin_norm,
                    "end_code": raw_code,
                    "reason": "BEGIN/END code mismatch",
                })
                # Mark both codes as potentially affected
                if do_validate:
                    if begin_norm not in code_counts:
                        code_counts[begin_norm] = 0
                    code_counts[begin_norm] = max(0, code_counts.get(begin_norm, 1) - 1)
                    # The END code gets a phantom count that will be caught later
                state = "outside"
                continue

            # Check empty block
            if not body:
                display_code = rev_map.get(begin_norm, begin_norm) if do_validate else begin_norm
                result.empty_codes.append(display_code)
                result.malformed_blocks.append({
                    "raw_code": display_code,
                    "reason": "empty block body",
                })
                state = "outside"
                continue

            # Valid block — store it (first occurrence only)
            display_code = rev_map.get(begin_norm, begin_norm) if do_validate else begin_norm
            if begin_norm not in result.blocks_by_code:
                result.blocks_by_code[begin_norm] = body

            state = "outside"

    # After scanning all markers: check for unclosed blocks
    if state != "outside":
        unclosed_display = rev_map.get(state, state) if do_validate else state
        result.unclosed_codes.append(unclosed_display)
        result.malformed_blocks.append({
            "raw_code": unclosed_display,
            "reason": "unclosed STOCK_BEGIN without matching END",
        })

    # --- Preamble and trailing ---
    # Preamble: everything before first BEGIN
    first_begin = _BEGIN_RE.search(content)
    if first_begin:
        result.preamble = content[:first_begin.start()].strip()
    else:
        result.preamble = content.strip()

    # Trailing: everything after last END
    all_ends = list(_END_RE.finditer(content))
    if all_ends:
        result.trailing_content = content[all_ends[-1].end():].strip()

    # --- Classify codes (only when validating) ---
    if do_validate:
        # Identify codes that appeared but are not in expected set
        for norm_code in code_counts:
            if norm_code not in expected_set:
                result.unknown_codes.append(rev_map.get(norm_code, norm_code))

        # Check each expected code
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

    return result
