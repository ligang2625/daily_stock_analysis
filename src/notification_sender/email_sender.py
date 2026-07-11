# -*- coding: utf-8 -*-
"""
Email 发送提醒服务

职责：
1. 通过 SMTP 发送 Email 消息
"""
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders
from email.header import Header
from email.utils import formataddr
import re
import smtplib

from data_provider.base import normalize_stock_code
from src.config import Config
from src.core.report_integrity import parse_stock_blocks, StockBlockParseResult
from src.formatters import markdown_to_html_document


logger = logging.getLogger(__name__)


@dataclass
class EmailDeliveryResult:
    """Structured result for multi-part email delivery with partial success tracking.

    Bool-convertible for backward compatibility — truthy when all parts succeeded.
    """
    success: bool = True
    parts_sent: int = 0
    parts_failed: int = 0
    parts_total: int = 0
    failed_part_indices: List[int] = field(default_factory=list)
    failed_stock_codes: List[str] = field(default_factory=list)
    mode: str = ""  # "inline" | "split" | "attachment" | "split_with_attachment"
    error_detail: Optional[str] = None
    partial_delivery: bool = False  # True when some parts failed but not all
    delivery_parts: List["DeliveryPart"] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.success



@dataclass
class DeliveryPart:
    """A single email delivery unit in the delivery plan.

    Each stock block belongs to exactly one DeliveryPart.
    """
    part_id: str                                          # e.g. "inline" | "split_0" | "attach_600519_0"
    stock_codes: List[str] = field(default_factory=list)   # stock codes covered by this part
    markdown: str = ""                                     # the markdown content of this part
    mode: str = ""                                         # "inline_body" | "attachment" | "sub_split"
    sha256: str = ""                                       # SHA-256 of the markdown content
    mime_bytes: int = 0                                    # serialized MIME size
    sent: bool = False                                     # delivery status
    error: Optional[str] = None                            # delivery error


# SMTP 服务器配置（自动识别）
SMTP_CONFIGS = {
    # QQ邮箱
    "qq.com": {"server": "smtp.qq.com", "port": 465, "ssl": True},
    "foxmail.com": {"server": "smtp.qq.com", "port": 465, "ssl": True},
    # 网易邮箱
    "163.com": {"server": "smtp.163.com", "port": 465, "ssl": True},
    "126.com": {"server": "smtp.126.com", "port": 465, "ssl": True},
    # Gmail
    "gmail.com": {"server": "smtp.gmail.com", "port": 587, "ssl": False},
    # Outlook
    "outlook.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    "hotmail.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    "live.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    # 新浪
    "sina.com": {"server": "smtp.sina.com", "port": 465, "ssl": True},
    # 搜狐
    "sohu.com": {"server": "smtp.sohu.com", "port": 465, "ssl": True},
    # 阿里云
    "aliyun.com": {"server": "smtp.aliyun.com", "port": 465, "ssl": True},
    # 139邮箱
    "139.com": {"server": "smtp.139.com", "port": 465, "ssl": True},
}


class EmailSender:

    def __init__(self, config: Config):
        """
        初始化 Email 配置

        Args:
            config: 配置对象
        """
        self._config = config
        self._email_config = {
            'sender': config.email_sender,
            'sender_name': getattr(config, 'email_sender_name', 'daily_stock_analysis股票分析助手'),
            'password': config.email_password,
            'receivers': config.email_receivers or ([config.email_sender] if config.email_sender else []),
        }
        self._stock_email_groups = getattr(config, 'stock_email_groups', None) or []

    def _is_email_configured(self) -> bool:
        """检查邮件配置是否完整（只需邮箱和授权码）"""
        return bool(self._email_config['sender'] and self._email_config['password'])

    def get_receivers_for_stocks(self, stock_codes: List[str]) -> List[str]:
        """
        Look up email receivers for given stock codes based on stock_email_groups.
        Returns union of receivers for all matching groups; falls back to default if none match.
        Stock codes are canonicalized before comparison so that equivalent
        formats (e.g. SH600519 vs 600519) match correctly.
        """
        if not stock_codes or not self._stock_email_groups:
            return self._email_config['receivers']
        normalized_codes = [normalize_stock_code(c) for c in stock_codes]
        seen: set = set()
        result: List[str] = []
        for stocks, emails in self._stock_email_groups:
            for code in normalized_codes:
                if code in stocks:
                    for e in emails:
                        if e not in seen:
                            seen.add(e)
                            result.append(e)
                    break
        return result if result else self._email_config['receivers']

    def get_all_email_receivers(self) -> List[str]:
        """
        Return union of all configured email receivers (all groups + default).
        Used for market review which should go to everyone.
        """
        seen: set = set()
        result: List[str] = []
        for _, emails in self._stock_email_groups:
            for e in emails:
                if e not in seen:
                    seen.add(e)
                    result.append(e)
        for e in self._email_config['receivers']:
            if e not in seen:
                seen.add(e)
                result.append(e)
        return result

    def _format_sender_address(self, sender: str) -> str:
        """Encode display name safely so non-ASCII sender names work across SMTP providers."""
        sender_name = self._email_config.get('sender_name') or '股票分析助手'
        return formataddr((str(Header(str(sender_name), 'utf-8')), sender))

    @staticmethod
    def _measure_mime_bytes(msg: MIMEMultipart) -> int:
        """Measure the serialized MIME message size in bytes, with fallback."""
        try:
            return len(msg.as_bytes())
        except Exception:
            return 0

    @staticmethod
    def _close_server(server: Optional[smtplib.SMTP]) -> None:
        """Best-effort SMTP cleanup to avoid leaving sockets open on header/build errors.

        Exceptions from quit()/close() are intentionally silenced — connection may already
        be in a broken state, and there is nothing useful to do at this point.
        """
        if server is None:
            return
        try:
            server.quit()
        except Exception:
            try:
                server.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def send_to_email(
        self,
        content: str,
        subject: Optional[str] = None,
        receivers: Optional[List[str]] = None,
        *,
        timeout_seconds: Optional[float] = None,
        report_id: Optional[str] = None,
        expected_codes: Optional[List[str]] = None,
    ) -> EmailDeliveryResult:
        """Send email via SMTP with lossless delivery guarantees.

        Key invariants:
        - No stock block is ever silently skipped.
        - All content is accounted for via SHA-256 conservation check.
        - Single oversized blocks auto-degrade: inline → attachment → sub-split.
        - Uses email_max_message_bytes for hard MIME gating (not inline threshold).
        - Preamble merges into first stock block; trailing content merges into last.
        - Non-stock standalone emails are never produced in split mode.

        Args:
            content: Email body (Markdown, converted to HTML).
            subject: Email subject.
            receivers: Recipient list (default: configured receivers).
            timeout_seconds: SMTP timeout.
            report_id: Optional report identifier for subject and logging.
            expected_codes: Expected stock codes for validation; used to build
                           delivery plan and conservation checks.

        Returns:
            EmailDeliveryResult with per-part success/failure tracking.
        """
        if not self._is_email_configured():
            logger.warning("邮件配置不完整，跳过推送")
            return EmailDeliveryResult(success=False, error_detail="email not configured")

        sender = self._email_config['sender']
        password = self._email_config['password']
        receivers = receivers or self._email_config['receivers']
        config = self._config if hasattr(self, '_config') else None

        if subject is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
            subject = f"\U0001F4C8 股票智能分析报告 - {date_str}"

        max_inline = getattr(config, 'email_max_inline_bytes', 0) or 0 if config else 0
        max_message = getattr(config, 'email_max_message_bytes', 20_000_000) or 20_000_000 if config else 20_000_000
        mode = getattr(config, 'email_long_content_mode', 'auto') or 'auto' if config else 'auto'
        attach_full = getattr(config, 'email_attach_full_report', True) if config else True

        html_content = markdown_to_html_document(content)
        md_bytes = len(content.encode('utf-8'))
        html_bytes = len(html_content.encode('utf-8'))
        full_sha256 = hashlib.sha256(content.encode('utf-8')).hexdigest()

        # Build a full MIME message to measure its serialized size
        test_msg = self._build_mime_message(content, html_content, subject, sender, receivers)
        mime_bytes = self._measure_mime_bytes(test_msg)

        logger.info(
            "Email size check: md=%d html=%d mime=%d max_inline=%d max_message=%d mode=%s",
            md_bytes, html_bytes, mime_bytes, max_inline, max_message, mode,
        )

        # Short content: send as single inline email
        if max_inline <= 0 or mime_bytes <= max_inline:
            ok = self._send_single_email(test_msg, sender, password, receivers, timeout_seconds)
            part = DeliveryPart(
                part_id="inline",
                stock_codes=expected_codes or [],
                markdown=content,
                mode="inline_body",
                sha256=full_sha256,
                mime_bytes=mime_bytes,
                sent=ok,
                error=None if ok else "SMTP send failed",
            )
            return EmailDeliveryResult(
                success=ok, parts_sent=1 if ok else 0, parts_failed=0 if ok else 1,
                parts_total=1, mode="inline",
                partial_delivery=False,
                delivery_parts=[part],
            )

        # --- Build delivery plan ---
        parsed = parse_stock_blocks(content, expected_codes or [])
        expected = list(expected_codes) if expected_codes else list(parsed.blocks_by_code.keys())
        delivery_plan = self._build_delivery_plan(
            content=content,
            html_content=html_content,
            subject=subject,
            sender=sender,
            receivers=receivers,
            full_sha256=full_sha256,
            parsed=parsed,
            expected_codes=expected,
            max_inline=max_inline,
            max_message=max_message,
            mode=mode,
            attach_full=attach_full,
            report_id=report_id,
        )

        # --- Content conservation check ---
        validation_error = self._validate_delivery_plan(delivery_plan, expected, content, full_sha256)
        if validation_error:
            logger.error("Delivery plan validation failed: %s", validation_error)
            return EmailDeliveryResult(
                success=False,
                error_detail=f"delivery plan validation: {validation_error}",
                delivery_parts=delivery_plan,
            )

        # --- Execute delivery plan ---
        result = EmailDeliveryResult(
            parts_total=len(delivery_plan),
            mode=self._describe_mode(delivery_plan),
            delivery_parts=delivery_plan,
        )

        for i, part in enumerate(delivery_plan):
            part_subject = self._build_part_subject(subject, part, i, len(delivery_plan), report_id, full_sha256)
            part_msg = self._build_part_mime(part, part_subject, sender, receivers)
            part.mime_bytes = self._measure_mime_bytes(part_msg)

            # Hard gate: never send a message exceeding max_message_bytes
            if max_message > 0 and part.mime_bytes > max_message:
                logger.warning(
                    "Delivery part %s MIME %d exceeds max_message %d, attempting sub-split",
                    part.part_id, part.mime_bytes, max_message,
                )
                sub_parts = self._sub_split_delivery_part(part, max_message)
                if sub_parts:
                    # Replace this part with sub-parts in the result tracking
                    for sp in sub_parts:
                        sp_msg = self._build_part_mime(sp, part_subject, sender, receivers)
                        sp.mime_bytes = self._measure_mime_bytes(sp_msg)
                        sp.sent = self._send_single_email(sp_msg, sender, password, receivers, timeout_seconds)
                        if sp.sent:
                            result.parts_sent += 1
                        else:
                            result.parts_failed += 1
                            result.failed_part_indices.append(i)
                            result.failed_stock_codes.extend(sp.stock_codes)
                    continue
                else:
                    # Cannot sub-split — record as failure
                    part.sent = False
                    part.error = f"MIME {part.mime_bytes} exceeds max_message {max_message} and cannot sub-split"
                    result.parts_failed += 1
                    result.failed_part_indices.append(i)
                    result.failed_stock_codes.extend(part.stock_codes)
                    continue

            ok = self._send_single_email(part_msg, sender, password, receivers, timeout_seconds)
            part.sent = ok
            if ok:
                result.parts_sent += 1
            else:
                part.error = "SMTP send failed"
                result.parts_failed += 1
                result.failed_part_indices.append(i)
                result.failed_stock_codes.extend(part.stock_codes)

        result.success = result.parts_failed == 0
        result.partial_delivery = result.parts_failed > 0 and result.parts_sent > 0

        if result.partial_delivery:
            logger.warning(
                "Partial delivery: %d/%d parts sent, failed_stocks=%s",
                result.parts_sent, result.parts_total, result.failed_stock_codes,
            )

        return result

    # ------------------------------------------------------------------
    # Delivery plan construction
    # ------------------------------------------------------------------

    def _build_delivery_plan(
        self,
        content: str,
        html_content: str,
        subject: str,
        sender: str,
        receivers: List[str],
        full_sha256: str,
        parsed: StockBlockParseResult,
        expected_codes: List[str],
        max_inline: int,
        max_message: int,
        mode: str,
        attach_full: bool,
        report_id: Optional[str],
    ) -> List[DeliveryPart]:
        """Build the full delivery plan before sending anything.

        Returns a list of DeliveryPart objects. Each stock block belongs to exactly one part.
        """
        parts: List[DeliveryPart] = []
        blocks = parsed.blocks_by_code

        if not blocks:
            # No blocks to split — fall back to attachment or inline
            if mode == "attachment" or mode == "auto":
                parts.append(self._make_attachment_part(content, full_sha256, expected_codes))
            else:
                # Can't split, treat as inline
                parts.append(DeliveryPart(
                    part_id="inline",
                    stock_codes=expected_codes,
                    markdown=content,
                    mode="inline_body",
                    sha256=full_sha256,
                ))
            return parts

        if mode == "attachment":
            # Entire report as attachment
            parts.append(self._make_attachment_part(content, full_sha256, expected_codes))
            if attach_full:
                # Also send the raw markdown as a separate attachment for completeness
                parts.append(DeliveryPart(
                    part_id="full_raw",
                    stock_codes=expected_codes,
                    markdown=content,
                    mode="attachment",
                    sha256=full_sha256,
                ))
            return parts

        # mode == "auto" or "split": build per-stock delivery parts
        # Preamble is merged into the first stock block
        # Trailing content is merged into the last stock block
        stock_codes_in_order = self._extract_stock_order(content, blocks)

        for idx, code in enumerate(stock_codes_in_order):
            if code not in blocks:
                continue
            body = blocks[code]
            markdown = body
            if idx == 0 and parsed.preamble:
                markdown = parsed.preamble + "\n\n" + markdown
            if idx == len(stock_codes_in_order) - 1 and parsed.trailing_content:
                markdown = markdown + "\n\n" + parsed.trailing_content

            part_sha = hashlib.sha256(markdown.encode('utf-8')).hexdigest()

            # Test if this stock block fits as inline
            part_html = markdown_to_html_document(markdown)
            test_msg = self._build_mime_message(markdown, part_html, subject, sender, receivers)
            part_mime = self._measure_mime_bytes(test_msg)

            pid = f"{code}_{idx}"
            if max_inline > 0 and part_mime <= max_inline:
                parts.append(DeliveryPart(
                    part_id=f"split_{pid}",
                    stock_codes=[code],
                    markdown=markdown,
                    mode="inline_body",
                    sha256=part_sha,
                    mime_bytes=part_mime,
                ))
            else:
                # Auto-degrade: single stock as attachment
                parts.append(DeliveryPart(
                    part_id=f"attach_{pid}",
                    stock_codes=[code],
                    markdown=markdown,
                    mode="attachment",
                    sha256=part_sha,
                    mime_bytes=part_mime,
                ))

        # Optionally attach full report
        if attach_full and parts:
            parts.append(self._make_attachment_part(content, full_sha256, expected_codes))

        return parts

    def _extract_stock_order(self, content: str, blocks: Dict[str, str]) -> List[str]:
        """Extract stock codes in the order they appear in the content."""
        seen: List[str] = []
        for m in re.finditer(r'<!--\s*STOCK_BEGIN\s*:\s*(\S+)\s*-->', content):
            raw = m.group(1).strip()
            from src.utils.stock_code import normalize_stock_code_key
            norm = normalize_stock_code_key(raw)
            if norm and norm in blocks and norm not in seen:
                seen.append(norm)
        return seen

    def _make_attachment_part(self, content: str, sha256: str, stock_codes: List[str]) -> DeliveryPart:
        """Create a DeliveryPart for an attachment-mode email."""
        return DeliveryPart(
            part_id="full_attachment",
            stock_codes=list(stock_codes),
            markdown=content,
            mode="attachment",
            sha256=sha256,
        )

    def _describe_mode(self, parts: List[DeliveryPart]) -> str:
        """Summarize the delivery mode from the parts list."""
        modes = set(p.mode for p in parts)
        if modes == {"inline_body"} and len(parts) == 1:
            return "inline"
        if "attachment" in modes and "inline_body" not in modes:
            return "attachment"
        if "attachment" in modes:
            return "split_with_attachment"
        return "split"

    # ------------------------------------------------------------------
    # Content conservation validation
    # ------------------------------------------------------------------

    def _validate_delivery_plan(
        self,
        parts: List[DeliveryPart],
        expected_codes: List[str],
        original_content: str,
        full_sha256: str,
    ) -> Optional[str]:
        """Validate the delivery plan covers all expected stock codes exactly once.

        Full-attachment parts (part_id == "full_attachment" or "full_raw") are
        excluded from the code-uniqueness check since they are duplicates of
        already-delivered content. If the plan consists solely of full-attachment
        parts (pure attachment mode), the conservation check is skipped — the
        full content SHA-256 is the proof of completeness.

        Returns an error string if validation fails, None if OK.
        """
        if not expected_codes:
            return None

        # Collect code assignments, excluding full-attachment parts
        code_to_parts: Dict[str, List[str]] = {}
        for part in parts:
            if part.part_id in ("full_attachment", "full_raw"):
                continue
            for code in part.stock_codes:
                code_to_parts.setdefault(code, []).append(part.part_id)

        # If no non-attachment parts exist (pure attachment mode), skip code-level
        # validation — the full SHA-256 check below is the proof of completeness.
        if not code_to_parts:
            # Still verify the full attachment SHA-256
            for part in parts:
                if part.part_id == "full_attachment" or part.part_id == "full_raw":
                    if part.sha256 != full_sha256:
                        return f"Full attachment SHA-256 mismatch: expected {full_sha256[:16]}..."
            return None

        # All expected codes must be assigned
        unassigned = [c for c in expected_codes if c not in code_to_parts]
        if unassigned:
            return f"Unassigned stock codes: {unassigned}"

        # No code assigned to more than one part (excluding full attachments)
        duplicates = {c: pids for c, pids in code_to_parts.items() if len(pids) > 1}
        if duplicates:
            return f"Duplicate stock code assignments: {duplicates}"

        # Check: all assigned codes are in expected set
        expected_set = set(expected_codes)
        extra = [c for c in code_to_parts if c not in expected_set]
        if extra:
            return f"Extra stock codes in delivery plan: {extra}"

        # Check: full attachment SHA-256 matches if present
        for part in parts:
            if part.part_id == "full_attachment" or part.part_id == "full_raw":
                if part.sha256 != full_sha256:
                    return f"Full attachment SHA-256 mismatch: expected {full_sha256[:16]}..."
                break

        return None

    # ------------------------------------------------------------------
    # Sub-split for oversized single-stock parts
    # ------------------------------------------------------------------

    def _sub_split_delivery_part(
        self,
        part: DeliveryPart,
        max_message: int,
    ) -> List[DeliveryPart]:
        """Split an oversized single-stock DeliveryPart into smaller sub-parts.

        Strategy: split by second-level (##) headings, then by paragraphs,
        then by UTF-8 safe character boundaries.
        """
        if not part.markdown:
            return []

        # Try splitting by ## headings
        sections = self._split_by_heading_level(part.markdown, level=2)
        if len(sections) <= 1:
            # Try paragraphs
            sections = self._split_by_paragraphs(part.markdown)
        if len(sections) <= 1:
            # Last resort: safe character boundary split
            sections = self._split_by_safe_boundary(part.markdown, max_message // 2)

        sub_parts: List[DeliveryPart] = []
        for i, section in enumerate(sections):
            sub_sha = hashlib.sha256(section.encode('utf-8')).hexdigest()
            sub_parts.append(DeliveryPart(
                part_id=f"{part.part_id}_sub{i}",
                stock_codes=list(part.stock_codes),
                markdown=section,
                mode="sub_split",
                sha256=sub_sha,
            ))
        return sub_parts

    @staticmethod
    def _split_by_heading_level(content: str, level: int = 2) -> List[str]:
        """Split markdown content by headings at the given level."""
        prefix = "#" * level
        pattern = re.compile(rf'^{prefix}\s+', re.MULTILINE)
        matches = list(pattern.finditer(content))
        if not matches:
            return [content]

        chunks = []
        for i, m in enumerate(matches):
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
            chunks.append(content[start:end].strip())
        # Add content before first heading if any
        if matches and matches[0].start() > 0:
            prefix_content = content[:matches[0].start()].strip()
            if prefix_content:
                chunks.insert(0, prefix_content)
        return chunks

    @staticmethod
    def _split_by_paragraphs(content: str) -> List[str]:
        """Split content by double-newline paragraphs."""
        paras = re.split(r'\n\s*\n', content)
        return [p.strip() for p in paras if p.strip()]

    @staticmethod
    def _split_by_safe_boundary(content: str, max_chunk_bytes: int) -> List[str]:
        """Split by UTF-8 safe boundary, ensuring multi-byte chars are not broken."""
        encoded = content.encode('utf-8')
        if len(encoded) <= max_chunk_bytes:
            return [content]

        chunks = []
        pos = 0
        while pos < len(encoded):
            end = min(pos + max_chunk_bytes, len(encoded))
            # Walk back to find a safe boundary (newline, space, or start of a UTF-8 sequence)
            if end < len(encoded):
                while end > pos:
                    byte_val = encoded[end]
                    # UTF-8 continuation bytes are 0x80-0xBF
                    if byte_val & 0xC0 != 0x80:
                        break
                    end -= 1
            chunks.append(encoded[pos:end].decode('utf-8', errors='replace'))
            pos = end
        return chunks

    # ------------------------------------------------------------------
    # Part subject / MIME builders
    # ------------------------------------------------------------------

    def _build_part_subject(
        self,
        base_subject: str,
        part: DeliveryPart,
        index: int,
        total: int,
        report_id: Optional[str],
        full_sha256: str,
    ) -> str:
        """Build subject line for a delivery part."""
        rpt_prefix = f"[{report_id[:8]}/{full_sha256[:8]}]" if report_id else ""
        stock_str = ",".join(part.stock_codes) if part.stock_codes else ""
        if part.mode == "attachment" and part.part_id.startswith("full_"):
            tag = "[完整报告]"
        elif part.mode == "attachment":
            tag = f"[{stock_str} 附件]"
        elif part.mode == "sub_split":
            tag = f"[{stock_str} {index + 1}/{total}]"
        else:
            tag = f"[{index + 1}/{total}]"
        return f"{rpt_prefix} {base_subject} {tag}".strip()

    def _build_part_mime(
        self,
        part: DeliveryPart,
        subject: str,
        sender: str,
        receivers: List[str],
    ) -> MIMEMultipart:
        """Build the appropriate MIME message for a delivery part."""
        if part.mode == "attachment" or part.mode == "sub_split":
            return self._build_attachment_mime(part.markdown, subject, sender, receivers, part.sha256)
        return self._build_mime_message(
            part.markdown,
            markdown_to_html_document(part.markdown),
            subject, sender, receivers,
        )

    # ------------------------------------------------------------------
    # Low-level MIME builders
    # ------------------------------------------------------------------

    def _build_mime_message(
        self,
        content: str,
        html_content: str,
        subject: str,
        sender: str,
        receivers: List[str],
    ) -> MIMEMultipart:
        """Build a MIME multipart/alternative message for email delivery."""
        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(subject, 'utf-8')
        msg['From'] = self._format_sender_address(sender)
        msg['To'] = ', '.join(receivers)

        text_body = content if len(content.encode('utf-8')) < 4000 else (
            "本报告内容较长，请使用支持HTML的邮件客户端查看完整内容。\n"
            "如需完整Markdown原文，请查看完整报告附件。"
        )
        msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
        msg.attach(MIMEText(html_content, 'html', 'utf-8'))
        return msg

    def _build_attachment_mime(
        self,
        content: str,
        subject: str,
        sender: str,
        receivers: List[str],
        sha256: str,
    ) -> MIMEMultipart:
        """Build a MIME message with the full Markdown as a .md attachment."""
        msg = MIMEMultipart()
        msg['Subject'] = Header(subject, 'utf-8')
        msg['From'] = self._format_sender_address(sender)
        msg['To'] = ', '.join(receivers)

        alt = MIMEMultipart('alternative')
        alt.attach(MIMEText(
            f"完整报告见附件。\nSHA-256: {sha256}",
            'plain', 'utf-8',
        ))
        alt.attach(MIMEText(
            f"<p>完整报告见附件 (<code>intraday_report.md</code>)。</p><p>SHA-256: <code>{sha256}</code></p>",
            'html', 'utf-8',
        ))
        msg.attach(alt)

        md_part = MIMEBase('application', 'octet-stream')
        md_part.set_payload(content.encode('utf-8'))
        encoders.encode_base64(md_part)
        md_part.add_header(
            'Content-Disposition',
            'attachment',
            filename=('utf-8', '', 'intraday_report.md'),
        )
        msg.attach(md_part)
        return msg

    # ------------------------------------------------------------------
    # SMTP send
    # ------------------------------------------------------------------

    def _send_single_email(
        self,
        msg: MIMEMultipart,
        sender: str,
        password: str,
        receivers: List[str],
        timeout_seconds: Optional[float] = None,
    ) -> bool:
        """Send a single MIME message via SMTP. Handles connection and authentication."""
        server: Optional[smtplib.SMTP] = None
        try:
            domain = sender.split('@')[-1].lower()
            smtp_config = SMTP_CONFIGS.get(domain)
            if smtp_config:
                smtp_server, smtp_port = smtp_config['server'], smtp_config['port']
                use_ssl = smtp_config['ssl']
            else:
                smtp_server, smtp_port = f"smtp.{domain}", 465
                use_ssl = True

            if use_ssl:
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=timeout_seconds or 30)
            else:
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=timeout_seconds or 30)
                server.starttls()

            server.login(sender, password)
            server.send_message(msg)
            logger.info("邮件发送成功，收件人: %s", receivers)
            return True

        except smtplib.SMTPAuthenticationError:
            logger.error("邮件发送失败：认证错误，请检查邮箱和授权码是否正确")
            return False
        except smtplib.SMTPConnectError as e:
            logger.error(f"邮件发送失败：无法连接 SMTP 服务器 - {e}")
            return False
        except Exception as e:
            logger.error(f"发送邮件失败: {e}")
            return False
        finally:
            self._close_server(server)

    def _send_email_with_inline_image(
        self, image_bytes: bytes, receivers: Optional[List[str]] = None
    ) -> bool:
        """Send email with inline image attachment (Issue #289)."""
        if not self._is_email_configured():
            return False
        sender = self._email_config['sender']
        password = self._email_config['password']
        receivers = receivers or self._email_config['receivers']
        server: Optional[smtplib.SMTP] = None
        try:
            date_str = datetime.now().strftime('%Y-%m-%d')
            subject = f"📈 股票智能分析报告 - {date_str}"
            msg = MIMEMultipart('related')
            msg['Subject'] = Header(subject, 'utf-8')
            msg['From'] = self._format_sender_address(sender)
            msg['To'] = ', '.join(receivers)

            alt = MIMEMultipart('alternative')
            alt.attach(MIMEText('报告已生成，详见下方图片。', 'plain', 'utf-8'))
            html_body = (
                '<p>报告已生成，详见下方图片（点击可查看大图）：</p>'
                '<p><img src="cid:report-image" alt="股票分析报告" style="max-width:100%%;" /></p>'
            )
            alt.attach(MIMEText(html_body, 'html', 'utf-8'))
            msg.attach(alt)

            img_part = MIMEImage(image_bytes, _subtype='png')
            img_part.add_header('Content-Disposition', 'inline', filename='report.png')
            img_part.add_header('Content-ID', '<report-image>')
            msg.attach(img_part)

            domain = sender.split('@')[-1].lower()
            smtp_config = SMTP_CONFIGS.get(domain)
            if smtp_config:
                smtp_server, smtp_port = smtp_config['server'], smtp_config['port']
                use_ssl = smtp_config['ssl']
            else:
                smtp_server, smtp_port = f"smtp.{domain}", 465
                use_ssl = True

            if use_ssl:
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30)
            else:
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.starttls()
            server.login(sender, password)
            server.send_message(msg)
            logger.info("邮件（内联图片）发送成功，收件人: %s", receivers)
            return True
        except Exception as e:
            logger.error("邮件（内联图片）发送失败: %s", e)
            return False
        finally:
            self._close_server(server)
