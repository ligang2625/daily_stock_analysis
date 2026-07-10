# -*- coding: utf-8 -*-
"""
Email 发送提醒服务

职责：
1. 通过 SMTP 发送 Email 消息
"""
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Optional, List
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
    mode: str = ""  # "inline" | "split" | "attachment"
    error_detail: Optional[str] = None

    def __bool__(self) -> bool:
        return self.success


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
    
    def send_to_email(
        self,
        content: str,
        subject: Optional[str] = None,
        receivers: Optional[List[str]] = None,
        *,
        timeout_seconds: Optional[float] = None,
        report_id: Optional[str] = None,
    ) -> EmailDeliveryResult:
        """
        Send email via SMTP with optional size-gating and stock-block splitting.

        When EMAIL_MAX_INLINE_BYTES is configured (>0) and the MIME body exceeds it:
        - If EMAIL_LONG_CONTENT_MODE is "split" or "auto", splits by stock blocks with [i/N] subjects.
        - If "attachment", sends a short index body with full Markdown as attachment.

        Each individual final MIME message is verified against max_inline_bytes before sending.

        Args:
            content: Email body (Markdown, converted to HTML).
            subject: Email subject.
            receivers: Recipient list (default: configured receivers).
            timeout_seconds: SMTP timeout.
            report_id: Optional report identifier for subject and logging.

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
            "Email size check: md=%d html=%d mime=%d max_inline=%d mode=%s",
            md_bytes, html_bytes, mime_bytes, max_inline, mode,
        )

        if max_inline <= 0 or mime_bytes <= max_inline:
            # Short content: send as single email — verify MIME size first
            if max_inline > 0 and mime_bytes > max_inline:
                logger.warning("Inline email exceeded max_inline after build: %d > %d", mime_bytes, max_inline)
                return EmailDeliveryResult(success=False, error_detail="inline size exceeded")
            ok = self._send_single_email(test_msg, sender, password, receivers, timeout_seconds)
            return EmailDeliveryResult(
                success=ok, parts_sent=1 if ok else 0, parts_failed=0 if ok else 1,
                parts_total=1, mode="inline",
            )

        # Long content: split or attachment mode
        if mode == "attachment":
            att_ok = self._send_attachment_email(
                content=content,
                html_content=html_content,
                subject=subject,
                sender=sender,
                password=password,
                receivers=receivers,
                timeout_seconds=timeout_seconds,
                report_id=report_id,
                sha256=full_sha256,
                max_inline=max_inline,
            )
            return EmailDeliveryResult(
                success=att_ok, parts_sent=1 if att_ok else 0, parts_failed=0 if att_ok else 1,
                parts_total=1, mode="attachment",
            )

        # mode == "auto" or "split": attempt stock-block splitting
        blocks = self._split_by_stock_blocks(content)
        if len(blocks) <= 1:
            # Can't split meaningfully, fall back to attachment
            att_ok = self._send_attachment_email(
                content=content,
                html_content=html_content,
                subject=subject,
                sender=sender,
                password=password,
                receivers=receivers,
                timeout_seconds=timeout_seconds,
                report_id=report_id,
                sha256=full_sha256,
                max_inline=max_inline,
            )
            return EmailDeliveryResult(
                success=att_ok, parts_sent=1 if att_ok else 0, parts_failed=0 if att_ok else 1,
                parts_total=1, mode="attachment",
            )

        # Send each stock block as a separate email with MIME size gating
        total = len(blocks)
        result = EmailDeliveryResult(parts_total=total, mode="split")
        for i, block in enumerate(blocks):
            part_subject = f"{subject} [{i + 1}/{total}]"
            block_html = markdown_to_html_document(block)
            block_msg = self._build_mime_message(block, block_html, part_subject, sender, receivers)
            block_mime = self._measure_mime_bytes(block_msg)
            if max_inline > 0 and block_mime > max_inline:
                logger.warning(
                    "Email part %d/%d MIME size %d exceeds max_inline %d, skipping",
                    i + 1, total, block_mime, max_inline,
                )
                result.parts_failed += 1
                result.failed_part_indices.append(i)
                continue
            ok = self._send_single_email(block_msg, sender, password, receivers, timeout_seconds)
            if not ok:
                logger.warning("Email part %d/%d failed: report_id=%s mime=%d", i + 1, total, report_id, block_mime)
                result.parts_failed += 1
                result.failed_part_indices.append(i)
            else:
                logger.info("Email part %d/%d sent: report_id=%s mime=%d", i + 1, total, report_id, block_mime)
                result.parts_sent += 1

        result.success = result.parts_failed == 0

        # Optionally attach full report as a final email
        if attach_full:
            final_subject = f"{subject} [完整报告]"
            final_html = markdown_to_html_document(content)
            final_msg = self._build_attachment_mime(content, final_html, final_subject, sender, receivers, full_sha256)
            final_mime = self._measure_mime_bytes(final_msg)
            if max_inline > 0 and final_mime > max_inline:
                logger.warning(
                    "Full report attachment MIME size %d exceeds max_inline %d, skipping",
                    final_mime, max_inline,
                )
                result.parts_failed += 1
                result.failed_part_indices.append(-1)  # -1 = attachment email
            else:
                final_ok = self._send_single_email(final_msg, sender, password, receivers, timeout_seconds)
                result.parts_total += 1
                if not final_ok:
                    logger.warning("Full report attachment email failed")
                    result.parts_failed += 1
                    result.failed_part_indices.append(-1)
                else:
                    result.parts_sent += 1
            result.success = result.parts_failed == 0

        return result

    def _split_by_stock_blocks(self, content: str) -> List[str]:
        """Split Markdown content by stock blocks for multi-part delivery.

        Splits on section headers (## ###) or stock entries (- **), preserving
        each stock's description as an intact unit. Never splits mid-block.

        Returns list of content chunks; empty content returns single-element list.
        """
        if not content:
            return [content]

        # Try splitting on ### headers (stock groups)
        section_pattern = re.compile(r'(?=^### )', re.MULTILINE)
        sections = section_pattern.split(content.strip())
        if len(sections) > 1:
            return [s.strip() for s in sections if s.strip()]

        # Try splitting on ### or **stock** entries within sections
        entry_pattern = re.compile(r'(?=^- \*\*)', re.MULTILINE)
        entries = entry_pattern.split(content.strip())
        if len(entries) > 2:  # Only split if we have meaningful groups
            # Combine preamble with first entry, then group entries
            result = []
            current = []
            for entry in entries:
                current.append(entry)
                if len(current) >= 5:
                    result.append('\n'.join(current).strip())
                    current = []
            if current:
                result.append('\n'.join(current).strip())
            if len(result) > 1:
                return result

        return [content]

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

        # For short messages: text + HTML. For long split messages: text is a summary.
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
        html_content: str,
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

        # Text + HTML body
        alt = MIMEMultipart('alternative')
        alt.attach(MIMEText(
            f"完整报告见附件。\nSHA-256: {sha256}",
            'plain', 'utf-8',
        ))
        # ponytail: attachment body is a short index only — full report is the .md attachment
        alt.attach(MIMEText(
            f"<p>完整报告见附件 (<code>intraday_report.md</code>)。</p><p>SHA-256: <code>{sha256}</code></p>",
            'html', 'utf-8',
        ))
        msg.attach(alt)

        # Attach .md file
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

    def _send_attachment_email(
        self,
        content: str,
        html_content: str,
        subject: str,
        sender: str,
        password: str,
        receivers: List[str],
        timeout_seconds: Optional[float],
        report_id: Optional[str],
        sha256: str,
        max_inline: int = 0,
    ) -> bool:
        """Send long report as attachment email, with optional MIME size check."""
        msg = self._build_attachment_mime(content, html_content, subject, sender, receivers, sha256)
        if max_inline > 0:
            att_mime = self._measure_mime_bytes(msg)
            if att_mime > max_inline:
                logger.warning(
                    "Attachment email MIME size %d exceeds max_inline %d, blocked",
                    att_mime, max_inline,
                )
                return False
        return self._send_single_email(msg, sender, password, receivers, timeout_seconds)

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
