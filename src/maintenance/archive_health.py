# -*- coding: utf-8 -*-
"""
Archive health check module (Phase 4).

Inspects the archive_runs ledger and emits a health status. Designed for
CI and monitoring consumption.

Usage:
    python -m src.maintenance.archive_health --max-stale-hours 48 --format text
    python -m src.maintenance.archive_health --max-stale-hours 48 --format json
    python -m src.maintenance.archive_health --max-stale-hours 48 --soft-fail
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config import get_config
from src.storage import DatabaseManager
from src.storage_historical import HistoricalDatabaseManager

logger = logging.getLogger(__name__)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _count_pending_archive_rows(config) -> int:
    """Count rows in main DB that are candidates for archive based on retention."""
    db = DatabaseManager.get_instance()
    cutoff_date = (datetime.now() - timedelta(days=config.archive_retention_days)).date()
    total = 0
    TABLES = [
        ('news_intel', 'fetched_at'),
        ('llm_usage', 'called_at'),
        ('agent_provider_turns', 'created_at'),
        ('conversation_messages', 'created_at'),
        ('fundamental_snapshot', 'created_at'),
        ('alert_triggers', 'triggered_at'),
        ('decision_signals', 'created_at'),
        ('analysis_history_payload', 'created_at'),
    ]
    from sqlalchemy import text as sa_text

    with db.session_scope() as session:
        for tbl, col in TABLES:
            try:
                row = session.execute(
                    sa_text(
                        f"SELECT COUNT(*) FROM {tbl} "
                        f"WHERE {col} < :cutoff"
                    ),
                    {"cutoff": cutoff_date.isoformat()},
                ).fetchone()
                total += row[0] if row else 0
            except Exception:
                pass
    return total


def check_archive_health(
    *,
    max_stale_hours: int = 48,
) -> Dict[str, Any]:
    """
    Check archive health and return a structured report.

    Returns a dict with:
        status: 'ok' | 'warning' | 'error'
        last_successful_run
        latest_run
        recent_failed_runs
        pending_archive_rows
        max_stale_hours
    """
    config = get_config()
    historical_db = HistoricalDatabaseManager()

    now = datetime.now()
    threshold = now - timedelta(hours=max_stale_hours)

    last_success = historical_db.get_last_successful_run()
    pending_rows = _count_pending_archive_rows(config)

    latest_run = None
    recent_failed: List[Dict[str, Any]] = []
    try:
        from sqlalchemy import desc, select as sa_select
        from src.storage_historical import ArchiveRun

        session = historical_db.get_session()
        try:
            latest = session.execute(
                sa_select(ArchiveRun)
                .order_by(desc(ArchiveRun.started_at))
                .limit(1)
            ).scalars().first()

            if latest:
                latest_run = {
                    'id': latest.id,
                    'started_at': str(latest.started_at),
                    'finished_at': str(latest.finished_at) if latest.finished_at else None,
                    'cutoff_date': latest.cutoff_date,
                    'status': latest.status,
                    'rows_read': latest.rows_read,
                    'rows_written': latest.rows_written,
                    'rows_deleted': latest.rows_deleted,
                    'error_message': latest.error_message,
                }

            failed_runs = session.execute(
                sa_select(ArchiveRun)
                .where(
                    ArchiveRun.status == 'failed',
                    ArchiveRun.started_at >= threshold,
                )
                .order_by(desc(ArchiveRun.started_at))
                .limit(10)
            ).scalars().all()

            for fr in failed_runs:
                recent_failed.append({
                    'id': fr.id,
                    'started_at': str(fr.started_at),
                    'cutoff_date': fr.cutoff_date,
                    'error_message': fr.error_message,
                })
        finally:
            session.close()
    except Exception:
        pass

    # Determine status
    status = 'ok'
    errors: List[str] = []
    warnings: List[str] = []

    if latest_run and latest_run['status'] == 'failed':
        status = 'error'
        errors.append("Latest archive run failed")
    elif last_success is None:
        if pending_rows > 0 or (latest_run is not None and latest_run['status'] == 'failed'):
            status = 'warning'
            warnings.append("No successful archive run yet, but pending rows exist")
        else:
            status = 'warning'
            warnings.append("No successful archive run yet")
    elif last_success.finished_at:
        staleness = now - last_success.finished_at
        if staleness > timedelta(hours=max_stale_hours) and pending_rows > 0:
            status = 'error'
            errors.append(
                f"Last successful archive is stale: "
                f"{staleness.total_seconds() / 3600:.1f}h ago (threshold: {max_stale_hours}h), "
                f"{pending_rows} pending rows"
            )
        elif staleness > timedelta(hours=max_stale_hours):
            status = 'warning'
            warnings.append(
                f"Last successful archive is stale: "
                f"{staleness.total_seconds() / 3600:.1f}h ago, but no pending rows"
            )

    last_success_info = None
    if last_success:
        last_success_info = {
            'id': last_success.id,
            'started_at': str(last_success.started_at),
            'finished_at': str(last_success.finished_at) if last_success.finished_at else None,
            'cutoff_date': last_success.cutoff_date,
            'rows_read': last_success.rows_read,
            'rows_written': last_success.rows_written,
            'status': last_success.status,
            'rows_deleted': last_success.rows_deleted,
        }

    return {
        'status': status,
        'generated_at': now.isoformat(),
        'last_successful_run': last_success_info,
        'latest_run': latest_run,
        'recent_failed_runs': recent_failed,
        'pending_archive_rows': pending_rows,
        'max_stale_hours': max_stale_hours,
        'warnings': warnings,
        'errors': errors,
    }


def _format_text(report: Dict[str, Any]) -> str:
    lines = []
    lines.append("=" * 50)
    lines.append("Archive Health Check")
    lines.append(f"Generated: {report['generated_at']}")
    lines.append(f"Status: {report['status']}")
    lines.append(f"Max Stale Hours: {report['max_stale_hours']}")
    lines.append(f"Pending Archive Rows: {report['pending_archive_rows']}")
    lines.append("=" * 50)

    last_success = report.get('last_successful_run')
    if last_success:
        lines.append(f"\nLast Successful Run:")
        lines.append(f"  id={last_success['id']} cutoff={last_success['cutoff_date']}")
        lines.append(f"  finished={last_success['finished_at']}")
        lines.append(f"  rows_read={last_success['rows_read']} written={last_success['rows_written']} deleted={last_success['rows_deleted']}")
    else:
        lines.append(f"\nNo successful archive run recorded.")

    latest = report.get('latest_run')
    if latest and latest != last_success:
        lines.append(f"\nLatest Run:")
        lines.append(f"  id={latest['id']} status={latest['status']} cutoff={latest['cutoff_date']}")
        if latest.get('error_message'):
            lines.append(f"  error: {latest['error_message']}")

    failed = report.get('recent_failed_runs', [])
    if failed:
        lines.append(f"\nRecent Failed Runs ({len(failed)}):")
        for fr in failed:
            lines.append(f"  id={fr['id']} cutoff={fr['cutoff_date']} error={fr.get('error_message', '?')}")

    for w in report.get('warnings', []):
        lines.append(f"\n[WARN] {w}")
    for e in report.get('errors', []):
        lines.append(f"\n[ERROR] {e}")

    lines.append("")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check archive health and report status.",
    )
    parser.add_argument(
        '--max-stale-hours', type=int, default=48,
        help='Maximum hours since last success before stale (default: 48)',
    )
    parser.add_argument(
        '--format', type=str, default='text', choices=('text', 'json'),
        help='Output format (default: text)',
    )
    parser.add_argument(
        '--soft-fail', action='store_true',
        help='Return exit code 0 even on warning/error (useful for CI observation)',
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    )

    try:
        report = check_archive_health(max_stale_hours=args.max_stale_hours)
    except Exception as exc:
        logger.exception("Health check failed")
        print(f"ERROR: {exc}")
        return 1

    if args.format == 'json':
        print(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default))
    else:
        print(_format_text(report))

    if args.soft_fail:
        return 0

    status = report['status']
    if status == 'error':
        return 1
    elif status == 'warning':
        # Non-zero for warning in strict mode
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
