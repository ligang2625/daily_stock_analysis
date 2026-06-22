# -*- coding: utf-8 -*-
"""
Database diagnostics module (Phase 4).

Collects metrics from the main and historical databases and emits a
human-readable text report or machine-readable JSON.

Usage:
    python -m src.maintenance.db_diagnostics --format text
    python -m src.maintenance.db_diagnostics --format json
    python -m src.maintenance.db_diagnostics --format json --output logs/db_diagnostics/latest.json
    python -m src.maintenance.db_diagnostics --days 5
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text as sa_text

from src.config import get_config
from src.storage import DatabaseManager
from src.storage_historical import HistoricalDatabaseManager

logger = logging.getLogger(__name__)

MAIN_TABLES_TO_PROBE = [
    ('analysis_history', 'created_at'),
    ('analysis_history_payload', 'created_at'),
    ('stock_daily', 'created_at'),
    ('news_intel', 'fetched_at'),
    ('llm_usage', 'called_at'),
    ('agent_provider_turns', 'created_at'),
    ('conversation_messages', 'created_at'),
    ('fundamental_snapshot', 'created_at'),
    ('alert_triggers', 'triggered_at'),
    ('decision_signals', 'created_at'),
]

HISTORICAL_TABLES = [
    'historical_stock_daily',
    'historical_intraday_quote_points',
    'historical_postmarket_technical_summary',
    'historical_market_light_daily',
    'historical_market_index_points',
    'archive_runs',
]


def _file_size_mb(path: str) -> Optional[float]:
    """Return file size in MB, or None if file does not exist."""
    p = Path(path)
    if p.is_file():
        return p.stat().st_size / (1024 * 1024)
    return None


def _json_default(obj: Any) -> Any:
    """Serialize non-JSON-native types as strings."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _collect_main_db_metrics(config, db: DatabaseManager, retention_days: int) -> Dict[str, Any]:
    """Collect main database metrics."""
    metrics: Dict[str, Any] = {
        'file_path': config.database_path,
        'file_size_mb': _file_size_mb(config.database_path),
    }

    db_path = str(config.database_path)
    wal_path = db_path + '-wal'
    shm_path = db_path + '-shm'
    wal_mb = _file_size_mb(wal_path)
    if wal_mb is not None:
        metrics['wal_size_mb'] = wal_mb
    shm_mb = _file_size_mb(shm_path)
    if shm_mb is not None:
        metrics['shm_size_mb'] = shm_mb

    table_metrics: Dict[str, Any] = {}
    cutoff_date = date.today() - timedelta(days=retention_days)

    with db.session_scope() as session:
        for tbl, ts_col in MAIN_TABLES_TO_PROBE:
            entry: Dict[str, Any] = {}
            try:
                # Row count
                row = session.execute(sa_text(f"SELECT COUNT(*) FROM {tbl}")).fetchone()
                entry['row_count'] = row[0] if row else 0
            except Exception as exc:
                entry['row_count'] = None
                entry['error'] = str(exc)
                table_metrics[tbl] = entry
                continue

            # Rows before cutoff (archive candidates)
            try:
                row = session.execute(
                    sa_text(
                        f"SELECT COUNT(*) FROM {tbl} "
                        f"WHERE {ts_col} < :cutoff"
                    ),
                    {"cutoff": cutoff_date.isoformat()},
                ).fetchone()
                entry['candidate_rows'] = row[0] if row else 0
            except Exception:
                entry['candidate_rows'] = None

            # Indices
            try:
                indices = session.execute(
                    sa_text(f"PRAGMA index_list({tbl})")
                ).fetchall()
                entry['indices'] = [
                    {'name': r[1], 'unique': bool(r[2])} for r in indices
                ]
            except Exception:
                entry['indices'] = None

            table_metrics[tbl] = entry

        # Largest payload records
        try:
            largest = session.execute(
                sa_text(
                    "SELECT id, payload_size, source_type, analysis_type, created_at "
                    "FROM analysis_history_payload "
                    "ORDER BY COALESCE(payload_size, 0) DESC "
                    "LIMIT 10"
                )
            ).fetchall()
            metrics['largest_payloads'] = [
                {
                    'id': r[0],
                    'payload_size': r[1],
                    'source_type': r[2],
                    'analysis_type': r[3],
                    'created_at': str(r[4]) if r[4] else None,
                }
                for r in largest
            ]
        except Exception as exc:
            metrics['largest_payloads'] = None
            metrics['largest_payloads_error'] = str(exc)

    metrics['tables'] = table_metrics
    return metrics


def _collect_historical_db_metrics(
    config, historical_db: HistoricalDatabaseManager, retention_days: int
) -> Optional[Dict[str, Any]]:
    """Collect historical database metrics. Returns None if DB does not exist."""
    hist_path = config.historical_database_path
    if not Path(hist_path).is_file():
        return None

    metrics: Dict[str, Any] = {
        'file_path': hist_path,
        'file_size_mb': _file_size_mb(hist_path),
    }

    table_metrics: Dict[str, Any] = {}
    session = historical_db.get_session()
    try:
        for tbl in HISTORICAL_TABLES:
            entry: Dict[str, Any] = {}
            try:
                row = session.execute(sa_text(f"SELECT COUNT(*) FROM {tbl}")).fetchone()
                entry['row_count'] = row[0] if row else 0
            except Exception as exc:
                entry['row_count'] = None
                entry['error'] = str(exc)
                table_metrics[tbl] = entry
                continue

            # Min/max trade_date/query_date if applicable
            for date_col in ('trade_date', 'query_date', 'started_at'):
                try:
                    row = session.execute(
                        sa_text(f"SELECT MIN({date_col}), MAX({date_col}) FROM {tbl}")
                    ).fetchone()
                    if row and row[0] is not None:
                        entry[f'min_{date_col}'] = str(row[0])
                    if row and row[1] is not None:
                        entry[f'max_{date_col}'] = str(row[1])
                except Exception:
                    pass

            table_metrics[tbl] = entry

        # Latest archive run
        try:
            from sqlalchemy import desc, select as sa_select
            from src.storage_historical import ArchiveRun

            run = session.execute(
                sa_select(ArchiveRun)
                .order_by(desc(ArchiveRun.started_at))
                .limit(1)
            ).scalars().first()
            if run:
                metrics['latest_archive_run'] = {
                    'id': run.id,
                    'started_at': str(run.started_at),
                    'finished_at': str(run.finished_at) if run.finished_at else None,
                    'cutoff_date': run.cutoff_date,
                    'status': run.status,
                    'rows_read': run.rows_read,
                    'rows_written': run.rows_written,
                    'rows_deleted': run.rows_deleted,
                    'error_message': run.error_message,
                }

            # Failed runs in last N days
            since = datetime.now() - timedelta(days=retention_days)
            failed = session.execute(
                sa_select(ArchiveRun)
                .where(
                    ArchiveRun.status == 'failed',
                    ArchiveRun.started_at >= since,
                )
            ).scalars().all()
            metrics['failed_runs_recent'] = len(failed)
        except Exception:
            pass
    finally:
        session.close()

    metrics['tables'] = table_metrics
    return metrics


def generate_diagnostics(
    *,
    retention_days: int = 5,
) -> Dict[str, Any]:
    """
    Generate a full diagnostics report dict.

    Returns a dict with keys:
        status, generated_at, main_db, historical_db, archive, warnings, errors
    """
    config = get_config()
    db = DatabaseManager.get_instance()
    historical_db = HistoricalDatabaseManager()

    warnings: List[str] = []
    errors_list: List[str] = []

    main_db = _collect_main_db_metrics(config, db, retention_days)

    historical_db_metrics = None
    try:
        historical_db_metrics = _collect_historical_db_metrics(config, historical_db, retention_days)
    except Exception as exc:
        warnings.append(f"Failed to collect historical DB metrics: {exc}")

    # Determine status
    status = "ok"
    if historical_db_metrics is None:
        status = "warning"
        warnings.append("Historical database does not exist yet")

    if errors_list:
        status = "error"

    # Check for large payloads
    largest_payloads = main_db.get('largest_payloads')
    if largest_payloads and isinstance(largest_payloads, list) and largest_payloads:
        if any((p.get("payload_size") or 0) > 500_000 for p in largest_payloads):
            warnings.append("Large payload records detected (>500KB)")

    # Check main DB size
    mb = main_db.get('file_size_mb')
    if mb and mb > 500:
        warnings.append(f"Main database is large ({mb:.1f} MB)")

    report: Dict[str, Any] = {
        'status': status,
        'generated_at': datetime.now().isoformat(),
        'main_db': main_db,
        'historical_db': historical_db_metrics,
        'archive': {
            'retention_days': retention_days,
            'last_successful_run': None,
        },
        'warnings': warnings,
        'errors': errors_list,
    }

    # Extract last successful run from historical metrics if available
    if historical_db_metrics and historical_db_metrics.get('latest_archive_run'):
        report['archive']['last_successful_run'] = historical_db_metrics['latest_archive_run']

    return report


def _format_text(report: Dict[str, Any]) -> str:
    """Format a diagnostics report as human-readable text."""
    lines = []
    lines.append("=" * 60)
    lines.append("Database Diagnostics Report")
    lines.append(f"Generated: {report['generated_at']}")
    lines.append(f"Status: {report['status']}")
    lines.append("=" * 60)

    # Main DB
    main = report.get('main_db', {})
    lines.append(f"\n--- Main Database ---")
    lines.append(f"  Path: {main.get('file_path', '?')}")
    mb = main.get('file_size_mb')
    lines.append(f"  Size: {mb:.2f} MB" if mb is not None else "  Size: N/A")
    wal_mb = main.get('wal_size_mb')
    if wal_mb is not None:
        lines.append(f"  WAL: {wal_mb:.2f} MB")
    shm_mb = main.get('shm_size_mb')
    if shm_mb is not None:
        lines.append(f"  SHM: {shm_mb:.2f} MB")

    tables = main.get('tables', {})
    if tables:
        lines.append(f"\n  Table Row Counts:")
        for tbl, info in sorted(tables.items()):
            if not isinstance(info, dict):
                continue
            count = info.get('row_count')
            candidate = info.get('candidate_rows')
            count_str = str(count) if count is not None else 'ERROR'
            candidate_str = f" (candidate: {candidate})" if candidate is not None else ""
            lines.append(f"    {tbl}: {count_str}{candidate_str}")

    largest = main.get('largest_payloads')
    if largest and isinstance(largest, list) and largest:
        lines.append(f"\n  Largest Payloads:")
        for p in largest[:5]:
            ps = p.get('payload_size', '?')
            st = p.get('source_type', '?')
            lines.append(f"    id={p['id']} size={ps} type={st}")

    # Historical DB
    hist = report.get('historical_db')
    if hist:
        lines.append(f"\n--- Historical Database ---")
        lines.append(f"  Path: {hist.get('file_path', '?')}")
        mb = hist.get('file_size_mb')
        lines.append(f"  Size: {mb:.2f} MB" if mb is not None else "  Size: N/A")

        h_tables = hist.get('tables', {})
        if h_tables:
            lines.append(f"\n  Table Row Counts:")
            for tbl, info in sorted(h_tables.items()):
                if not isinstance(info, dict):
                    continue
                count = info.get('row_count')
                count_str = str(count) if count is not None else 'ERROR'
                min_date = info.get('min_trade_date') or info.get('min_query_date') or info.get('min_started_at')
                max_date = info.get('max_trade_date') or info.get('max_query_date') or info.get('max_started_at')
                date_range = f" [{min_date} -> {max_date}]" if min_date and max_date else ""
                lines.append(f"    {tbl}: {count_str}{date_range}")

        latest = hist.get('latest_archive_run')
        if latest:
            lines.append(f"\n  Latest Archive Run: id={latest['id']} status={latest['status']} cutoff={latest['cutoff_date']}")
        failed_count = hist.get('failed_runs_recent', 0)
        if failed_count:
            lines.append(f"  Recent Failed Runs: {failed_count}")
    else:
        lines.append(f"\n--- Historical Database ---")
        lines.append(f"  (does not exist)")

    # Warnings and errors
    warnings = report.get('warnings', [])
    if warnings:
        lines.append(f"\n--- Warnings ---")
        for w in warnings:
            lines.append(f"  [WARN] {w}")
    errors_list = report.get('errors', [])
    if errors_list:
        lines.append(f"\n--- Errors ---")
        for e in errors_list:
            lines.append(f"  [ERROR] {e}")

    lines.append("")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point for database diagnostics."""
    parser = argparse.ArgumentParser(
        description="Collect and report database diagnostics.",
    )
    parser.add_argument(
        '--format', type=str, default='text', choices=('text', 'json'),
        help='Output format (default: text)',
    )
    parser.add_argument(
        '--output', type=str, default=None,
        help='Write output to file path (prints to stdout if omitted)',
    )
    parser.add_argument(
        '--days', type=int, default=5,
        help='Retention window for candidate row counts (default: 5)',
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    )

    try:
        report = generate_diagnostics(retention_days=args.days)
    except Exception as exc:
        logger.exception("Failed to generate diagnostics")
        error_report = {
            'status': 'error',
            'generated_at': datetime.now().isoformat(),
            'errors': [str(exc)],
        }
        if args.format == 'json':
            result = json.dumps(error_report, ensure_ascii=False, indent=2, default=_json_default)
        else:
            result = f"ERROR: Failed to generate diagnostics: {exc}"
        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(result, encoding='utf-8')
            logger.info("Diagnostics written to %s", args.output)
        else:
            print(result)
        return 1

    if args.format == 'json':
        result = json.dumps(report, ensure_ascii=False, indent=2, default=_json_default)
    else:
        result = _format_text(report)

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(result, encoding='utf-8')
        logger.info("Diagnostics written to %s", args.output)
    else:
        print(result)

    return 0


if __name__ == "__main__":
    sys.exit(main())
