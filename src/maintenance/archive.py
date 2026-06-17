# -*- coding: utf-8 -*-
"""
Safe archive maintenance command.

Phase 1: Deletes expired runtime records and large payloads from the main
database. Summary and technical rows (analysis_history, stock_daily) are
NOT deleted.

Usage:
    python -m src.maintenance.archive --days 5 --dry-run
    python -m src.maintenance.archive --cutoff-date 2026-06-10
    python -m src.maintenance.archive --days 5 --skip-vacuum
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text as sa_text, func, select as sa_select

from src.config import get_config
from src.storage import DatabaseManager
from src.storage_historical import HistoricalDatabaseManager

logger = logging.getLogger(__name__)

# Tables allowed for Phase 1 deletion (runtime / large-payload rows only)
# Each tuple: (table_name, timestamp_column)
# Explicitly NOT including: analysis_history (summary rows), stock_daily (technical)
ALLOWED_DELETE_TABLES: List[Tuple[str, str]] = [
    ('analysis_history_payload', 'created_at'),
    ('news_intel', 'fetched_at'),
    ('llm_usage', 'called_at'),
    ('agent_provider_turns', 'created_at'),
    ('conversation_messages', 'created_at'),
    ('fundamental_snapshot', 'created_at'),
    ('alert_triggers', 'triggered_at'),
    ('decision_signals', 'created_at'),
]

# Tables to count in dry-run (includes additional tables for reporting)
DRY_RUN_COUNT_TABLES: List[Tuple[str, str]] = ALLOWED_DELETE_TABLES + [
    ('analysis_history', 'created_at'),   # counted but NOT deleted
    ('stock_daily', 'created_at'),         # counted but NOT deleted
]


def _count_rows_before_date(
    session, table_name: str, timestamp_col: str, cutoff_date: date
) -> int:
    """Count rows in a table older than the given cutoff date."""
    row = session.execute(
        sa_text(
            f"SELECT COUNT(*) FROM {table_name} "
            f"WHERE {timestamp_col} < :cutoff"
        ),
        {"cutoff": cutoff_date.isoformat()},
    ).fetchone()
    return row[0] if row else 0


def _delete_rows_before_date(
    session, table_name: str, timestamp_col: str, cutoff_date: date
) -> int:
    """Delete rows in a table older than the given cutoff date.

    Returns the number of deleted rows.
    """
    result = session.execute(
        sa_text(
            f"DELETE FROM {table_name} "
            f"WHERE {timestamp_col} < :cutoff"
        ),
        {"cutoff": cutoff_date.isoformat()},
    )
    return result.rowcount or 0


def _delete_legacy_payload_rows(
    session, cutoff_date: date
) -> int:
    """
    Null out legacy large payload columns in analysis_history for rows older
    than cutoff, but ONLY if the corresponding analysis_history_payload row
    exists (indicating a successful backfill).

    This reduces main DB bloat for rows that have already been split.
    Does NOT delete the summary row itself.
    """
    deleted = 0

    # Null raw_result on old rows where payload exists
    result = session.execute(
        sa_text(
            "UPDATE analysis_history "
            "SET raw_result = NULL "
            "WHERE id IN ("
            "  SELECT ah.id FROM analysis_history ah"
            "  INNER JOIN analysis_history_payload ahp ON ahp.history_id = ah.id"
            "  WHERE ah.created_at < :cutoff AND ah.raw_result IS NOT NULL"
            ")"
        ),
        {"cutoff": cutoff_date.isoformat()},
    )
    deleted += result.rowcount or 0

    # Null news_content on old rows where payload exists
    result = session.execute(
        sa_text(
            "UPDATE analysis_history "
            "SET news_content = NULL "
            "WHERE id IN ("
            "  SELECT ah.id FROM analysis_history ah"
            "  INNER JOIN analysis_history_payload ahp ON ahp.history_id = ah.id"
            "  WHERE ah.created_at < :cutoff AND ah.news_content IS NOT NULL"
            ")"
        ),
        {"cutoff": cutoff_date.isoformat()},
    )
    deleted += result.rowcount or 0

    # Null context_snapshot on old rows where payload exists
    result = session.execute(
        sa_text(
            "UPDATE analysis_history "
            "SET context_snapshot = NULL "
            "WHERE id IN ("
            "  SELECT ah.id FROM analysis_history ah"
            "  INNER JOIN analysis_history_payload ahp ON ahp.history_id = ah.id"
            "  WHERE ah.created_at < :cutoff AND ah.context_snapshot IS NOT NULL"
            ")"
        ),
        {"cutoff": cutoff_date.isoformat()},
    )
    deleted += result.rowcount or 0

    return deleted


def _dry_run(db: DatabaseManager, cutoff_date: date) -> Dict[str, int]:
    """Count candidate rows for each table without deleting anything."""
    counts: Dict[str, int] = {}

    with db.session_scope() as session:
        for table_name, ts_col in DRY_RUN_COUNT_TABLES:
            try:
                count = _count_rows_before_date(session, table_name, ts_col, cutoff_date)
                counts[table_name] = count
            except Exception as exc:
                logger.warning("Dry-run count failed for %s: %s", table_name, exc)
                counts[table_name] = 0

        # Count legacy payload columns that would be nulled
        try:
            legacy_count = _count_legacy_payload_candidates(session, cutoff_date)
            counts['analysis_history_legacy_payload_cols'] = legacy_count
        except Exception as exc:
            logger.warning("Dry-run legacy payload count failed: %s", exc)
            counts['analysis_history_legacy_payload_cols'] = 0

    return counts


def _count_legacy_payload_candidates(session, cutoff_date: date) -> int:
    """Count rows whose legacy payload could be nulled (has payload row)."""
    row = session.execute(
        sa_text(
            "SELECT COUNT(*) FROM analysis_history ah"
            " INNER JOIN analysis_history_payload ahp ON ahp.history_id = ah.id"
            " WHERE ah.created_at < :cutoff"
            "  AND (ah.raw_result IS NOT NULL OR ah.news_content IS NOT NULL OR ah.context_snapshot IS NOT NULL)"
        ),
        {"cutoff": cutoff_date.isoformat()},
    ).fetchone()
    return row[0] if row else 0


def run_archive(
    *,
    days: int = 5,
    dry_run: bool = False,
    cutoff_date: Optional[date] = None,
    skip_vacuum: bool = False,
) -> int:
    """
    Execute archive maintenance.

    Returns 0 on success, 1 on error.
    """
    config = get_config()

    if cutoff_date is None:
        cutoff_date = date.today() - timedelta(days=days)

    cutoff_str = cutoff_date.isoformat()
    days_val = (date.today() - cutoff_date).days

    logger.info(
        "Archive maintenance starting: cutoff=%s days_back=%s dry_run=%s",
        cutoff_str, days_val, dry_run,
    )

    db = DatabaseManager.get_instance()
    historical_db = HistoricalDatabaseManager()

    main_db_path = config.database_path
    historical_db_path = config.historical_database_path

    # Create archive run ledger entry
    run_id = historical_db.create_archive_run(
        cutoff_date=cutoff_str,
        main_db_path=main_db_path,
        historical_db_path=historical_db_path,
    )

    counts: Dict[str, int] = {}

    try:
        if dry_run:
            logger.info("--- DRY RUN --- No data will be deleted ---")
            counts = _dry_run(db, cutoff_date)

            logger.info("=== Dry-run results (cutoff: %s) ===", cutoff_str)
            total = 0
            for table_name, _ts_col in DRY_RUN_COUNT_TABLES:
                c = counts.get(table_name, 0)
                total += c
                logger.info("  %s: %d rows", table_name, c)
            legacy = counts.get('analysis_history_legacy_payload_cols', 0)
            if legacy:
                logger.info("  analysis_history (legacy payload nulls): %d rows", legacy)
            logger.info("  TOTAL candidate rows: %d", total + legacy)
            logger.info("=== End dry-run ===")

            historical_db.finish_archive_run(
                run_id=run_id,
                success=True,
                rows_read=total + legacy,
                rows_written=0,
                rows_deleted=0,
            )
            return 0

        # --- Actual deletion (non-dry-run) ---
        total_deleted = 0
        total_read = 0

        with db.session_scope() as session:
            for table_name, ts_col in ALLOWED_DELETE_TABLES:
                try:
                    count_before = _count_rows_before_date(session, table_name, ts_col, cutoff_date)
                    total_read += count_before
                    deleted = _delete_rows_before_date(session, table_name, ts_col, cutoff_date)
                    counts[table_name] = deleted
                    total_deleted += deleted
                    logger.info("  Deleted %d rows from %s (counted %d before)", deleted, table_name, count_before)
                except Exception as exc:
                    logger.error("Delete failed for %s: %s", table_name, exc)
                    raise

            # Null legacy payload columns on backfilled rows
            try:
                legacy_deleted = _delete_legacy_payload_rows(session, cutoff_date)
                counts['analysis_history_legacy_payload_cols'] = legacy_deleted
                total_deleted += legacy_deleted
                total_read += legacy_deleted
                logger.info("  Nulled %d legacy payload columns in analysis_history", legacy_deleted)
            except Exception as exc:
                logger.error("Legacy payload nulling failed: %s", exc)
                raise

        # VACUUM to reclaim space
        if not skip_vacuum:
            logger.info("Running VACUUM ...")
            try:
                with db.session_scope() as session:
                    session.execute(sa_text("VACUUM"))
                logger.info("VACUUM complete")
            except Exception as exc:
                logger.warning("VACUUM failed (non-fatal): %s", exc)

        logger.info(
            "Archive complete: deleted=%d rows across %d tables",
            total_deleted, len(ALLOWED_DELETE_TABLES),
        )

        historical_db.finish_archive_run(
            run_id=run_id,
            success=True,
            rows_read=total_read,
            rows_written=0,
            rows_deleted=total_deleted,
        )
        return 0

    except Exception as exc:
        logger.error("Archive failed: %s", exc)
        historical_db.finish_archive_run(
            run_id=run_id,
            success=False,
            rows_read=sum(counts.values()),
            rows_deleted=0,
            error_message=str(exc)[:2000],
        )
        return 1


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point for archive maintenance."""
    parser = argparse.ArgumentParser(
        description="Archive expired runtime records from the main database.",
    )
    parser.add_argument(
        '--days', type=int, default=5,
        help='Delete records older than N days (default: 5)',
    )
    parser.add_argument(
        '--cutoff-date', type=str, default=None,
        help='Explicit cutoff date (YYYY-MM-DD), overrides --days',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Count candidate rows without deleting',
    )
    parser.add_argument(
        '--skip-vacuum', action='store_true',
        help='Skip VACUUM after deletion',
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    )

    cutoff_date = None
    if args.cutoff_date:
        try:
            cutoff_date = datetime.strptime(args.cutoff_date, "%Y-%m-%d").date()
        except ValueError:
            logger.error("Invalid cutoff date: %s (expected YYYY-MM-DD)", args.cutoff_date)
            return 1

    return run_archive(
        days=args.days,
        dry_run=args.dry_run,
        cutoff_date=cutoff_date,
        skip_vacuum=args.skip_vacuum,
    )


if __name__ == "__main__":
    sys.exit(main())
