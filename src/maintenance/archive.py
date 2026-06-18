# -*- coding: utf-8 -*-
"""
Safe archive maintenance command.

Phase 1: Deletes expired runtime records and large payloads from the main
database. Summary and technical rows (analysis_history, stock_daily) are
NOT deleted.

Phase 2: Before Phase 1 cleanup, extracts structured technical history from
the main database and upserts it into historical_market.db.

Usage:
    python -m src.maintenance.archive --days 5 --dry-run
    python -m src.maintenance.archive --cutoff-date 2026-06-10
    python -m src.maintenance.archive --days 5 --skip-vacuum
    python -m src.maintenance.archive --days 5 --technical-only
    python -m src.maintenance.archive --days 5 --skip-technical-archive
    python -m src.maintenance.archive --days 5 --validate-only
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
from src.maintenance.archive_extractors import extract_phase2_technical_history

logger = logging.getLogger(__name__)

# Tables allowed for Phase 1 deletion (runtime rows only — NOT payload/summary/technical)
# analysis_history_payload is handled explicitly AFTER legacy column nulling.
# analysis_history and stock_daily are NEVER deleted.
ALLOWED_DELETE_TABLES: List[Tuple[str, str]] = [
    ('news_intel', 'fetched_at'),
    ('llm_usage', 'called_at'),
    ('agent_provider_turns', 'created_at'),
    ('conversation_messages', 'created_at'),
    ('fundamental_snapshot', 'created_at'),
    ('alert_triggers', 'triggered_at'),
    ('decision_signals', 'created_at'),
]

# Tables to count in dry-run (includes protected tables for reporting)
DRY_RUN_COUNT_TABLES: List[Tuple[str, str]] = ALLOWED_DELETE_TABLES + [
    ('analysis_history', 'created_at'),       # counted but NEVER deleted
    ('analysis_history_payload', 'created_at'),  # counted but handled explicitly
    ('stock_daily', 'created_at'),             # counted but NEVER deleted
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


def _null_legacy_payload_columns(
    session, cutoff_date: date
) -> int:
    """
    Null out legacy large payload columns in analysis_history for rows older
    than cutoff. Does NOT delete the summary row itself.

    This reduces main DB bloat and marks rows as migrated to the payload table.
    """
    nulled = 0

    # Null raw_result on old rows
    result = session.execute(
        sa_text(
            "UPDATE analysis_history "
            "SET raw_result = NULL "
            "WHERE created_at < :cutoff AND raw_result IS NOT NULL"
        ),
        {"cutoff": cutoff_date.isoformat()},
    )
    nulled += result.rowcount or 0

    # Null news_content on old rows
    result = session.execute(
        sa_text(
            "UPDATE analysis_history "
            "SET news_content = NULL "
            "WHERE created_at < :cutoff AND news_content IS NOT NULL"
        ),
        {"cutoff": cutoff_date.isoformat()},
    )
    nulled += result.rowcount or 0

    # Null context_snapshot on old rows
    result = session.execute(
        sa_text(
            "UPDATE analysis_history "
            "SET context_snapshot = NULL "
            "WHERE created_at < :cutoff AND context_snapshot IS NOT NULL"
        ),
        {"cutoff": cutoff_date.isoformat()},
    )
    nulled += result.rowcount or 0

    return nulled


def _delete_old_payload_rows(
    session, cutoff_date: date
) -> int:
    """Delete analysis_history_payload rows older than cutoff."""
    result = session.execute(
        sa_text(
            "DELETE FROM analysis_history_payload "
            "WHERE created_at < :cutoff"
        ),
        {"cutoff": cutoff_date.isoformat()},
    )
    return result.rowcount or 0


def _count_payload_legacy_cols(session, cutoff_date: date) -> int:
    """Count legacy payload columns that would be nulled."""
    row = session.execute(
        sa_text(
            "SELECT COUNT(*) FROM analysis_history "
            "WHERE created_at < :cutoff"
            "  AND (raw_result IS NOT NULL OR news_content IS NOT NULL OR context_snapshot IS NOT NULL)"
        ),
        {"cutoff": cutoff_date.isoformat()},
    ).fetchone()
    return row[0] if row else 0


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
            legacy_count = _count_payload_legacy_cols(session, cutoff_date)
            counts['analysis_history_legacy_payload_cols'] = legacy_count
        except Exception as exc:
            logger.warning("Dry-run legacy payload count failed: %s", exc)
            counts['analysis_history_legacy_payload_cols'] = 0

    return counts


def _run_phase2_technical_archive(
    db: DatabaseManager,
    historical_db: HistoricalDatabaseManager,
    cutoff_date: date,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run Phase 2 technical extraction and return stats dict.

    On failure, raises the exception so the caller can handle the safety gate.
    """
    return extract_phase2_technical_history(
        main_db=db,
        historical_db=historical_db,
        cutoff_date=cutoff_date,
        dry_run=dry_run,
    )


def run_archive(
    *,
    days: int = 5,
    dry_run: bool = False,
    cutoff_date: Optional[date] = None,
    skip_vacuum: bool = False,
    technical_only: bool = False,
    skip_technical_archive: bool = False,
    validate_only: bool = False,
) -> int:
    """
    Execute archive maintenance.

    Order:
    1. (Phase 2) Extract structured technical history into historical_market.db
       (skip if --skip-technical-archive)
    2. Null legacy large payload columns (raw_result, news_content, context_snapshot)
    3. Delete expired analysis_history_payload rows
    4. Delete other allowed runtime tables
    5. If not --skip-vacuum: wal_checkpoint(TRUNCATE), VACUUM, ANALYZE
       (outside the destructive transaction)

    Steps 2-5 are skipped in --technical-only mode.
    Step 1 is skipped in --skip-technical-archive mode.

    Returns 0 on success, 1 on error.
    """
    config = get_config()

    if cutoff_date is None:
        cutoff_date = date.today() - timedelta(days=days)

    cutoff_str = cutoff_date.isoformat()
    days_val = (date.today() - cutoff_date).days

    logger.info(
        "Archive maintenance starting: cutoff=%s days_back=%s dry_run=%s "
        "technical_only=%s skip_technical=%s validate_only=%s",
        cutoff_str, days_val, dry_run, technical_only, skip_technical_archive, validate_only,
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
    total_deleted = 0
    total_read = 0
    total_written = 0

    try:
        # ------------------------------------------------------------------
        # Phase 2: Technical archive extraction
        # ------------------------------------------------------------------
        if not skip_technical_archive:
            tech_dry_run = dry_run or validate_only
            logger.info("--- Phase 2: Technical history extraction (dry_run=%s) ---", tech_dry_run)
            try:
                phase2_stats = _run_phase2_technical_archive(
                    db=db,
                    historical_db=historical_db,
                    cutoff_date=cutoff_date,
                    dry_run=tech_dry_run,
                )
                tech_read = sum(
                    s['read'] for s in phase2_stats.values()
                    if isinstance(s, dict) and 'read' in s
                )
                tech_written = sum(
                    s['written'] for s in phase2_stats.values()
                    if isinstance(s, dict) and 'written' in s
                )
                total_read += tech_read
                total_written += tech_written
                logger.info(
                    "Phase 2 extraction complete: read=%s written=%s details=%s",
                    tech_read, tech_written,
                    {k: {'read': v.get('read', 0), 'written': v.get('written', 0)}
                     for k, v in phase2_stats.items() if isinstance(v, dict)},
                )
            except Exception as exc:
                logger.error("Phase 2 technical archive failed: %s", exc)
                historical_db.finish_archive_run(
                    run_id=run_id,
                    success=False,
                    rows_read=total_read,
                    rows_written=total_written,
                    rows_deleted=0,
                    error_message=f"Phase2 extraction failed: {str(exc)[:1900]}",
                )
                return 1

        # --- validate-only: stop after technical extraction ---
        if validate_only:
            logger.info("--- validate-only mode: stopping after technical extraction ---")
            historical_db.finish_archive_run(
                run_id=run_id,
                success=True,
                rows_read=total_read,
                rows_written=total_written,
                rows_deleted=0,
            )
            return 0

        # --- technical-only: stop after technical extraction, no main DB cleanup ---
        if technical_only:
            logger.info("--- technical-only mode: stopping after technical extraction ---")
            historical_db.finish_archive_run(
                run_id=run_id,
                success=True,
                rows_read=total_read,
                rows_written=total_written,
                rows_deleted=0,
            )
            return 0

        # ------------------------------------------------------------------
        # Phase 1: Dry-run or actual cleanup
        # ------------------------------------------------------------------
        if dry_run:
            logger.info("--- DRY RUN --- No data will be deleted ---")
            counts = _dry_run(db, cutoff_date)

            logger.info("=== Dry-run results (cutoff: %s) ===", cutoff_str)
            dry_total = 0
            for table_name, _ts_col in DRY_RUN_COUNT_TABLES:
                c = counts.get(table_name, 0)
                dry_total += c
                protected = table_name in ('analysis_history', 'stock_daily')
                marker = " (PROTECTED — counted, NOT deleted)" if protected else ""
                logger.info("  %s: %d rows%s", table_name, c, marker)
            legacy = counts.get('analysis_history_legacy_payload_cols', 0)
            if legacy:
                logger.info("  analysis_history (legacy payload columns to null): %d columns", legacy)
            logger.info("  TOTAL candidate rows: %d", dry_total + legacy)
            logger.info("=== End dry-run ===")

            total_read += dry_total + legacy

            historical_db.finish_archive_run(
                run_id=run_id,
                success=True,
                rows_read=total_read,
                rows_written=total_written,
                rows_deleted=0,
            )
            return 0

        # --- Actual cleanup (non-dry-run) ---
        legacy_nulled = 0
        payload_deleted = 0

        # Phase 1: Transaction-protected destructive cleanup
        with db.session_scope() as session:
            # 1. Count and null legacy payload columns first
            try:
                legacy_count = _count_payload_legacy_cols(session, cutoff_date)
                if legacy_count:
                    logger.info("  Nulling %d legacy payload columns in analysis_history ...", legacy_count)
                    legacy_nulled = _null_legacy_payload_columns(session, cutoff_date)
                    total_read += legacy_count
                    total_deleted += legacy_nulled
                    logger.info("  Nulled %d legacy payload columns", legacy_nulled)
                counts['analysis_history_legacy_payload_cols'] = legacy_nulled
            except Exception as exc:
                logger.error("Legacy payload nulling failed: %s", exc)
                raise

            # 2. Delete old payload rows
            try:
                payload_count = _count_rows_before_date(session, 'analysis_history_payload', 'created_at', cutoff_date)
                if payload_count:
                    logger.info("  Deleting %d analysis_history_payload rows ...", payload_count)
                    payload_deleted = _delete_old_payload_rows(session, cutoff_date)
                    total_read += payload_count
                    total_deleted += payload_deleted
                    logger.info("  Deleted %d payload rows", payload_deleted)
                counts['analysis_history_payload'] = payload_deleted
            except Exception as exc:
                logger.error("Payload row deletion failed: %s", exc)
                raise

            # 3. Delete other allowed runtime tables
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

        # 4. SQLite maintenance (outside destructive transaction)
        if not skip_vacuum:
            logger.info("Running SQLite maintenance (wal_checkpoint + VACUUM + ANALYZE) ...")
            try:
                with db.session_scope() as session:
                    conn = session.connection()
                    conn.execute(sa_text("PRAGMA wal_checkpoint(TRUNCATE)"))
                    session.commit()
            except Exception as exc:
                logger.warning("wal_checkpoint(TRUNCATE) failed (non-fatal): %s", exc)

            try:
                with db.session_scope() as session:
                    conn = session.connection()
                    conn.execute(sa_text("VACUUM"))
                    session.commit()
            except Exception as exc:
                logger.warning("VACUUM failed (non-fatal): %s", exc)

            try:
                with db.session_scope() as session:
                    conn = session.connection()
                    conn.execute(sa_text("ANALYZE"))
                    session.commit()
            except Exception as exc:
                logger.warning("ANALYZE failed (non-fatal): %s", exc)

            logger.info("SQLite maintenance complete")

        logger.info(
            "Archive complete: deleted=%d rows (legacy_nulled=%d payload_deleted=%d) "
            "across %d tables | historical_written=%d",
            total_deleted, legacy_nulled, payload_deleted, len(ALLOWED_DELETE_TABLES) + 1,
            total_written,
        )

        historical_db.finish_archive_run(
            run_id=run_id,
            success=True,
            rows_read=total_read,
            rows_written=total_written,
            rows_deleted=total_deleted,
        )
        return 0

    except Exception as exc:
        logger.error("Archive failed: %s", exc)
        historical_db.finish_archive_run(
            run_id=run_id,
            success=False,
            rows_read=total_read,
            rows_written=total_written,
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
    parser.add_argument(
        '--technical-only', action='store_true',
        help='Only extract Phase 2 technical history; skip main DB cleanup',
    )
    parser.add_argument(
        '--skip-technical-archive', action='store_true',
        help='Skip Phase 2 technical extraction entirely',
    )
    parser.add_argument(
        '--validate-only', action='store_true',
        help='Run Phase 2 extraction in dry-run mode and stop (no writes, no cleanup)',
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
        technical_only=args.technical_only,
        skip_technical_archive=args.skip_technical_archive,
        validate_only=args.validate_only,
    )


if __name__ == "__main__":
    sys.exit(main())
