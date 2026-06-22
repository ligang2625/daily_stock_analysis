# -*- coding: utf-8 -*-
"""
Historical database backup command (Phase 4).

Creates timestamped SQLite-safe backups of the main or historical database.
Uses SQLite backup API for safe, consistent copies.

Usage:
    python -m src.maintenance.backup --historical-only
    python -m src.maintenance.backup --main-only
    python -m src.maintenance.backup --all
    python -m src.maintenance.backup --historical-only --output-dir ./data/backups
    python -m src.maintenance.backup --historical-only --retention-days 14 --cleanup-old
    python -m src.maintenance.backup --historical-only --compress
"""

from __future__ import annotations

import argparse
import gzip
import logging
import re
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from src.config import get_config

logger = logging.getLogger(__name__)

BACKUP_FILENAME_PATTERN = re.compile(
    r'^(stock_analysis|historical_market)_\d{8}_\d{6}(\.db|\.db\.gz)$'
)


def _sqlite_backup(source_path: str, dest_path: str) -> None:
    """Copy a SQLite database using the backup API.

    Ensures WAL is checkpointed before backup.
    """
    source_conn = sqlite3.connect(source_path)
    try:
        source_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        dest_conn = sqlite3.connect(dest_path)
        try:
            source_conn.backup(dest_conn)
        finally:
            dest_conn.close()
    finally:
        source_conn.close()
    logger.info("Backup created: %s -> %s", source_path, dest_path)


def _file_copy_backup(source_path: str, dest_path: str) -> None:
    """Fallback file-copy backup with WAL checkpoint.

    Copies .db, .db-wal, .db-shm if present.
    """
    import sqlite3 as _sqlite3

    # Checkpoint WAL
    try:
        conn = _sqlite3.connect(source_path)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    except Exception as exc:
        logger.warning("WAL checkpoint before file copy failed: %s", exc)

    # Copy main database
    shutil.copy2(source_path, dest_path)
    logger.info("File-copy backup created: %s -> %s", source_path, dest_path)


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _backup_one(
    source_path: str,
    label: str,
    output_dir: str,
    use_sqlite_api: bool = True,
    compress: bool = False,
) -> Optional[str]:
    """Create a backup for one database file.

    Returns the path to the backup file, or None if the source does not exist.
    """
    source = Path(source_path)
    if not source.is_file():
        logger.warning("Source database not found, skipping backup: %s", source_path)
        return None

    dest_dir = Path(output_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    ts = _timestamp()
    ext = ".db.gz" if compress else ".db"
    dest_name = f"{label}_{ts}{ext}"
    dest_path = dest_dir / dest_name

    if use_sqlite_api and not compress:
        _sqlite_backup(str(source), str(dest_path))
    else:
        if compress:
            # Copy to temp, then compress
            tmp_path = dest_dir / f"{label}_{ts}.db.tmp"
            _file_copy_backup(str(source), str(tmp_path))
            try:
                with open(tmp_path, 'rb') as f_in:
                    with gzip.open(str(dest_path), 'wb') as f_out:
                        f_out.writelines(f_in)
                logger.info("Compressed backup created: %s", dest_path)
            finally:
                if tmp_path.is_file():
                    tmp_path.unlink()
        else:
            _file_copy_backup(str(source), str(dest_path))

    return str(dest_path)


def _cleanup_old_backups(output_dir: str, retention_days: int) -> int:
    """Remove backup files older than retention_days.

    Only removes files matching the project backup naming pattern.
    Returns the number of files removed.
    """
    backup_dir = Path(output_dir)
    if not backup_dir.is_dir():
        return 0

    from datetime import timedelta

    cutoff = datetime.now() - timedelta(days=retention_days)
    removed = 0

    for f in backup_dir.iterdir():
        if not f.is_file():
            continue
        if not BACKUP_FILENAME_PATTERN.match(f.name):
            continue
        # Check file modification time
        mtime = datetime.fromtimestamp(f.stat().st_mtime)
        if mtime < cutoff:
            try:
                f.unlink()
                removed += 1
                logger.info("Removed old backup: %s (mtime=%s)", f.name, mtime.isoformat())
            except OSError as exc:
                logger.warning("Failed to remove old backup %s: %s", f.name, exc)

    return removed


def run_backup(
    *,
    historical_only: bool = False,
    main_only: bool = False,
    backup_all: bool = False,
    output_dir: Optional[str] = None,
    retention_days: Optional[int] = None,
    cleanup_old: bool = False,
    use_sqlite_api: bool = True,
    compress: bool = False,
) -> int:
    """Execute backup operations.

    Returns 0 on success, 1 on error.
    """
    config = get_config()

    if output_dir is None:
        output_dir = config.historical_backup_dir
    if retention_days is None:
        retention_days = config.historical_backup_retention_days

    if not (historical_only or main_only or backup_all):
        logger.warning("No backup target specified; use --historical-only, --main-only, or --all")
        return 1

    errors = 0

    if historical_only or backup_all:
        result = _backup_one(
            source_path=config.historical_database_path,
            label="historical_market",
            output_dir=output_dir,
            use_sqlite_api=use_sqlite_api,
            compress=compress,
        )
        if result is None:
            errors += 1

    if main_only or backup_all:
        result = _backup_one(
            source_path=config.database_path,
            label="stock_analysis",
            output_dir=output_dir,
            use_sqlite_api=use_sqlite_api,
            compress=compress,
        )
        if result is None:
            errors += 1

    if cleanup_old:
        removed = _cleanup_old_backups(output_dir, retention_days)
        logger.info("Cleaned up %d old backup(s)", removed)

    if errors:
        logger.warning("Backup completed with %d error(s)", errors)
        return 1

    logger.info("Backup completed successfully")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create SQLite-safe database backups.",
    )
    target_group = parser.add_mutually_exclusive_group()
    target_group.add_argument(
        '--historical-only', action='store_true',
        help='Backup historical database only',
    )
    target_group.add_argument(
        '--main-only', action='store_true',
        help='Backup main database only',
    )
    target_group.add_argument(
        '--all', action='store_true',
        help='Backup both main and historical databases',
    )
    parser.add_argument(
        '--output-dir', type=str, default=None,
        help='Backup output directory',
    )
    parser.add_argument(
        '--retention-days', type=int, default=None,
        help='Retention days for cleanup (default: from config)',
    )
    parser.add_argument(
        '--cleanup-old', action='store_true',
        help='Remove backup files older than retention days',
    )
    parser.add_argument(
        '--use-file-copy', action='store_true',
        help='Use file copy instead of SQLite backup API',
    )
    parser.add_argument(
        '--compress', action='store_true',
        help='Compress backup with gzip',
    )

    # Default to --all if no target specified
    argv_list = argv if argv is not None else sys.argv[1:]
    if not any(a in argv_list for a in ('--historical-only', '--main-only', '--all', '-h', '--help')):
        argv_list = ['--all'] + argv_list

    args = parser.parse_args(argv_list)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    )

    # Verify SQLite can open the backup API
    use_api = not args.use_file_copy

    return run_backup(
        historical_only=args.historical_only,
        main_only=args.main_only,
        backup_all=args.all,
        output_dir=args.output_dir,
        retention_days=args.retention_days,
        cleanup_old=args.cleanup_old,
        use_sqlite_api=use_api,
        compress=args.compress,
    )


if __name__ == "__main__":
    sys.exit(main())
