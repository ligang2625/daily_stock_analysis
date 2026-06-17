# -*- coding: utf-8 -*-
"""
===================================
Historical Database Manager
===================================

Independent SQLite database for archived/expired runtime records.
Separate from the main DatabaseManager singleton to avoid leakage.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session as OrmSession

from src.config import get_config

logger = logging.getLogger(__name__)

HistoricalBase = declarative_base()


class ArchiveRun(HistoricalBase):
    """Ledger for each archive maintenance run."""

    __tablename__ = 'archive_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, nullable=False, default=datetime.now)
    finished_at = Column(DateTime, nullable=True)
    cutoff_date = Column(String(16), nullable=False)
    status = Column(String(16), nullable=False, default='running')  # running / success / failed
    main_db_path = Column(Text, nullable=True)
    historical_db_path = Column(Text, nullable=True)
    rows_read = Column(Integer, nullable=True, default=0)
    rows_written = Column(Integer, nullable=True, default=0)
    rows_deleted = Column(Integer, nullable=True, default=0)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)


class HistoricalDatabaseManager:
    """
    Manages the historical/archive database.

    Independent from the main DatabaseManager — no singleton leakage.
    Each instance creates its own engine and session factory.
    """

    def __init__(self, db_path: Optional[str] = None):
        config = get_config()
        if db_path is None:
            db_path = config.historical_database_path

        db_path = str(db_path)
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        db_url = f"sqlite:///{Path(db_path).absolute()}"
        self._engine = create_engine(db_url, echo=False, pool_pre_ping=True)
        self._SessionLocal = sessionmaker(bind=self._engine, autocommit=False, autoflush=False)
        HistoricalBase.metadata.create_all(self._engine)
        logger.info("Historical database initialized: %s", db_path)

    def get_session(self) -> OrmSession:
        return self._SessionLocal()

    def create_archive_run(
        self,
        cutoff_date: str,
        main_db_path: Optional[str] = None,
        historical_db_path: Optional[str] = None,
    ) -> int:
        """Create a new archive_run row and return its id."""
        session = self.get_session()
        try:
            run = ArchiveRun(
                started_at=datetime.now(),
                cutoff_date=cutoff_date,
                status='running',
                main_db_path=main_db_path,
                historical_db_path=historical_db_path,
                rows_read=0,
                rows_written=0,
                rows_deleted=0,
            )
            session.add(run)
            session.commit()
            run_id = run.id
            logger.info("Archive run started: id=%s cutoff=%s", run_id, cutoff_date)
            return run_id
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def finish_archive_run(
        self,
        run_id: int,
        success: bool,
        rows_read: int = 0,
        rows_written: int = 0,
        rows_deleted: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """Mark an archive run as finished (success or failed)."""
        session = self.get_session()
        try:
            run = session.get(ArchiveRun, run_id)
            if run is not None:
                run.finished_at = datetime.now()
                run.status = 'success' if success else 'failed'
                run.rows_read = rows_read or 0
                run.rows_written = rows_written or 0
                run.rows_deleted = rows_deleted or 0
                if error_message:
                    run.error_message = error_message[:2000]
                session.commit()
                logger.info(
                    "Archive run finished: id=%s status=%s read=%s written=%s deleted=%s",
                    run_id, run.status, rows_read, rows_written, rows_deleted,
                )
            else:
                logger.warning("Archive run not found: id=%s", run_id)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_last_successful_run(self) -> Optional[ArchiveRun]:
        """Return the most recent successful archive run, or None."""
        from sqlalchemy import desc, select as sa_select

        session = self.get_session()
        try:
            run = session.execute(
                sa_select(ArchiveRun)
                .where(ArchiveRun.status == 'success')
                .order_by(desc(ArchiveRun.finished_at))
                .limit(1)
            ).scalars().first()
            return run
        except Exception:
            return None
        finally:
            session.close()
