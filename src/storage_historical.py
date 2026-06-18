# -*- coding: utf-8 -*-
"""
===================================
Historical Database Manager
===================================

Independent SQLite database for archived/expired runtime records.
Separate from the main DatabaseManager singleton to avoid leakage.

Phase 2 adds structured technical history tables:
- historical_stock_daily
- historical_intraday_quote_points
- historical_postmarket_technical_summary
- historical_market_light_daily
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Index,
    create_engine,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
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


# ---------------------------------------------------------------------------
# Phase 2 historical tables
# ---------------------------------------------------------------------------

class HistoricalStockDaily(HistoricalBase):
    """Archived stock daily data extracted from main stock_daily table."""

    __tablename__ = 'historical_stock_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(16), nullable=False)
    code_norm = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=True)
    trade_date = Column(String(16), nullable=False)

    # OHLC
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    pre_close = Column(Float, nullable=True)

    pct_chg = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    amount = Column(Float, nullable=True)
    turnover_rate = Column(Float, nullable=True)

    # Technical indicators
    ma5 = Column(Float, nullable=True)
    ma10 = Column(Float, nullable=True)
    ma20 = Column(Float, nullable=True)
    ma60 = Column(Float, nullable=True)
    volume_ratio = Column(Float, nullable=True)
    bias5 = Column(Float, nullable=True)
    bias10 = Column(Float, nullable=True)
    bias20 = Column(Float, nullable=True)

    data_source = Column(String(64), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint('code_norm', 'trade_date', name='uix_hsd_code_norm_date'),
        Index('ix_hsd_code_norm', 'code_norm'),
        Index('ix_hsd_trade_date', 'trade_date'),
        Index('ix_hsd_code_norm_trade_date', 'code_norm', 'trade_date'),
    )


class HistoricalIntradayQuotePoint(HistoricalBase):
    """Archived intraday quote points extracted from intraday_events."""

    __tablename__ = 'historical_intraday_quote_points'

    id = Column(Integer, primary_key=True, autoincrement=True)
    query_date = Column(String(16), nullable=False)
    market = Column(String(8), nullable=True)
    stock_code = Column(String(16), nullable=False)
    stock_code_norm = Column(String(16), nullable=False, index=True)
    stock_name = Column(String(64), nullable=True)
    timestamp = Column(String(32), nullable=False)
    market_local_timestamp = Column(String(32), nullable=True)
    snapshot_id = Column(String(64), nullable=True)
    current_price = Column(Float, nullable=True)
    change_pct = Column(Float, nullable=True)
    volume_ratio = Column(Float, nullable=True)
    turnover = Column(Float, nullable=True)
    amount = Column(Float, nullable=True)
    current_volume = Column(Float, nullable=True)
    event_type = Column(String(32), nullable=True)
    technical_position = Column(String(32), nullable=True)
    price_position = Column(String(32), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('stock_code_norm', 'timestamp', 'snapshot_id',
                         name='uix_hiqp_code_ts_sid'),
        Index('ix_hiqp_code_norm', 'stock_code_norm'),
        Index('ix_hiqp_query_date', 'query_date'),
        Index('ix_hiqp_code_norm_query_date', 'stock_code_norm', 'query_date'),
        Index('ix_hiqp_code_norm_timestamp', 'stock_code_norm', 'timestamp'),
    )


class HistoricalPostmarketTechnicalSummary(HistoricalBase):
    """Archived postmarket technical analysis summaries from analysis_history."""

    __tablename__ = 'historical_postmarket_technical_summary'

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_history_id = Column(Integer, nullable=True, index=True)
    code = Column(String(16), nullable=False)
    code_norm = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=True)
    trade_date = Column(String(16), nullable=False)
    analysis_phase = Column(String(16), nullable=True)
    report_type = Column(String(16), nullable=True)
    close = Column(Float, nullable=True)
    pct_chg = Column(Float, nullable=True)
    volume_ratio = Column(Float, nullable=True)
    ma5 = Column(Float, nullable=True)
    ma10 = Column(Float, nullable=True)
    ma20 = Column(Float, nullable=True)
    ma60 = Column(Float, nullable=True)
    bias5 = Column(Float, nullable=True)
    bias10 = Column(Float, nullable=True)
    bias20 = Column(Float, nullable=True)
    ideal_buy = Column(Float, nullable=True)
    secondary_buy = Column(Float, nullable=True)
    stop_loss = Column(Float, nullable=True)
    take_profit = Column(Float, nullable=True)
    support_level = Column(Float, nullable=True)
    resistance_level = Column(Float, nullable=True)
    trend_prediction = Column(String(64), nullable=True)
    operation_advice = Column(String(32), nullable=True)
    risk_level = Column(Integer, nullable=True)
    technical_summary = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code_norm', 'trade_date', 'analysis_phase', 'report_type',
                         name='uix_hpts_code_date_phase_type'),
        Index('ix_hpts_code_norm', 'code_norm'),
        Index('ix_hpts_trade_date', 'trade_date'),
        Index('ix_hpts_code_norm_trade_date', 'code_norm', 'trade_date'),
    )


class HistoricalMarketLightDaily(HistoricalBase):
    """Archived Market Light daily snapshots from market review analysis."""

    __tablename__ = 'historical_market_light_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    region = Column(String(8), nullable=False)
    trade_date = Column(String(16), nullable=False)
    score = Column(Integer, nullable=True)
    status = Column(String(16), nullable=True)
    label = Column(String(32), nullable=True)
    temperature_label = Column(String(32), nullable=True)
    data_quality = Column(String(16), nullable=True)
    risk_notes = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('region', 'trade_date', name='uix_hmld_region_date'),
        Index('ix_hmld_region', 'region'),
        Index('ix_hmld_trade_date', 'trade_date'),
        Index('ix_hmld_region_trade_date', 'region', 'trade_date'),
    )


# ---------------------------------------------------------------------------
# HistoricalDatabaseManager
# ---------------------------------------------------------------------------


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

    # ------------------------------------------------------------------
    # Phase 2 upsert methods (idempotent)
    # ------------------------------------------------------------------

    def upsert_stock_daily_batch(self, rows: List[Dict[str, Any]]) -> int:
        """Idempotent upsert of HistoricalStockDaily rows.

        Returns the number of rows inserted (not updated).
        """
        if not rows:
            return 0
        session = self.get_session()
        written = 0
        try:
            for row in rows:
                stmt = sqlite_insert(HistoricalStockDaily).values(**row).on_conflict_do_update(
                    index_elements=['code_norm', 'trade_date'],
                    set_={
                        'code': row.get('code'),
                        'market': row.get('market'),
                        'open': row.get('open'),
                        'high': row.get('high'),
                        'low': row.get('low'),
                        'close': row.get('close'),
                        'pre_close': row.get('pre_close'),
                        'pct_chg': row.get('pct_chg'),
                        'volume': row.get('volume'),
                        'amount': row.get('amount'),
                        'turnover_rate': row.get('turnover_rate'),
                        'ma5': row.get('ma5'),
                        'ma10': row.get('ma10'),
                        'ma20': row.get('ma20'),
                        'ma60': row.get('ma60'),
                        'volume_ratio': row.get('volume_ratio'),
                        'bias5': row.get('bias5'),
                        'bias10': row.get('bias10'),
                        'bias20': row.get('bias20'),
                        'data_source': row.get('data_source'),
                        'updated_at': datetime.now(),
                    },
                )
                session.execute(stmt)
                written += 1
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        return written

    def upsert_intraday_quote_points_batch(self, rows: List[Dict[str, Any]]) -> int:
        """Idempotent upsert of HistoricalIntradayQuotePoint rows."""
        if not rows:
            return 0
        session = self.get_session()
        written = 0
        try:
            for row in rows:
                stmt = sqlite_insert(HistoricalIntradayQuotePoint).values(**row).on_conflict_do_update(
                    index_elements=['stock_code_norm', 'timestamp', 'snapshot_id'],
                    set_={
                        'query_date': row.get('query_date'),
                        'market': row.get('market'),
                        'stock_code': row.get('stock_code'),
                        'stock_name': row.get('stock_name'),
                        'market_local_timestamp': row.get('market_local_timestamp'),
                        'current_price': row.get('current_price'),
                        'change_pct': row.get('change_pct'),
                        'volume_ratio': row.get('volume_ratio'),
                        'turnover': row.get('turnover'),
                        'amount': row.get('amount'),
                        'current_volume': row.get('current_volume'),
                        'event_type': row.get('event_type'),
                        'technical_position': row.get('technical_position'),
                        'price_position': row.get('price_position'),
                    },
                )
                session.execute(stmt)
                written += 1
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        return written

    def upsert_postmarket_summaries_batch(self, rows: List[Dict[str, Any]]) -> int:
        """Idempotent upsert of HistoricalPostmarketTechnicalSummary rows."""
        if not rows:
            return 0
        session = self.get_session()
        written = 0
        try:
            for row in rows:
                stmt = sqlite_insert(HistoricalPostmarketTechnicalSummary).values(**row).on_conflict_do_update(
                    index_elements=['code_norm', 'trade_date', 'analysis_phase', 'report_type'],
                    set_={
                        'source_history_id': row.get('source_history_id'),
                        'code': row.get('code'),
                        'market': row.get('market'),
                        'close': row.get('close'),
                        'pct_chg': row.get('pct_chg'),
                        'volume_ratio': row.get('volume_ratio'),
                        'ma5': row.get('ma5'),
                        'ma10': row.get('ma10'),
                        'ma20': row.get('ma20'),
                        'ma60': row.get('ma60'),
                        'bias5': row.get('bias5'),
                        'bias10': row.get('bias10'),
                        'bias20': row.get('bias20'),
                        'ideal_buy': row.get('ideal_buy'),
                        'secondary_buy': row.get('secondary_buy'),
                        'stop_loss': row.get('stop_loss'),
                        'take_profit': row.get('take_profit'),
                        'support_level': row.get('support_level'),
                        'resistance_level': row.get('resistance_level'),
                        'trend_prediction': row.get('trend_prediction'),
                        'operation_advice': row.get('operation_advice'),
                        'risk_level': row.get('risk_level'),
                        'technical_summary': row.get('technical_summary'),
                    },
                )
                session.execute(stmt)
                written += 1
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        return written

    def upsert_market_light_daily_batch(self, rows: List[Dict[str, Any]]) -> int:
        """Idempotent upsert of HistoricalMarketLightDaily rows."""
        if not rows:
            return 0
        session = self.get_session()
        written = 0
        try:
            for row in rows:
                stmt = sqlite_insert(HistoricalMarketLightDaily).values(**row).on_conflict_do_update(
                    index_elements=['region', 'trade_date'],
                    set_={
                        'score': row.get('score'),
                        'status': row.get('status'),
                        'label': row.get('label'),
                        'temperature_label': row.get('temperature_label'),
                        'data_quality': row.get('data_quality'),
                        'risk_notes': row.get('risk_notes'),
                    },
                )
                session.execute(stmt)
                written += 1
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        return written
