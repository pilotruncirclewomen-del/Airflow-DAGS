"""
Helpers for reading BigQuery sync state from the warehouse DB.
Used by Airflow tasks to determine incremental query windows.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger(__name__)


def get_watermark(warehouse_dsn: str, table_key: str) -> Any | None:
    """
    Return the stored watermark for *table_key*, or None if no sync has run yet.
    The watermark is the max value of the incremental_column from the last run.
    """
    with psycopg.connect(warehouse_dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT last_watermark_value FROM bq_ops.sync_state WHERE table_key = %s",
            (table_key,),
        ).fetchone()
    if not row or row["last_watermark_value"] is None:
        return None
    return json.loads(row["last_watermark_value"])


def get_all_states(warehouse_dsn: str) -> list[dict]:
    """Return all rows from bq_ops.sync_state (for dashboards / logging)."""
    with psycopg.connect(warehouse_dsn, row_factory=dict_row) as conn:
        rows = conn.execute("SELECT * FROM bq_ops.sync_state ORDER BY table_key").fetchall()
    return [dict(r) for r in rows]


def is_historical_complete(warehouse_dsn: str, table_key: str) -> bool:
    """Return True if the historical load has been successfully completed for *table_key*."""
    with psycopg.connect(warehouse_dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT historical_completed_at FROM bq_ops.sync_state WHERE table_key = %s",
            (table_key,),
        ).fetchone()
    if not row:
        return False
    return row["historical_completed_at"] is not None
