"""
Core BigQuery → PostgreSQL table transfer logic.

Design principles:
- Chunked streaming: never materialise the full table in memory
- Idempotent: upsert rows by conflict key so reruns are safe
- Lineage: records a transfer_run row in bq_ops for every execution
- Observable: returns a stats dict consumed by downstream Airflow tasks
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, date, time, timezone
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .client import BigQueryClient, BQTableInfo
from .schema import create_table_ddl, alter_table_add_columns_sql, upsert_sql, insert_sql, pg_type_for

log = logging.getLogger(__name__)


# ── Type coercion ─────────────────────────────────────────────────────────────

def _coerce(value: Any) -> Any:
    """Convert BigQuery Python types to psycopg3-compatible types."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return json.dumps([_coerce(v) for v in value])
    if isinstance(value, dict):
        return json.dumps({k: _coerce(v) for k, v in value.items()})
    if isinstance(value, Decimal):
        return float(value)
    # datetime / date / time are handled natively by psycopg3
    return value


def _coerce_row(row: dict, loaded_at: datetime) -> dict:
    coerced = {k: _coerce(v) for k, v in row.items()}
    coerced["_bq_loaded_at"] = loaded_at
    return coerced


# ── Transfer run tracking ─────────────────────────────────────────────────────

def _start_run(
    conn: psycopg.Connection,
    table_key: str,
    run_type: str,
    dag_run_id: str | None,
    task_id: str | None,
    watermark_start: Any | None,
) -> str:
    row = conn.execute(
        """
        INSERT INTO bq_ops.transfer_runs
            (table_key, run_type, dag_run_id, airflow_task_id, watermark_start, status)
        VALUES (%s, %s, %s, %s, %s, 'running')
        RETURNING run_id::TEXT
        """,
        (
            table_key,
            run_type,
            dag_run_id,
            task_id,
            json.dumps(watermark_start) if watermark_start is not None else None,
        ),
    ).fetchone()
    conn.commit()
    return row["run_id"]


def _finish_run(
    conn: psycopg.Connection,
    run_id: str,
    rows_transferred: int,
    bytes_scanned: int,
    watermark_end: Any | None,
    status: str = "success",
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE bq_ops.transfer_runs SET
            finished_at      = NOW(),
            status           = %s,
            rows_transferred = %s,
            bytes_scanned    = %s,
            watermark_end    = %s,
            error_message    = %s
        WHERE run_id = %s::UUID
        """,
        (
            status,
            rows_transferred,
            bytes_scanned,
            json.dumps(watermark_end, default=str) if watermark_end is not None else None,
            error_message,
            run_id,
        ),
    )
    conn.commit()


def _upsert_sync_state(
    conn: psycopg.Connection,
    table_config: dict,
    rows_transferred: int,
    watermark_end: Any | None,
    status: str,
    error_message: str | None,
    is_historical: bool,
) -> None:
    wm = json.dumps(watermark_end, default=str) if watermark_end is not None else None
    conn.execute(
        """
        INSERT INTO bq_ops.sync_state (
            table_key, bq_project, bq_dataset, bq_table,
            pg_schema, pg_table, incremental_column,
            last_watermark_value, last_synced_at,
            rows_synced_last_run, total_rows_synced,
            last_run_status, error_message,
            historical_completed_at
        ) VALUES (
            %(table_key)s, %(bq_project)s, %(bq_dataset)s, %(bq_table)s,
            %(pg_schema)s, %(pg_table)s, %(incremental_column)s,
            %(wm)s, NOW(),
            %(rows)s, %(rows)s,
            %(status)s, %(err)s,
            %(hist_at)s
        )
        ON CONFLICT (table_key) DO UPDATE SET
            last_watermark_value    = EXCLUDED.last_watermark_value,
            last_synced_at          = EXCLUDED.last_synced_at,
            rows_synced_last_run    = EXCLUDED.rows_synced_last_run,
            total_rows_synced       = bq_ops.sync_state.total_rows_synced + EXCLUDED.rows_synced_last_run,
            last_run_status         = EXCLUDED.last_run_status,
            error_message           = CASE WHEN EXCLUDED.last_run_status = 'success'
                                          THEN NULL ELSE EXCLUDED.error_message END,
            historical_completed_at = COALESCE(
                                          EXCLUDED.historical_completed_at,
                                          bq_ops.sync_state.historical_completed_at
                                      ),
            updated_at              = NOW()
        """,
        {
            "table_key":          table_config["table_key"],
            "bq_project":         table_config["bq_project"],
            "bq_dataset":         table_config["bq_dataset"],
            "bq_table":           table_config["bq_table"],
            "pg_schema":          table_config["pg_schema"],
            "pg_table":           table_config.get("pg_table", table_config["bq_table"]),
            "incremental_column": table_config.get("incremental_column"),
            "wm":                 wm,
            "rows":               rows_transferred,
            "status":             status,
            "err":                error_message,
            "hist_at":            None,  # set only by verify task after all chunks complete
        },
    )
    conn.commit()


# ── Core transfer ─────────────────────────────────────────────────────────────

def transfer_table(
    bq_client: BigQueryClient,
    warehouse_dsn: str,
    table_config: dict,
    run_type: str = "historical",
    dag_run_id: str | None = None,
    task_id: str | None = None,
    where_clause: str | None = None,
    watermark_start: Any | None = None,
    chunk_size: int = 50_000,
) -> dict:
    """
    Transfer one BigQuery table to PostgreSQL.

    table_config keys:
        bq_project         str   – GCP project
        bq_dataset         str   – BigQuery dataset
        bq_table           str   – BigQuery table name
        pg_schema          str   – Target PG schema  (created if absent)
        pg_table           str   – Target PG table   (defaults to bq_table)
        conflict_columns   list  – PK columns for ON CONFLICT; omit for append
        update_columns     list  – Columns to overwrite on conflict (optional)
        incremental_column str   – Column used as watermark (optional)
        table_key          str   – "{bq_dataset}.{bq_table}" (auto-generated if absent)

    Returns a stats dict with rows_transferred, elapsed_seconds, etc.
    """
    # Normalise config
    tc = {**table_config}
    tc.setdefault("pg_table", tc["bq_table"])
    tc.setdefault("table_key", f"{tc['bq_dataset']}.{tc['bq_table']}")
    tc.setdefault("conflict_columns", [])
    tc.setdefault("update_columns", None)
    tc.setdefault("incremental_column", None)

    pg_schema = tc["pg_schema"]
    pg_table  = tc["pg_table"]
    table_key = tc["table_key"]

    log.info("Starting %s transfer: %s → %s.%s", run_type, table_key, pg_schema, pg_table)

    table_info: BQTableInfo = bq_client.get_table_info(tc["bq_dataset"], tc["bq_table"])
    columns = [f.name for f in table_info.schema] + ["_bq_loaded_at"]

    # Build SQL
    if tc["conflict_columns"]:
        dml = upsert_sql(pg_schema, pg_table, columns, tc["conflict_columns"], tc["update_columns"])
    else:
        dml = insert_sql(pg_schema, pg_table, columns)

    ddl = create_table_ddl(
        pg_schema, pg_table, table_info.schema,
        primary_key=tc["conflict_columns"] or None,
    )

    total_rows  = 0
    chunks_done = 0
    max_wm: Any = None
    started_at  = datetime.now(timezone.utc)

    alter_stmts = alter_table_add_columns_sql(pg_schema, pg_table, table_info.schema)

    with psycopg.connect(warehouse_dsn, autocommit=False, row_factory=dict_row) as conn:
        # Ensure table exists, then add any new columns from BQ schema evolution
        conn.execute(ddl)
        for stmt in alter_stmts:
            conn.execute(stmt)
        conn.commit()

        # Open run record
        run_id = _start_run(conn, table_key, run_type, dag_run_id, task_id, watermark_start)

        conflict_cols = tc["conflict_columns"]

        try:
            with conn.cursor() as cur:
                for chunk in bq_client.stream_table(table_info, where_clause=where_clause, chunk_size=chunk_size):
                    loaded_at = datetime.now(timezone.utc)
                    rows = [_coerce_row(r, loaded_at) for r in chunk]

                    # Skip rows with NULL values in any conflict (PK) column —
                    # they would violate the NOT NULL constraint on the PK.
                    if conflict_cols:
                        skipped = sum(1 for r in rows if any(r.get(c) is None for c in conflict_cols))
                        if skipped:
                            log.warning("[%s] skipping %d rows with NULL PK columns", table_key, skipped)
                            rows = [r for r in rows if all(r.get(c) is not None for c in conflict_cols)]

                    if not rows:
                        continue

                    cur.executemany(dml, rows)
                    conn.commit()

                    # Track max watermark within this chunk
                    inc_col = tc["incremental_column"]
                    if inc_col:
                        for r in rows:
                            v = r.get(inc_col)
                            if v is not None and (max_wm is None or v > max_wm):
                                max_wm = v

                    total_rows  += len(rows)
                    chunks_done += 1
                    log.info(
                        "[%s] chunk %d → %d rows (running total: %d)",
                        table_key, chunks_done, len(rows), total_rows,
                    )

        except Exception as exc:
            error_msg = str(exc)
            log.exception("Transfer failed for %s", table_key)
            _finish_run(conn, run_id, total_rows, 0, max_wm, "failed", error_msg)
            _upsert_sync_state(conn, tc, total_rows, max_wm, "failed", error_msg, is_historical=(run_type == "historical"))
            raise

        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        _finish_run(conn, run_id, total_rows, 0, max_wm, "success")
        _upsert_sync_state(
            conn, tc, total_rows, max_wm, "success", None,
            is_historical=(run_type == "historical"),
        )

    log.info(
        "Completed %s transfer of %s: %d rows in %.1fs (%.0f rows/s)",
        run_type, table_key, total_rows, elapsed, total_rows / max(elapsed, 1),
    )

    # Serialise watermark for XCom (must be JSON-safe)
    wm_serialised = str(max_wm) if isinstance(max_wm, (datetime, date, time)) else max_wm

    return {
        "table_key":         table_key,
        "pg_target":         f"{pg_schema}.{pg_table}",
        "rows_transferred":  total_rows,
        "chunks":            chunks_done,
        "elapsed_seconds":   round(elapsed, 2),
        "rows_per_second":   round(total_rows / max(elapsed, 1), 1),
        "watermark_end":     wm_serialised,
        "run_type":          run_type,
    }
