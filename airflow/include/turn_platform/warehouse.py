from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from turn_platform.config import load_settings


REFRESHABLE_MATERIALIZED_VIEWS = [
    "turn_analytics.mv_pipeline_freshness",
    "turn_analytics.mv_message_volume_daily",
    "turn_analytics.mv_status_funnel_daily",
    "turn_analytics.mv_contact_snapshots_daily",
    "turn_analytics.mv_data_sanity_snapshot",
    "turn_analytics.mv_data_sanity_trend_30d",
]


def connect_warehouse() -> psycopg.Connection:
    settings = load_settings()
    return psycopg.connect(settings.warehouse.dsn, row_factory=dict_row)


def apply_sql_file(conn: psycopg.Connection, path: Path) -> None:
    conn.execute(path.read_text())
    conn.commit()


def ensure_warehouse_objects() -> None:
    settings = load_settings()
    sql_files = [
        settings.pipeline.sql_dir / "001_turn_platform_schema.sql",
        settings.pipeline.sql_dir / "002_turn_platform_analytics.sql",
    ]
    with connect_warehouse() as conn:
        for sql_file in sql_files:
            apply_sql_file(conn, sql_file)


def ensure_raw_warehouse_objects() -> None:
    settings = load_settings()
    sql_file = settings.pipeline.sql_dir / "001_turn_platform_schema.sql"
    with connect_warehouse() as conn:
        apply_sql_file(conn, sql_file)


def bootstrap_warehouse() -> None:
    ensure_warehouse_objects()
    with connect_warehouse() as conn:
        refresh_materialized_views(conn)


def refresh_materialized_views(conn: psycopg.Connection) -> None:
    for view_name in REFRESHABLE_MATERIALIZED_VIEWS:
        conn.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
    conn.commit()


def start_run(
    conn: psycopg.Connection,
    *,
    run_id: str,
    dag_id: str,
    task_id: str,
    entity_type: str,
    source: str,
    window_start,
    window_end,
) -> None:
    conn.execute(
        """
        INSERT INTO turn_ops.ingestion_runs (
            run_id,
            dag_id,
            task_id,
            entity_type,
            source,
            window_start,
            window_end
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO NOTHING
        """,
        (run_id, dag_id, task_id, entity_type, source, window_start, window_end),
    )
    conn.commit()


def finish_run(
    conn: psycopg.Connection,
    *,
    run_id: str,
    status: str,
    rows_seen: int,
    rows_written: int,
    details: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    rows_deduped = max(rows_seen - rows_written, 0)
    conn.execute(
        """
        UPDATE turn_ops.ingestion_runs
        SET finished_at = NOW(),
            status = %s,
            rows_seen = %s,
            rows_written = %s,
            rows_deduped = %s,
            details = %s::jsonb,
            error_message = %s
        WHERE run_id = %s
        """,
        (
            status,
            rows_seen,
            rows_written,
            rows_deduped,
            json.dumps(details or {}),
            error_message,
            run_id,
        ),
    )
    conn.commit()


def record_error(
    conn: psycopg.Connection,
    *,
    run_id: str,
    entity_type: str,
    error_stage: str,
    error_message: str,
    error_context: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO turn_ops.ingestion_errors (
            run_id,
            entity_type,
            error_stage,
            error_message,
            error_context
        )
        VALUES (%s, %s, %s, %s, %s::jsonb)
        """,
        (run_id, entity_type, error_stage, error_message, json.dumps(error_context or {})),
    )
    conn.commit()


def get_pipeline_state(conn: psycopg.Connection, pipeline_name: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM turn_ops.pipeline_state WHERE pipeline_name = %s",
        (pipeline_name,),
    ).fetchone()
    return dict(row) if row else None


def upsert_pipeline_state(
    conn: psycopg.Connection,
    *,
    pipeline_name: str,
    entity_type: str,
    last_cursor_at,
    last_window_start,
    last_window_end,
    last_run_id: str,
    rows_seen_last_run: int,
    rows_written_last_run: int,
    state_json: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO turn_ops.pipeline_state (
            pipeline_name,
            entity_type,
            last_cursor_at,
            last_window_start,
            last_window_end,
            last_run_id,
            last_success_at,
            rows_seen_last_run,
            rows_written_last_run,
            state_json,
            updated_at
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            NOW(),
            %s,
            %s,
            %s::jsonb,
            NOW()
        )
        ON CONFLICT (pipeline_name) DO UPDATE
        SET entity_type = EXCLUDED.entity_type,
            last_cursor_at = EXCLUDED.last_cursor_at,
            last_window_start = EXCLUDED.last_window_start,
            last_window_end = EXCLUDED.last_window_end,
            last_run_id = EXCLUDED.last_run_id,
            last_success_at = NOW(),
            rows_seen_last_run = EXCLUDED.rows_seen_last_run,
            rows_written_last_run = EXCLUDED.rows_written_last_run,
            state_json = EXCLUDED.state_json,
            updated_at = NOW()
        """,
        (
            pipeline_name,
            entity_type,
            last_cursor_at,
            last_window_start,
            last_window_end,
            last_run_id,
            rows_seen_last_run,
            rows_written_last_run,
            json.dumps(state_json or {}),
        ),
    )
    conn.commit()


def _insert_many(
    conn: psycopg.Connection,
    *,
    table_name: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    conflict_clause: str,
) -> int:
    if not rows:
        return 0

    value_placeholder = "(" + ", ".join(["%s"] * len(columns)) + ")"
    values_sql = ", ".join([value_placeholder] * len(rows))
    sql = (
        f"INSERT INTO {table_name} ({', '.join(columns)}) "
        f"VALUES {values_sql} "
        f"{conflict_clause} "
        "RETURNING 1"
    )
    params: list[Any] = [value for row in rows for value in row]
    with conn.cursor() as cur:
        cur.execute(sql, params)
        inserted = len(cur.fetchall())
    conn.commit()
    return inserted


def insert_messages(conn: psycopg.Connection, rows: list[tuple[Any, ...]]) -> int:
    return _insert_many(
        conn,
        table_name="turn_raw.messages_raw",
        columns=[
            "source",
            "source_delivery_id",
            "source_run_id",
            "turn_message_id",
            "number_id",
            "contact_wa_id",
            "contact_profile_name",
            "chat_uuid",
            "contact_uuid",
            "direction",
            "message_type",
            "author_id",
            "author_name",
            "author_type",
            "message_timestamp",
            "turn_inserted_at",
            "last_status",
            "last_status_timestamp",
            "labels",
            "payload",
            "payload_hash",
        ],
        rows=rows,
        conflict_clause="ON CONFLICT (source, turn_message_id) DO NOTHING",
    )


def insert_statuses(conn: psycopg.Connection, rows: list[tuple[Any, ...]]) -> int:
    return _insert_many(
        conn,
        table_name="turn_raw.statuses_raw",
        columns=[
            "source",
            "source_delivery_id",
            "source_run_id",
            "turn_message_id",
            "status_event_id",
            "status_event_key",
            "recipient_wa_id",
            "conversation_id",
            "conversation_origin_type",
            "status",
            "status_timestamp",
            "pricing_category",
            "pricing_type",
            "pricing_model",
            "billable",
            "payload",
            "payload_hash",
        ],
        rows=rows,
        conflict_clause="ON CONFLICT (source, status_event_key) DO NOTHING",
    )


def insert_contacts(conn: psycopg.Connection, rows: list[tuple[Any, ...]]) -> int:
    return _insert_many(
        conn,
        table_name="turn_raw.contacts_raw",
        columns=[
            "source",
            "source_run_id",
            "turn_contact_id",
            "number_id",
            "whatsapp_id",
            "whatsapp_profile_name",
            "turn_inserted_at",
            "turn_updated_at",
            "details",
            "payload",
            "payload_hash",
        ],
        rows=rows,
        conflict_clause=(
            "ON CONFLICT (source, turn_contact_id, turn_updated_at, payload_hash) DO NOTHING"
        ),
    )
