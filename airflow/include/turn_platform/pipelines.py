from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from turn_platform.config import load_settings
from turn_platform.turnio import TurnExportClient
from turn_platform.utils import (
    chunked,
    first_present,
    json_hash,
    parse_turn_timestamp,
    safe_bigint,
    utc_now,
)
from turn_platform.warehouse import (
    connect_warehouse,
    ensure_warehouse_objects,
    finish_run,
    get_pipeline_state,
    insert_contacts,
    insert_messages,
    insert_statuses,
    record_error,
    refresh_materialized_views,
    start_run,
    upsert_pipeline_state,
)


@dataclass(frozen=True)
class EntityPipelineConfig:
    entity_type: str
    source_name: str
    pipeline_name: str
    default_lookback_minutes: int
    overlap_seconds: int


def _entity_config(entity_type: str) -> EntityPipelineConfig:
    settings = load_settings()
    mapping = {
        "messages": EntityPipelineConfig(
            entity_type="messages",
            source_name="export_messages",
            pipeline_name="turn_messages_incremental",
            default_lookback_minutes=settings.pipeline.messages_default_lookback_minutes,
            overlap_seconds=settings.pipeline.messages_overlap_seconds,
        ),
        "statuses": EntityPipelineConfig(
            entity_type="statuses",
            source_name="export_statuses",
            pipeline_name="turn_statuses_incremental",
            default_lookback_minutes=settings.pipeline.statuses_default_lookback_minutes,
            overlap_seconds=settings.pipeline.statuses_overlap_seconds,
        ),
        "contacts": EntityPipelineConfig(
            entity_type="contacts",
            source_name="export_contacts",
            pipeline_name="turn_contacts_incremental",
            default_lookback_minutes=settings.pipeline.contacts_default_lookback_minutes,
            overlap_seconds=settings.pipeline.contacts_overlap_seconds,
        ),
    }
    return mapping[entity_type]


def _message_contact_wa_id(message: dict, direction: str | None) -> str | None:
    if direction == "outbound":
        return message.get("to") or message.get("recipient_id")
    return message.get("from") or message.get("recipient_id")


def _normalize_message_row(record: dict, run_id: str) -> tuple | None:
    message = record["message"]
    message_timestamp = parse_turn_timestamp(message.get("timestamp"))
    if message_timestamp is None or not message.get("id"):
        return None

    payload = {
        "message": message,
        "contact_profile_name": record.get("contact_profile_name"),
    }
    direction = first_present(message, ("_vnd", "v1", "direction"))
    return (
        "export_messages",
        None,
        run_id,
        str(message["id"]),
        safe_bigint(first_present(message, ("_vnd", "v1", "number", "id"), ("number_id",))),
        _message_contact_wa_id(message, direction),
        record.get("contact_profile_name"),
        first_present(message, ("_vnd", "v1", "chat", "uuid")),
        first_present(message, ("_vnd", "v1", "chat", "contact_uuid")),
        direction,
        message.get("type"),
        first_present(message, ("_vnd", "v1", "author", "id")),
        first_present(message, ("_vnd", "v1", "author", "name")),
        first_present(message, ("_vnd", "v1", "author", "type")),
        message_timestamp,
        parse_turn_timestamp(first_present(message, ("_vnd", "v1", "inserted_at"))),
        first_present(message, ("_vnd", "v1", "last_status")),
        parse_turn_timestamp(first_present(message, ("_vnd", "v1", "last_status_timestamp"))),
        json.dumps(first_present(message, ("_vnd", "v1", "labels")) or []),
        json.dumps(payload),
        json_hash(payload),
    )


def _normalize_status_row(status: dict, run_id: str) -> tuple | None:
    status_timestamp = parse_turn_timestamp(status.get("timestamp"))
    if status_timestamp is None or not status.get("id") or not status.get("status"):
        return None

    return (
        "export_statuses",
        None,
        run_id,
        str(status["id"]),
        str(status.get("id")),
        json_hash(
            {
                "message_id": status.get("id"),
                "status": status.get("status"),
                "timestamp": status.get("timestamp"),
                "recipient_id": status.get("recipient_id"),
            }
        ),
        status.get("recipient_id"),
        first_present(status, ("conversation", "id")),
        first_present(status, ("conversation", "origin", "type")),
        status.get("status"),
        status_timestamp,
        first_present(status, ("pricing", "category")),
        first_present(status, ("pricing", "type")),
        first_present(status, ("pricing", "pricing_model")),
        first_present(status, ("pricing", "billable")),
        json.dumps(status),
        json_hash(status),
    )


def _normalize_contact_row(contact: dict, run_id: str) -> tuple | None:
    updated_at = parse_turn_timestamp(contact.get("updated_at"))
    if updated_at is None or not contact.get("id"):
        return None

    details = contact.get("details") or {}
    return (
        "export_contacts",
        run_id,
        str(contact["id"]),
        safe_bigint(contact.get("number_id")),
        details.get("whatsapp_id"),
        details.get("whatsapp_profile_name"),
        parse_turn_timestamp(contact.get("inserted_at")),
        updated_at,
        json.dumps(details),
        json.dumps(contact),
        json_hash(contact),
    )


def _sync_entity_window(
    *,
    entity_type: str,
    pipeline_name: str,
    source_name: str,
    from_ts: datetime,
    until_ts: datetime,
    dag_id: str,
    task_id: str,
    advance_cursor: bool,
) -> dict:
    settings = load_settings()
    ensure_warehouse_objects()
    client = TurnExportClient()
    run_id = f"{pipeline_name}-{uuid4()}"
    rows_seen = 0
    rows_written = 0
    last_record_ts = from_ts

    with connect_warehouse() as conn:
        start_run(
            conn,
            run_id=run_id,
            dag_id=dag_id,
            task_id=task_id,
            entity_type=entity_type,
            source=source_name,
            window_start=from_ts,
            window_end=until_ts,
        )

        try:
            for batch in chunked(
                client.iter_records(entity_type, from_ts=from_ts, until_ts=until_ts),
                settings.pipeline.batch_size,
            ):
                if entity_type == "messages":
                    normalized = [
                        row
                        for row in (
                            _normalize_message_row(item, run_id)
                            for item in batch
                        )
                        if row is not None
                    ]
                    rows_seen += len(normalized)
                    if normalized:
                        rows_written += insert_messages(conn, normalized)
                        last_record_ts = max(row[14] for row in normalized)

                elif entity_type == "statuses":
                    normalized = [
                        row
                        for row in (
                            _normalize_status_row(item, run_id)
                            for item in batch
                        )
                        if row is not None
                    ]
                    rows_seen += len(normalized)
                    if normalized:
                        rows_written += insert_statuses(conn, normalized)
                        last_record_ts = max(row[9] for row in normalized)

                elif entity_type == "contacts":
                    normalized = [
                        row
                        for row in (
                            _normalize_contact_row(item, run_id)
                            for item in batch
                        )
                        if row is not None
                    ]
                    rows_seen += len(normalized)
                    if normalized:
                        rows_written += insert_contacts(conn, normalized)
                        last_record_ts = max(row[7] for row in normalized)

            if advance_cursor:
                upsert_pipeline_state(
                    conn,
                    pipeline_name=pipeline_name,
                    entity_type=entity_type,
                    last_cursor_at=until_ts,
                    last_window_start=from_ts,
                    last_window_end=until_ts,
                    last_run_id=run_id,
                    rows_seen_last_run=rows_seen,
                    rows_written_last_run=rows_written,
                    state_json={"last_record_ts": last_record_ts.isoformat()},
                )

            finish_run(
                conn,
                run_id=run_id,
                status="success",
                rows_seen=rows_seen,
                rows_written=rows_written,
                details={
                    "advance_cursor": advance_cursor,
                    "last_record_ts": last_record_ts.isoformat(),
                },
            )

        except Exception as exc:
            conn.rollback()
            record_error(
                conn,
                run_id=run_id,
                entity_type=entity_type,
                error_stage="sync_entity_window",
                error_message=str(exc),
                error_context={
                    "window_start": from_ts.isoformat(),
                    "window_end": until_ts.isoformat(),
                },
            )
            finish_run(
                conn,
                run_id=run_id,
                status="failed",
                rows_seen=rows_seen,
                rows_written=rows_written,
                details={"advance_cursor": advance_cursor},
                error_message=str(exc),
            )
            raise

    return {
        "entity_type": entity_type,
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "rows_seen": rows_seen,
        "rows_written": rows_written,
        "window_start": from_ts.isoformat(),
        "window_end": until_ts.isoformat(),
        "advance_cursor": advance_cursor,
    }


def sync_incremental(entity_type: str, *, dag_id: str, task_id: str) -> dict:
    settings = load_settings()
    entity = _entity_config(entity_type)
    now_utc = utc_now()
    until_ts = now_utc - timedelta(seconds=settings.turnio.safety_lag_seconds)

    with connect_warehouse() as conn:
        state = get_pipeline_state(conn, entity.pipeline_name)

    if state and state.get("last_cursor_at"):
        cursor_at = state["last_cursor_at"]
        from_ts = cursor_at - timedelta(seconds=entity.overlap_seconds)
    else:
        from_ts = until_ts - timedelta(minutes=entity.default_lookback_minutes)

    return _sync_entity_window(
        entity_type=entity.entity_type,
        pipeline_name=entity.pipeline_name,
        source_name=entity.source_name,
        from_ts=from_ts,
        until_ts=until_ts,
        dag_id=dag_id,
        task_id=task_id,
        advance_cursor=True,
    )


def run_backfill(
    entity_type: str,
    *,
    from_iso: str,
    until_iso: str,
    dag_id: str,
    task_id: str,
) -> dict:
    entity = _entity_config(entity_type)
    return _sync_entity_window(
        entity_type=entity.entity_type,
        pipeline_name=f"turn_{entity_type}_backfill",
        source_name=entity.source_name,
        from_ts=datetime.fromisoformat(from_iso.replace("Z", "+00:00")),
        until_ts=datetime.fromisoformat(until_iso.replace("Z", "+00:00")),
        dag_id=dag_id,
        task_id=task_id,
        advance_cursor=False,
    )


def refresh_analytics(dag_id: str, task_id: str) -> dict:
    ensure_warehouse_objects()
    run_id = f"turn_refresh_analytics-{uuid4()}"
    with connect_warehouse() as conn:
        start_run(
            conn,
            run_id=run_id,
            dag_id=dag_id,
            task_id=task_id,
            entity_type="analytics",
            source="warehouse",
            window_start=utc_now(),
            window_end=utc_now(),
        )
        try:
            refresh_materialized_views(conn)
            finish_run(
                conn,
                run_id=run_id,
                status="success",
                rows_seen=0,
                rows_written=0,
                details={"refreshed_materialized_views": True},
            )
        except Exception as exc:
            conn.rollback()
            record_error(
                conn,
                run_id=run_id,
                entity_type="analytics",
                error_stage="refresh_materialized_views",
                error_message=str(exc),
            )
            finish_run(
                conn,
                run_id=run_id,
                status="failed",
                rows_seen=0,
                rows_written=0,
                error_message=str(exc),
            )
            raise

    return {"run_id": run_id, "refreshed_materialized_views": True}
