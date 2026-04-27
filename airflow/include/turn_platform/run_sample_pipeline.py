from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any
from uuid import uuid4

from turn_platform.config import load_settings
from turn_platform.pipelines import (
    _normalize_contact_row,
    _normalize_message_row,
    _normalize_status_row,
)
from turn_platform.turnio import TurnExportClient
from turn_platform.utils import chunked, isoformat_utc, utc_now
from turn_platform.warehouse import (
    connect_warehouse,
    ensure_raw_warehouse_objects,
    finish_run,
    insert_contacts,
    insert_messages,
    insert_statuses,
    record_error,
    start_run,
)


@dataclass(frozen=True)
class EntityWindow:
    entity_type: str
    lookback_hours: int


def _entity_windows(
    *, messages_hours: int, statuses_hours: int, contacts_hours: int
) -> list[EntityWindow]:
    return [
        EntityWindow(entity_type="messages", lookback_hours=messages_hours),
        EntityWindow(entity_type="statuses", lookback_hours=statuses_hours),
        EntityWindow(entity_type="contacts", lookback_hours=contacts_hours),
    ]


def _table_summary(conn, table_name: str, timestamp_column: str) -> dict[str, Any]:
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS row_count,
            MIN({timestamp_column}) AS min_ts,
            MAX({timestamp_column}) AS max_ts
        FROM {table_name}
        """
    ).fetchone()
    return {
        "table_name": table_name,
        "row_count": row["row_count"] if row else 0,
        "min_ts": row["min_ts"].isoformat() if row and row["min_ts"] else None,
        "max_ts": row["max_ts"].isoformat() if row and row["max_ts"] else None,
    }


def _run_entity_sample(
    *,
    client: TurnExportClient,
    entity_type: str,
    lookback_hours: int,
    now_utc,
) -> dict[str, Any]:
    settings = load_settings()
    from_ts = now_utc - timedelta(hours=lookback_hours)
    run_id = f"manual-sample-{entity_type}-{uuid4()}"
    rows_seen = 0
    rows_written = 0

    with connect_warehouse() as conn:
        start_run(
            conn,
            run_id=run_id,
            dag_id="manual_turnio_sample_pipeline",
            task_id=f"sample_{entity_type}",
            entity_type=entity_type,
            source=f"sample_export_{entity_type}",
            window_start=from_ts,
            window_end=now_utc,
        )

        try:
            for batch in chunked(
                client.iter_records(entity_type, from_ts=from_ts, until_ts=now_utc),
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

                else:
                    raise ValueError(f"Unsupported entity type: {entity_type}")

            finish_run(
                conn,
                run_id=run_id,
                status="success",
                rows_seen=rows_seen,
                rows_written=rows_written,
                details={
                    "sample_window_hours": lookback_hours,
                    "window_start": isoformat_utc(from_ts),
                    "window_end": isoformat_utc(now_utc),
                },
            )

        except Exception as exc:
            conn.rollback()
            record_error(
                conn,
                run_id=run_id,
                entity_type=entity_type,
                error_stage="manual_sample_pipeline",
                error_message=str(exc),
                error_context={
                    "sample_window_hours": lookback_hours,
                    "window_start": isoformat_utc(from_ts),
                    "window_end": isoformat_utc(now_utc),
                },
            )
            finish_run(
                conn,
                run_id=run_id,
                status="failed",
                rows_seen=rows_seen,
                rows_written=rows_written,
                details={"sample_window_hours": lookback_hours},
                error_message=str(exc),
            )
            raise

    return {
        "entity_type": entity_type,
        "lookback_hours": lookback_hours,
        "run_id": run_id,
        "rows_seen": rows_seen,
        "rows_written": rows_written,
        "window_start": isoformat_utc(from_ts),
        "window_end": isoformat_utc(now_utc),
    }


def _run_sample_pipeline(
    *,
    messages_hours: int,
    statuses_hours: int,
    contacts_hours: int,
) -> dict[str, Any]:
    ensure_raw_warehouse_objects()
    now_utc = utc_now()
    client = TurnExportClient()

    results = [
        _run_entity_sample(
            client=client,
            entity_type=window.entity_type,
            lookback_hours=window.lookback_hours,
            now_utc=now_utc,
        )
        for window in _entity_windows(
            messages_hours=messages_hours,
            statuses_hours=statuses_hours,
            contacts_hours=contacts_hours,
        )
    ]

    with connect_warehouse() as conn:
        raw_table_summaries = [
            _table_summary(conn, "turn_raw.messages_raw", "message_timestamp"),
            _table_summary(conn, "turn_raw.statuses_raw", "status_timestamp"),
            _table_summary(conn, "turn_raw.contacts_raw", "turn_updated_at"),
        ]
        recent_runs = conn.execute(
            """
            SELECT
                run_id,
                entity_type,
                source,
                status,
                rows_seen,
                rows_written,
                window_start,
                window_end,
                finished_at
            FROM turn_ops.ingestion_runs
            WHERE dag_id = 'manual_turnio_sample_pipeline'
            ORDER BY started_at DESC
            LIMIT 10
            """
        ).fetchall()

    return {
        "window_end_utc": isoformat_utc(now_utc),
        "entity_windows": [
            asdict(window)
            for window in _entity_windows(
                messages_hours=messages_hours,
                statuses_hours=statuses_hours,
                contacts_hours=contacts_hours,
            )
        ],
        "backfill_results": results,
        "raw_table_summaries": raw_table_summaries,
        "recent_runs": [
            {
                "run_id": row["run_id"],
                "entity_type": row["entity_type"],
                "source": row["source"],
                "status": row["status"],
                "rows_seen": row["rows_seen"],
                "rows_written": row["rows_written"],
                "window_start": row["window_start"].isoformat() if row["window_start"] else None,
                "window_end": row["window_end"].isoformat() if row["window_end"] else None,
                "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
            }
            for row in recent_runs
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a narrow Turn.io sample pipeline into separate raw warehouse tables."
    )
    parser.add_argument("--messages-hours", type=int, default=72)
    parser.add_argument("--statuses-hours", type=int, default=72)
    parser.add_argument("--contacts-hours", type=int, default=168)
    args = parser.parse_args()

    result = _run_sample_pipeline(
        messages_hours=args.messages_hours,
        statuses_hours=args.statuses_hours,
        contacts_hours=args.contacts_hours,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
