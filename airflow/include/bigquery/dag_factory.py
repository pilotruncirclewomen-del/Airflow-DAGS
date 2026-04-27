"""
Per-table DAG factory.

Every table DAG has exactly 3 micro-tasks:

  1. plan_load
       Reads the current state from bq_ops.sync_state (and the warehouse
       table itself).  Decides whether we are in historical or incremental
       mode and returns a list of chunk configs.

       Historical mode  → list of monthly/quarterly date-range chunks
                          starting from the highest updated_at already in
                          the warehouse table (auto-resume after any crash).
       Incremental mode → single chunk: watermark → now.

  2. transfer_chunk   (dynamically mapped over the chunk list)
       Each chunk is an independent, atomic unit of work:
         • its own WHERE clause
         • committed to PG in 50k-row batches (already upserted, idempotent)
         • retries only this chunk on failure
       Because every chunk is idempotent, re-running is always safe.

  3. verify
       Compares total BQ row count vs PG row count.
       Marks historical_completed_at in bq_ops.sync_state once all
       chunks succeed.
       Never fails the DAG — only logs warnings.  Dashboard visibility
       over hard failures.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta, date
from dateutil.relativedelta import relativedelta

from airflow.sdk import dag as dag_decorator, task as task_decorator

log = logging.getLogger(__name__)

# ── Environment (resolved at import time, same as other DAGs) ─────────────────
BQ_CREDENTIALS_PATH = os.environ.get("BQ_CREDENTIALS_PATH", "/opt/airflow/credentials/bigquery-sa.json")
BQ_CREDENTIALS_JSON = os.environ.get("BQ_CREDENTIALS_JSON", "")
WAREHOUSE_DSN       = os.environ.get("WAREHOUSE_DSN", "")

# ── Tier settings ─────────────────────────────────────────────────────────────
_TIER = {
    #            chunk_size  chunk_months  retries  retry_min
    "heavy":  (  50_000,     1,            3,        10),
    "medium": (  25_000,     3,            3,         5),
    "light":  (  10_000,     None,         2,         2),
}


# ── Chunk planning ────────────────────────────────────────────────────────────

def _monthly_chunks(start: date, end: date, months: int) -> list[dict]:
    """Return list of {where_clause, label} dicts covering [start, end)."""
    chunks = []
    cursor = start.replace(day=1)
    while cursor < end:
        chunk_end = cursor + relativedelta(months=months)
        if chunk_end > end:
            chunk_end = end
        where = (
            f"`updated_at` >= '{cursor.isoformat()}' "
            f"AND `updated_at` < '{chunk_end.isoformat()}'"
        )
        chunks.append({
            "where_clause":    where,
            "watermark_start": cursor.isoformat(),
            "watermark_end":   chunk_end.isoformat(),
            "label":           f"{cursor.strftime('%Y-%m')}",
        })
        cursor = chunk_end
    return chunks


# ── Factory ───────────────────────────────────────────────────────────────────

def make_sync_dag(tc: dict):
    """
    Returns a fully wired Airflow DAG for a single BigQuery table.
    tc must come from table_configs.TABLE_CONFIGS (already enriched).
    """
    bq_table    = tc["bq_table"]
    table_key   = tc["table_key"]
    size_tier   = tc.get("size_tier", "medium")
    history_start = tc.get("history_start", "2022-01-01")

    chunk_size, chunk_months, n_retries, retry_min = _TIER[size_tier]
    chunk_size = tc.get("chunk_size_override", chunk_size)

    dag_id = f"bq_sync__{bq_table}"

    @dag_decorator(
        dag_id=dag_id,
        description=f"Daily BQ → warehouse sync for {bq_table} ({size_tier})",
        schedule="0 2 * * *",
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        tags=["bigquery", "warehouse", size_tier],
        default_args={
            "retries":     n_retries,
            "retry_delay": timedelta(minutes=retry_min),
        },
    )
    def sync_dag():

        # ── Task 1: plan ──────────────────────────────────────────────────────
        @task_decorator(retries=1)
        def plan_load() -> list[dict]:
            """
            Determine what needs loading and return a chunk list.

            Logic:
              • Query bq_ops.sync_state for this table.
              • If historical_completed_at IS NULL:
                  - Query MAX(updated_at) already in the warehouse table.
                  - Generate monthly chunks from that point to NOW.
                  - (If warehouse table is empty, start from history_start.)
              • If historical_completed_at IS NOT NULL:
                  - Return a single incremental chunk from stored watermark to NOW.
            """
            import psycopg
            from psycopg.rows import dict_row

            now = datetime.now(timezone.utc).date()

            with psycopg.connect(WAREHOUSE_DSN, row_factory=dict_row) as conn:

                # Read sync_state
                state = conn.execute(
                    "SELECT last_watermark_value, historical_completed_at "
                    "FROM bq_ops.sync_state WHERE table_key = %s",
                    (table_key,),
                ).fetchone()

                historical_done = (
                    state is not None and state["historical_completed_at"] is not None
                )

                if historical_done:
                    # ── Incremental: single chunk from last watermark ──────────
                    wm = state["last_watermark_value"]
                    if wm:
                        # wm stored as JSON string e.g. '"2026-04-01T12:00:00+00:00"'
                        import json as _json
                        wm_val = _json.loads(wm) if isinstance(wm, str) and wm.startswith('"') else wm
                    else:
                        # safety fallback: last 3 days
                        wm_val = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()

                    # BQ DATETIME columns do not accept timezone offsets —
                    # strip +00:00 / Z suffix (all our datetimes are UTC).
                    wm_bq = (
                        str(wm_val)
                        .replace("+00:00", "")
                        .replace("Z", "")
                        .replace(" ", "T")   # psycopg may return "2026-04-18 10:16:54"
                    )

                    chunks = [{
                        "where_clause":    f"`updated_at` > '{wm_bq}'",
                        "watermark_start": str(wm_val),
                        "watermark_end":   now.isoformat(),
                        "label":           "incremental",
                        "run_type":        "incremental",
                    }]
                    log.info("[%s] incremental mode — watermark=%s", bq_table, wm_bq)

                else:
                    # ── Historical: chunked date ranges ───────────────────────
                    # Find highest updated_at already loaded (auto-resume)
                    try:
                        row = conn.execute(
                            f'SELECT MAX(updated_at) AS max_wm FROM "{tc["pg_schema"]}"."{bq_table}"'
                        ).fetchone()
                        max_loaded = row["max_wm"].date() if row["max_wm"] else None
                    except Exception:
                        max_loaded = None

                    if max_loaded:
                        resume_from = max_loaded
                        log.info("[%s] resuming historical from %s", bq_table, resume_from)
                    else:
                        resume_from = date.fromisoformat(history_start)
                        log.info("[%s] starting historical from %s", bq_table, resume_from)

                    if chunk_months:
                        chunks = _monthly_chunks(resume_from, now, chunk_months)
                    else:
                        # light tables: single full load
                        chunks = [{
                            "where_clause":    None,
                            "watermark_start": history_start,
                            "watermark_end":   now.isoformat(),
                            "label":           "full",
                            "run_type":        "historical",
                        }]

                    for c in chunks:
                        c["run_type"] = "historical"

                    log.info("[%s] historical plan: %d chunks", bq_table, len(chunks))

            # Inject table config into every chunk (needed by transfer_chunk)
            for c in chunks:
                c["table_config"] = tc

            return chunks

        # ── Task 2: transfer_chunk (dynamically mapped) ───────────────────────
        @task_decorator(retries=n_retries, retry_delay=timedelta(minutes=retry_min))
        def transfer_chunk(chunk: dict) -> dict:
            """
            Transfer one date-range chunk from BigQuery into the warehouse.
            Fully idempotent — safe to retry or re-run.
            """
            from bigquery.client import BigQueryClient
            from bigquery.transfer import transfer_table as _transfer

            table_cfg = chunk["table_config"]
            label     = chunk.get("label", "?")
            run_type  = chunk.get("run_type", "historical")

            log.info("[%s] transferring chunk %s  run_type=%s  where=%s",
                     bq_table, label, run_type, chunk.get("where_clause"))

            bq = BigQueryClient(
                project=table_cfg["bq_project"],
                credentials_path=BQ_CREDENTIALS_PATH or None,
                credentials_json=BQ_CREDENTIALS_JSON or None,
            )

            stats = _transfer(
                bq_client       = bq,
                warehouse_dsn   = WAREHOUSE_DSN,
                table_config    = table_cfg,
                run_type        = run_type,
                where_clause    = chunk.get("where_clause"),
                watermark_start = chunk.get("watermark_start"),
                chunk_size      = chunk_size,
            )
            stats["chunk_label"] = label
            return stats

        # ── Task 3: verify ────────────────────────────────────────────────────
        @task_decorator(retries=1)
        def verify(all_stats: list[dict]) -> None:
            """
            Compare BQ total row count vs PG total row count.
            Mark historical_completed_at if all chunks were historical and
            no chunks remain (i.e., we've caught up to today).
            Never fails the DAG — row-count mismatches are warnings only.
            """
            import psycopg
            from psycopg.rows import dict_row
            from bigquery.client import BigQueryClient

            bq = BigQueryClient(
                project=tc["bq_project"],
                credentials_path=BQ_CREDENTIALS_PATH or None,
                credentials_json=BQ_CREDENTIALS_JSON or None,
            )

            bq_count = bq.count_rows(tc["bq_dataset"], bq_table)

            with psycopg.connect(WAREHOUSE_DSN, row_factory=dict_row) as conn:
                pg_row = conn.execute(
                    f'SELECT COUNT(*) AS cnt FROM "{tc["pg_schema"]}"."{bq_table}"'
                ).fetchone()
                pg_count = pg_row["cnt"]

                match_symbol = "✓" if bq_count == pg_count else "⚠"
                log.info(
                    "%s [%s]  BQ=%d  PG=%d  diff=%d",
                    match_symbol, bq_table, bq_count, pg_count, bq_count - pg_count,
                )

                # Mark historical complete if:
                # - all chunks were historical
                # - PG has >= 99% of BQ rows (allow minor BQ eventual consistency lag)
                all_historical = all(
                    s.get("run_type") == "historical" for s in all_stats if s
                )
                coverage = pg_count / bq_count if bq_count > 0 else 0

                if all_historical and coverage >= 0.99:
                    conn.execute(
                        """
                        UPDATE bq_ops.sync_state
                        SET historical_completed_at = NOW(),
                            updated_at = NOW()
                        WHERE table_key = %s
                        """,
                        (table_key,),
                    )
                    conn.commit()
                    log.info("[%s] historical load COMPLETE — marked in sync_state", bq_table)
                elif all_historical:
                    log.warning(
                        "[%s] historical chunks done but coverage=%.1f%% — "
                        "will retry remaining rows next run",
                        bq_table, coverage * 100,
                    )

        # ── Wire tasks ────────────────────────────────────────────────────────
        chunks   = plan_load()
        stats    = transfer_chunk.expand(chunk=chunks)
        verify(stats)

    return sync_dag()
