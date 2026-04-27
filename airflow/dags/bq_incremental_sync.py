"""
DAG: bq_incremental_sync
─────────────────────────
Daily incremental sync of BigQuery tables into the warehouse PostgreSQL.
Only rows newer than the stored watermark (last_watermark_value per table)
are fetched, coerced, and upserted.

Schedule: 02:00 UTC daily
Prerequisite: bq_historical_transfer must have completed for each table.

Flow
────
load_sync_manifest
    └─► [parallel] sync_incremental_table  ×N  (dynamic task mapping)
            └─► refresh_analytics_views
                    └─► log_sync_summary
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

log = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
BQ_PROJECT            = os.environ["BQ_PROJECT"]
BQ_DATASET            = os.environ.get("BQ_DATASET", "")
BQ_CREDENTIALS_PATH   = os.environ.get("BQ_CREDENTIALS_PATH", "/opt/airflow/credentials/bigquery-sa.json")
BQ_CREDENTIALS_JSON   = os.environ.get("BQ_CREDENTIALS_JSON", "")
WAREHOUSE_DSN         = os.environ["WAREHOUSE_DSN"]
BQ_CHUNK_ROWS         = int(os.environ.get("BQ_CHUNK_ROWS", "50000"))

# Fallback lookback window if no watermark is stored (safety net)
_DEFAULT_LOOKBACK_DAYS = int(os.environ.get("BQ_INCREMENTAL_LOOKBACK_DAYS", "3"))


# ── DAG ───────────────────────────────────────────────────────────────────────
@dag(
    dag_id="bq_incremental_sync",
    description="[RETIRED] Daily incremental sync — replaced by per-table bq_sync__* DAGs",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "warehouse", "incremental", "retired"],
    doc_md=__doc__,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    is_paused_upon_creation=True,
)
def bq_incremental_sync():

    # ── Step 1: load manifest + watermarks ───────────────────────────────────
    @task
    def load_sync_manifest() -> list[dict]:
        """
        Load the table manifest (same format as bq_historical_transfer) from
        Airflow Variable 'BQ_TABLE_MANIFEST'.
        Injects the stored watermark for each table so transfer tasks know
        where to start querying BigQuery.

        Tables without an incremental_column get a full reload each day.
        """
        from bigquery.state import get_watermark

        manifest_json = Variable.get("BQ_TABLE_MANIFEST", default_var=None)
        if not manifest_json:
            raise ValueError(
                "Airflow Variable 'BQ_TABLE_MANIFEST' not set. "
                "Configure it before running incremental syncs."
            )

        raw_manifest: list[dict] = json.loads(manifest_json)
        enriched: list[dict] = []

        for entry in raw_manifest:
            entry = {**entry}
            entry.setdefault("bq_project",         BQ_PROJECT)
            entry.setdefault("pg_schema",           "bq_import")
            entry.setdefault("pg_table",            entry["bq_table"])
            entry.setdefault("conflict_columns",    [])
            entry.setdefault("update_columns",      None)
            entry.setdefault("incremental_column",  None)
            entry["table_key"] = f"{entry['bq_dataset']}.{entry['bq_table']}"

            # Inject watermark
            watermark = get_watermark(WAREHOUSE_DSN, entry["table_key"])
            entry["_watermark"] = str(watermark) if watermark else None

            inc_col = entry.get("incremental_column")
            if inc_col and watermark:
                entry["_where_clause"] = f"`{inc_col}` > '{watermark}'"
            elif inc_col:
                # No watermark yet → use safety lookback
                fallback = (
                    datetime.now(timezone.utc) - timedelta(days=_DEFAULT_LOOKBACK_DAYS)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
                entry["_where_clause"] = f"`{inc_col}` > '{fallback}'"
                log.warning(
                    "%s has no stored watermark; falling back to last %d days.",
                    entry["table_key"], _DEFAULT_LOOKBACK_DAYS,
                )
            else:
                # No incremental column → full reload
                entry["_where_clause"] = None
                log.info("%s has no incremental_column — full reload.", entry["table_key"])

            log.info(
                "%-40s  watermark=%-30s  where=%s",
                entry["table_key"],
                entry["_watermark"] or "none",
                entry["_where_clause"] or "full reload",
            )
            enriched.append(entry)

        return enriched

    # ── Step 2: incremental table sync (dynamic mapping) ─────────────────────
    @task(retries=2, retry_delay=timedelta(seconds=120))
    def sync_incremental_table(table_config: dict) -> dict:
        """
        Fetch rows from BigQuery newer than the stored watermark and upsert
        them into the warehouse PostgreSQL table.
        """
        from bigquery.client import BigQueryClient
        from bigquery.transfer import transfer_table as _transfer

        bq = BigQueryClient(
            project=table_config["bq_project"],
            credentials_path=BQ_CREDENTIALS_PATH or None,
            credentials_json=BQ_CREDENTIALS_JSON or None,
        )

        stats = _transfer(
            bq_client     = bq,
            warehouse_dsn = WAREHOUSE_DSN,
            table_config  = table_config,
            run_type      = "incremental",
            where_clause  = table_config.get("_where_clause"),
            watermark_start = table_config.get("_watermark"),
            chunk_size    = BQ_CHUNK_ROWS,
        )
        log.info("Incremental sync stats: %s", json.dumps(stats, default=str))
        return stats

    # ── Step 3: refresh analytics materialized views ──────────────────────────
    @task
    def refresh_analytics_views(sync_results: list[dict]) -> None:
        """
        Refresh PostgreSQL materialized views after incremental loads.
        Targets the same 6 views that the Turn.io pipeline refreshes.
        """
        import psycopg

        views = [
            "turn_analytics.mv_pipeline_freshness",
            "turn_analytics.mv_message_volume_daily",
            "turn_analytics.mv_status_funnel_daily",
            "turn_analytics.mv_contact_snapshots_daily",
            "turn_analytics.mv_data_sanity_snapshot",
            "turn_analytics.mv_data_sanity_trend_30d",
        ]

        with psycopg.connect(WAREHOUSE_DSN, autocommit=True) as conn:
            for view in views:
                try:
                    conn.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
                    log.info("Refreshed %s", view)
                except Exception as exc:
                    # Don't fail the DAG if a view doesn't exist yet
                    log.warning("Could not refresh %s: %s", view, exc)

    # ── Step 4: summary ───────────────────────────────────────────────────────
    @task
    def log_sync_summary(sync_results: list[dict]) -> None:
        total = sum(r.get("rows_transferred", 0) for r in sync_results)
        log.info("=" * 60)
        log.info("INCREMENTAL SYNC COMPLETE — %s UTC", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
        log.info("Tables synced : %d", len(sync_results))
        log.info("Total new rows: %d", total)
        for r in sync_results:
            log.info(
                "  %-40s  +%d rows  %.1fs  wm=%s",
                r.get("table_key", "?"),
                r.get("rows_transferred", 0),
                r.get("elapsed_seconds", 0),
                r.get("watermark_end", "—"),
            )
        log.info("=" * 60)

    # ── Wire up ───────────────────────────────────────────────────────────────
    manifest      = load_sync_manifest()
    sync_results  = sync_incremental_table.expand(table_config=manifest)
    refresh_analytics_views(sync_results) >> log_sync_summary(sync_results)


bq_incremental_sync()
