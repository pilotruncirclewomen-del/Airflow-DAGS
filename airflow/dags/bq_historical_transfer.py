"""
DAG: bq_historical_transfer
────────────────────────────
One-time (manually triggered) full-load of every configured BigQuery table
into the DigitalOcean warehouse PostgreSQL database.

Flow
────
discover_tables_and_estimate_cost
    └─► [parallel] transfer_table  ×N  (dynamic task mapping)
            └─► [parallel] verify_row_counts  ×N
                    └─► finalize

Operator notes
──────────────
• Set BQ_TABLE_MANIFEST Airflow Variable (JSON list) to control which tables
  are transferred.  If the variable is absent the DAG auto-discovers every
  table in BQ_DATASET.
• Table manifest schema (each entry):
    {
      "bq_project":         "my-gcp-project",
      "bq_dataset":         "analytics",
      "bq_table":           "messages",
      "pg_schema":          "bq_import",         // optional, default "bq_import"
      "pg_table":           "messages",           // optional, default bq_table
      "conflict_columns":   ["id"],              // [] = append, no PK dedup
      "update_columns":     ["status", "updated_at"], // optional
      "incremental_column": "created_at"         // optional
    }
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


# ── DAG ───────────────────────────────────────────────────────────────────────
@dag(
    dag_id="bq_historical_transfer",
    description="[RETIRED] One-time full-load — replaced by per-table bq_sync__* DAGs",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "warehouse", "historical", "retired"],
    doc_md=__doc__,
    is_paused_upon_creation=True,
)
def bq_historical_transfer():

    # ── Step 1: discover tables and estimate transfer cost ────────────────────
    @task
    def discover_tables_and_estimate_cost() -> list[dict]:
        """
        Load table manifest from Airflow Variable 'BQ_TABLE_MANIFEST'.
        If the variable does not exist, auto-discover all tables in BQ_DATASET.
        Runs a BigQuery dry-run per table and logs the cost estimate.
        """
        from bigquery.client import BigQueryClient

        bq = BigQueryClient(
            project=BQ_PROJECT,
            credentials_path=BQ_CREDENTIALS_PATH or None,
            credentials_json=BQ_CREDENTIALS_JSON or None,
        )

        # Load manifest from Variable, or auto-discover
        manifest_json = Variable.get("BQ_TABLE_MANIFEST", default_var=None)
        if manifest_json:
            raw_manifest: list[dict] = json.loads(manifest_json)
            log.info("Loaded %d tables from BQ_TABLE_MANIFEST variable", len(raw_manifest))
        else:
            if not BQ_DATASET:
                raise ValueError(
                    "Set BQ_TABLE_MANIFEST Airflow Variable OR set BQ_DATASET env var "
                    "so the DAG can auto-discover tables."
                )
            discovered = bq.list_tables(BQ_DATASET)
            raw_manifest = [
                {
                    "bq_project":         BQ_PROJECT,
                    "bq_dataset":         BQ_DATASET,
                    "bq_table":           t.table,
                    "pg_schema":          "bq_import",
                    "conflict_columns":   [],
                    "incremental_column": t.partition_field,
                }
                for t in discovered
            ]
            log.info("Auto-discovered %d tables in %s.%s", len(raw_manifest), BQ_PROJECT, BQ_DATASET)

        # Normalise and enrich each entry
        total_cost_usd   = 0.0
        total_bytes      = 0
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

            try:
                tbl_info = bq.get_table_info(entry["bq_dataset"], entry["bq_table"])
                estimate = bq.estimate_cost(tbl_info)
                entry["_size_gb"]       = round(tbl_info.size_gb, 3)
                entry["_num_rows"]      = tbl_info.num_rows
                entry["_cost_estimate"] = estimate.as_dict()
                total_cost_usd += estimate.total_cost_usd
                total_bytes    += estimate.bytes_to_scan

                log.info(
                    "  %-40s  rows=%-10d  size=%6.2fGB  est_cost=$%.4f",
                    entry["table_key"],
                    tbl_info.num_rows,
                    tbl_info.size_gb,
                    estimate.total_cost_usd,
                )
            except Exception as exc:
                log.warning("Could not estimate cost for %s: %s", entry["table_key"], exc)
                entry["_size_gb"]       = None
                entry["_num_rows"]      = None
                entry["_cost_estimate"] = {}

            enriched.append(entry)

        # ── Cost summary ──────────────────────────────────────────────────────
        log.info("=" * 70)
        log.info("TRANSFER COST ESTIMATE SUMMARY")
        log.info("=" * 70)
        log.info("Tables to transfer : %d", len(enriched))
        log.info("Total bytes scanned: %.2f GB", total_bytes / 1e9)
        log.info("BQ on-demand cost  : first 1 TB/month is FREE ($5/TB after)")
        log.info("Estimated BQ cost  : $%.4f", total_cost_usd)
        log.info("Estimated egress   : ~$%.4f (%.2f GB × $0.12)", total_bytes / 1e9 * 0.12, total_bytes / 1e9)
        log.info("TOTAL ESTIMATE     : $%.4f", total_cost_usd)
        log.info(
            "Note: < 1 TB scanned this month → BigQuery query cost is $0 (free tier)."
            " Egress charges still apply."
        )
        log.info("=" * 70)

        return enriched

    # ── Step 2: ensure bq_ops schema exists ──────────────────────────────────
    @task
    def bootstrap_warehouse_ops() -> None:
        """Apply sql/010_bq_ops_schema.sql to the warehouse DB."""
        import pathlib
        import psycopg

        sql_path = pathlib.Path("/opt/airflow/dags/../sql/010_bq_ops_schema.sql")
        # Resolve relative to the repo root mounted inside the container
        candidates = [
            pathlib.Path("/opt/airflow/sql/010_bq_ops_schema.sql"),
            pathlib.Path("/opt/airflow/dags/sql/010_bq_ops_schema.sql"),
        ]
        resolved = next((p for p in candidates if p.exists()), None)
        if resolved is None:
            log.warning(
                "010_bq_ops_schema.sql not found; assuming bq_ops schema already exists."
            )
            return

        ddl = resolved.read_text()
        with psycopg.connect(WAREHOUSE_DSN) as conn:
            conn.execute(ddl)
            conn.commit()
        log.info("bq_ops schema bootstrapped from %s", resolved)

    # ── Step 3: transfer each table (dynamic mapping) ─────────────────────────
    @task(retries=2, retry_delay=timedelta(seconds=60))
    def transfer_table(table_config: dict) -> dict:
        """
        Stream one BigQuery table into the warehouse PostgreSQL.
        Returns a stats dict that is passed downstream via XCom.
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
            run_type      = "historical",
            chunk_size    = BQ_CHUNK_ROWS,
        )
        log.info("Transfer stats: %s", json.dumps(stats, default=str))
        return stats

    # ── Step 4: verify row counts match ──────────────────────────────────────
    @task
    def verify_row_counts(stats: dict) -> dict:
        """
        Compare row count in BigQuery vs PostgreSQL.
        Logs a warning if counts differ (does not fail — BQ may be eventually consistent).
        """
        import psycopg
        from psycopg.rows import dict_row
        from bigquery.client import BigQueryClient

        table_key  = stats["table_key"]
        pg_target  = stats["pg_target"]
        bq_dataset, bq_table = table_key.split(".", 1)

        bq = BigQueryClient(
            project=BQ_PROJECT,
            credentials_path=BQ_CREDENTIALS_PATH or None,
            credentials_json=BQ_CREDENTIALS_JSON or None,
        )
        bq_count = bq.count_rows(bq_dataset, bq_table)

        pg_schema, pg_table = pg_target.split(".", 1)
        with psycopg.connect(WAREHOUSE_DSN, row_factory=dict_row) as conn:
            pg_count = conn.execute(
                f'SELECT COUNT(*) AS cnt FROM "{pg_schema}"."{pg_table}"'
            ).fetchone()["cnt"]

        match = "✓ MATCH" if bq_count == pg_count else "⚠ MISMATCH"
        log.info(
            "%s  %s — BQ rows: %d, PG rows: %d",
            match, table_key, bq_count, pg_count,
        )
        return {
            **stats,
            "bq_row_count": bq_count,
            "pg_row_count": pg_count,
            "count_match":  bq_count == pg_count,
        }

    # ── Step 5: print final summary ───────────────────────────────────────────
    @task
    def finalize(verification_results: list[dict]) -> None:
        total_rows = sum(r.get("rows_transferred", 0) for r in verification_results)
        mismatches = [r for r in verification_results if not r.get("count_match", True)]

        log.info("=" * 70)
        log.info("HISTORICAL TRANSFER COMPLETE")
        log.info("=" * 70)
        log.info("Tables transferred : %d", len(verification_results))
        log.info("Total rows loaded  : %d", total_rows)
        for r in verification_results:
            elapsed = r.get("elapsed_seconds", 0)
            log.info(
                "  %-40s  rows=%d  time=%.1fs  match=%s",
                r.get("table_key", "?"),
                r.get("rows_transferred", 0),
                elapsed,
                r.get("count_match", "?"),
            )
        if mismatches:
            log.warning("Row-count mismatches detected for: %s", [r["table_key"] for r in mismatches])
        log.info("=" * 70)
        log.info("Incremental syncs will now run daily via bq_incremental_sync DAG.")

    # ── Wire up ───────────────────────────────────────────────────────────────
    table_configs = discover_tables_and_estimate_cost()
    bootstrap_warehouse_ops()

    transfer_results  = transfer_table.expand(table_config=table_configs)
    verify_results    = verify_row_counts.expand(stats=transfer_results)
    finalize(verify_results)


bq_historical_transfer()
