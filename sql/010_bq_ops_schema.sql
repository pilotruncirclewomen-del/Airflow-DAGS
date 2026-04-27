-- BigQuery → Warehouse operational tracking schema
-- Applied once on warehouse-db before running any BQ transfer DAGs.

CREATE SCHEMA IF NOT EXISTS bq_ops;

-- ─────────────────────────────────────────────────────────────────────────────
-- Sync state: one row per source table, updated after every successful run
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bq_ops.sync_state (
    table_key               TEXT PRIMARY KEY,          -- "{bq_dataset}.{bq_table}"
    bq_project              TEXT NOT NULL,
    bq_dataset              TEXT NOT NULL,
    bq_table                TEXT NOT NULL,
    pg_schema               TEXT NOT NULL,
    pg_table                TEXT NOT NULL,
    incremental_column      TEXT,                      -- NULL → full reload each run
    last_watermark_value    TEXT,                      -- JSON-encoded (datetime / int / null)
    last_synced_at          TIMESTAMPTZ,
    rows_synced_last_run    BIGINT NOT NULL DEFAULT 0,
    total_rows_synced       BIGINT NOT NULL DEFAULT 0,
    last_run_status         TEXT NOT NULL DEFAULT 'pending',
    error_message           TEXT,
    historical_completed_at TIMESTAMPTZ,              -- set once historical load finishes
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Transfer run log: one row per DAG task execution
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bq_ops.transfer_runs (
    run_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_key           TEXT NOT NULL,
    run_type            TEXT NOT NULL CHECK (run_type IN ('historical', 'incremental')),
    dag_run_id          TEXT,
    airflow_task_id     TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'running'
                            CHECK (status IN ('running', 'success', 'failed')),
    rows_transferred    BIGINT NOT NULL DEFAULT 0,
    bytes_scanned       BIGINT NOT NULL DEFAULT 0,
    watermark_start     TEXT,                          -- JSON
    watermark_end       TEXT,                          -- JSON
    error_message       TEXT,
    metadata            JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_transfer_runs_table_key
    ON bq_ops.transfer_runs (table_key, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_transfer_runs_status
    ON bq_ops.transfer_runs (status) WHERE status = 'running';

-- ─────────────────────────────────────────────────────────────────────────────
-- Handy view: latest status per table
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW bq_ops.transfer_freshness AS
SELECT
    s.table_key,
    s.bq_project || '.' || s.bq_dataset || '.' || s.bq_table AS bq_full_id,
    s.pg_schema || '.' || s.pg_table                         AS pg_full_id,
    s.incremental_column,
    s.last_synced_at,
    EXTRACT(EPOCH FROM (NOW() - s.last_synced_at)) / 60      AS minutes_since_sync,
    s.last_run_status,
    s.rows_synced_last_run,
    s.total_rows_synced,
    s.historical_completed_at IS NOT NULL                    AS historical_done,
    s.error_message
FROM bq_ops.sync_state s
ORDER BY s.last_synced_at DESC NULLS LAST;
