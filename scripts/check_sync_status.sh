#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# check_sync_status.sh
# Shows current BQ→warehouse sync state for all 16 tables.
# Run from your local machine — SSHes to the server and queries the warehouse.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
SERVER="root@airflow.circlewomen.com"

ssh "${SERVER}" bash <<'ENDSSH'
docker exec -i warehouse-db psql "${WAREHOUSE_DSN:-$WAREHOUSE_DSN}" 2>/dev/null || \
docker exec -i $(docker ps --format '{{.Names}}' | grep postgres | head -1) \
  psql "$WAREHOUSE_DSN" -c "
SELECT
  bq_table,
  last_synced_at::date                              AS last_synced,
  CASE WHEN historical_completed_at IS NULL
       THEN 'historical' ELSE 'incremental' END     AS mode,
  historical_completed_at::date                     AS hist_done,
  to_char(total_rows_synced, 'FM999,999,999')       AS total_rows,
  last_run_status                                   AS status,
  left(error_message, 60)                           AS last_error
FROM bq_ops.sync_state
ORDER BY
  CASE last_run_status WHEN 'failed' THEN 0 WHEN 'running' THEN 1 ELSE 2 END,
  total_rows_synced ASC;
" 2>/dev/null
ENDSSH
