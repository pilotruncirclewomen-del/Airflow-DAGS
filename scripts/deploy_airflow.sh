#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy_airflow.sh
#
# Deploys the Airflow BQ pipeline stack to airflow-server (134.209.96.53).
#
# Prerequisites (run once):
#   1. SSH access configured: ssh root@134.209.96.53
#   2. .env.airflow filled out and saved as .env.airflow (NOT committed to git)
#   3. BigQuery service-account key placed at ./credentials/bigquery-sa.json
#
# Usage:
#   chmod +x scripts/deploy_airflow.sh
#   ./scripts/deploy_airflow.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SERVER="root@134.209.96.53"
REMOTE_DIR="/opt/bq-pipeline"
COMPOSE_FILE="docker-compose.airflow-server.yml"
ENV_FILE=".env.airflow"

echo "═══════════════════════════════════════════════════════════"
echo " Deploying Airflow BQ pipeline → $SERVER"
echo "═══════════════════════════════════════════════════════════"

# ── Validate local prerequisites ──────────────────────────────────────────────
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. Copy .env.airflow.example → .env.airflow and fill in values."
  exit 1
fi
if [[ ! -f "credentials/bigquery-sa.json" ]]; then
  echo "ERROR: credentials/bigquery-sa.json not found. Place your GCP service-account key there."
  exit 1
fi

# ── Create remote directory structure ─────────────────────────────────────────
echo "→ Creating remote directory structure..."
ssh "$SERVER" "mkdir -p $REMOTE_DIR/{airflow/dags,airflow/include,sql,credentials,docker/airflow}"

# ── Sync source files ──────────────────────────────────────────────────────────
echo "→ Syncing DAGs and include files..."
rsync -avz --delete \
  airflow/dags/ \
  "$SERVER:$REMOTE_DIR/airflow/dags/"

rsync -avz --delete \
  airflow/include/ \
  "$SERVER:$REMOTE_DIR/airflow/include/"

echo "→ Syncing SQL schemas..."
rsync -avz \
  sql/010_bq_ops_schema.sql \
  "$SERVER:$REMOTE_DIR/sql/"

echo "→ Syncing Docker build files..."
rsync -avz \
  docker/airflow/Dockerfile \
  docker/airflow/requirements.txt \
  "$SERVER:$REMOTE_DIR/docker/airflow/"

echo "→ Syncing compose file..."
rsync -avz "$COMPOSE_FILE" "$SERVER:$REMOTE_DIR/docker-compose.yml"

echo "→ Syncing environment file..."
rsync -avz "$ENV_FILE" "$SERVER:$REMOTE_DIR/.env"

echo "→ Syncing BigQuery credentials (read-only)..."
rsync -avz credentials/bigquery-sa.json "$SERVER:$REMOTE_DIR/credentials/"
ssh "$SERVER" "chmod 600 $REMOTE_DIR/credentials/bigquery-sa.json"

# ── Bootstrap bq_ops schema on warehouse-db ────────────────────────────────────
echo "→ Applying bq_ops schema to warehouse-db..."
WAREHOUSE_DSN=$(grep '^WAREHOUSE_DSN=' "$ENV_FILE" | cut -d= -f2-)
ssh "$SERVER" "docker run --rm \
  -e PGPASSWORD='' \
  postgres:16-alpine \
  psql '$WAREHOUSE_DSN' -f /dev/stdin" < sql/010_bq_ops_schema.sql \
  && echo "   bq_ops schema applied." \
  || echo "   WARNING: Could not apply schema remotely — run manually if needed."

# ── Pull / build image and restart stack ──────────────────────────────────────
echo "→ Building custom Airflow image (with BQ dependencies)..."
ssh "$SERVER" "cd $REMOTE_DIR && docker compose build --pull"

echo "→ Running airflow-init (DB migration + admin user)..."
ssh "$SERVER" "cd $REMOTE_DIR && docker compose run --rm airflow-init"

echo "→ Starting Airflow services..."
ssh "$SERVER" "cd $REMOTE_DIR && docker compose up -d --remove-orphans"

echo "→ Waiting for API server to be healthy..."
sleep 15
ssh "$SERVER" "docker compose -f $REMOTE_DIR/docker-compose.yml ps"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " Airflow deployed successfully!"
echo " UI: http://134.209.96.53:8080"
echo ""
echo " Next steps:"
echo "   1. Open the UI and log in with your admin credentials."
echo "   2. Set the Airflow Variable 'BQ_TABLE_MANIFEST' (JSON)."
echo "      Example:"
echo '      [{"bq_project":"my-project","bq_dataset":"analytics","bq_table":"messages",'
echo '        "pg_schema":"bq_import","conflict_columns":["id"],"incremental_column":"created_at"}]'
echo "   3. Trigger bq_historical_transfer manually (one time)."
echo "   4. bq_incremental_sync will then run automatically at 02:00 UTC daily."
echo "═══════════════════════════════════════════════════════════"
