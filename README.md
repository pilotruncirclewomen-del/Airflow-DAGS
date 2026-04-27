# Circle Women — Airflow DAGs

Apache Airflow pipeline that syncs 16 BigQuery tables into the Circle warehouse (PostgreSQL) daily.

## What it does

- Pulls all Turn.io conversation data (messages, contacts, chats, journeys, AI events, etc.) from BigQuery
- Upserts into the `bq_import` schema in the warehouse
- Runs historical backfill on first deploy, then switches to incremental daily syncs automatically

## Structure

```
airflow/
  dags/
    bq_sync_tables.py          # Registers all 16 per-table DAGs
    bq_historical_transfer.py  # [RETIRED]
    bq_incremental_sync.py     # [RETIRED]
  include/
    bigquery/                  # BQ client, transfer logic, schema, DAG factory
    turn_platform/             # Turn.io API client + warehouse helpers

docker/airflow/
  Dockerfile                   # Custom Airflow image
  requirements.txt             # Python deps (psycopg, google-cloud-bigquery, etc.)
  init.sh                      # Container init script
  create_admin.py              # Creates Airflow admin user on first boot

docker-compose.airflow.yml     # Local dev
docker-compose.airflow-server.yml  # Production (Digital Ocean)

sql/
  010_bq_ops_schema.sql        # bq_ops schema: sync_state tracking table

scripts/
  deploy_airflow.sh            # Deploy to server
  check_sync_status.sh         # Monitor sync progress
```

## Setup

1. Copy `.env.airflow.example` → `.env.airflow` and fill in values
2. Place BigQuery service account key at `credentials/bigquery-sa.json`
3. Run: `docker compose -f docker-compose.airflow-server.yml up -d`

## Tables synced

`messages`, `contacts`, `chats`, `statuses`, `flow_results`, `message_attachments`, `chat_events`, `journey_insights`, `attachments`, `ai_events`, `accounts`, `flow_results_data_packages`, `journey_summary`, `message_tags`, `number_tags`, `cards`
