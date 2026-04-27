# Circle Women — Airflow DAGs

Apache Airflow 3.1.8 pipeline that syncs 16 BigQuery tables into the Circle warehouse (PostgreSQL) daily.

**Production:** `http://airflow.circlewomen.com`

---

## What it does

- Pulls all Turn.io conversation data (messages, contacts, chats, journeys, AI events, etc.) from BigQuery
- Upserts into the `bq_import` schema in the warehouse (PostgreSQL on Digital Ocean)
- Runs a full historical backfill on first deploy, then automatically switches to incremental daily syncs
- Tracks sync state in `bq_ops.sync_state` — this table controls whether each table is in historical or incremental mode

---

## How the pipeline works

Each of the 16 tables has its own independent DAG (`bq_sync__<table>`), built by a shared factory. Every DAG runs 3 tasks:

```
plan_load  →  transfer_chunk (×N, parallel)  →  verify
```

1. **plan_load** — reads `bq_ops.sync_state` and decides what to load:
   - First time (no `historical_completed_at`): generates monthly date-range chunks from the earliest known data up to today
   - After historical is done: generates a single incremental chunk from the last watermark to now

2. **transfer_chunk** — each chunk is independent and idempotent:
   - Fetches rows from BigQuery for that date range
   - Upserts into PostgreSQL in batches
   - Safe to retry or re-run — will never duplicate data

3. **verify** — compares BQ row count vs PG row count, logs any gap, and marks `historical_completed_at` in `bq_ops.sync_state` once coverage hits ≥99%

---

## Structure

```
airflow/
  dags/
    bq_sync_tables.py          # Registers all 16 per-table BQ sync DAGs
    bq_historical_transfer.py  # [RETIRED — replaced by bq_sync_tables]
    bq_incremental_sync.py     # [RETIRED — replaced by bq_sync_tables]
    turn_contacts_sync.py      # Turn.io API → warehouse (contacts)
    turn_historical_backfill.py # Turn.io API historical backfill
    turn_incremental_sync.py   # Turn.io API incremental sync

  include/
    bigquery/
      dag_factory.py           # Builds one DAG per table (plan → transfer → verify)
      table_configs.py         # Registry of all 16 tables (tier, chunk size, history start)
      transfer.py              # Core upsert logic + schema evolution
      schema.py                # DDL generation, ALTER TABLE add column
      client.py                # BigQuery client wrapper
      state.py                 # bq_ops.sync_state read/write helpers

    turn_platform/
      turnio.py                # Turn.io REST API client
      warehouse.py             # Warehouse write helpers
      pipelines.py             # Ingestion pipeline logic
      config.py                # Env/config loading

docker/airflow/
  Dockerfile                   # Custom Airflow 3.1.8 image
  requirements.txt             # Python deps (psycopg, google-cloud-bigquery, etc.)
  init.sh                      # Container init: DB migrations, admin user, start
  create_admin.py              # Creates Airflow admin user on first boot

docker-compose.airflow.yml         # Local development
docker-compose.airflow-server.yml  # Production (Digital Ocean)

sql/
  010_bq_ops_schema.sql        # Creates bq_ops.sync_state tracking table

scripts/
  deploy_airflow.sh            # SSH deploy to production server
  check_sync_status.sh         # Monitor live sync progress
```

---

## Table tiers

Tables are assigned a tier that controls chunk size and retry behaviour:

| Tier | Tables | Chunk size | Date range per chunk |
|---|---|---|---|
| heavy | messages, contacts, chats, statuses | 50K rows (contacts: 1K) | 1 month |
| medium | flow_results, chat_events, journey_insights, ai_events, attachments, message_attachments | 25K rows | 3 months |
| light | accounts, cards, message_tags, number_tags, journey_summary, flow_results_data_packages | 10K rows | full load |

---

## Tables synced (16)

`messages`, `contacts`, `chats`, `statuses`, `flow_results`, `message_attachments`, `chat_events`, `journey_insights`, `attachments`, `ai_events`, `accounts`, `flow_results_data_packages`, `journey_summary`, `message_tags`, `number_tags`, `cards`

---

## Setup

1. Copy `.env.airflow.example` → `.env.airflow` and fill in all values
2. Place BigQuery service account key at `credentials/bigquery-sa.json`
3. Run the bq_ops schema migration: `psql $WAREHOUSE_DSN -f sql/010_bq_ops_schema.sql`
4. Start the stack: `docker compose -f docker-compose.airflow-server.yml up -d`

On first run, each DAG will automatically detect that `historical_completed_at` is NULL and begin the full historical backfill. Once complete, it switches to daily incremental syncs at 02:00 UTC.

---

## Resetting a table sync

To force a full re-sync of a specific table:

```sql
UPDATE bq_ops.sync_state
SET historical_completed_at = NULL, last_watermark_value = NULL
WHERE table_key = '923141851055.messages';
```

Then re-trigger the DAG manually in the Airflow UI.
