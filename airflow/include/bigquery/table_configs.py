"""
Central registry of all BigQuery → warehouse table configurations.

size_tier   controls chunk size and retry behaviour:
  heavy     large tables (>5M rows)   — 1-month chunks, 50k rows/chunk
  medium    mid tables (100k–5M rows) — 3-month chunks, 25k rows/chunk
  light     small tables (<100k rows) — single full load, 10k rows/chunk

history_start  earliest date we expect data from (safe floor, not exact).
               Used to generate the historical chunk plan.
"""
from __future__ import annotations

BQ_PROJECT = "pilot-run-turn-bq-integration"
BQ_DATASET = "923141851055"
PG_SCHEMA  = "bq_import"

# history_start: conservative floor — chunks before actual data are fast no-ops
TABLE_CONFIGS: list[dict] = [
    # ── Heavy tables ──────────────────────────────────────────────────────────
    {
        "bq_table":           "messages",
        "conflict_columns":   ["id"],
        "incremental_column": "updated_at",
        "size_tier":          "heavy",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "statuses",
        "conflict_columns":   ["id"],
        "incremental_column": "updated_at",
        "size_tier":          "heavy",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "contacts",
        "conflict_columns":   ["id"],
        "incremental_column": "updated_at",
        "size_tier":          "heavy",
        "history_start":      "2025-07-01",  # first month with data
        "chunk_size_override": 1_000,         # ~300 wide-profile columns → OOM at 5k+
    },
    {
        "bq_table":           "chats",
        "conflict_columns":   ["id"],
        "incremental_column": "updated_at",
        "size_tier":          "heavy",
        "history_start":      "2022-01-01",
    },
    # ── Medium tables ─────────────────────────────────────────────────────────
    {
        "bq_table":           "flow_results",
        "conflict_columns":   ["uuid"],
        "incremental_column": "updated_at",
        "size_tier":          "medium",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "message_attachments",
        "conflict_columns":   ["id"],
        "incremental_column": "updated_at",
        "size_tier":          "medium",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "chat_events",
        "conflict_columns":   ["uuid"],
        "incremental_column": "updated_at",
        "size_tier":          "medium",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "journey_insights",
        "conflict_columns":   ["uuid"],
        "incremental_column": "updated_at",
        "size_tier":          "medium",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "attachments",
        "conflict_columns":   ["id"],
        "incremental_column": "updated_at",
        "size_tier":          "medium",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "ai_events",
        "conflict_columns":   ["uuid"],
        "incremental_column": "updated_at",
        "size_tier":          "medium",
        "history_start":      "2022-01-01",
    },
    # ── Light tables ──────────────────────────────────────────────────────────
    {
        "bq_table":           "accounts",
        "conflict_columns":   ["uuid"],
        "incremental_column": "updated_at",
        "size_tier":          "light",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "flow_results_data_packages",
        "conflict_columns":   ["uuid"],
        "incremental_column": "updated_at",
        "size_tier":          "light",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "journey_summary",
        "conflict_columns":   ["uuid"],
        "incremental_column": "updated_at",
        "size_tier":          "light",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "message_tags",
        "conflict_columns":   ["id"],
        "incremental_column": "updated_at",
        "size_tier":          "light",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "number_tags",
        "conflict_columns":   ["id"],
        "incremental_column": "updated_at",
        "size_tier":          "light",
        "history_start":      "2022-01-01",
    },
    {
        "bq_table":           "cards",
        "conflict_columns":   ["id"],
        "incremental_column": "updated_at",
        "size_tier":          "light",
        "history_start":      "2022-01-01",
    },
    # ── Staging variants (15-minute incremental, memory-profiled) ─────────────
    # Same BQ source, separate PG destination with `_staging` suffix. Run on
    # a tighter schedule than the daily DAGs so we can validate freshness and
    # memory characteristics in parallel before cutting over.
    {
        "bq_table":           "accounts",
        "pg_table":           "accounts_staging",
        "conflict_columns":   ["uuid"],
        "incremental_column": "updated_at",
        "size_tier":          "light",
        "history_start":      "2022-01-01",
        "schedule":           "*/15 * * * *",
        "memory_profile":     True,
    },
    {
        "bq_table":           "ai_events",
        "pg_table":           "ai_events_staging",
        "conflict_columns":   ["uuid"],
        "incremental_column": "updated_at",
        "size_tier":          "medium",
        "history_start":      "2022-01-01",
        "schedule":           "*/15 * * * *",
        "memory_profile":     True,
    },
    {
        "bq_table":           "attachments",
        "pg_table":           "attachments_staging",
        "conflict_columns":   ["id"],
        "incremental_column": "updated_at",
        "size_tier":          "medium",
        "history_start":      "2022-01-01",
        "schedule":           "*/15 * * * *",
        "memory_profile":     True,
    },
]

# Enrich with shared fields
for _tc in TABLE_CONFIGS:
    _tc.setdefault("bq_project",   BQ_PROJECT)
    _tc.setdefault("bq_dataset",   BQ_DATASET)
    _tc.setdefault("pg_schema",    PG_SCHEMA)
    _tc.setdefault("pg_table",     _tc["bq_table"])
    _tc.setdefault("update_columns", None)
    # table_key keys off pg_table so multiple destinations for the same BQ
    # source (e.g. staging variants) get distinct sync_state rows.
    _tc["table_key"] = f"{_tc['bq_dataset']}.{_tc['pg_table']}"

# Quick lookup by table name
TABLE_CONFIG_MAP: dict[str, dict] = {tc["bq_table"]: tc for tc in TABLE_CONFIGS}
