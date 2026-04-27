"""
BigQuery → PostgreSQL schema mapping and DDL generation.
"""
from __future__ import annotations

from google.cloud import bigquery

# Maps BigQuery field types → PostgreSQL column types
_BQ_TO_PG: dict[str, str] = {
    "STRING":       "TEXT",
    "BYTES":        "BYTEA",
    "INTEGER":      "BIGINT",
    "INT64":        "BIGINT",
    "FLOAT":        "DOUBLE PRECISION",
    "FLOAT64":      "DOUBLE PRECISION",
    "NUMERIC":      "NUMERIC(38,9)",
    "BIGNUMERIC":   "NUMERIC(76,38)",
    "BOOLEAN":      "BOOLEAN",
    "BOOL":         "BOOLEAN",
    "TIMESTAMP":    "TIMESTAMPTZ",
    "DATE":         "DATE",
    "TIME":         "TIME",
    "DATETIME":     "TIMESTAMP",   # BQ DATETIME has no TZ
    "GEOGRAPHY":    "TEXT",        # WKT representation
    "RECORD":       "JSONB",       # nested RECORD → JSONB
    "STRUCT":       "JSONB",
    "JSON":         "JSONB",
}


def pg_type_for(field: bigquery.SchemaField) -> str:
    """Return the PostgreSQL type string for a BigQuery SchemaField."""
    if field.mode == "REPEATED":
        # Any repeated field (array) → JSONB regardless of element type
        return "JSONB"
    return _BQ_TO_PG.get(field.field_type.upper(), "TEXT")


def field_ddl(field: bigquery.SchemaField) -> str:
    """Return a single column definition clause, e.g. '"name" TEXT NOT NULL'."""
    pg_type = pg_type_for(field)
    not_null = " NOT NULL" if field.mode == "REQUIRED" else ""
    return f'"{field.name}" {pg_type}{not_null}'


def create_table_ddl(
    schema_name: str,
    table_name: str,
    bq_schema: list[bigquery.SchemaField],
    primary_key: list[str] | None = None,
) -> str:
    """
    Generate a CREATE TABLE IF NOT EXISTS statement.
    Always appends a '_bq_loaded_at' column for lineage tracking.
    """
    col_defs = [field_ddl(f) for f in bq_schema]
    col_defs.append('"_bq_loaded_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()')

    pk_clause = ""
    if primary_key:
        pk_cols = ", ".join(f'"{c}"' for c in primary_key)
        pk_clause = f",\n    PRIMARY KEY ({pk_cols})"

    body = ",\n    ".join(col_defs)
    return (
        f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";\n'
        f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (\n'
        f"    {body}{pk_clause}\n"
        f");"
    )


def alter_table_add_columns_sql(
    schema_name: str,
    table_name: str,
    bq_schema: list[bigquery.SchemaField],
) -> list[str]:
    """
    Return a list of ALTER TABLE … ADD COLUMN IF NOT EXISTS statements for
    every column in bq_schema (plus _bq_loaded_at).  Safe to run on an
    already-up-to-date table — IF NOT EXISTS is a no-op.
    """
    stmts = []
    for f in bq_schema:
        pg_type = pg_type_for(f)
        stmts.append(
            f'ALTER TABLE "{schema_name}"."{table_name}" '
            f'ADD COLUMN IF NOT EXISTS "{f.name}" {pg_type};'
        )
    stmts.append(
        f'ALTER TABLE "{schema_name}"."{table_name}" '
        f'ADD COLUMN IF NOT EXISTS "_bq_loaded_at" TIMESTAMPTZ;'
    )
    return stmts


def upsert_sql(
    schema_name: str,
    table_name: str,
    columns: list[str],
    conflict_columns: list[str],
    update_columns: list[str] | None = None,
) -> str:
    """
    Return a parameterised INSERT … ON CONFLICT … SQL string.
    Uses %(name)s placeholders (psycopg named-param style).
    """
    col_list = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(f"%({c})s" for c in columns)
    conflict_cols = ", ".join(f'"{c}"' for c in conflict_columns)

    if update_columns:
        set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_columns)
        set_clause += ', "_bq_loaded_at" = NOW()'
        action = f"DO UPDATE SET {set_clause}"
    else:
        action = "DO NOTHING"

    return (
        f'INSERT INTO "{schema_name}"."{table_name}" ({col_list})\n'
        f"VALUES ({placeholders})\n"
        f"ON CONFLICT ({conflict_cols}) {action};"
    )


def insert_sql(schema_name: str, table_name: str, columns: list[str]) -> str:
    """Simple INSERT … ON CONFLICT DO NOTHING (for tables with no PK defined)."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(f"%({c})s" for c in columns)
    return (
        f'INSERT INTO "{schema_name}"."{table_name}" ({col_list})\n'
        f"VALUES ({placeholders})\n"
        f"ON CONFLICT DO NOTHING;"
    )
