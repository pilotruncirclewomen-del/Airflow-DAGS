"""
BigQuery client wrapper: schema discovery, streaming extraction, cost estimation.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Iterator

from google.cloud import bigquery
from google.oauth2 import service_account

log = logging.getLogger(__name__)

# Rows per page when streaming from BigQuery
_DEFAULT_CHUNK = int(os.getenv("BQ_CHUNK_ROWS", "50000"))
# BigQuery on-demand price per TB processed (USD)
_BQ_PRICE_PER_TB = 5.00
# Network egress price per GB from GCP (USD, conservative estimate)
_EGRESS_PRICE_PER_GB = 0.12


@dataclass
class BQTableInfo:
    project: str
    dataset: str
    table: str
    schema: list[bigquery.SchemaField]
    num_rows: int
    num_bytes: int
    partition_field: str | None = None          # None if not partitioned
    clustering_fields: list[str] = field(default_factory=list)

    @property
    def full_id(self) -> str:
        return f"`{self.project}.{self.dataset}.{self.table}`"

    @property
    def size_gb(self) -> float:
        return self.num_bytes / 1_000_000_000

    @property
    def table_key(self) -> str:
        return f"{self.dataset}.{self.table}"


@dataclass
class CostEstimate:
    table_key: str
    bytes_in_storage: int
    bytes_to_scan: int
    bq_query_cost_usd: float
    egress_cost_usd: float
    total_cost_usd: float
    notes: str

    def as_dict(self) -> dict:
        return {
            "table_key": self.table_key,
            "bytes_in_storage": self.bytes_in_storage,
            "bytes_to_scan": self.bytes_to_scan,
            "bq_query_cost_usd": round(self.bq_query_cost_usd, 4),
            "egress_cost_usd": round(self.egress_cost_usd, 4),
            "total_cost_usd": round(self.total_cost_usd, 4),
            "notes": self.notes,
        }


def _build_credentials(
    credentials_path: str | None,
    credentials_json: str | None,
) -> service_account.Credentials | None:
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    if credentials_path and os.path.exists(credentials_path):
        return service_account.Credentials.from_service_account_file(
            credentials_path, scopes=scopes
        )
    if credentials_json:
        info = json.loads(credentials_json)
        return service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return None  # fall through to ADC


class BigQueryClient:
    """
    Thin wrapper around google-cloud-bigquery.
    Supports service-account JSON file, JSON string, or ADC (Application Default Credentials).
    """

    def __init__(
        self,
        project: str,
        credentials_path: str | None = None,
        credentials_json: str | None = None,
    ):
        self.project = project
        creds = _build_credentials(credentials_path, credentials_json)
        self._client = bigquery.Client(project=project, credentials=creds)
        log.info("BigQuery client initialised for project %s", project)

    # ── Discovery ────────────────────────────────────────────────────────────

    def list_tables(self, dataset: str) -> list[BQTableInfo]:
        """Return metadata for every table in *dataset*."""
        dataset_ref = self._client.dataset(dataset)
        infos = []
        for t in self._client.list_tables(dataset_ref):
            infos.append(self._fetch_table_info(dataset, t.table_id))
        log.info("Discovered %d tables in %s.%s", len(infos), self.project, dataset)
        return infos

    def get_table_info(self, dataset: str, table: str) -> BQTableInfo:
        return self._fetch_table_info(dataset, table)

    def _fetch_table_info(self, dataset: str, table: str) -> BQTableInfo:
        tbl = self._client.get_table(f"{self.project}.{dataset}.{table}")
        partition_field: str | None = None
        if tbl.time_partitioning:
            partition_field = tbl.time_partitioning.field or "_PARTITIONTIME"
        return BQTableInfo(
            project=self.project,
            dataset=dataset,
            table=table,
            schema=list(tbl.schema),
            num_rows=tbl.num_rows or 0,
            num_bytes=tbl.num_bytes or 0,
            partition_field=partition_field,
            clustering_fields=list(tbl.clustering_fields or []),
        )

    # ── Extraction ───────────────────────────────────────────────────────────

    def stream_table(
        self,
        table_info: BQTableInfo,
        where_clause: str | None = None,
        order_by: str | None = None,
        chunk_size: int = _DEFAULT_CHUNK,
    ) -> Iterator[list[dict]]:
        """
        Yield chunks of rows (list[dict]) from BigQuery.
        Streams through query result pages to avoid materialising the full table.
        """
        cols = ", ".join(f"`{f.name}`" for f in table_info.schema)
        sql = f"SELECT {cols} FROM {table_info.full_id}"
        if where_clause:
            sql += f"\nWHERE {where_clause}"
        if order_by:
            sql += f"\nORDER BY {order_by}"

        log.info("BQ query:\n%s", sql)
        query_job = self._client.query(sql)
        chunk: list[dict] = []
        for row in query_job.result(page_size=chunk_size):
            chunk.append(dict(row))
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def get_max_value(self, dataset: str, table: str, column: str) -> object | None:
        """Return MAX(column) from a BQ table — used to set initial watermarks."""
        sql = f"SELECT MAX(`{column}`) AS mx FROM `{self.project}.{dataset}.{table}`"
        result = self._client.query(sql).result()
        row = next(result, None)
        return row.mx if row else None

    def count_rows(
        self, dataset: str, table: str, where_clause: str | None = None
    ) -> int:
        sql = f"SELECT COUNT(*) AS cnt FROM `{self.project}.{dataset}.{table}`"
        if where_clause:
            sql += f" WHERE {where_clause}"
        result = self._client.query(sql).result()
        return next(result).cnt

    # ── Cost estimation ──────────────────────────────────────────────────────

    def estimate_cost(
        self,
        table_info: BQTableInfo,
        where_clause: str | None = None,
    ) -> CostEstimate:
        """
        Dry-run the full-table query to get bytes that would be scanned,
        then calculate approximate USD cost.
        """
        cols = ", ".join(f"`{f.name}`" for f in table_info.schema)
        sql = f"SELECT {cols} FROM {table_info.full_id}"
        if where_clause:
            sql += f" WHERE {where_clause}"

        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self._client.query(sql, job_config=job_config)
        bytes_scanned = job.total_bytes_processed or 0

        tb_scanned = bytes_scanned / 1e12
        gb_egress = bytes_scanned / 1e9          # approximate: data read ≈ data exported

        # First 1 TB/month is free on on-demand
        billable_tb = max(0.0, tb_scanned - 1.0)
        bq_cost = billable_tb * _BQ_PRICE_PER_TB
        egress_cost = gb_egress * _EGRESS_PRICE_PER_GB
        total = bq_cost + egress_cost

        notes = (
            "BQ on-demand: first 1TB/month free, $5/TB after. "
            "Egress: ~$0.12/GB from GCP to external. "
            "Actual egress may vary by destination region."
        )

        return CostEstimate(
            table_key=table_info.table_key,
            bytes_in_storage=table_info.num_bytes,
            bytes_to_scan=bytes_scanned,
            bq_query_cost_usd=bq_cost,
            egress_cost_usd=egress_cost,
            total_cost_usd=total,
            notes=notes,
        )
