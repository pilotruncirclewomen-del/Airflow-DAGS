"""
Per-table BigQuery → warehouse DAGs.

This single file registers one DAG per entry in table_configs.TABLE_CONFIGS.
Each DAG is named  bq_sync__<table_name>  and runs independently.

Architecture:
  plan_load → transfer_chunk (mapped) → verify

  • Historical mode:  chunked date-range loads with auto-resume
  • Incremental mode: single watermark-to-now chunk (after history complete)

To add a new table: add an entry to table_configs.TABLE_CONFIGS — a new DAG
appears automatically on next Airflow scheduler scan.
"""
from __future__ import annotations

from bigquery.table_configs import TABLE_CONFIGS
from bigquery.dag_factory import make_sync_dag

# Register every table DAG in this module's global namespace so Airflow picks
# them up via its standard DAG discovery mechanism.
for _tc in TABLE_CONFIGS:
    _dag = make_sync_dag(_tc)
    globals()[_dag.dag_id] = _dag
