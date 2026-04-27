from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from turn_platform.pipelines import refresh_analytics, sync_incremental
from turn_platform.warehouse import ensure_warehouse_objects


@dag(
    dag_id="turn_incremental_sync",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 3, 13, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["turnio", "incremental", "warehouse"],
)
def turn_incremental_sync():
    @task
    def bootstrap():
        ensure_warehouse_objects()
        return {"bootstrapped": True}

    @task
    def sync_messages():
        return sync_incremental(
            "messages",
            dag_id="turn_incremental_sync",
            task_id="sync_messages",
        )

    @task
    def sync_statuses():
        return sync_incremental(
            "statuses",
            dag_id="turn_incremental_sync",
            task_id="sync_statuses",
        )

    @task
    def refresh():
        return refresh_analytics(
            dag_id="turn_incremental_sync",
            task_id="refresh_analytics",
        )

    foundation = bootstrap()
    messages = sync_messages()
    statuses = sync_statuses()
    analytics = refresh()

    foundation >> [messages, statuses] >> analytics


turn_incremental_sync()
