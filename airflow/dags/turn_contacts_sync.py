from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from turn_platform.pipelines import refresh_analytics, sync_incremental
from turn_platform.warehouse import ensure_warehouse_objects


@dag(
    dag_id="turn_contacts_sync",
    schedule="17 * * * *",
    start_date=pendulum.datetime(2026, 3, 13, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["turnio", "contacts", "warehouse"],
)
def turn_contacts_sync():
    @task
    def bootstrap():
        ensure_warehouse_objects()
        return {"bootstrapped": True}

    @task
    def sync_contacts():
        return sync_incremental(
            "contacts",
            dag_id="turn_contacts_sync",
            task_id="sync_contacts",
        )

    @task
    def refresh():
        return refresh_analytics(
            dag_id="turn_contacts_sync",
            task_id="refresh_analytics",
        )

    foundation = bootstrap()
    contacts = sync_contacts()
    analytics = refresh()

    foundation >> contacts >> analytics


turn_contacts_sync()
