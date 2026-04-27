from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models.param import Param

from turn_platform.pipelines import refresh_analytics, run_backfill
from turn_platform.warehouse import ensure_warehouse_objects


@dag(
    dag_id="turn_historical_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 13, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={
        "entity_type": Param("messages", enum=["messages", "statuses", "contacts"]),
        "from_iso": Param("2026-01-01T00:00:00Z", type="string"),
        "until_iso": Param("2026-03-13T00:00:00Z", type="string"),
    },
    tags=["turnio", "backfill", "warehouse"],
)
def turn_historical_backfill():
    @task
    def bootstrap():
        ensure_warehouse_objects()
        return {"bootstrapped": True}

    @task
    def backfill():
        context = get_current_context()
        params = context["params"]
        return run_backfill(
            params["entity_type"],
            from_iso=params["from_iso"],
            until_iso=params["until_iso"],
            dag_id="turn_historical_backfill",
            task_id="backfill",
        )

    @task
    def refresh():
        return refresh_analytics(
            dag_id="turn_historical_backfill",
            task_id="refresh_analytics",
        )

    foundation = bootstrap()
    backfill_task = backfill()
    analytics = refresh()

    foundation >> backfill_task >> analytics


turn_historical_backfill()
