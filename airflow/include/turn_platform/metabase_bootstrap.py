from __future__ import annotations

import time
from typing import Any

import requests

from turn_platform.config import load_settings


DASHBOARD_NAME = "Turn.io Data Sanity"
COLLECTION_NAME = "Turn.io Analytics"


CARDS = [
    {
        "name": "Pipeline freshness",
        "display": "table",
        "query": """
            SELECT
                pipeline_name,
                entity_type,
                freshness_minutes,
                rows_seen_last_run,
                rows_written_last_run,
                last_success_at
            FROM turn_analytics.mv_pipeline_freshness
            ORDER BY freshness_minutes ASC NULLS LAST, pipeline_name
        """,
        "col": 0,
        "row": 0,
        "size_x": 12,
        "size_y": 5,
    },
    {
        "name": "Sanity snapshot",
        "display": "table",
        "query": """
            SELECT
                generated_at,
                webhook_freshness_minutes,
                message_freshness_minutes,
                status_freshness_minutes,
                contact_freshness_minutes,
                duplicate_messages,
                duplicate_contact_snapshots,
                orphan_status_events,
                messages_missing_direction,
                messages_missing_type,
                failed_runs_24h
            FROM turn_analytics.mv_data_sanity_snapshot
        """,
        "col": 12,
        "row": 0,
        "size_x": 12,
        "size_y": 5,
    },
    {
        "name": "30 day ingestion trend",
        "display": "line",
        "query": """
            SELECT
                metric_date,
                webhook_deliveries,
                raw_messages,
                raw_statuses,
                raw_contact_snapshots,
                failed_runs
            FROM turn_analytics.mv_data_sanity_trend_30d
            ORDER BY metric_date
        """,
        "col": 0,
        "row": 5,
        "size_x": 24,
        "size_y": 7,
    },
    {
        "name": "Daily delivery funnel",
        "display": "bar",
        "query": """
            SELECT
                metric_date,
                outbound_messages,
                sent_messages,
                delivered_messages,
                read_messages,
                failed_messages,
                delivery_rate_pct,
                read_rate_pct
            FROM turn_analytics.mv_status_funnel_daily
            ORDER BY metric_date DESC
            LIMIT 30
        """,
        "col": 0,
        "row": 12,
        "size_x": 12,
        "size_y": 7,
    },
    {
        "name": "Daily message volume",
        "display": "table",
        "query": """
            SELECT
                metric_date,
                direction,
                message_type,
                message_count,
                unique_contacts
            FROM turn_analytics.mv_message_volume_daily
            ORDER BY metric_date DESC, direction, message_type
            LIMIT 100
        """,
        "col": 12,
        "row": 12,
        "size_x": 12,
        "size_y": 7,
    },
    {
        "name": "Daily contact snapshots",
        "display": "line",
        "query": """
            SELECT
                metric_date,
                snapshot_count,
                contacts_touched,
                opted_in_snapshots,
                onboarded_snapshots
            FROM turn_analytics.mv_contact_snapshots_daily
            ORDER BY metric_date
        """,
        "col": 0,
        "row": 19,
        "size_x": 24,
        "size_y": 7,
    },
]


class MetabaseClient:
    def __init__(self) -> None:
        self.settings = load_settings()
        self.base_url = self.settings.metabase.url
        self.session = requests.Session()

    def wait_for_metabase(self) -> None:
        deadline = time.time() + self.settings.metabase.bootstrap_timeout_seconds
        while time.time() < deadline:
            try:
                response = self.session.get(f"{self.base_url}/api/health", timeout=10)
                if response.ok:
                    return
            except requests.RequestException:
                pass
            time.sleep(5)
        raise RuntimeError("Metabase did not become healthy before timeout")

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.session.request(method, f"{self.base_url}{path}", timeout=30, **kwargs)
        response.raise_for_status()
        if not response.content:
            return None
        return response.json()

    def ensure_setup(self) -> None:
        session_properties = self._request("GET", "/api/session/properties")
        setup_token = session_properties.get("setup-token")
        if not setup_token:
            return

        warehouse = self.settings.warehouse
        payload = {
            "token": setup_token,
            "prefs": {
                "site_name": self.settings.metabase.site_name,
                "allow_tracking": False,
            },
            "user": {
                "first_name": self.settings.metabase.admin_first_name,
                "last_name": self.settings.metabase.admin_last_name,
                "email": self.settings.metabase.admin_email,
                "password": self.settings.metabase.admin_password,
            },
            "database": {
                "engine": "postgres",
                "name": self.settings.metabase.database_name,
                "details": {
                    "host": warehouse.host,
                    "port": warehouse.port,
                    "dbname": warehouse.database,
                    "user": warehouse.user,
                    "password": warehouse.password,
                    "ssl": warehouse.sslmode == "require",
                },
            },
        }
        self._request("POST", "/api/setup", json=payload)

    def login(self) -> None:
        body = self._request(
            "POST",
            "/api/session",
            json={
                "username": self.settings.metabase.admin_email,
                "password": self.settings.metabase.admin_password,
            },
        )
        self.session.headers.update({"X-Metabase-Session": body["id"]})

    def ensure_database(self) -> int:
        databases = self._request("GET", "/api/database")
        database_list = databases.get("data", []) if isinstance(databases, dict) else databases
        for database in database_list:
            if database.get("name") == self.settings.metabase.database_name:
                return database["id"]

        warehouse = self.settings.warehouse
        created = self._request(
            "POST",
            "/api/database",
            json={
                "engine": "postgres",
                "name": self.settings.metabase.database_name,
                "details": {
                    "host": warehouse.host,
                    "port": warehouse.port,
                    "dbname": warehouse.database,
                    "user": warehouse.user,
                    "password": warehouse.password,
                    "ssl": warehouse.sslmode == "require",
                },
            },
        )
        return created["id"]

    def ensure_collection(self) -> int | None:
        try:
            collections = self._request("GET", "/api/collection")
        except requests.HTTPError:
            return None

        collection_list = collections.get("data", []) if isinstance(collections, dict) else collections
        for collection in collection_list:
            if collection.get("name") == COLLECTION_NAME:
                return collection["id"]

        created = self._request(
            "POST",
            "/api/collection",
            json={
                "name": COLLECTION_NAME,
                "description": "Managed Turn.io ingestion and data quality dashboards",
                "color": "#1F6FEB",
            },
        )
        return created.get("id")

    def dashboard_exists(self) -> bool:
        dashboards = self._request("GET", "/api/dashboard")
        dashboard_list = dashboards.get("data", []) if isinstance(dashboards, dict) else dashboards
        return any(dashboard.get("name") == DASHBOARD_NAME for dashboard in dashboard_list)

    def create_dashboard(self, collection_id: int | None) -> int:
        payload = {"name": DASHBOARD_NAME}
        if collection_id is not None:
            payload["collection_id"] = collection_id
        dashboard = self._request("POST", "/api/dashboard", json=payload)
        return dashboard["id"]

    def create_card(self, database_id: int, card: dict[str, Any], collection_id: int | None) -> int:
        payload = {
            "name": card["name"],
            "display": card["display"],
            "dataset_query": {
                "type": "native",
                "database": database_id,
                "native": {
                    "query": card["query"],
                    "template-tags": {},
                },
            },
            "visualization_settings": {},
        }
        if collection_id is not None:
            payload["collection_id"] = collection_id
        created = self._request("POST", "/api/card", json=payload)
        return created["id"]

    def add_cards_to_dashboard(self, dashboard_id: int, cards: list[dict[str, Any]]) -> None:
        self._request(
            "POST",
            f"/api/dashboard/{dashboard_id}/cards",
            json={
                "cards": [
                    {
                        "cardId": card["card_id"],
                        "row": card["row"],
                        "col": card["col"],
                        "sizeX": card["size_x"],
                        "sizeY": card["size_y"],
                    }
                    for card in cards
                ]
            },
        )


def bootstrap_metabase() -> None:
    client = MetabaseClient()
    client.wait_for_metabase()
    client.ensure_setup()
    client.login()
    database_id = client.ensure_database()
    collection_id = client.ensure_collection()

    if client.dashboard_exists():
        return

    dashboard_id = client.create_dashboard(collection_id)
    prepared_cards = []
    for card in CARDS:
        card_id = client.create_card(database_id, card, collection_id)
        prepared_cards.append({**card, "card_id": card_id})
    client.add_cards_to_dashboard(dashboard_id, prepared_cards)


if __name__ == "__main__":
    bootstrap_metabase()
