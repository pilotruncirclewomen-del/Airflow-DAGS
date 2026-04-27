from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Generator

import requests

from turn_platform.config import load_settings
from turn_platform.utils import isoformat_utc


ENTITY_CHUNK_DAYS = {
    "messages": 7,
    "statuses": 7,
    "contacts": 30,
}


class TurnExportClient:
    def __init__(self) -> None:
        settings = load_settings()
        self.settings = settings.turnio
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.settings.token}",
                "Accept": "application/vnd.v1+json",
                "Content-Type": "application/json",
            }
        )

    def _request(self, method: str, path: str, **kwargs: Any) -> dict:
        url = f"{self.settings.base_url}{path}"
        timeout = kwargs.pop("timeout", self.settings.request_timeout_seconds)

        for attempt in range(1, self.settings.max_retries + 1):
            try:
                response = self.session.request(method, url, timeout=timeout, **kwargs)
            except requests.RequestException:
                if attempt == self.settings.max_retries:
                    raise
                time.sleep(min(2**attempt, 60))
                continue
            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == self.settings.max_retries:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                wait_seconds = int(retry_after) if retry_after else min(2**attempt, 60)
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            return response.json()

        raise RuntimeError(f"Failed to call Turn.io endpoint {path}")

    def _window_ranges(
        self, entity_type: str, from_ts: datetime, until_ts: datetime
    ) -> Generator[tuple[datetime, datetime], None, None]:
        chunk_days = ENTITY_CHUNK_DAYS[entity_type]
        chunk = timedelta(days=chunk_days)
        current = from_ts
        while current < until_ts:
            next_boundary = min(current + chunk, until_ts)
            yield current, next_boundary
            current = next_boundary

    def _create_cursor(self, entity_type: str, from_ts: datetime, until_ts: datetime) -> str:
        payload: dict[str, Any] = {
            "from": isoformat_utc(from_ts),
            "until": isoformat_utc(until_ts),
            "ordering": "asc",
        }
        if entity_type in {"messages", "statuses"}:
            payload["page_size"] = self.settings.page_size

        body = self._request("POST", f"/v1/data/{entity_type}/cursor", json=payload)
        cursor = body.get("cursor")
        if not cursor:
            raise RuntimeError(f"No cursor returned for {entity_type}: {body}")
        return cursor

    def _fetch_page(self, entity_type: str, cursor: str) -> dict:
        return self._request("GET", f"/v1/data/{entity_type}/cursor/{cursor}")

    def iter_records(
        self, entity_type: str, from_ts: datetime, until_ts: datetime
    ) -> Generator[dict[str, Any], None, None]:
        for window_start, window_end in self._window_ranges(entity_type, from_ts, until_ts):
            cursor = self._create_cursor(entity_type, window_start, window_end)
            while cursor:
                page = self._fetch_page(entity_type, cursor)
                data_items = page.get("data") or []

                if entity_type == "messages":
                    for envelope in data_items:
                        contacts = envelope.get("contacts") or []
                        contacts_by_wa_id = {
                            contact.get("wa_id"): (contact.get("profile") or {}).get("name")
                            for contact in contacts
                            if isinstance(contact, dict)
                        }
                        for message in envelope.get("messages") or []:
                            if not isinstance(message, dict):
                                continue
                            wa_id = message.get("from") or message.get("to") or message.get("recipient_id")
                            yield {
                                "message": message,
                                "contact_profile_name": contacts_by_wa_id.get(wa_id),
                            }
                elif entity_type == "statuses":
                    for envelope in data_items:
                        for status in envelope.get("statuses") or []:
                            if isinstance(status, dict):
                                yield status
                elif entity_type == "contacts":
                    for contact in data_items:
                        if isinstance(contact, dict):
                            yield contact
                else:
                    raise ValueError(f"Unsupported entity type: {entity_type}")

                cursor = (page.get("paging") or {}).get("next")
