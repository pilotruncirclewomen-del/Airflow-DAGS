from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Iterable


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_turn_timestamp(raw: Any) -> datetime | None:
    if raw in (None, ""):
        return None

    if isinstance(raw, (int, float)) or (isinstance(raw, str) and raw.isdigit()):
        epoch = int(raw)
        if epoch > 10**12:
            epoch = epoch / 1000
        return datetime.fromtimestamp(epoch, tz=timezone.utc)

    if isinstance(raw, str):
        candidate = raw.strip()
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    return None


def isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def chunked(items: Iterable[Any], size: int) -> Iterable[list[Any]]:
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def first_present(payload: dict, *paths: tuple[str, ...]) -> Any:
    for path in paths:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        if current not in (None, ""):
            return current
    return None


def safe_bigint(raw: Any) -> int | None:
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None
