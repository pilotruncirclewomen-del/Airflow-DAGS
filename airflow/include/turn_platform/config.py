from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Environment variable {name} is required")
    return value


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    return int(raw)


@dataclass(frozen=True)
class TurnIOSettings:
    base_url: str
    token: str
    page_size: int
    request_timeout_seconds: int
    max_retries: int
    safety_lag_seconds: int


@dataclass(frozen=True)
class WarehouseSettings:
    dsn: str
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str


@dataclass(frozen=True)
class MetabaseSettings:
    url: str
    site_name: str
    admin_first_name: str
    admin_last_name: str
    admin_email: str
    admin_password: str
    database_name: str
    bootstrap_timeout_seconds: int


@dataclass(frozen=True)
class PipelineSettings:
    sql_dir: Path
    messages_default_lookback_minutes: int
    statuses_default_lookback_minutes: int
    contacts_default_lookback_minutes: int
    messages_overlap_seconds: int
    statuses_overlap_seconds: int
    contacts_overlap_seconds: int
    batch_size: int


@dataclass(frozen=True)
class AppSettings:
    turnio: TurnIOSettings
    warehouse: WarehouseSettings
    metabase: MetabaseSettings
    pipeline: PipelineSettings


def load_settings() -> AppSettings:
    return AppSettings(
        turnio=TurnIOSettings(
            base_url=os.getenv("TURNIO_BASE_URL", "https://whatsapp.turn.io").rstrip("/"),
            token=_env("TURNIO_BEARER_TOKEN"),
            page_size=_int_env("TURNIO_PAGE_SIZE", 200),
            request_timeout_seconds=_int_env("TURNIO_REQUEST_TIMEOUT_SECONDS", 60),
            max_retries=_int_env("TURNIO_MAX_RETRIES", 5),
            safety_lag_seconds=_int_env("TURNIO_SAFETY_LAG_SECONDS", 120),
        ),
        warehouse=WarehouseSettings(
            dsn=_env("WAREHOUSE_DB_DSN"),
            host=_env("WAREHOUSE_DB_HOST"),
            port=_int_env("WAREHOUSE_DB_PORT", 25060),
            database=_env("WAREHOUSE_DB_NAME"),
            user=_env("WAREHOUSE_DB_USER"),
            password=_env("WAREHOUSE_DB_PASSWORD"),
            sslmode=os.getenv("WAREHOUSE_DB_SSLMODE", "require"),
        ),
        metabase=MetabaseSettings(
            url=os.getenv("METABASE_URL", "http://metabase:3000").rstrip("/"),
            site_name=os.getenv("METABASE_SITE_NAME", "Turn.io Analytics"),
            admin_first_name=os.getenv("METABASE_ADMIN_FIRST_NAME", "Turn"),
            admin_last_name=os.getenv("METABASE_ADMIN_LAST_NAME", "Admin"),
            admin_email=_env("METABASE_ADMIN_EMAIL"),
            admin_password=_env("METABASE_ADMIN_PASSWORD"),
            database_name=os.getenv("METABASE_WAREHOUSE_NAME", "Turn.io Warehouse"),
            bootstrap_timeout_seconds=_int_env("METABASE_BOOTSTRAP_TIMEOUT_SECONDS", 300),
        ),
        pipeline=PipelineSettings(
            sql_dir=Path(os.getenv("TURN_PLATFORM_SQL_DIR", "/opt/pipeline/sql")),
            messages_default_lookback_minutes=_int_env(
                "PIPELINE_MESSAGES_DEFAULT_LOOKBACK_MINUTES", 240
            ),
            statuses_default_lookback_minutes=_int_env(
                "PIPELINE_STATUSES_DEFAULT_LOOKBACK_MINUTES", 240
            ),
            contacts_default_lookback_minutes=_int_env(
                "PIPELINE_CONTACTS_DEFAULT_LOOKBACK_MINUTES", 1440
            ),
            messages_overlap_seconds=_int_env("PIPELINE_MESSAGES_OVERLAP_SECONDS", 300),
            statuses_overlap_seconds=_int_env("PIPELINE_STATUSES_OVERLAP_SECONDS", 300),
            contacts_overlap_seconds=_int_env("PIPELINE_CONTACTS_OVERLAP_SECONDS", 600),
            batch_size=_int_env("PIPELINE_BATCH_SIZE", 250),
        ),
    )

