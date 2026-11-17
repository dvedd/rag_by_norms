import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    """Store database and session settings."""

    database_url: str
    session_ttl_days: int


@lru_cache
def get_settings() -> Settings:
    """Return configuration loaded once from environment variables."""
    return Settings(
        database_url=os.getenv(
            key="USER_API_DATABASE_URL",
            default="postgresql+asyncpg://app_user:app_password@localhost:5432/app_db",
        ),
        session_ttl_days=_read_int(name="USER_API_SESSION_TTL_DAYS", default=30),
    )


def _read_int(name: str, default: int) -> int:
    raw_value = os.getenv(key=name)
    if raw_value is None:
        return default

    return int(raw_value)
