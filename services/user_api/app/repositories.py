from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Select, Update, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.concurrency import run_in_threadpool

from app.database import search_history, user_preferences, user_sessions, users
from app.security import create_session_token, hash_password, hash_token, verify_password

DEFAULT_THEME_KEY = "light"


class DuplicateUserError(Exception):
    """Indicate a conflicting account identifier."""


class NotFoundError(Exception):
    """Indicate an absent search owned by the requested account."""


@dataclass(frozen=True)
class CreatedSession:
    """Return the newly issued secret alongside its persisted identifiers."""

    session_id: UUID
    token: str
    expires_at: datetime


@dataclass(frozen=True)
class AuthContext:
    """Describe an authenticated account and its current session."""

    user: dict[str, Any]
    session_id: UUID
    token_hash: str


async def create_user(
    engine: AsyncEngine,
    email: str,
    password: str,
    display_name: str | None,
) -> dict[str, Any]:
    """Create an account without blocking the event loop on password hashing."""
    password_hash = await run_in_threadpool(func=hash_password, password=password)
    now = _now()
    statement = (
        users.insert()
        .values(
            id=uuid4(),
            email=email,
            display_name=_display_name(email=email, display_name=display_name),
            password_hash=password_hash,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        .returning(*users.c)
    )

    try:
        async with engine.begin() as connection:
            row = (await connection.execute(statement=statement)).mappings().one()
    except IntegrityError as error:
        if getattr(error.orig, "sqlstate", None) != "23505":
            raise

        raise DuplicateUserError from error

    return dict(row)


async def authenticate_user(
    engine: AsyncEngine,
    email: str,
    password: str,
) -> dict[str, Any] | None:
    """Verify account credentials without blocking concurrent requests."""
    user = await get_user_by_email(engine=engine, email=email)

    if user is None:
        return None

    password_matches = await run_in_threadpool(
        func=verify_password,
        password=password,
        encoded_hash=user["password_hash"],
    )

    if not password_matches:
        return None

    return user if user["is_active"] else None


async def get_user_by_email(engine: AsyncEngine, email: str) -> dict[str, Any] | None:
    """Return a stored account by its normalized email."""
    statement = select(*users.c).where(users.c.email == email)

    async with engine.begin() as connection:
        row = (await connection.execute(statement=statement)).mappings().first()

    return dict(row) if row else None


async def create_session(
    engine: AsyncEngine,
    user_id: UUID,
    ttl_days: int,
) -> CreatedSession:
    """Persist only a bearer token's hash and return the original token once."""
    token = create_session_token()
    expires_at = _now() + timedelta(days=ttl_days)
    session_id = uuid4()
    statement = user_sessions.insert().values(
        id=session_id,
        user_id=user_id,
        token_hash=hash_token(token=token),
        expires_at=expires_at,
        created_at=_now(),
    )

    async with engine.begin() as connection:
        await connection.execute(statement=statement)

    return CreatedSession(session_id=session_id, token=token, expires_at=expires_at)


async def get_auth_context(engine: AsyncEngine, token: str) -> AuthContext | None:
    """Resolve an active account with a live, unrevoked bearer session."""
    token_hash = hash_token(token=token)
    statement = _auth_context_statement(token_hash=token_hash)

    async with engine.begin() as connection:
        row = (await connection.execute(statement=statement)).mappings().first()

    if not row:
        return None

    return AuthContext(
        user=_user_from_auth_row(row=row),
        session_id=row["session_id"],
        token_hash=token_hash,
    )


async def revoke_session(engine: AsyncEngine, token_hash: str) -> None:
    """Revoke a session idempotently by its stored token hash."""
    statement = (
        update(table=user_sessions)
        .where(
            user_sessions.c.token_hash == token_hash,
            user_sessions.c.revoked_at.is_(other=None),
        )
        .values(revoked_at=_now())
    )

    async with engine.begin() as connection:
        await connection.execute(statement=statement)


async def get_preferences(engine: AsyncEngine, user_id: UUID) -> dict[str, Any]:
    """Return stored account preferences or the default theme."""
    statement = select(*user_preferences.c).where(user_preferences.c.user_id == user_id)

    async with engine.begin() as connection:
        row = (await connection.execute(statement=statement)).mappings().first()

    return dict(row) if row else {"user_id": user_id, "theme_key": DEFAULT_THEME_KEY}


async def set_preferences(
    engine: AsyncEngine,
    user_id: UUID,
    theme_key: str,
) -> dict[str, Any]:
    """Create or update the account's theme preference atomically."""
    now = _now()
    statement = (
        pg_insert(table=user_preferences)
        .values(
            user_id=user_id,
            theme_key=theme_key,
            created_at=now,
            updated_at=now,
        )
        .on_conflict_do_update(
            index_elements=[user_preferences.c.user_id],
            set_={"theme_key": theme_key, "updated_at": now},
        )
        .returning(*user_preferences.c)
    )

    async with engine.begin() as connection:
        row = (await connection.execute(statement=statement)).mappings().one()

    return dict(row)


async def list_searches(
    engine: AsyncEngine,
    user_id: UUID,
    limit: int,
) -> list[dict[str, Any]]:
    """List recent, nondeleted searches belonging to one account."""
    statement = (
        select(*search_history.c)
        .where(
            search_history.c.user_id == user_id,
            search_history.c.deleted_at.is_(other=None),
        )
        .order_by(search_history.c.created_at.desc())
        .limit(limit=limit)
    )

    async with engine.begin() as connection:
        rows = (await connection.execute(statement=statement)).mappings().all()

    return [_search_from_row(row=row) for row in rows]


async def create_search(
    engine: AsyncEngine,
    user_id: UUID,
    query: str,
    response: dict[str, Any],
) -> dict[str, Any]:
    """Persist a search response owned by the requested account."""
    now = _now()
    statement = (
        search_history.insert()
        .values(
            id=uuid4(),
            user_id=user_id,
            query=query,
            response_json=response,
            created_at=now,
        )
        .returning(*search_history.c)
    )

    async with engine.begin() as connection:
        row = (await connection.execute(statement=statement)).mappings().one()

    return _search_from_row(row=row)


async def update_search(
    engine: AsyncEngine,
    user_id: UUID,
    search_id: UUID,
    values: dict[str, Any],
) -> dict[str, Any]:
    """Update an existing search only when it belongs to the account."""
    statement = _search_update_statement(
        user_id=user_id,
        search_id=search_id,
        values=values,
    )

    async with engine.begin() as connection:
        row = (await connection.execute(statement=statement)).mappings().first()

    if not row:
        raise NotFoundError

    return _search_from_row(row=row)


async def delete_search(engine: AsyncEngine, user_id: UUID, search_id: UUID) -> None:
    """Soft-delete an existing search only when it belongs to the account."""
    statement = (
        update(table=search_history)
        .where(
            search_history.c.id == search_id,
            search_history.c.user_id == user_id,
            search_history.c.deleted_at.is_(other=None),
        )
        .values(deleted_at=_now(), updated_at=_now())
    )

    async with engine.begin() as connection:
        result = await connection.execute(statement=statement)

    if result.rowcount == 0:
        raise NotFoundError


def _auth_context_statement(token_hash: str) -> Select[Any]:
    return (
        select(
            users.c.id,
            users.c.email,
            users.c.display_name,
            users.c.is_active,
            user_sessions.c.id.label(name="session_id"),
        )
        .select_from(
            user_sessions.join(
                right=users, onclause=users.c.id == user_sessions.c.user_id
            )
        )
        .where(
            user_sessions.c.token_hash == token_hash,
            user_sessions.c.expires_at > _now(),
            user_sessions.c.revoked_at.is_(other=None),
            users.c.is_active.is_(other=True),
        )
    )


def _search_update_statement(
    user_id: UUID,
    search_id: UUID,
    values: dict[str, Any],
) -> Update:
    payload = {**values, "updated_at": _now()}

    return (
        update(table=search_history)
        .where(
            search_history.c.id == search_id,
            search_history.c.user_id == user_id,
            search_history.c.deleted_at.is_(other=None),
        )
        .values(**payload)
        .returning(*search_history.c)
    )


def _search_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    search = dict(row)
    search["response"] = search.pop("response_json")

    return search


def _user_from_auth_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "email": row["email"],
        "display_name": row["display_name"],
        "is_active": row["is_active"],
    }


def _display_name(email: str, display_name: str | None) -> str:
    if display_name and display_name.strip():
        return display_name.strip()

    return email.split("@", 1)[0]


def _now() -> datetime:
    return datetime.now(tz=UTC)
