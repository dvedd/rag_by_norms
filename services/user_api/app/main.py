from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
import logging
from typing import Annotated, Any
from uuid import UUID

from anyio import CancelScope
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config import get_settings
from app.database import build_engine, ensure_storage
from app.dependencies import get_current_auth, get_engine
from app.errors import SafeErrorMiddleware, log_failure
from app.repositories import (
    AuthContext,
    CreatedSession,
    DuplicateUserError,
    NotFoundError,
    authenticate_user,
    create_search,
    create_session,
    create_user,
    delete_search,
    get_preferences,
    list_searches,
    revoke_session,
    set_preferences,
    update_search,
)
from app.schemas import (
    AuthResponse,
    LoginRequest,
    PreferencesRead,
    PreferencesUpdate,
    RegisterRequest,
    SearchCreate,
    SearchRead,
    SearchUpdate,
    UserRead,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Own the database engine, including failed startup and cancelled shutdown."""
    try:
        settings = get_settings()
        engine = build_engine(database_url=settings.database_url)
        app.state.engine = engine

        try:
            await ensure_storage(engine=engine)

            yield
        finally:
            with CancelScope(shield=True):
                await engine.dispose()
    except Exception as error:
        log_failure(logger=logger, operation="User API lifecycle", error=error)

        raise RuntimeError("User API lifecycle failed") from None


app = FastAPI(title="EngineeringRAG User API", lifespan=lifespan)
app.add_middleware(SafeErrorMiddleware)
router = APIRouter()
EngineDep = Annotated[AsyncEngine, Depends(dependency=get_engine)]
AuthDep = Annotated[AuthContext, Depends(dependency=get_current_auth)]


@router.get("/health")
async def health() -> dict[str, str]:
    """Report process health without contacting storage."""
    return {"status": "ok"}


@router.post(
    "/auth/register",
    response_model=AuthResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register(payload: RegisterRequest, engine: EngineDep) -> AuthResponse:
    """Register an account and issue its first session."""
    try:
        user = await create_user(
            engine=engine,
            email=payload.email,
            password=payload.password,
            display_name=payload.display_name,
        )
    except DuplicateUserError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User already exists",
        ) from error

    session = await create_session(
        engine=engine,
        user_id=user["id"],
        ttl_days=get_settings().session_ttl_days,
    )

    return _auth_response(user=user, session=session)


@router.post("/auth/login", response_model=AuthResponse)
async def login(payload: LoginRequest, engine: EngineDep) -> AuthResponse:
    """Authenticate an account and issue a session."""
    user = await authenticate_user(
        engine=engine,
        email=payload.email,
        password=payload.password,
    )

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    session = await create_session(
        engine=engine,
        user_id=user["id"],
        ttl_days=get_settings().session_ttl_days,
    )

    return _auth_response(user=user, session=session)


@router.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(auth: AuthDep, engine: EngineDep) -> Response:
    """Revoke the caller's current session."""
    await revoke_session(engine=engine, token_hash=auth.token_hash)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/me", response_model=UserRead)
async def me(auth: AuthDep) -> UserRead:
    """Return the authenticated account profile."""
    return _user_response(user=auth.user)


@router.get("/me/preferences", response_model=PreferencesRead)
async def read_preferences(auth: AuthDep, engine: EngineDep) -> PreferencesRead:
    """Return saved preferences or their defaults."""
    preferences = await get_preferences(engine=engine, user_id=auth.user["id"])

    return PreferencesRead(theme_key=preferences["theme_key"])


@router.patch("/me/preferences", response_model=PreferencesRead)
async def patch_preferences(
    payload: PreferencesUpdate,
    auth: AuthDep,
    engine: EngineDep,
) -> PreferencesRead:
    """Save the authenticated account's theme preference."""
    preferences = await set_preferences(
        engine=engine,
        user_id=auth.user["id"],
        theme_key=payload.theme_key,
    )

    return PreferencesRead(theme_key=preferences["theme_key"])


@router.get("/me/searches", response_model=list[SearchRead])
async def read_searches(
    auth: AuthDep,
    engine: EngineDep,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[dict[str, Any]]:
    """List the caller's most recent searches."""
    return await list_searches(engine=engine, user_id=auth.user["id"], limit=limit)


@router.post(
    "/me/searches", response_model=SearchRead, status_code=status.HTTP_201_CREATED
)
async def post_search(
    payload: SearchCreate,
    auth: AuthDep,
    engine: EngineDep,
) -> dict[str, Any]:
    """Save a search response for the authenticated account."""
    return await create_search(
        engine=engine,
        user_id=auth.user["id"],
        query=payload.query,
        response=payload.response,
    )


@router.patch("/me/searches/{search_id}", response_model=SearchRead)
async def patch_search(
    search_id: UUID,
    payload: SearchUpdate,
    auth: AuthDep,
    engine: EngineDep,
) -> dict[str, Any]:
    """Update a search owned by the authenticated account."""
    values = _search_update_values(payload=payload)

    if not values:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nothing to update",
        )

    try:
        return await update_search(
            engine=engine,
            user_id=auth.user["id"],
            search_id=search_id,
            values=values,
        )
    except NotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Search not found",
        ) from error


@router.delete("/me/searches/{search_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_search(search_id: UUID, auth: AuthDep, engine: EngineDep) -> Response:
    """Soft-delete a search owned by the authenticated account."""
    try:
        await delete_search(engine=engine, user_id=auth.user["id"], search_id=search_id)
    except NotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Search not found",
        ) from error

    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _auth_response(user: Mapping[str, Any], session: CreatedSession) -> AuthResponse:
    return AuthResponse(
        token=session.token,
        session_id=session.session_id,
        expires_at=session.expires_at,
        user=_user_response(user=user),
    )


def _user_response(user: Mapping[str, Any]) -> UserRead:
    return UserRead(
        id=user["id"],
        email=user["email"],
        display_name=user["display_name"],
    )


def _search_update_values(payload: SearchUpdate) -> dict[str, Any]:
    values = {}
    if payload.query is not None:
        values["query"] = payload.query

    if payload.response is not None:
        values["response_json"] = payload.response

    return values


app.include_router(router=router)
