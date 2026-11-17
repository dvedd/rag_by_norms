from fastapi import Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncEngine

from app.repositories import AuthContext, get_auth_context
from app.security import extract_bearer_token


def get_engine(request: Request) -> AsyncEngine:
    """Return the engine owned by the application lifespan."""
    return request.app.state.engine


async def get_current_auth(
    request: Request,
    authorization: str | None = Header(default=None),
) -> AuthContext:
    """Resolve a live bearer session or reject the request."""
    token = extract_bearer_token(authorization=authorization)

    if not token:
        raise _unauthorized()

    auth_context = await get_auth_context(engine=get_engine(request=request), token=token)

    if auth_context is None:
        raise _unauthorized()

    return auth_context


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required",
    )
