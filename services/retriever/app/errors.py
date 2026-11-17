import logging
from traceback import walk_tb

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)


def log_failure(logger: logging.Logger, operation: str, error: BaseException) -> None:
    """Log exception types and stack locations without values or source lines."""
    frames = "\n".join(
        f"{frame.f_code.co_filename}:{line_number} in {frame.f_code.co_name}"
        for frame, line_number in walk_tb(tb=error.__traceback__)
    )
    logger.error("%s failed (%s)\n%s", operation, type(error).__name__, frames)


class SafeErrorMiddleware:
    """Sanitize unexpected failures before they reach clients or the server."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Return a safe error or raise one when a response has already started."""
        if scope["type"] != "http":
            await self.app(scope=scope, receive=receive, send=send)

            return

        response_started = False

        async def track_response(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True

            await send(message)

        try:
            await self.app(scope=scope, receive=receive, send=track_response)
        except Exception as error:
            log_failure(logger=logger, operation="HTTP request", error=error)
            if response_started:
                raise RuntimeError("Internal service error") from None

            response = JSONResponse(
                status_code=500,
                content={"detail": "Internal service error"},
            )
            await response(scope=scope, receive=receive, send=send)
