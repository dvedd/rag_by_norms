import asyncio
import logging
from collections.abc import Callable
from typing import Any

from anyio import CancelScope
from starlette.concurrency import run_in_threadpool

from app.errors import log_failure

logger = logging.getLogger(__name__)


async def run_blocking[T](function: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Wait for an active worker before propagating cancellation to its owner."""
    task = asyncio.create_task(coro=run_in_threadpool(function, *args, **kwargs))

    try:
        return await asyncio.shield(arg=task)
    except asyncio.CancelledError:
        with CancelScope(shield=True):
            while not task.done():
                try:
                    await asyncio.shield(arg=task)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break

        if not task.cancelled() and (error := task.exception()) is not None:
            log_failure(logger=logger, operation="cancelled_worker", error=error)

        raise
