import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from aiohttp import ClientResponseError
from airflow.providers.http.hooks.http import HttpAsyncHook, HttpHook

POLL_CONCURRENCY = 8


async def read_status(
    conn_id: str, endpoint: str, allow_missing: bool = False
) -> dict[str, Any]:
    """Read a status with an async client, recognizing optional missing tasks."""
    hook = HttpAsyncHook(method="GET", http_conn_id=conn_id, retry_limit=1)

    try:
        async with hook.session() as session:
            async with await session.run(endpoint=endpoint) as response:
                payload = await response.json()
    except ClientResponseError as error:
        if allow_missing and error.status == 404:
            return {"status": "missing", "reason": "task_not_found"}

        raise

    if not isinstance(payload, dict):
        raise ValueError("Task status response must be an object")

    return payload


def read_status_sync(hook: HttpHook, endpoint: str) -> dict[str, Any]:
    """Read a blocking fallback status without logging remote response bodies."""
    with hook.get_conn() as session:
        url = hook.url_from_endpoint(endpoint=endpoint)
        options = hook.merged_extra

        with session.get(
            url=url,
            timeout=options.get("timeout"),
            allow_redirects=options.get("allow_redirects", True),
        ) as response:
            response.raise_for_status()
            payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("Task status response must be an object")

    return payload


async def check_pending(
    task_ids: list[str],
    check_status: Callable[..., Awaitable[dict[str, Any]]],
) -> list[dict[str, Any] | BaseException]:
    """Poll independent tasks in bounded groups, preserving result order."""
    results: list[dict[str, Any] | BaseException] = []

    for index in range(0, len(task_ids), POLL_CONCURRENCY):
        batch = task_ids[index : index + POLL_CONCURRENCY]
        results.extend(
            await asyncio.gather(
                *(check_status(task_id=task_id) for task_id in batch),
                return_exceptions=True,
            )
        )

    return results
