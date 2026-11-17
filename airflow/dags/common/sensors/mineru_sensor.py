from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import BaseSensorOperator, Context
from airflow.triggers.base import BaseTrigger, TriggerEvent
from common.logging import log_failure
from common.sensors.polling import check_pending, read_status, read_status_sync

logger = logging.getLogger(__name__)


class MineruBatchTrigger(BaseTrigger):
    """Yield completion results or failure after the configured missing-task limit."""

    def __init__(
        self,
        task_ids: list[str],
        conn_id: str = "mineru",
        poll_interval: int = 60,
        missing_limit: int = 3,
    ) -> None:
        super().__init__()
        self.task_ids = task_ids
        self.conn_id = conn_id
        self.poll_interval = poll_interval
        self.missing_limit = missing_limit

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Keep the existing trigger path and constructor arguments serializable."""
        return (
            "common.sensors.mineru_sensor.MineruBatchTrigger",
            {
                "task_ids": self.task_ids,
                "conn_id": self.conn_id,
                "poll_interval": self.poll_interval,
                "missing_limit": self.missing_limit,
            },
        )

    async def _check_status(self, task_id: str) -> dict[str, Any]:
        return await read_status(
            conn_id=self.conn_id,
            endpoint=f"/tasks/{task_id}",
            allow_missing=True,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll in bounded groups, keeping each task's missing-state counter."""
        pending = list(self.task_ids)
        completed_results: dict[str, list[str]] = {}
        missing_counts: dict[str, int] = {}

        while pending:
            checks = await check_pending(
                task_ids=pending, check_status=self._check_status
            )

            for task_id, result in zip(list(pending), checks):
                if isinstance(result, asyncio.CancelledError):
                    raise result

                if isinstance(result, Exception):
                    log_failure(logger=logger, operation="Poll MinerU task", error=result)
                    continue

                status = result.get("status")
                if status == "missing":
                    count = missing_counts.get(task_id, 0) + 1
                    missing_counts[task_id] = count
                    logger.warning(
                        "MinerU task missing (%s/%s)", count, self.missing_limit
                    )
                    if count < self.missing_limit:
                        continue

                if status == "failed" or status == "missing":
                    reason = "task_not_found" if status == "missing" else "remote_failure"
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "error": f"MinerU task {task_id} failed: {reason}",
                        }
                    )

                    return

                if status == "completed":
                    completed_results[task_id] = result.get("file_names", [])
                    pending.remove(task_id)
                    logger.debug("MinerU task completed")

            if pending:
                logger.debug("Waiting for %s MinerU tasks", len(pending))
                await asyncio.sleep(delay=self.poll_interval)

        logger.info("MinerU batch completed: %s tasks", len(self.task_ids))
        yield TriggerEvent(
            {
                "status": "completed",
                "task_ids": self.task_ids,
                "results": completed_results,
            }
        )


class MineruBatchStatusSensor(BaseSensorOperator):
    """Return completed task IDs through XCom after a MinerU batch completes."""

    ui_color = "#73deff"
    template_fields = ("external_task_ids", "poll_interval")

    def __init__(
        self,
        *,
        external_task_ids: list[str],
        mineru_conn_id: str = "mineru",
        poll_interval: int = 60,
        deferrable: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.external_task_ids = external_task_ids
        self.mineru_conn_id = mineru_conn_id
        self.poll_interval = poll_interval
        self.deferrable = deferrable

    def execute(self, context: Context) -> list[str] | None:
        """Defer polling when supported; otherwise block until the batch completes."""
        if self.deferrable:
            self.defer(
                trigger=MineruBatchTrigger(
                    task_ids=self.external_task_ids,
                    conn_id=self.mineru_conn_id,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

            return None

        hook = HttpHook(method="GET", http_conn_id=self.mineru_conn_id)
        pending = set(self.external_task_ids)

        while pending:
            for task_id in list(pending):
                result = read_status_sync(hook=hook, endpoint=f"/tasks/{task_id}")
                status = result.get("status")
                if status == "completed":
                    pending.remove(task_id)
                elif status == "failed":
                    raise RuntimeError(f"MinerU task {task_id} failed")

            if pending:
                time.sleep(self.poll_interval)

        return self.external_task_ids

    def execute_complete(
        self,
        context: Context,
        event: dict[str, Any] | None = None,
    ) -> list[str]:
        """Resume a deferred sensor, preserving the completed task ID payload."""
        if not event or event.get("status") == "failed":
            raise RuntimeError("MinerU batch failed")

        task_ids = event["task_ids"]
        logger.info("MinerU batch completed: %s tasks", len(task_ids))

        return task_ids
