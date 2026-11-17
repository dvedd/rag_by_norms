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
COMPLETED_STATUSES = {"success", "partial_success", "skipped"}


class DoclingBatchTrigger(BaseTrigger):
    """Yield one event after the first failure or completion of the whole batch."""

    def __init__(
        self,
        task_ids: list[str],
        conn_id: str = "docling",
        poll_interval: int = 60,
    ) -> None:
        super().__init__()
        self.task_ids = task_ids
        self.conn_id = conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Keep the existing trigger path and constructor arguments serializable."""
        return (
            "common.sensors.docling_sensor.DoclingBatchTrigger",
            {
                "task_ids": self.task_ids,
                "conn_id": self.conn_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def _check_status(self, task_id: str) -> dict[str, Any]:
        return await read_status(
            conn_id=self.conn_id,
            endpoint=f"/v1/status/poll/{task_id}",
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll independent task statuses with bounded concurrency."""
        pending = list(self.task_ids)

        while pending:
            checks = await check_pending(
                task_ids=pending, check_status=self._check_status
            )

            for task_id, result in zip(list(pending), checks):
                if isinstance(result, asyncio.CancelledError):
                    raise result

                if isinstance(result, Exception):
                    log_failure(
                        logger=logger, operation="Poll Docling task", error=result
                    )
                    continue

                status = result.get("task_status")
                if status == "failure":
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "error": f"Docling task {task_id} failed",
                        }
                    )

                    return

                if status in COMPLETED_STATUSES:
                    pending.remove(task_id)
                    logger.debug("Docling task completed")

            if pending:
                logger.debug("Waiting for %s Docling tasks", len(pending))
                await asyncio.sleep(delay=self.poll_interval)

        logger.info("Docling batch completed: %s tasks", len(self.task_ids))
        yield TriggerEvent({"status": "completed", "task_ids": self.task_ids})


class DoclingBatchStatusSensor(BaseSensorOperator):
    """Return completed task IDs through XCom after a Docling batch completes."""

    ui_color = "#a3d9ff"
    template_fields = ("external_task_ids", "poll_interval")

    def __init__(
        self,
        *,
        external_task_ids: list[str],
        docling_conn_id: str = "docling",
        poll_interval: int = 60,
        deferrable: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.external_task_ids = external_task_ids
        self.docling_conn_id = docling_conn_id
        self.poll_interval = poll_interval
        self.deferrable = deferrable

    def execute(self, context: Context) -> list[str] | None:
        """Defer polling when supported; otherwise block until the batch completes."""
        if self.deferrable:
            self.defer(
                trigger=DoclingBatchTrigger(
                    task_ids=self.external_task_ids,
                    conn_id=self.docling_conn_id,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

            return None

        hook = HttpHook(method="GET", http_conn_id=self.docling_conn_id)
        pending = list(self.external_task_ids)

        while pending:
            for task_id in list(pending):
                result = read_status_sync(
                    hook=hook, endpoint=f"/v1/status/poll/{task_id}"
                )
                status = result.get("task_status")
                if status in COMPLETED_STATUSES:
                    pending.remove(task_id)
                elif status == "failure":
                    raise RuntimeError(f"Docling task {task_id} failed")

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
            raise RuntimeError("Docling batch failed")

        task_ids = event["task_ids"]
        logger.info("Docling batch completed: %s tasks", len(task_ids))

        return task_ids
