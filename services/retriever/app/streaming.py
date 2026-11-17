import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import suppress

from anyio import CancelScope, move_on_after

from app.errors import log_failure
from app.pipeline.schemas import PipelineResult
from app.pipeline.search_pipeline import SearchPipeline
from app.pipeline.services.llm_stream import compose_streamed
from app.pipeline.services.query_logging import TraceLogger
from app.schemas import QueryTrace, SearchRequest, SearchResponse

logger = logging.getLogger(__name__)


class SearchStream:
    """Own a pipeline task until completion, including disconnect cleanup."""

    def __init__(
        self,
        request: SearchRequest,
        pipeline: SearchPipeline,
        logger: TraceLogger,
        apply_trace: Callable[[QueryTrace, PipelineResult], None],
    ) -> None:
        self.request = request
        self.pipeline = pipeline
        self.logger = logger
        self.apply_trace = apply_trace
        self.queue: asyncio.Queue[tuple[str, dict]] = asyncio.Queue(maxsize=32)
        self.trace = QueryTrace(
            query=request.query,
            user_id=request.user_id,
            session_id=request.session_id,
            search_mode=request.mode,
            top_k=request.top_k,
            prefetch_k=request.prefetch_k,
        )
        self.pipeline.on_event = self.emit
        self.pipeline.composer = self.compose

    async def emit(self, event: str, data: dict) -> None:
        """Queue an event, applying backpressure to the pipeline."""
        await self.queue.put((event, data))

    async def token(self, text: str) -> None:
        """Preserve the partial answer before publishing its next token."""
        self.trace.answer = (self.trace.answer or "") + text

        await self.emit(event="token", data={"text": text})

    async def compose(self, **kwargs) -> str:
        """Connect answer generation to this stream's token callback."""
        return await compose_streamed(on_token=self.token, **kwargs)

    async def events(self) -> AsyncIterator[str]:
        """Yield SSE frames and wait for task cleanup before closing."""
        task = asyncio.create_task(self._run())
        terminal = False

        try:
            yield _encode(
                event="status",
                data={"stage": "accepted", "query_id": self.trace.query_id},
            )
            while not terminal:
                try:
                    event, data = await asyncio.wait_for(self.queue.get(), timeout=15)
                except TimeoutError:
                    yield ": keep-alive\n\n"
                    continue

                terminal = event in {"done", "error"}
                yield _encode(event=event, data=data)
        finally:
            if not terminal:
                task.cancel()
            with CancelScope(shield=True), suppress(asyncio.CancelledError):
                await task

    async def _run(self) -> None:
        try:
            result = await self.pipeline.run(
                request=self.request, query_id=self.trace.query_id
            )
            self.apply_trace(self.trace, result)
            await self._complete(result=result)
        except asyncio.CancelledError:
            self.trace.error = "client_disconnected"
            raise
        except Exception as error:
            self.trace.error = type(error).__name__
            log_failure(logger=logger, operation="stream_search", error=error)
            await self.emit(
                event="error",
                data={
                    "message": "Не удалось завершить ответ. Повторите запрос.",
                    "query_id": self.trace.query_id,
                },
            )
        finally:
            await self._save_trace()

    async def _complete(self, result: PipelineResult) -> None:
        response = SearchResponse(
            results=result.context_included,
            answer=result.answer,
            effective_query=result.effective_question,
            was_rewritten=result.was_rewritten,
            answer_mode=result.answer_mode,
        )

        await self.emit(
            event="done",
            data={
                **response.model_dump(mode="json"),
                "query_id": self.trace.query_id,
                "latency_ms": result.timings.latency_ms,
            },
        )

    async def _save_trace(self) -> None:
        with move_on_after(5, shield=True):
            try:
                await self.logger.log(self.trace)
            except Exception as error:
                log_failure(logger=logger, operation="stream_trace", error=error)


def _encode(event: str, data: dict) -> str:
    payload = json.dumps(data, ensure_ascii=False)

    return f"event: {event}\ndata: {payload}\n\n"
