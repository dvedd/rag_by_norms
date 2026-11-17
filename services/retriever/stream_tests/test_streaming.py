import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

from app.pipeline.services import llm_stream
from app.schemas import StreamSearchRequest
from app.streaming import SearchStream


@pytest.fixture
def anyio_backend():
    return "asyncio"


def make_session(run):
    pipeline = SimpleNamespace(run=run)
    logger = SimpleNamespace(log=AsyncMock())
    session = SearchStream(
        StreamSearchRequest(query="Арматура"), pipeline, logger, Mock()
    )

    return session, pipeline, logger


def final_result(answer="Готово"):
    return SimpleNamespace(
        context_included=[],
        answer=answer,
        effective_question="Арматура",
        was_rewritten=False,
        answer_mode="direct_supported",
        timings=SimpleNamespace(latency_ms=25),
    )


@pytest.mark.anyio
async def test_token_precedes_completion():
    release = asyncio.Event()

    async def run(*args, **kwargs):
        await session.token("Первый ")
        await release.wait()
        await session.token("ответ")
        return final_result("Первый ответ")

    session, _, logger = make_session(run)
    events = session.events()
    assert "accepted" in await anext(events)
    assert "Первый " in await asyncio.wait_for(anext(events), 1)
    assert not release.is_set()
    release.set()
    remaining = [event async for event in events]

    assert '"text": "ответ"' in remaining[0]
    assert "event: done" in remaining[1]
    logger.log.assert_awaited_once()


@pytest.mark.anyio
async def test_disconnect_cancels_work():
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def run(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    session, _, logger = make_session(run)
    events = session.events()
    await anext(events)
    await started.wait()
    await asyncio.wait_for(events.aclose(), 1)

    assert cancelled.is_set()
    assert session.trace.error == "client_disconnected"
    logger.log.assert_awaited_once()


@pytest.mark.anyio
async def test_partial_error_is_terminal():
    async def run(*args, **kwargs):
        await session.token("Часть ответа")
        raise RuntimeError("secret upstream detail")

    session, _, _ = make_session(run)
    events = [event async for event in session.events()]

    assert "event: token" in events[1]
    assert "event: error" in events[-1]
    assert not any("event: done" in event for event in events)
    assert "secret upstream detail" not in "".join(events)
    assert session.trace.answer == "Часть ответа"


@pytest.mark.anyio
async def test_empty_search_finishes():
    session, _, _ = make_session(AsyncMock(return_value=final_result(None)))
    events = [event async for event in session.events()]
    data = json.loads(events[-1].split("data: ")[1])

    assert data["results"] == []
    assert data["answer"] is None


@pytest.mark.parametrize("query", ["", "  \n ", "a" * 4001])
def test_stream_rejects_bad_query(query):
    with pytest.raises(ValidationError):
        StreamSearchRequest(query=query)


def test_stream_defaults():
    request = StreamSearchRequest(query="  Арматура  ")

    assert request.query == "Арматура"
    assert request.compose_system_prompt
    assert request.rewrite_system_prompt
    assert request.top_k == 6


class TokenStream:
    def __init__(self, failure=None):
        self.failure = failure
        self.close = AsyncMock()

    async def __aiter__(self):
        yield SimpleNamespace(choices=[])
        for text in (None, "Ответ", " по нормам"):
            yield SimpleNamespace(
                choices=[SimpleNamespace(delta=SimpleNamespace(content=text))]
            )
        if self.failure:
            raise self.failure


@pytest.mark.anyio
async def test_llm_stream_closes(monkeypatch):
    stream = TokenStream()
    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(create=AsyncMock(return_value=stream))
        ),
        close=AsyncMock(),
    )
    factory = Mock(return_value=client)
    monkeypatch.setattr(llm_stream, "AsyncOpenAI", factory)
    monkeypatch.setattr(
        llm_stream, "_fit_answer_prompt", lambda *args: ("prompt", 100, 30)
    )
    tokens = AsyncMock()

    answer = await llm_stream.compose_streamed(
        system_prompt="system",
        static_prompt="static",
        packed_context=SimpleNamespace(text="context", max_output_tokens=100),
        on_token=tokens,
    )

    assert answer == "Ответ по нормам"
    assert tokens.await_count == 2
    assert client.chat.completions.create.call_args.kwargs["stream"] is True
    assert factory.call_args.kwargs["base_url"] == llm_stream.LLMConfig.ANSWER_BASE_URL
    stream.close.assert_awaited_once()
    client.close.assert_awaited_once()


@pytest.mark.anyio
async def test_upstream_error_closes():
    stream = TokenStream(RuntimeError("lost connection"))
    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(create=AsyncMock(return_value=stream)),
        )
    )

    with pytest.raises(RuntimeError, match="lost connection"):
        await llm_stream._read_answer(client, "system", "prompt", 100, AsyncMock())

    stream.close.assert_awaited_once()
