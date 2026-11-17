import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.pipeline.services.llm_stream import _read_answer


@pytest.fixture
def anyio_backend():
    return "asyncio"


class LegacyStream:
    def __init__(self, failure):
        self.response = SimpleNamespace(aclose=AsyncMock())
        self.failure = failure

    async def __aiter__(self):
        yield SimpleNamespace(
            choices=[SimpleNamespace(delta=SimpleNamespace(content="Ответ"))]
        )

        if self.failure is not None:
            raise self.failure


@pytest.mark.anyio
@pytest.mark.parametrize(
    "failure", [None, RuntimeError("lost connection"), asyncio.CancelledError()]
)
async def test_legacy_response_closes(failure):
    stream = LegacyStream(failure)
    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(create=AsyncMock(return_value=stream))
        )
    )
    on_token = AsyncMock()

    if failure is None:
        answer = await _read_answer(client, "system", "prompt", 100, on_token)
        assert answer == "Ответ"
    else:
        with pytest.raises(type(failure)) as raised:
            await _read_answer(client, "system", "prompt", 100, on_token)
        assert raised.value is failure

    on_token.assert_awaited_once_with("Ответ")
    stream.response.aclose.assert_awaited_once()
