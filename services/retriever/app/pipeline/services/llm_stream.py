import os
from collections.abc import Awaitable, Callable

from anyio import CancelScope
from openai import AsyncOpenAI
from app.concurrency import run_blocking

from app.pipeline.services.context_packer import PackedContext
from app.pipeline.services.llm_tools import _fit_answer_prompt
from app.schemas import LLMConfig


async def compose_streamed(
    system_prompt: str,
    static_prompt: str,
    packed_context: PackedContext,
    on_token: Callable[[str], Awaitable[None]],
    **kwargs,
) -> str:
    """Generate answer tokens and own the upstream client until cleanup ends."""
    user_prompt, max_tokens, _ = await run_blocking(
        _fit_answer_prompt,
        system_prompt,
        static_prompt,
        packed_context.text,
        packed_context.max_output_tokens,
    )
    client = AsyncOpenAI(
        base_url=LLMConfig.ANSWER_BASE_URL,
        api_key=os.getenv("OPENAI_API_KEY") or "EMPTY",
        timeout=120,
        max_retries=0,
    )

    try:
        return await _read_answer(
            client=client,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=max_tokens,
            on_token=on_token,
        )
    finally:
        with CancelScope(shield=True):
            await client.close()


async def _read_answer(
    client: AsyncOpenAI,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
    on_token: Callable[[str], Awaitable[None]],
) -> str:
    stream = await client.chat.completions.create(
        model=LLMConfig.ANSWER_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.15,
        max_tokens=max_tokens,
        stream=True,
    )
    parts: list[str] = []

    try:
        async for chunk in stream:
            text = chunk.choices[0].delta.content if chunk.choices else None
            if text:
                parts.append(text)
                await on_token(text)
    finally:
        with CancelScope(shield=True):
            # OpenAI 1.0 streams expose only the HTTP response for cleanup.
            close = getattr(stream, "close", None) or stream.response.aclose
            await close()

    if not parts:
        raise RuntimeError("The model returned an empty answer")

    return "".join(parts)
