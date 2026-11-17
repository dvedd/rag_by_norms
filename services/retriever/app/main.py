import logging
import os
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

import certifi
from anyio import CancelScope
from dotenv import load_dotenv
from fastapi import APIRouter, Body, FastAPI
from fastapi.responses import StreamingResponse
from minio import Minio
from openai import AsyncOpenAI
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from urllib3 import PoolManager
from urllib3.util import Retry, Timeout

from app.concurrency import run_blocking
from app.errors import SafeErrorMiddleware, log_failure
from app.pipeline.search_pipeline import SearchPipeline
from app.pipeline.services import (
    MinioTraceLogger,
    PGTraceLogger,
    QdrantRetriever,
    TraceLogger,
    compose_answer,
    get_bge_m3,
    rewrite_query,
)
from app.pipeline.services.retriever import open_retriever
from app.schemas import (
    LLMConfig,
    QueryTrace,
    RetrievalResult,
    SearchRequest,
    SearchResponse,
    StreamSearchRequest,
)
from app.streaming import SearchStream
from app.tracing import apply_pipeline_trace as _apply_pipeline_trace

logger = logging.getLogger(__name__)
load_dotenv()
retriever: QdrantRetriever | None = None
llm_client: AsyncOpenAI | None = None
trace_logger: TraceLogger

RETRIEVER_DATABASE_URL = os.getenv(
    "RETRIEVER_DATABASE_URL",
    "postgresql+asyncpg://app_user:app_password@localhost:5432/app_db",
)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
TRACE_BUCKET_NAME = os.getenv("TRACE_BUCKET_NAME", "ragfiles")
TRACE_PREFIX = os.getenv("TRACE_PREFIX", "dev_data/logs/query-traces")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Own clients and release them on startup failure or application shutdown."""
    global retriever, llm_client, trace_logger
    stack = AsyncExitStack()

    try:
        try:
            engine = create_async_engine(url=RETRIEVER_DATABASE_URL, pool_pre_ping=True)
            stack.push_async_callback(engine.dispose)
            retriever = await run_blocking(open_retriever, stack=stack)
            llm_client = await stack.enter_async_context(
                cm=AsyncOpenAI(
                    base_url=LLMConfig.REWRITER_BASE_URL,
                    api_key=os.getenv("OPENAI_API_KEY") or "EMPTY",
                    timeout=120,
                )
            )
            trace_logger = _create_trace_logger(stack=stack, engine=engine)

            await trace_logger.ensure_storage()
            await run_blocking(get_bge_m3)

            yield
        finally:
            with CancelScope(shield=True):
                await stack.aclose()
    except Exception as error:
        log_failure(logger=logger, operation="retriever_lifespan", error=error)
        raise RuntimeError("Retriever lifecycle failed") from None


def _create_trace_logger(stack: AsyncExitStack, engine: AsyncEngine) -> TraceLogger:
    pool = PoolManager(
        timeout=Timeout(connect=300, read=300),
        maxsize=10,
        cert_reqs="CERT_REQUIRED",
        ca_certs=os.getenv("SSL_CERT_FILE") or certifi.where(),
        retries=Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]),
    )
    stack.push_async_callback(run_blocking, function=pool.clear)
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
        http_client=pool,
    )

    return TraceLogger(
        minio_logger=MinioTraceLogger(
            client=minio_client,
            bucket_name=TRACE_BUCKET_NAME,
            prefix=TRACE_PREFIX,
        ),
        trace_repository=PGTraceLogger(engine=engine),
    )


app = FastAPI(title="Construction RAG API", lifespan=lifespan)
app.add_middleware(SafeErrorMiddleware)
router = APIRouter()


@router.get("/he", response_model=None)
async def root() -> dict[str, str]:
    """Return the existing health response."""
    return {"message": "Hello World"}


@router.post("/search", response_model=SearchResponse)
async def search(
    request: SearchRequest = Body(..., description="Search parameters"),
) -> SearchResponse:
    """Run retrieval and generation while persisting the query trace."""
    trace = QueryTrace(
        query=request.query,
        user_id=request.user_id,
        session_id=request.session_id,
        search_mode=request.mode,
        top_k=request.top_k,
        prefetch_k=request.prefetch_k,
    )

    try:
        pipeline = SearchPipeline(
            retriever=retriever,
            llm_client=llm_client,
            rewriter=rewrite_query,
            composer=compose_answer,
        )
        result = await pipeline.run(request=request, query_id=trace.query_id)
        _apply_pipeline_trace(trace=trace, result=result)

        return SearchResponse(
            results=result.results,
            answer=result.answer,
            effective_query=result.effective_question,
            was_rewritten=result.was_rewritten,
            answer_mode=result.answer_mode,
        )
    except Exception as error:
        trace.error = type(error).__name__
        raise
    finally:
        await trace_logger.log(trace=trace)


@router.post("/search/stream", response_class=StreamingResponse)
async def search_stream(request: StreamSearchRequest) -> StreamingResponse:
    """Stream ordered pipeline events and cancel work on client disconnect."""
    pipeline = SearchPipeline(
        retriever=retriever,
        llm_client=llm_client,
        rewriter=rewrite_query,
        composer=compose_answer,
    )
    stream = SearchStream(
        request=request,
        pipeline=pipeline,
        logger=trace_logger,
        apply_trace=_apply_pipeline_trace,
    )

    return StreamingResponse(
        content=stream.events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"},
    )


@router.post("/rewrite_query", response_model=None)
async def rewrite_query_endpoint(
    query: str = Body(..., description="User query"),
    system_prompt: str = Body(..., description="System prompt"),
) -> dict[str, str | bool]:
    """Rewrite a user query using the configured LLM."""
    rewritten, was_rewritten = await rewrite_query(
        client=llm_client,
        query=query,
        system_prompt=system_prompt,
    )

    return {"rewritten": rewritten, "was_rewritten": was_rewritten}


@router.post("/compose_answer", response_model=None)
async def compose_answer_endpoint(
    query: str = Body(..., description="User query"),
    effective_query: str = Body(..., description="Rewritten queru"),
    system_prompt: str = Body(..., description="System prompt"),
    results: list[RetrievalResult] = Body(..., description="/search result"),
) -> dict[str, str]:
    """Compose an answer from caller-provided retrieval results."""
    answer = await compose_answer(
        client=llm_client,
        query=query,
        effective_query=effective_query,
        system_prompt=system_prompt,
        results=results,
    )

    return {"answer": answer}


app.include_router(router=router)
