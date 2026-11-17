from __future__ import annotations

import asyncio
import logging
from io import BytesIO
from pathlib import PurePosixPath
from uuid import UUID

from minio import Minio
from sqlalchemy import Column, DateTime, MetaData, Numeric, Table, Text, text
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from app.concurrency import run_blocking

from app.errors import log_failure
from app.schemas import QueryTrace

logger = logging.getLogger(__name__)
metadata = MetaData()

query_traces = Table(
    "query_traces",
    metadata,
    Column("query_id", PostgreSQLUUID(as_uuid=True), primary_key=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("user_id", Text, nullable=True),
    Column("session_id", Text, nullable=True),
    Column("query", Text, nullable=False),
    Column("latency_ms", Numeric, nullable=True),
    Column("rewrite_latency_ms", Numeric, nullable=True),
    Column("retrieval_latency_ms", Numeric, nullable=True),
    Column("generation_latency_ms", Numeric, nullable=True),
)


class TraceLogger:
    """Persist both trace copies and isolate storage errors from search results."""

    def __init__(
        self,
        minio_logger: MinioTraceLogger,
        trace_repository: PGTraceLogger,
    ) -> None:
        self.minio_logger = minio_logger
        self.trace_repository = trace_repository

    async def ensure_storage(self) -> None:
        """Finish both initialization operations before releasing their resources."""
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(coro=self.minio_logger.ensure_bucket())
            tasks.create_task(coro=self.trace_repository.ensure_tables())

    async def log(self, trace: QueryTrace) -> None:
        """Attempt both independent writes without exposing storage error details."""
        results = await asyncio.gather(
            self.minio_logger.log(trace),
            self.trace_repository.log(trace),
            return_exceptions=True,
        )

        self._log_errors(query_id=trace.query_id, results=results)

    def _log_errors(
        self,
        query_id: str,
        results: list[object],
    ) -> None:
        storages = ("minio", "postgres")

        for storage, result in zip(storages, results, strict=True):
            if isinstance(result, Exception):
                log_failure(
                    logger=logger,
                    operation=f"trace_logging:{storage}:{query_id}",
                    error=result,
                )


class MinioTraceLogger:
    """Store full trace documents in the configured object storage."""

    def __init__(
        self,
        client: Minio,
        bucket_name: str,
        prefix: str = "retriever-traces",
    ) -> None:
        self.client = client
        self.bucket_name = bucket_name
        self.prefix = prefix.strip("/")

    async def ensure_bucket(self) -> None:
        """Create the trace bucket if it does not exist."""
        exists = await run_blocking(
            self.client.bucket_exists, bucket_name=self.bucket_name
        )
        if not exists:
            await run_blocking(self.client.make_bucket, bucket_name=self.bucket_name)

    async def log(self, trace: QueryTrace) -> None:
        """Write the trace without blocking the event loop."""
        await run_blocking(self._write_trace, trace=trace)

    def _write_trace(self, trace: QueryTrace) -> None:
        payload = trace.model_dump_json(ensure_ascii=False).encode("utf-8")
        object_name = self._build_object_name(trace=trace)

        # The worker owns the buffer even when its async caller is cancelled.
        with BytesIO(payload) as buffer:
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=buffer,
                length=len(payload),
                content_type="application/json",
            )

    def _build_object_name(self, trace: QueryTrace) -> str:
        date_path = trace.created_at.strftime("%Y/%m/%d")

        return str(PurePosixPath(self.prefix) / date_path / f"{trace.query_id}.json")


class PGTraceLogger:
    """Store searchable trace metadata in PostgreSQL."""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def ensure_tables(self) -> None:
        """Create trace storage and add optional identity columns when needed."""
        async with self.engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
            await _ensure_query_trace_columns(connection=connection)

        logger.info("PostgreSQL trace storage initialized")

    async def log(self, trace: QueryTrace) -> None:
        """Insert searchable timing and identity metadata for one query."""
        statement = query_traces.insert().values(
            query_id=UUID(trace.query_id),
            created_at=trace.created_at,
            user_id=trace.user_id,
            session_id=trace.session_id,
            query=trace.query,
            latency_ms=trace.latency_ms,
            rewrite_latency_ms=trace.rewrite_latency_ms,
            retrieval_latency_ms=trace.retrieval_latency_ms,
            generation_latency_ms=trace.generation_latency_ms,
        )

        async with self.engine.begin() as connection:
            await connection.execute(statement)

        logger.debug("PostgreSQL trace stored query_id=%s", trace.query_id)


async def _ensure_query_trace_columns(connection: AsyncConnection) -> None:
    await connection.execute(
        text("ALTER TABLE query_traces ADD COLUMN IF NOT EXISTS user_id TEXT")
    )

    await connection.execute(
        text("ALTER TABLE query_traces ADD COLUMN IF NOT EXISTS session_id TEXT")
    )
