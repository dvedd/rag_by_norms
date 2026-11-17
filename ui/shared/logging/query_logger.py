import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from uuid import uuid4

from config import RetrievalResult
from shared.logging.failures import log_failure

logger = logging.getLogger(__name__)
_WRITE_LOCK = Lock()


class QueryLogger:
    """Serialize query-history updates within the UI process."""

    def __init__(
        self, log_path: str | Path = "retriever_service/logs/user_queries_logs.json"
    ) -> None:
        """Create the directory that owns the query history."""
        self._path = Path(log_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict:
        try:
            data = json.loads(s=self._path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError:
            logger.warning("Query history contains invalid JSON")
            return {}

        if not isinstance(data, dict):
            logger.warning("Query history has an invalid format")
            return {}

        return data

    def log(
        self,
        query: str,
        effective_query: str,
        was_rewritten: bool,
        mode: str,
        top_k: int,
        results: list[RetrievalResult],
        user_answer_md: str | None,
    ) -> None:
        """Atomically save a query record without logging its contents."""
        record = _query_record(
            effective_query=effective_query,
            was_rewritten=was_rewritten,
            mode=mode,
            top_k=top_k,
            results=results,
            user_answer_md=user_answer_md,
        )

        try:
            with _WRITE_LOCK:
                data = self._load()
                data[f"{query}_{uuid4()}"] = record
                self._write(data=data)
        except (OSError, TypeError, ValueError) as error:
            log_failure(logger=logger, operation="Save query history", error=error)

    def _write(self, data: dict) -> None:
        temporary_path: Path | None = None

        try:
            with NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._path.parent,
                delete=False,
            ) as file:
                temporary_path = Path(file.name)
                json.dump(obj=data, fp=file, ensure_ascii=False, indent=2)

            temporary_path.replace(target=self._path)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)


def _query_record(
    effective_query: str,
    was_rewritten: bool,
    mode: str,
    top_k: int,
    results: list[RetrievalResult],
    user_answer_md: str | None,
) -> dict:
    return {
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "effective_query": effective_query,
        "was_rewritten": was_rewritten,
        "mode": mode,
        "top_k": top_k,
        "answer": user_answer_md,
        "chunks": [
            _chunk_record(idx=idx, result=result)
            for idx, result in enumerate(results, start=1)
        ],
    }


def _chunk_record(idx: int, result: RetrievalResult) -> dict:
    return {
        "rank": idx,
        "score": result.score,
        "filename": result.filename,
        "chunk_index": result.chunk_index,
        "section_path": result.section_path,
        "headings": result.headings,
        "is_table": result.is_table,
        "man_refs": result.man_refs,
        "cross_refs": result.cross_refs,
        "anchor_refs": result.anchor_refs,
        "expanded_from": result.expanded_from,
        "text": result.text.strip(),
    }
