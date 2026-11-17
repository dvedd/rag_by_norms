from app.pipeline.schemas import PipelineResult
from app.schemas import QueryTrace, RetrievedChunkTrace


def apply_pipeline_trace(trace: QueryTrace, result: PipelineResult) -> None:
    """Copy the completed pipeline stages into the query trace."""
    trace.rewritten_query = result.effective_question if result.was_rewritten else None
    trace.answer = result.answer
    trace.answer_mode = result.answer_mode
    trace.latency_ms = result.timings.latency_ms
    trace.rewrite_latency_ms = result.timings.rewrite_latency_ms
    trace.retrieval_latency_ms = result.timings.retrieval_latency_ms
    trace.generation_latency_ms = result.timings.generation_latency_ms
    trace.pipeline_result = result.model_dump(mode="json")
    trace.context_chunks = [chunk.id for chunk in result.context_included]
    trace.retrieved = [
        RetrievedChunkTrace(
            chunk_id=chunk.id,
            filename=chunk.filename,
            rank=rank,
            score=chunk.score,
            text=chunk.text,
        )
        for rank, chunk in enumerate(result.retrieved, start=1)
    ]
