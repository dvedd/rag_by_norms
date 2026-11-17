from __future__ import annotations

import json
import logging
import re
import uuid
from collections import defaultdict
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.sdk import dag, task
from common.batching import batch_list
from common.embeddings import get_bge_m3
from common.logging import log_failure
from common.storage import del_file, load_to_s3, read_s3_text
from pendulum import datetime

if TYPE_CHECKING:
    from airflow.sdk import DAG

logger = logging.getLogger(__name__)

RAG_DATA_BUCKET = "ragfiles"
DEV_DATA = "dev_data"
DEV_DATA_MINERU_MD = f"{DEV_DATA}/mineru_md"
DEV_DATA_DOCLING_JSON = f"{DEV_DATA}/docling_jsons"
SUP_FORMATS = ["pdf/"]
BATCH_SIZE = 16

QDRANT_URL = "http://qdrant:6333"
QDRANT_COLLECTION = "construction_docs"
QDRANT_VECTOR_SIZE = 1024
QDRANT_ENCODE_BATCH = 16
QDRANT_UPSERT_BATCH = 32
QDRANT_COLBERT_SIZE = 1024
TABLE_ID_RE = re.compile(r"\bTABLE_ID\s*=\s*(?P<table_id>[a-z0-9_\-]+)", re.IGNORECASE)


# ============================================================
# Helpers
# ============================================================


def chunk_text(chunk: dict) -> str:
    """Combine the normalized and raw text of a document chunk."""

    return "\n".join(str(chunk.get(key) or "") for key in ("text", "raw_text"))


def table_text_ids(chunks: list[dict]) -> set[str]:
    """Collect table IDs embedded in chunk text."""
    ids: set[str] = set()
    for chunk in chunks:
        matches = TABLE_ID_RE.finditer(chunk_text(chunk=chunk))
        ids.update(match.group("table_id") for match in matches)

    return ids


def table_payload_ids(chunks: list[dict]) -> set[str]:
    """Collect table IDs present in enriched chunk metadata."""

    return {chunk["table_id"] for chunk in chunks if chunk.get("table_id")}


def validate_table_payload(
    source_table_ids: set[str],
    enriched_chunks: list[dict],
    file_name: str,
) -> None:
    """Fail if chunk enrichment loses any source table IDs."""
    payload_ids = table_payload_ids(chunks=enriched_chunks)
    missing_ids = sorted(source_table_ids - payload_ids)
    logger.info(
        ">>> Table metadata quality: file=%s, annotated_input=%s, payload_output=%s",
        file_name,
        len(source_table_ids),
        len(payload_ids),
    )

    if missing_ids:
        raise ValueError(
            f"Table metadata was lost for {file_name}: missing table_ids={missing_ids}"
        )


# ============================================================
# DAG
# ============================================================


def build_batch_pipeline(dag_id: str, mode: str) -> DAG:
    """Batch document processing pipeline — see module docstring for full description."""

    @dag(
        dag_id=dag_id,
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["data_pipeline", mode],
        doc_md=__doc__,
    )
    def batch_pipeline() -> None:
        """Declare document processing tasks and their dependencies."""

        # ------------------------------------------------------------------------------------------------
        # 1. Discovery
        # ------------------------------------------------------------------------------------------------

        @task
        def get_buckets_data(
            bucket_name_filter: str | list[str] | None = None,
        ) -> dict[str, list[str]]:
            """List accessible storage buckets and their top-level prefixes."""
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook(aws_conn_id="minio")
            try:
                buckets_data = hook.get_conn().list_buckets()
                buckets = buckets_data.get("Buckets", [])

                if bucket_name_filter:
                    """
                    "Buckets": [
                        {
                            "Name": "...",
                            "CreationDate": "..."
                        }, ...
                    ],
                    """
                    target = (
                        [bucket_name_filter]
                        if isinstance(bucket_name_filter, str)
                        else bucket_name_filter
                    )
                    names = [b["Name"] for b in buckets if b["Name"] in target]
                else:
                    names = [b["Name"] for b in buckets]

                logger.info("Found buckets: %s", names)
                data = defaultdict(list)

                for name in names:
                    prefs = hook.list_prefixes(bucket_name=name, delimiter="/")
                    data[name].extend(prefs)

                logger.info("Found prefixes %s", data)

                return data
            except Exception as ex:
                log_failure(logger=logger, operation="List source buckets", error=ex)

                return []

        @task
        def list_files_to_process(
            bucket: str,
            prefix: str | None = None,
            skip_process: bool = True,
        ) -> list[str]:
            """List eligible source keys while optionally skipping processed documents."""
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook(aws_conn_id="minio")

            if prefix:
                candidates = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
            else:
                candidates = []
                for pref in hook.list_prefixes(bucket_name=bucket, delimiter="/"):
                    if pref in SUP_FORMATS:
                        candidates.extend(
                            hook.list_keys(bucket_name=bucket, prefix=pref) or []
                        )

            if not skip_process:
                return candidates

            to_process = []
            for key in candidates:
                stem = Path(key).stem
                md_key = f"{DEV_DATA_MINERU_MD}/{stem}.md"

                if hook.check_for_key(key=md_key, bucket_name=bucket):
                    logger.info(
                        ">>> Skipping already processed: %s (found %s)", key, md_key
                    )
                else:
                    to_process.append(key)

            logger.info(">>> %s total, %s to process", len(candidates), len(to_process))

            return to_process

        @task
        def check_mineru_health() -> int:
            """Verify that the MinerU API is reachable and healthy"""
            from airflow.providers.http.hooks.http import HttpHook

            hook_mineru = HttpHook(method="GET", http_conn_id="mineru")
            req = hook_mineru.run(endpoint="/health")

            return req.status_code

        # ------------------------------------------------------------------------------------------------
        # 2. Batching
        # ------------------------------------------------------------------------------------------------

        @task
        def create_file_batches(
            files: list[str],
            batch_size: int = BATCH_SIZE,
        ) -> list[list[str]]:
            """Split source file keys into ordered batches."""
            batches = batch_list(items=files, batch_size=batch_size)
            logger.info(">>> %s files split into %s batches", len(files), len(batches))

            for i, batch in enumerate(batches):
                logger.info("    Batch %s: %s files -> %s", i, len(batch), batch)

            return batches

        # ------------------------------------------------------------------------------------------------
        # 3. Submit batch to MinerU
        # ------------------------------------------------------------------------------------------------

        @task()
        def batch_mineru_submit(
            health_status: int,
            qerant_status: str,
            file_keys: list[str],
            bucket_name: str,
        ) -> list[str]:
            """Submit source documents sequentially to MinerU."""
            import requests
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            if health_status != 200:
                raise ValueError(f"MinerU unhealthy: {health_status}")

            if not qerant_status:
                raise ValueError("Qdrant unhealthy")

            hook_minio = S3Hook(aws_conn_id="minio")
            task_ids: list[str] = []

            for file_key in file_keys:
                # ----- download -----
                file_name = Path("/tmp") / file_key.split("/")[-1]
                del_file(file=file_name)

                file_path = hook_minio.download_file(
                    key=file_key,
                    bucket_name=bucket_name,
                    local_path="/tmp",
                    preserve_file_name=True,
                    use_autogenerated_subdir=False,
                )
                logger.info(">>> Downloaded: %s", file_path)

                # ----- submit -----
                file_size = Path(file_path).stat().st_size
                logger.info(
                    ">>> Submitting file to MinerU: %s (size: %s bytes)",
                    file_key,
                    file_size,
                )

                with open(file_path, "rb") as f:
                    resp = requests.post(
                        "http://mineru-api:8000/tasks",
                        files={"files": f},
                        data={
                            "lang_list": "east_slavic",
                            "backend": "pipeline",
                            "parse_method": "ocr",
                            "formula_enable": "true",
                            "table_enable": "true",
                            "return_md": "true",
                            "return_middle_json": "false",
                            "return_model_output": "false",
                            "return_content_list": "false",
                            "return_images": "false",
                        },
                        timeout=120,
                    )

                logger.info(">>> MinerU response status: %s", resp.status_code)
                logger.info("MinerU response received")

                resp.raise_for_status()
                response_json = resp.json()
                logger.info("MinerU task submitted")

                tid = response_json.get("task_id")
                if not tid:
                    raise ValueError("Task ID not found in MinerU response")
                task_ids.append(tid)
                logger.info(">>> Submitted %s -> task_id: %s", file_key, tid)

                del_file(file=file_path)

            logger.info(">>> Batch submitted: %s", task_ids)

            return task_ids

        # ------------------------------------------------------------------------------------------------
        # 4. Save MinerU results
        # ------------------------------------------------------------------------------------------------

        @task()
        def save_mineru_results(mineru_task_ids: list[str]) -> list[str]:
            """Save completed MinerU Markdown results to local files."""
            from airflow.providers.http.hooks.http import HttpHook

            logger.info(
                ">>> save_mineru_results() received %s task_ids", len(mineru_task_ids)
            )
            logger.info(">>> Task IDs: %s", mineru_task_ids)

            hook_mineru = HttpHook(method="GET", http_conn_id="mineru")
            all_paths: list[str] = []

            for task_id in mineru_task_ids:
                logger.info(">>> Fetching result for task_id: %s", task_id)
                req = hook_mineru.run(endpoint=f"/tasks/{task_id}/result")
                logger.info(">>> Result response status: %s", req.status_code)
                req.raise_for_status()
                results = req.json().get("results", {})
                logger.info("MinerU results received")

                for name, payload in results.items():
                    file_path = f"/tmp/{name}.md"
                    del_file(file=file_path)

                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(payload["md_content"])
                    all_paths.append(file_path)
                    logger.info(">>> Saved result to: %s", file_path)

            logger.info(">>> Saved %s .md files from batch", len(all_paths))

            return all_paths

        # ------------------------------------------------------------------------------------------------
        # 5. Upload .md to MinIO
        # ------------------------------------------------------------------------------------------------

        @task
        def load_md_to_minio(mineru_result: list[str]) -> list[str]:
            """Upload generated Markdown files and return storage keys."""
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook(aws_conn_id="minio")
            s3_keys = load_to_s3(
                hook=hook, filepath=mineru_result, prefix=DEV_DATA_MINERU_MD
            )

            return s3_keys

        # ------------------------------------------------------------------------------------------------
        # 6. Chanking
        # ------------------------------------------------------------------------------------------------
        @task()
        def single_docling() -> list[list[str]]:
            """List stored Markdown keys in ordered batches."""
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook_minio = S3Hook(aws_conn_id="minio")

            keys = (
                hook_minio.list_keys(
                    bucket_name=RAG_DATA_BUCKET, prefix=DEV_DATA_MINERU_MD
                )
                or []
            )

            return [keys[i : i + BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)]

        @task()
        def docling_chunk_submit(
            mineru_result: list[str],
            bucket_name: str,
        ) -> list[str]:
            """Normalize Markdown and submit documents to Docling sequentially."""
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from airflow.providers.http.hooks.http import HttpHook
            from common.txt_feature.cleaner import (
                attach_table_captions,
                norm_md_heads,
                strip_watermarks,
            )
            from common.txt_feature.table_repair import (
                expand_tables_for_docling,
                repair_split_tables,
            )

            chunk_task_ids: list[str] = []
            hook_docling = HttpHook(method="POST", http_conn_id="docling")
            hook_minio = S3Hook(aws_conn_id="minio")

            for filepath in mineru_result:
                file_name = Path("/tmp") / filepath.split("/")[-1]
                del_file(file=file_name)

                file_path = hook_minio.download_file(
                    key=filepath,
                    bucket_name=bucket_name,
                    local_path="/tmp",
                    preserve_file_name=True,
                    use_autogenerated_subdir=False,
                )
                logger.info(">>> Downloaded: %s", file_path)
                with open(file_path, "r", encoding="utf-8") as f:
                    raw_md = f.read()

                cleaned_md = strip_watermarks(raw_md)
                cleaned_md = norm_md_heads(cleaned_md)
                cleaned_md = attach_table_captions(cleaned_md)
                cleaned_md = repair_split_tables(cleaned_md)
                cleaned_md = expand_tables_for_docling(cleaned_md)

                if cleaned_md != raw_md:
                    logger.info(">>> Normalized markdown before Docling: %s", file_path)
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(cleaned_md)

                path = Path(file_path)
                with open(file_path, "rb") as f:
                    resp = hook_docling.run(
                        endpoint="/v1/chunk/hierarchical/file/async",
                        files={
                            "files": (
                                path.name,
                                f,
                                "text/markdown",
                            )
                        },
                        data={
                            "convert_from_formats": "json_docling",
                            "convert_do_ocr": "false",
                            "convert_do_table_structure": "false",
                            "convert_include_images": "false",
                            "convert_do_formula_enrichment": "false",
                            "target_type": "inbody",
                            "include_converted_doc": "false",
                            "chunking_use_markdown_tables": "true",
                            "chunking_include_raw_text": "true",
                        },
                    )
                    resp.raise_for_status()
                    task_id = resp.json()["task_id"]
                    chunk_task_ids.append(task_id)
                    logger.info(">>> Chunk submit: %s -> task_id: %s", path.name, task_id)

            logger.info(">>> Chunk batch submitted: %s", chunk_task_ids)

            return chunk_task_ids

        @task
        def save_docling_results(docling_task_ids: list[str]) -> list[str]:
            """Enrich Docling chunks and persist their metadata."""
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from airflow.providers.http.hooks.http import HttpHook
            from common.txt_feature.cleaner import process_chunks, repair_anchor_loss

            hook_docling = HttpHook(method="GET", http_conn_id="docling")
            hook_minio = S3Hook(aws_conn_id="minio")
            out_paths: list[str] = []

            for task_id in docling_task_ids:
                resp = hook_docling.run(endpoint=f"/v1/result/{task_id}")
                resp.raise_for_status()
                result = resp.json()

                all_chunks = result.get("chunks", [])
                documents = result.get("documents", [])

                if not documents:
                    logger.warning(">>> No documents for task_id=%s. Skip", task_id)
                    continue

                for doc in documents:
                    file_name = doc.get("content", {}).get("filename")
                    stem = Path(file_name).stem if "." in file_name else file_name

                    doc_chunks = [
                        chunk
                        for chunk in all_chunks
                        if chunk.get("meta", {}).get("origin", {}).get("filename")
                        == file_name
                    ]
                    if not doc_chunks and len(documents) == 1:
                        doc_chunks = all_chunks

                    if not doc_chunks:
                        logger.warning(">>> No chunks for %s. Skip", file_name)
                        continue

                    text_chunks = [c for c in doc_chunks if c.get("type") != "table"]
                    table_chunks = [c for c in doc_chunks if c.get("type") == "table"]

                    for tc in table_chunks:
                        tc["is_table"] = True
                        # The prefix preserves table context during embedding.
                        tc["text"] = f"[ТАБЛИЦА] {tc.get('text', '')}"

                    logger.info(
                        ">>> Processing Docling chunks: file=%s, task_id=%s",
                        file_name,
                        task_id,
                    )
                    try:
                        source_chunks = text_chunks + table_chunks
                        source_table_ids = table_text_ids(chunks=source_chunks)
                        all_enriched = process_chunks(chunks=source_chunks)
                        validate_table_payload(
                            source_table_ids=source_table_ids,
                            enriched_chunks=all_enriched,
                            file_name=file_name,
                        )
                        source_md = read_s3_text(
                            hook=hook_minio,
                            key=f"{DEV_DATA_MINERU_MD}/{Path(file_name).name}",
                        )
                        if source_md:
                            all_enriched = repair_anchor_loss(
                                markdown=source_md,
                                chunks=all_enriched,
                                filename=file_name,
                            )
                    except Exception as error:
                        log_failure(
                            logger=logger, operation="Process Docling chunks", error=error
                        )
                        raise

                    json_path = f"/tmp/{stem}.json"
                    del_file(file=json_path)
                    with open(json_path, "w", encoding="utf-8") as jf:
                        json.dump(all_enriched, jf, ensure_ascii=False, indent=2)

                    load_to_s3(
                        hook=hook_minio, filepath=json_path, prefix=DEV_DATA_DOCLING_JSON
                    )
                    out_paths.append(json_path)
                    logger.info(
                        ">>> Saved %s chunks (was %s) -> %s",
                        len(all_enriched),
                        len(doc_chunks),
                        json_path,
                    )

            return out_paths

        @task()
        def create_qdrant_collection() -> str:
            """Ensure the collection and configured payload indexes exist."""
            from qdrant_client import QdrantClient
            from qdrant_client.http.exceptions import UnexpectedResponse
            from qdrant_client.models import (
                Distance,
                HnswConfigDiff,
                Modifier,
                MultiVectorComparator,
                MultiVectorConfig,
                PayloadSchemaType,
                SparseVectorParams,
                VectorParams,
            )

            with closing(thing=QdrantClient(url=QDRANT_URL, timeout=20)) as client:
                existing = {c.name for c in client.get_collections().collections}
                if QDRANT_COLLECTION not in existing:
                    client.create_collection(
                        collection_name=QDRANT_COLLECTION,
                        vectors_config={
                            "dense": VectorParams(
                                size=QDRANT_VECTOR_SIZE,  # 1024
                                distance=Distance.COSINE,
                            ),
                            "colbert": VectorParams(
                                size=QDRANT_COLBERT_SIZE,  # 1024
                                distance=Distance.COSINE,
                                multivector_config=MultiVectorConfig(
                                    comparator=MultiVectorComparator.MAX_SIM,
                                ),
                                hnsw_config=HnswConfigDiff(m=0),
                            ),
                        },
                        sparse_vectors_config={
                            "sparse": SparseVectorParams(modifier=Modifier.IDF),
                        },
                    )
                    logger.info(">>> Collection '%s' created", QDRANT_COLLECTION)
                else:
                    logger.info(
                        ">>> Collection '%s' already exists — adding missing indices",
                        QDRANT_COLLECTION,
                    )

                # Payload indices
                indices: list[tuple[str, PayloadSchemaType]] = [
                    ("filename", PayloadSchemaType.TEXT),
                    ("headings", PayloadSchemaType.KEYWORD),
                    ("is_table", PayloadSchemaType.BOOL),
                    ("man_refs", PayloadSchemaType.KEYWORD),
                    ("cross_refs", PayloadSchemaType.KEYWORD),
                    ("anchor_refs", PayloadSchemaType.KEYWORD),
                    # hierarchy metadata
                    ("section_level", PayloadSchemaType.INTEGER),
                    ("section_path", PayloadSchemaType.KEYWORD),
                    ("parent_heading", PayloadSchemaType.KEYWORD),
                    ("leaf_heading", PayloadSchemaType.KEYWORD),
                    # sliding-window markers
                    ("is_overlap_window", PayloadSchemaType.BOOL),
                    ("window_index", PayloadSchemaType.INTEGER),
                    # table continuation metadata
                    ("table_id", PayloadSchemaType.KEYWORD),
                    ("table_caption", PayloadSchemaType.TEXT),
                    ("table_part_index", PayloadSchemaType.INTEGER),
                    ("table_part_total", PayloadSchemaType.INTEGER),
                    ("table_window_index", PayloadSchemaType.INTEGER),
                    ("table_window_total", PayloadSchemaType.INTEGER),
                    ("table_orientation", PayloadSchemaType.KEYWORD),
                ]

                for field, schema in indices:
                    try:
                        client.create_payload_index(
                            collection_name=QDRANT_COLLECTION,
                            field_name=field,
                            field_schema=schema,
                        )
                        logger.info(">>>   index ok: %s (%s)", field, schema.value)
                    except UnexpectedResponse as ex:
                        logger.warning(
                            "Qdrant payload index could not be created (%s)",
                            type(ex).__name__,
                        )

                return QDRANT_COLLECTION

        @task()
        def save_to_qdrant(docling_json_paths: list[str]) -> int:
            """Encode chunks and upsert them sequentially by document and batch."""
            import gc

            import torch
            from qdrant_client import QdrantClient
            from qdrant_client.models import (
                FieldCondition,
                Filter,
                FilterSelector,
                MatchValue,
                PointStruct,
                SparseVector,
            )

            # Report the selected embedding execution provider.
            providers = (
                ["CUDAExecutionProvider"]
                if torch.cuda.is_available()
                else ["CPUExecutionProvider"]
            )
            logger.info(">>> PROVIDERS: %s", providers)
            logger.info(">>> CUDA available: %s", torch.cuda.is_available())
            logger.info(">>> PROVIDERS: %s", providers)
            if torch.cuda.is_available():
                logger.info(">>> GPU: %s", torch.cuda.get_device_name(device=0))

            with closing(thing=QdrantClient(url=QDRANT_URL, timeout=120)) as client:
                model = get_bge_m3()
                total_upserted = 0

                for json_path in docling_json_paths:
                    path = Path(json_path)
                    with open(json_path, encoding="utf-8") as f:
                        chunks = json.load(f)

                    if not chunks:
                        logger.warning(">>> Empty chunk list in %s, skip", json_path)
                        continue

                    filename = chunks[0].get("filename") or path.stem
                    client.delete(
                        collection_name=QDRANT_COLLECTION,
                        points_selector=FilterSelector(
                            filter=Filter(
                                must=[
                                    FieldCondition(
                                        key="filename",
                                        match=MatchValue(value=filename),
                                    )
                                ]
                            )
                        ),
                        wait=True,
                    )
                    logger.info(
                        ">>> Processing %s chunks from %s", len(chunks), path.name
                    )

                    # -----Iterate in QDRANT_ENCODE_BATCH sized windows -----
                    for enc_start in range(0, len(chunks), QDRANT_ENCODE_BATCH):
                        # ----- 1) Batching and Cheaning text into chanks -----
                        enc_batch = chunks[enc_start : enc_start + QDRANT_ENCODE_BATCH]

                        valid_chunks = []
                        texts = []
                        for c in enc_batch:
                            text = c.get("text") if isinstance(c, dict) else None
                            if isinstance(text, str) and text.strip():
                                valid_chunks.append(c)
                                texts.append(text.strip())

                        if not texts:
                            logger.warning(
                                ">>> No valid text payloads in file=%s, enc_offset=%s; skip",
                                path.name,
                                enc_start,
                            )
                            continue

                        # ----- 2) Vectors -----
                        output = model.encode(
                            sentences=texts,
                            batch_size=QDRANT_ENCODE_BATCH,
                            max_length=400,
                            return_dense=True,
                            return_sparse=True,
                            return_colbert_vecs=True,
                        )
                        dense_vecs = output["dense_vecs"]  # ndarray (B, 1024)
                        lexical_vecs = output[
                            "lexical_weights"
                        ]  # list[dict[token_id → weight]]
                        colbert_vecs = output[
                            "colbert_vecs"
                        ]  # list[ndarray(n_tok, 1024)]

                        # ----- 3) Build PointStruct list -----
                        points: list[PointStruct] = []
                        for i, chunk in enumerate(valid_chunks):
                            # Deterministic UUID from filename + chunk_index so re-runs
                            # overwrite the same point instead of creating duplicates.
                            point_id = str(
                                uuid.uuid5(
                                    uuid.NAMESPACE_URL,
                                    f"{path.stem}:{chunk.get('chunk_index', enc_start + i)}",
                                )
                            )

                            lw = lexical_vecs[i]  # dict {token_id_str: weight}
                            sparse_indices = [int(k) for k in lw.keys()]
                            sparse_values = [float(v) for v in lw.values()]
                            points.append(
                                PointStruct(
                                    id=point_id,
                                    vector={
                                        "dense": dense_vecs[i].tolist(),
                                        "sparse": SparseVector(
                                            indices=sparse_indices, values=sparse_values
                                        ),
                                        "colbert": colbert_vecs[i].tolist(),
                                    },
                                    payload={
                                        # ----- Core -----
                                        "text": chunk.get("text", ""),
                                        "filename": chunk.get("filename", path.stem),
                                        "chunk_index": chunk.get("chunk_index"),
                                        "num_tokens": chunk.get("num_tokens", 0),
                                        "is_table": chunk.get("is_table", False),
                                        # -----References -----
                                        "man_refs": chunk.get("man_refs", []),
                                        "cross_refs": chunk.get("cross_refs", []),
                                        "anchor_refs": chunk.get("anchor_refs", []),
                                        # ----- Hierarchy -----
                                        "headings": chunk.get("headings", []),
                                        "doc_items": chunk.get("doc_items", []),
                                        # ----- Hierarchy (enrich_metadata fields)-----
                                        "section_level": chunk.get("section_level"),
                                        "section_path": chunk.get("section_path", ""),
                                        "parent_heading": chunk.get("parent_heading"),
                                        "leaf_heading": chunk.get("leaf_heading"),
                                        # ----- Sliding window markers -----
                                        "is_overlap_window": chunk.get(
                                            "is_overlap_window", False
                                        ),
                                        "window_index": chunk.get("window_index", 0),
                                        # ----- Table continuation markers -----
                                        "table_id": chunk.get("table_id"),
                                        "table_caption": chunk.get("table_caption"),
                                        "table_part_index": chunk.get("table_part_index"),
                                        "table_part_total": chunk.get("table_part_total"),
                                        "table_window_index": chunk.get(
                                            "table_window_index"
                                        ),
                                        "table_window_total": chunk.get(
                                            "table_window_total"
                                        ),
                                        "table_orientation": chunk.get(
                                            "table_orientation"
                                        ),
                                    },
                                )
                            )

                        # ----- Upsert in sub-batches to control request size -----
                        for ups_start in range(0, len(points), QDRANT_UPSERT_BATCH):
                            sub = points[ups_start : ups_start + QDRANT_UPSERT_BATCH]
                            client.upsert(collection_name=QDRANT_COLLECTION, points=sub)
                            total_upserted += len(sub)
                            logger.info(
                                ">>> Upserted %s points(file=%s,enc_offset=%s,ups_offset=%s)",
                                len(sub),
                                path.name,
                                enc_start,
                                ups_start,
                            )
                        del dense_vecs, lexical_vecs, colbert_vecs, points, enc_batch
                        del valid_chunks
                        gc.collect()
                        if torch.cuda.is_available():
                            torch.cuda.empty_cache()

                    logger.info(
                        ">>> Done: %s, running total=%s", path.name, total_upserted
                    )

                return total_upserted

        # ==========================================================
        # Graph
        # ==========================================================
        def build_docling_graph() -> None:
            """Connect tasks for the Docling-only processing graph."""
            from common.sensors.docling_sensor import DoclingBatchStatusSensor

            md_loaded = single_docling()
            docling_task_ids = docling_chunk_submit.partial(
                bucket_name=RAG_DATA_BUCKET
            ).expand(mineru_result=md_loaded)
            docling_wait = DoclingBatchStatusSensor.partial(
                task_id="wait_docling_batch",
                docling_conn_id="docling",
                poll_interval=10,
            ).expand(external_task_ids=docling_task_ids)
            docling_chunks = save_docling_results.expand(
                docling_task_ids=docling_wait.output
            )
            qdrant_collection = create_qdrant_collection()
            qdrant_collection >> save_to_qdrant.expand(docling_json_paths=docling_chunks)

        def build_full_graph() -> None:
            """Connect tasks for the complete document processing graph."""
            from common.sensors.docling_sensor import DoclingBatchStatusSensor
            from common.sensors.mineru_sensor import MineruBatchStatusSensor

            # ------------------------------------------------------------
            # ----- 1) Discovery -----
            get_buckets_data()
            mineru_health = check_mineru_health()
            qdrant_health = create_qdrant_collection()
            files_task = list_files_to_process(bucket=RAG_DATA_BUCKET)
            # ----- 2) Split into batches of BATCH_SIZE -----
            file_batches = create_file_batches(files_task)
            # [["f1","f2",...,"f8"], ["f9","f10",...], ...]

            # ----- 3) Submit each batch to MinerU (one dynamic task per batch) -----
            mineru_task_ids = batch_mineru_submit.partial(
                bucket_name=RAG_DATA_BUCKET,
                health_status=mineru_health,
                qerant_status=qdrant_health,
            ).expand(file_keys=file_batches)
            # [["tid1","tid2",...], ["tid9",...], ...]

            # ----- 4) Deferrable sensor - async wait for each batch (frees workers) -----
            mineru_wait = MineruBatchStatusSensor.partial(
                task_id="wait_mineru_batch",
                mineru_conn_id="mineru",
                poll_interval=60,
            ).expand(external_task_ids=mineru_task_ids)
            # output: [["tid1","tid2",...], ["tid9",...], ...]

            # ----- 5) Save .md files per batch -----
            md_files = save_mineru_results.expand(mineru_task_ids=mineru_wait.output)

            # ----- 6) Upload .md to MinIO per batch -----
            md_loaded = load_md_to_minio.expand(mineru_result=md_files)

            # ----- 7) Chanking -----
            docling_task_ids = docling_chunk_submit.partial(
                bucket_name=RAG_DATA_BUCKET
            ).expand(mineru_result=md_loaded)

            # ----- 8) Deferrable sensor - async wait for each batch (frees workers) -----
            docling_wait = DoclingBatchStatusSensor.partial(
                task_id="wait_docling_batch",
                docling_conn_id="docling",
                poll_interval=30,
            ).expand(external_task_ids=docling_task_ids)

            # ----- 6) Upload .jsom to MinIO per batch -----
            docling_chunks = save_docling_results.expand(
                docling_task_ids=docling_wait.output
            )
            # ----- 8) Qdrant  -----
            save_to_qdrant.expand(docling_json_paths=docling_chunks)

        if mode == "docling_only":
            build_docling_graph()
        elif mode == "full":
            build_full_graph()
        else:
            raise ValueError(f"Unknown pipeline mode: {mode}")

    return batch_pipeline()


batch_pipeline_docling_only = build_batch_pipeline(
    dag_id="batch_pipeline_docling_only",
    mode="docling_only",
)
batch_pipeline_full = build_batch_pipeline(
    dag_id="batch_pipeline_full",
    mode="full",
)
