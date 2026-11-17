import logging
import os
from typing import Any

import requests
from shared.logging.failures import log_failure

DEFAULT_RETRIEVER_API_URL = "http://127.0.0.1:9123"
logger = logging.getLogger(__name__)


class RetrieverClient:
    """Fetch complete search responses without exposing transport details."""

    def __init__(self) -> None:
        """Resolve the retriever address without opening a connection."""
        self.url = os.getenv(
            "RETRIEVER_API_URL",
            DEFAULT_RETRIEVER_API_URL,
        ).rstrip("/")

    def search(
        self,
        query: str,
        rewrite_system_prompt: str,
        compose_system_prompt: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Return a search result or raise a safe request error."""
        request_body = self._build_search_payload(
            query=query,
            rewrite_system_prompt=rewrite_system_prompt,
            compose_system_prompt=compose_system_prompt,
            options=kwargs,
        )

        try:
            response = self._post_search(request_body=request_body)
        except requests.RequestException as error:
            log_failure(logger=logger, operation="Retriever request", error=error)
            raise requests.RequestException(
                "Retriever service is temporarily unavailable"
            ) from None

        with response:
            return self._decode_response(response=response)

    def _build_search_payload(
        self,
        query: str,
        rewrite_system_prompt: str,
        compose_system_prompt: str,
        options: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "query": query,
            "user_id": options.get("user_id"),
            "session_id": options.get("session_id"),
            "rewrite_system_prompt": rewrite_system_prompt,
            "compose_system_prompt": compose_system_prompt,
            "top_k": options.get("top_k", 10),
            "prefetch_k": options.get("prefetch_k", 40),
            "mode": options.get("mode", "hybrid"),
            "only_tables": options.get("only_tables"),
            "use_rewriter": options.get("use_rewriter", True),
            "expand_refs": options.get("expand_refs", True),
            "ref_depth": options.get("ref_depth", 1),
            "answer_strategy": "auto",
            "filename_filter": options.get("filename_filter"),
            "section_filter": options.get("section_filter"),
        }

    def _post_search(self, request_body: dict) -> requests.Response:
        return requests.post(
            url=f"{self.url}/search",
            json=request_body,
            timeout=(5, 180),
        )

    def _decode_response(self, response: requests.Response) -> dict[str, Any]:
        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            log_failure(logger=logger, operation="Retriever response", error=error)
            raise requests.RequestException(
                "Retriever service could not complete the search"
            ) from None

        try:
            payload = response.json()
        except ValueError as error:
            log_failure(logger=logger, operation="Decode retriever response", error=error)
            raise requests.RequestException(
                "Retriever service returned an invalid response"
            ) from None

        if not isinstance(payload, dict):
            raise requests.RequestException(
                "Retriever service returned an invalid response"
            )

        return payload
