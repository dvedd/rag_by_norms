from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient

from app import main


@pytest.fixture
def client(monkeypatch):
    result = SimpleNamespace(
        results=[],
        context_included=[],
        answer="Готово",
        effective_question="Арматура",
        was_rewritten=False,
        answer_mode="direct_supported",
        timings=SimpleNamespace(latency_ms=10),
    )
    pipeline = SimpleNamespace(run=AsyncMock(return_value=result))
    monkeypatch.setattr(main, "SearchPipeline", Mock(return_value=pipeline))
    monkeypatch.setattr(main, "_apply_pipeline_trace", Mock())
    monkeypatch.setattr(
        main, "trace_logger", SimpleNamespace(log=AsyncMock()), raising=False
    )

    return TestClient(main.app)


def test_stream_route(client):
    response = client.post("/search/stream", json={"query": "Арматура"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["x-accel-buffering"] == "no"
    assert "event: status" in response.text
    assert "event: done" in response.text
    assert "Готово" in response.text


def test_stream_validation(client):
    response = client.post("/search/stream", json={"query": "  "})

    assert response.status_code == 422
    assert response.headers["content-type"] == "application/json"


def test_json_route_unchanged(client):
    response = client.post(
        "/search",
        json={
            "query": "Арматура",
            "rewrite_system_prompt": "",
            "compose_system_prompt": "",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert response.json() == {
        "answer": "Готово",
        "effective_query": "Арматура",
        "was_rewritten": False,
        "results": [],
        "answer_mode": "direct_supported",
    }
