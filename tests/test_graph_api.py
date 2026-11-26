from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.deps import verify_api_key
from app.api import graph as graph_api


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(graph_api.router)
    app.dependency_overrides[verify_api_key] = lambda: None
    return TestClient(app)


def test_graph_query_entity_subgraph_route(monkeypatch) -> None:
    client = _client()
    monkeypatch.setattr(
        graph_api,
        "run_graph_query",
        lambda **kwargs: {"status": "ready", "matches": [{"canonical_name": "Acme"}], "nodes": [], "edges": []},
    )

    response = client.post("/graph/query", json={"mode": "entity_subgraph", "query": "Acme"})

    assert response.status_code == 200
    assert response.json()["status"] == "ready"
    assert response.json()["matches"][0]["canonical_name"] == "Acme"


def test_graph_query_relation_search_requires_subject() -> None:
    client = _client()

    response = client.post("/graph/query", json={"mode": "relation_search"})

    assert response.status_code == 400
    assert response.json()["detail"] == "subject is required for relation_search mode"


def test_graph_query_surfaces_pending_status(monkeypatch) -> None:
    client = _client()
    monkeypatch.setattr(
        graph_api,
        "run_graph_query",
        lambda **kwargs: {"status": "pending", "doc_id": "doc-1", "matches": [], "nodes": [], "edges": []},
    )

    response = client.post("/graph/query", json={"mode": "entity_subgraph", "query": "Acme", "doc_id": "doc-1"})

    assert response.status_code == 200
    assert response.json()["status"] == "pending"
