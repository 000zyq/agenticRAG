from __future__ import annotations

import os
from pathlib import Path

import pytest
from docx import Document

from app.config import get_settings
from app.graph import builder as graph_builder
from app.graph.query import entity_subgraph, relation_search
from app.retrieval.milvus_client import get_collection


def _cleanup_doc(doc_id: str) -> None:
    from app.storage.db import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM graph_relation_evidence WHERE relation_id IN (SELECT relation_id FROM graph_relations WHERE doc_id = %s)",
                (doc_id,),
            )
            cur.execute("DELETE FROM graph_relations WHERE doc_id = %s", (doc_id,))
            cur.execute("DELETE FROM graph_entity_mentions WHERE doc_id = %s", (doc_id,))
            cur.execute("DELETE FROM graph_entities WHERE doc_id = %s", (doc_id,))
            cur.execute("DELETE FROM graph_build_runs WHERE doc_id = %s", (doc_id,))
            cur.execute("DELETE FROM document_chunks WHERE doc_id = %s", (doc_id,))
            cur.execute("DELETE FROM documents WHERE doc_id = %s", (doc_id,))
        conn.commit()

    try:
        collection = get_collection()
        collection.delete(expr=f'doc_id == "{doc_id}"')
        collection.flush()
    except Exception:
        pass


@pytest.mark.integration
def test_generic_graph_pipeline_roundtrip(monkeypatch, tmp_path: Path) -> None:
    pytest.importorskip("psycopg")
    if os.getenv("RUN_DB_TESTS") != "1":
        pytest.skip("RUN_DB_TESTS not enabled")
    if not os.getenv("POSTGRES_DSN"):
        pytest.skip("POSTGRES_DSN not set")
    if not os.getenv("MILVUS_URI"):
        pytest.skip("MILVUS_URI not set")

    from app.ingest import indexer
    from app.storage.db import get_conn
    from app.storage.repository import get_document_by_hash

    doc_path = tmp_path / "graph-source.docx"
    document = Document()
    document.add_paragraph("Acme uses Atlas platform.")
    document.add_paragraph("Atlas belongs to Contoso Group.")
    document.save(str(doc_path))

    settings = get_settings()
    monkeypatch.setattr(indexer, "embed_texts", lambda texts: [[0.01] * settings.embedding_dim for _ in texts])
    monkeypatch.setattr(
        graph_builder,
        "extract_chunk_graph",
        lambda text: (
            {
                "entities": [
                    {"name": "Acme", "type": "organization", "aliases": [], "confidence": 0.95},
                    {"name": "Atlas platform", "type": "product_system", "aliases": ["Atlas"], "confidence": 0.92},
                ],
                "relations": [{"subject": "Acme", "predicate": "uses", "object": "Atlas platform", "confidence": 0.93}],
            }
            if "Acme uses" in text
            else {
                "entities": [
                    {"name": "Atlas platform", "type": "product_system", "aliases": ["Atlas"], "confidence": 0.9},
                    {"name": "Contoso Group", "type": "organization", "aliases": ["Contoso"], "confidence": 0.9},
                ],
                "relations": [{"subject": "Atlas platform", "predicate": "belongs to", "object": "Contoso Group", "confidence": 0.9}],
            }
        ),
    )

    result = indexer.index_directory(str(tmp_path))
    assert result["indexed"] == 1

    file_hash = indexer._file_hash(doc_path)
    document_row = get_document_by_hash(file_hash)
    assert document_row is not None
    doc_id = str(document_row["doc_id"])

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT graph_status, num_chunks FROM documents WHERE doc_id = %s", (doc_id,))
                row = cur.fetchone()
                assert row[0] == "pending"
                assert row[1] >= 2
                cur.execute("SELECT COUNT(*) FROM document_chunks WHERE doc_id = %s", (doc_id,))
                assert cur.fetchone()[0] >= 2

        summary = graph_builder.build_document_graph(doc_id)
        assert summary["entities"] >= 3
        assert summary["relations"] >= 3

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT graph_status FROM documents WHERE doc_id = %s", (doc_id,))
                assert cur.fetchone()[0] == "ready"
                cur.execute("SELECT COUNT(*) FROM graph_entities WHERE doc_id = %s", (doc_id,))
                assert cur.fetchone()[0] >= 3
                cur.execute("SELECT COUNT(*) FROM graph_relations WHERE doc_id = %s", (doc_id,))
                assert cur.fetchone()[0] >= 3

        subgraph = entity_subgraph(query="Acme", doc_id=doc_id, depth=2, limit=10)
        assert subgraph["status"] == "ready"
        assert any(node["canonical_name"] == "Acme" for node in subgraph["nodes"])
        assert any(edge["predicate_text"] == "uses" for edge in subgraph["edges"])
        assert any(edge["is_aggregated"] for edge in subgraph["edges"])

        rel = relation_search(subject="Acme", doc_id=doc_id, limit=10)
        assert rel["status"] == "ready"
        assert any(edge["object"] == "Atlas platform" for edge in rel["edges"])
    finally:
        _cleanup_doc(doc_id)


@pytest.mark.integration
def test_markdown_report_build_reuses_graph_and_keeps_source_page(monkeypatch, tmp_path):
    if os.getenv("RUN_DB_TESTS") != "1" or not os.getenv("POSTGRES_DSN"):
        pytest.skip("PostgreSQL integration tests are disabled")

    from app.ingest.financial_report import PageContent
    from scripts import index_graph_report

    report = tmp_path / "graph-fixture.pdf"
    text = f"{tmp_path.name} Acme controls Beta."
    monkeypatch.setattr(index_graph_report, "extract_pdf_to_markdown",
                        lambda *_: ([PageContent(page=12, text_md=text, text_raw=text)], "mineru"))
    calls = []

    def extract(chunk):
        calls.append(chunk)
        return {
            "entities": [
                {"name": "Acme", "type": "organization"},
                {"name": "Beta", "type": "organization"},
            ],
            "relations": [{"subject": "Acme", "predicate": "controls", "object": "Beta"}],
            "extraction_mode": "llm",
            "usage": {"prompt_tokens": 20, "completion_tokens": 6},
        }

    monkeypatch.setattr(graph_builder, "extract_chunk_graph", extract)
    built = index_graph_report.index_report(report, "mineru")
    try:
        reused = index_graph_report.index_report(report, "mineru")
        assert len(calls) == 1
        assert not built["cached"] and reused["cached"]
        assert built["doc_id"] == reused["doc_id"]
        assert reused["usage"]["prompt_tokens"] == 20
        graph = entity_subgraph("Acme", doc_id=built["doc_id"])
        assert graph["edges"][0]["object"] == "Beta"
        assert graph["edges"][0]["evidence"][0]["page"] == 12
    finally:
        _cleanup_doc(built["doc_id"])
