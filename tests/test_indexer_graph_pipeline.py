from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.ingest import indexer


def test_index_directory_persists_chunks_and_marks_graph_pending(monkeypatch, tmp_path: Path) -> None:
    sample = tmp_path / "sample.pdf"
    sample.write_bytes(b"pdf")

    calls: dict[str, object] = {}

    monkeypatch.setattr(indexer, "get_settings", lambda: SimpleNamespace(milvus_insert_batch=10))
    monkeypatch.setattr(indexer, "get_document_by_hash", lambda file_hash: None)
    monkeypatch.setattr(indexer, "parse_pdf", lambda path: [{"text": "Acme uses Atlas", "page": 1}])
    monkeypatch.setattr(indexer, "chunk_text", lambda text: [text])
    monkeypatch.setattr(indexer, "embed_texts", lambda texts: [[0.1, 0.2, 0.3] for _ in texts])
    monkeypatch.setattr(indexer, "insert_chunks", lambda chunks, flush=False: calls.setdefault("inserted", len(chunks)))
    monkeypatch.setattr(indexer, "flush_collection", lambda: calls.setdefault("flushed", True))
    monkeypatch.setattr(indexer, "upsert_document", lambda *args, **kwargs: calls.setdefault("upsert", []).append((args, kwargs)))
    monkeypatch.setattr(indexer, "mark_document_status", lambda doc_id, status: calls.setdefault("mark_status", []).append((doc_id, status)))
    monkeypatch.setattr(indexer, "replace_document_chunks", lambda doc_id, chunks: calls.setdefault("chunks", []).extend(chunks))
    monkeypatch.setattr(indexer, "mark_document_graph_status", lambda doc_id, status: calls.setdefault("graph_status", []).append((doc_id, status)))
    monkeypatch.setattr(indexer, "create_graph_build_run", lambda doc_id, version, status="pending": calls.setdefault("graph_runs", []).append((doc_id, version, status)))

    result = indexer.index_directory(str(tmp_path))

    assert result["indexed"] == 1
    assert len(calls["chunks"]) == 1
    assert calls["chunks"][0]["chunk_order"] == 0
    assert calls["graph_status"][-1][1] == "pending"
    assert calls["graph_runs"][-1][2] == "pending"


def test_index_directory_marks_graph_failed_on_exception(monkeypatch, tmp_path: Path) -> None:
    sample = tmp_path / "sample.pdf"
    sample.write_bytes(b"pdf")
    calls: dict[str, object] = {}

    monkeypatch.setattr(indexer, "get_settings", lambda: SimpleNamespace(milvus_insert_batch=10))
    monkeypatch.setattr(indexer, "get_document_by_hash", lambda file_hash: None)
    monkeypatch.setattr(indexer, "parse_pdf", lambda path: [{"text": "bad", "page": 1}])
    monkeypatch.setattr(indexer, "chunk_text", lambda text: [text])
    monkeypatch.setattr(indexer, "embed_texts", lambda texts: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(indexer, "upsert_document", lambda *args, **kwargs: None)
    monkeypatch.setattr(indexer, "replace_document_chunks", lambda doc_id, chunks: None)
    monkeypatch.setattr(indexer, "mark_document_status", lambda doc_id, status: calls.setdefault("status", []).append(status))
    monkeypatch.setattr(indexer, "mark_document_graph_status", lambda doc_id, status: calls.setdefault("graph_status", []).append(status))

    result = indexer.index_directory(str(tmp_path))

    assert result["indexed"] == 0
    assert result["errors"]
    assert calls["status"][-1] == "failed"
    assert calls["graph_status"][-1] == "failed"
