from __future__ import annotations

from collections import Counter
from time import perf_counter
from typing import Any

from app.config import get_settings
from app.graph.canonicalize import canonicalize_chunk_graphs
from app.graph.extractor import extract_chunk_graph
from app.graph.repository import (
    create_graph_build_run,
    get_latest_graph_build_run,
    list_documents_with_graph_status,
    load_document_chunks,
    mark_document_graph_status,
    mark_graph_build_run_running,
    replace_document_graph,
    update_graph_build_run,
)


EXTRACTOR_VERSION = "generic_doc_kg_v1"


def build_document_graph(doc_id: str, *, extractor=None, extractor_version: str = EXTRACTOR_VERSION) -> dict[str, Any]:
    started = perf_counter()
    settings = get_settings()
    run = get_latest_graph_build_run(doc_id, statuses=("pending", "running"))
    run_id = int(run["run_id"]) if run else create_graph_build_run(doc_id, extractor_version, status="running")
    mark_graph_build_run_running(run_id)
    mark_document_graph_status(doc_id, "running")

    try:
        chunks = load_document_chunks(doc_id)
        if not chunks:
            summary = {"doc_id": doc_id, "chunks": 0, "entities": 0, "mentions": 0, "relations": 0, "evidence": 0}
            mark_document_graph_status(doc_id, "empty")
            update_graph_build_run(run_id, "ready", summary_json=summary)
            return summary

        chunk_graphs = []
        extraction_modes: Counter[str] = Counter()
        usage_totals: Counter[str] = Counter()
        calls_without_usage = 0
        for chunk in chunks:
            extracted = (extractor or extract_chunk_graph)(chunk["text"])
            mode = extracted.get("extraction_mode", "unknown")
            extraction_modes[mode] += 1
            usage = extracted.get("usage")
            if usage:
                for key in ("prompt_tokens", "completion_tokens"):
                    usage_totals[key] += usage.get(key) or 0
                usage_totals["cached_prompt_tokens"] += (
                    (usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
                )
            elif mode not in {"unknown", "financial_rules"}:
                calls_without_usage += 1
            chunk_graphs.append(
                {
                    "chunk_order": chunk["chunk_order"],
                    "text": chunk["text"],
                    "entities": extracted.get("entities", []),
                    "relations": extracted.get("relations", []),
                }
            )

        graph = canonicalize_chunk_graphs(
            chunk_graphs,
            hop_window=settings.graph_relation_hop_window,
            min_aggregate_confidence=settings.graph_aggregate_min_confidence,
        )
        counts = replace_document_graph(
            doc_id=doc_id,
            entities=graph["entities"],
            mentions=graph["mentions"],
            relations=graph["relations"],
        )
        summary = {"doc_id": doc_id, "chunks": len(chunks), **counts,
                   "build_seconds": perf_counter() - started,
                   "extraction_model": settings.graph_extraction_model if extractor is None else None,
                   "extraction_modes": dict(extraction_modes),
                   "usage": dict(usage_totals),
                   "calls_without_usage": calls_without_usage}
        mark_document_graph_status(doc_id, "ready")
        update_graph_build_run(run_id, "ready", summary_json=summary)
        return summary
    except Exception as exc:
        mark_document_graph_status(doc_id, "failed")
        update_graph_build_run(run_id, "failed", error_message=str(exc))
        raise


def build_pending_document_graphs(limit: int = 10) -> dict[str, Any]:
    doc_ids = list_documents_with_graph_status("pending", limit=limit)
    results = []
    failures = 0
    for doc_id in doc_ids:
        try:
            results.append(build_document_graph(doc_id))
        except Exception:
            failures += 1
    return {"processed": len(doc_ids), "failed": failures, "results": results}
