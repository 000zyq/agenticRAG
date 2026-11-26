"""Build a PostgreSQL graph from the same PDF-to-Markdown input as Search Tools."""
import argparse
from hashlib import sha256
import json
from pathlib import Path
from time import perf_counter

from app.config import get_settings
from app.graph.builder import EXTRACTOR_VERSION, build_document_graph
from app.graph.repository import get_latest_graph_build_run, load_document_chunks, replace_document_chunks
from app.ingest.financial_report import extract_pdf_to_markdown
from app.storage.repository import upsert_document
from scripts.chat_markdown import MarkdownTools


def index_report(path: Path, engine: str | None = None) -> dict:
    start = perf_counter()
    pages, parsed_with = extract_pdf_to_markdown(path, engine)
    corpus = MarkdownTools([(page.page, page.text_md) for page in pages], embed=None)
    settings = get_settings()
    fingerprint = json.dumps(
        [EXTRACTOR_VERSION, settings.openai_base_url, settings.graph_extraction_model,
         settings.graph_extraction_max_chars, settings.graph_relation_hop_window,
         settings.graph_aggregate_min_confidence, corpus.passages],
        ensure_ascii=False,
    )
    doc_id = sha256(fingerprint.encode()).hexdigest()
    run = get_latest_graph_build_run(doc_id)
    if run and run["status"] == "ready":
        return {**run["summary_json"], "cached": True, "engine": parsed_with,
                "elapsed_seconds": perf_counter() - start}

    upsert_document(doc_id, str(path), doc_id, "ready", len(corpus.passages), graph_status="pending")
    if not load_document_chunks(doc_id):
        replace_document_chunks(doc_id, [
            {"chunk_order": i, "chunk_index": i, "source_kind": "page", "source_position": page,
             "page": page, "text": passage, "source_path": str(path)}
            for i, (page, passage) in enumerate(corpus.passages)
        ])
    summary = build_document_graph(doc_id)
    return {**summary, "cached": False, "engine": parsed_with, "elapsed_seconds": perf_counter() - start}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--engine", choices=["mineru", "pypdf"])
    args = parser.parse_args()
    print(json.dumps(index_report(args.source, args.engine), ensure_ascii=False))


if __name__ == "__main__":
    main()
