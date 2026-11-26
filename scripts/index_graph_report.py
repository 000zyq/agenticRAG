"""Build a PostgreSQL graph from the same PDF-to-Markdown input as Search Tools."""
import argparse
from hashlib import sha256
import json
from pathlib import Path
from time import perf_counter

from app.config import get_settings
from app.graph.builder import EXTRACTOR_VERSION, build_document_graph
from app.graph.financial import FINANCIAL_EXTRACTOR_VERSION, FinancialExtractor
from app.graph.repository import get_latest_graph_build_run, load_document_chunks, replace_document_chunks
from app.ingest.financial_report import extract_pdf_to_markdown
from app.storage.repository import upsert_document
from scripts.chat_markdown import MarkdownTools


def index_report(path: Path, engine: str | None = None, *, issuer: str | None = None,
                 fiscal_year: int | None = None, currency: str = "CNY") -> dict:
    start = perf_counter()
    pages, parsed_with = extract_pdf_to_markdown(path, engine)
    if bool(issuer) != bool(fiscal_year):
        raise ValueError("issuer and fiscal_year are required together")
    typed = FinancialExtractor(issuer, fiscal_year, currency) if issuer else None
    # Keep financial tables intact. Both arms use the same Markdown conversion.
    page_text = [(page.page, page.text_md) for page in pages]
    passages = page_text if typed else MarkdownTools(page_text, embed=None).passages
    settings = get_settings()
    version = FINANCIAL_EXTRACTOR_VERSION if typed else EXTRACTOR_VERSION
    config = ([issuer, fiscal_year, currency] if typed else
              [settings.openai_base_url, settings.graph_extraction_model, settings.graph_extraction_max_chars])
    fingerprint = json.dumps(
        [version, config, settings.graph_relation_hop_window, settings.graph_aggregate_min_confidence, passages],
        ensure_ascii=False,
    )
    doc_id = sha256(fingerprint.encode()).hexdigest()
    run = get_latest_graph_build_run(doc_id)
    if run and run["status"] == "ready":
        return {**run["summary_json"], "cached": True, "engine": parsed_with,
                "elapsed_seconds": perf_counter() - start}

    upsert_document(doc_id, str(path), doc_id, "ready", len(passages), graph_status="pending")
    if not load_document_chunks(doc_id):
        replace_document_chunks(doc_id, [
            {"chunk_order": i, "chunk_index": i, "source_kind": "page", "source_position": page,
             "page": page, "text": passage, "source_path": str(path)}
            for i, (page, passage) in enumerate(passages)
        ])
    summary = build_document_graph(doc_id, extractor=typed, extractor_version=version)
    return {**summary, "cached": False, "engine": parsed_with, "elapsed_seconds": perf_counter() - start}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--engine", choices=["mineru", "pypdf"])
    parser.add_argument("--issuer")
    parser.add_argument("--fiscal-year", type=int)
    parser.add_argument("--currency", default="CNY")
    args = parser.parse_args()
    print(json.dumps(index_report(args.source, args.engine, issuer=args.issuer,
                                  fiscal_year=args.fiscal_year, currency=args.currency), ensure_ascii=False))


if __name__ == "__main__":
    main()
