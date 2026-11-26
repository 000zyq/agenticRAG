from __future__ import annotations

import argparse
import json

from app.graph.builder import build_document_graph, build_pending_document_graphs


def main() -> None:
    parser = argparse.ArgumentParser(description="Build document knowledge graphs for generic-ingested documents.")
    parser.add_argument("--doc-id", help="Build the graph for a specific doc_id.")
    parser.add_argument("--pending", action="store_true", help="Build graphs for pending documents.")
    parser.add_argument("--limit", type=int, default=10, help="Max pending documents to process.")
    args = parser.parse_args()

    if args.doc_id:
        print(json.dumps(build_document_graph(args.doc_id), ensure_ascii=False))
        return
    if args.pending:
        print(json.dumps(build_pending_document_graphs(limit=args.limit), ensure_ascii=False))
        return
    raise SystemExit("Provide --doc-id or --pending")


if __name__ == "__main__":
    main()
