from __future__ import annotations

from app.retrieval.search import search_docs


def retrieve_contexts(query: str) -> list[dict]:
    return search_docs(query)
