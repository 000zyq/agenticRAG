from __future__ import annotations

from app.config import get_settings
from app.ingest.embedding_client import embed_texts
from app.retrieval.milvus_client import search as milvus_search
from app.retrieval.rerank import rerank


def search_docs(query: str) -> list[dict]:
    settings = get_settings()
    embedding = embed_texts([query])[0]
    candidates = milvus_search(embedding, settings.retrieval_top_n)
    reranked = rerank(query, candidates)
    return reranked[: settings.retrieval_top_k]
