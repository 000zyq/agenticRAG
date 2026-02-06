from __future__ import annotations

from typing import Iterable

from pymilvus import (
    connections,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
    utility,
)

from app.config import get_settings


_COLLECTION_CACHE: Collection | None = None


def _connect():
    settings = get_settings()
    connections.connect(uri=settings.milvus_uri, token=settings.milvus_token or "")


def get_collection() -> Collection:
    global _COLLECTION_CACHE
    if _COLLECTION_CACHE is not None:
        return _COLLECTION_CACHE

    settings = get_settings()
    _connect()

    if not utility.has_collection(settings.milvus_collection):
        fields = [
            FieldSchema(name="chunk_id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="doc_id", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="source_path", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="page", dtype=DataType.INT64),
            FieldSchema(name="chunk_index", dtype=DataType.INT64),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=settings.embedding_dim),
        ]
        schema = CollectionSchema(fields, description="RAG chunks")
        collection = Collection(name=settings.milvus_collection, schema=schema)
        index_params = {
            "metric_type": "COSINE",
            "index_type": "HNSW",
            "params": {"M": 16, "efConstruction": 200},
        }
        collection.create_index(field_name="embedding", index_params=index_params)
        collection.load()
    else:
        collection = Collection(settings.milvus_collection)
        collection.load()

    _COLLECTION_CACHE = collection
    return collection


def insert_chunks(chunks: list[dict], flush: bool = True) -> None:
    if not chunks:
        return
    collection = get_collection()
    data = [
        [c["doc_id"] for c in chunks],
        [c["source_path"] for c in chunks],
        [c.get("page", 0) for c in chunks],
        [c["chunk_index"] for c in chunks],
        [c["text"] for c in chunks],
        [c["embedding"] for c in chunks],
    ]
    collection.insert(data)
    if flush:
        collection.flush()


def flush_collection() -> None:
    collection = get_collection()
    collection.flush()


def search(embedding: list[float], top_n: int) -> list[dict]:
    collection = get_collection()
    res = collection.search(
        data=[embedding],
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {"ef": 64}},
        limit=top_n,
        output_fields=["doc_id", "source_path", "page", "chunk_index", "text"],
    )
    hits = []
    for hit in res[0]:
        hits.append(
            {
                "doc_id": hit.entity.get("doc_id"),
                "source_path": hit.entity.get("source_path"),
                "page": hit.entity.get("page"),
                "chunk_index": hit.entity.get("chunk_index"),
                "text": hit.entity.get("text"),
                "score": float(hit.score),
            }
        )
    return hits
