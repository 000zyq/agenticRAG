from __future__ import annotations

import hashlib
import logging
import math
import os
import time
from pathlib import Path

from app.chunking.index import chunk_text
from app.config import get_settings
from app.ingest.embedding_client import embed_texts
from app.ingest.parser_pdf import parse_pdf
from app.ingest.parser_docx import parse_docx
from app.retrieval.milvus_client import insert_chunks, flush_collection
from app.storage.repository import upsert_document, get_document_by_hash, mark_document_status


SUPPORTED_EXTENSIONS = {".pdf", ".docx"}
_LOGGER = logging.getLogger("ingest")


def _file_hash(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8192), b""):
            sha.update(block)
    return sha.hexdigest()


def index_directory(path: str) -> dict:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

    settings = get_settings()
    base = Path(path)
    if not base.exists():
        return {"indexed": 0, "skipped": 0, "errors": [f"Path not found: {path}"]}

    indexed = 0
    skipped = 0
    errors: list[str] = []

    for file_path in base.rglob("*"):
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            continue

        doc_id = None
        try:
            file_start = time.perf_counter()
            _LOGGER.info("Indexing file: %s", file_path)
            file_hash = _file_hash(file_path)
            doc_id = file_hash[:32]
            existing = get_document_by_hash(file_hash)
            if existing and existing.get("status") == "indexed":
                _LOGGER.info("Skip (already indexed): %s", file_path)
                skipped += 1
                continue
            if existing:
                _LOGGER.info("Reindexing file with previous status=%s: %s", existing.get("status"), file_path)

            upsert_document(doc_id, str(file_path), file_hash, "processing", 0)

            chunks = _extract_chunks(file_path)
            if not chunks:
                mark_document_status(doc_id, "empty")
                _LOGGER.info("No chunks extracted: %s", file_path)
                continue

            for chunk in chunks:
                chunk["doc_id"] = doc_id
                chunk["source_path"] = str(file_path)

            _LOGGER.info("Extracted %d chunks. Embedding...", len(chunks))
            embed_start = time.perf_counter()
            embeddings = embed_texts([c["text"] for c in chunks])
            _LOGGER.info("Embedding done in %.2fs", time.perf_counter() - embed_start)

            if len(embeddings) != len(chunks):
                raise RuntimeError(
                    f"Embedding count mismatch: {len(embeddings)} vs {len(chunks)}"
                )
            for i, emb in enumerate(embeddings):
                chunks[i]["embedding"] = emb

            batch_size = max(1, settings.milvus_insert_batch)
            total_batches = math.ceil(len(chunks) / batch_size)
            for b in range(0, len(chunks), batch_size):
                batch = chunks[b : b + batch_size]
                insert_chunks(batch, flush=False)
                _LOGGER.info(
                    "Inserted batch %d/%d (size=%d)",
                    (b // batch_size) + 1,
                    total_batches,
                    len(batch),
                )
            flush_collection()
            upsert_document(doc_id, str(file_path), file_hash, "indexed", len(chunks))
            indexed += 1
            _LOGGER.info(
                "Indexed %s in %.2fs",
                file_path,
                time.perf_counter() - file_start,
            )
        except Exception as exc:
            errors.append(f"{file_path}: {exc}")
            if doc_id:
                mark_document_status(doc_id, "failed")
            _LOGGER.exception("Failed indexing %s", file_path)

    return {"indexed": indexed, "skipped": skipped, "errors": errors}


def _extract_chunks(file_path: Path) -> list[dict]:
    if file_path.suffix.lower() == ".pdf":
        parts = parse_pdf(str(file_path))
        _LOGGER.info("Parsed PDF pages: %d", len(parts))
        return _chunks_from_parts(parts, page_key="page")
    if file_path.suffix.lower() == ".docx":
        parts = parse_docx(str(file_path))
        _LOGGER.info("Parsed DOCX paragraphs: %d", len(parts))
        return _chunks_from_parts(parts, page_key="paragraph")
    return []


def _chunks_from_parts(parts: list[dict], page_key: str) -> list[dict]:
    chunks: list[dict] = []
    for part in parts:
        text = part.get("text", "")
        for idx, chunk in enumerate(chunk_text(text)):
            chunks.append(
                {
                    "text": chunk,
                    "page": int(part.get(page_key, 0)),
                    "chunk_index": idx,
                }
            )
    return chunks
