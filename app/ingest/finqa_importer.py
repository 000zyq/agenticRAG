from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from app.chunking.index import chunk_text
from app.ingest.embedding_client import embed_texts
from app.retrieval.milvus_client import insert_chunks
from app.storage.repository import upsert_document, get_document_by_hash, upsert_finqa_qa


def index_finqa(dataset_path: str) -> dict:
    base = Path(dataset_path)
    if not base.exists():
        return {"indexed": 0, "skipped": 0, "errors": [f"Path not found: {dataset_path}"]}

    json_files = list(base.rglob("*.json"))
    if not json_files:
        return {"indexed": 0, "skipped": 0, "errors": [f"No .json files under: {dataset_path}"]}

    indexed = 0
    skipped = 0
    errors: list[str] = []

    for json_path in json_files:
        try:
            items = _load_json_any(json_path)
            for item in items:
                try:
                    doc_text = _assemble_doc_text(item)
                    if not doc_text:
                        continue

                    file_hash = _hash_text(doc_text)
                    existing = get_document_by_hash(file_hash)
                    if existing:
                        skipped += 1
                        continue

                    item_id = str(item.get("id") or item.get("uid") or file_hash[:12])
                    doc_id = file_hash[:32]
                    source_path = f"finqa::{json_path.name}::{item_id}"

                    upsert_document(doc_id, source_path, file_hash, "processing", 0)
                    _store_qa(item, item_id, doc_id)

                    chunks = []
                    for idx, chunk in enumerate(chunk_text(doc_text)):
                        chunks.append(
                            {
                                "text": chunk,
                                "page": 0,
                                "chunk_index": idx,
                                "doc_id": doc_id,
                                "source_path": source_path,
                            }
                        )

                    embeddings = embed_texts([c["text"] for c in chunks])
                    for i, emb in enumerate(embeddings):
                        chunks[i]["embedding"] = emb

                    insert_chunks(chunks)
                    upsert_document(doc_id, source_path, file_hash, "indexed", len(chunks))
                    indexed += 1
                except Exception as exc:
                    errors.append(f"{json_path} item failed: {exc}")
        except Exception as exc:
            errors.append(f"{json_path} failed: {exc}")

    return {"indexed": indexed, "skipped": skipped, "errors": errors}


def _hash_text(text: str) -> str:
    sha = hashlib.sha256()
    sha.update(text.encode("utf-8"))
    return sha.hexdigest()


def _load_json_any(path: Path) -> list[dict]:
    raw = path.read_text(encoding="utf-8")
    raw = raw.strip()
    if not raw:
        return []
    if raw.startswith("["):
        return json.loads(raw)
    # JSONL fallback
    items = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        items.append(json.loads(line))
    return items


def _assemble_doc_text(item: dict) -> str:
    pre_text = item.get("pre_text", [])
    post_text = item.get("post_text", [])
    table = item.get("table", [])

    parts: list[str] = []

    if pre_text:
        if isinstance(pre_text, list):
            parts.append("\n".join([str(x) for x in pre_text if x]))
        else:
            parts.append(str(pre_text))

    if table:
        table_lines = []
        if isinstance(table, list):
            for row in table:
                if isinstance(row, list):
                    table_lines.append("\t".join([str(x) for x in row]))
                else:
                    table_lines.append(str(row))
        else:
            table_lines.append(str(table))
        parts.append("TABLE:\n" + "\n".join(table_lines))

    if post_text:
        if isinstance(post_text, list):
            parts.append("\n".join([str(x) for x in post_text if x]))
        else:
            parts.append(str(post_text))

    return "\n\n".join([p for p in parts if p.strip()]).strip()


def _store_qa(item: dict, qa_id: str, doc_id: str) -> None:
    question = (
        item.get("question")
        or item.get("query")
        or item.get("qa", {}).get("question")
        or item.get("q")
        or ""
    )
    answer = (
        item.get("answer")
        or item.get("final")
        or item.get("qa", {}).get("answer")
        or item.get("a")
        or None
    )
    raw_json = json.dumps(item, ensure_ascii=False)
    if question:
        upsert_finqa_qa(qa_id=qa_id, doc_id=doc_id, question=question, answer=answer, raw_json=raw_json)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import FinQA dataset into Milvus")
    parser.add_argument("--path", required=True, help="Path to FinQA dataset folder")
    args = parser.parse_args()
    result = index_finqa(args.path)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
