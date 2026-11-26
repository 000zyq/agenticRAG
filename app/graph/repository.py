from __future__ import annotations

import datetime as dt
import json
from typing import Any

from app.storage.db import get_conn


def mark_document_graph_status(doc_id: str, status: str) -> None:
    now = dt.datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE documents SET graph_status = %s, graph_updated_at = %s, updated_at = %s WHERE doc_id = %s",
                (status, now, now, doc_id),
            )
        conn.commit()


def replace_document_chunks(doc_id: str, chunks: list[dict[str, Any]]) -> None:
    now = dt.datetime.utcnow()
    rows = [
        (
            doc_id,
            int(chunk.get("chunk_order", 0)),
            int(chunk.get("chunk_index", 0)),
            str(chunk.get("source_kind") or "chunk"),
            chunk.get("source_position"),
            chunk.get("page"),
            str(chunk.get("text") or ""),
            json.dumps(
                {
                    "part_index": chunk.get("part_index"),
                    "source_path": chunk.get("source_path"),
                }
            ),
            now,
        )
        for chunk in chunks
    ]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM document_chunks WHERE doc_id = %s", (doc_id,))
            if rows:
                cur.executemany(
                    """
                    INSERT INTO document_chunks (
                        doc_id, chunk_order, chunk_index, source_kind, source_position, page, text, extra, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )
        conn.commit()


def create_graph_build_run(doc_id: str, extractor_version: str, status: str = "pending") -> int:
    now = dt.datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO graph_build_runs (doc_id, status, extractor_version, started_at)
                VALUES (%s, %s, %s, %s)
                RETURNING run_id
                """,
                (doc_id, status, extractor_version, now),
            )
            run_id = int(cur.fetchone()[0])
        conn.commit()
    return run_id


def get_latest_graph_build_run(doc_id: str, statuses: tuple[str, ...] | None = None) -> dict[str, Any] | None:
    query = """
        SELECT run_id, doc_id, status, extractor_version, started_at, finished_at, summary_json, error_message
        FROM graph_build_runs
        WHERE doc_id = %s
    """
    params: list[Any] = [doc_id]
    if statuses:
        query += " AND status = ANY(%s)"
        params.append(list(statuses))
    query += " ORDER BY run_id DESC LIMIT 1"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
    if not row:
        return None
    return {
        "run_id": int(row[0]),
        "doc_id": row[1],
        "status": row[2],
        "extractor_version": row[3],
        "started_at": row[4],
        "finished_at": row[5],
        "summary_json": row[6],
        "error_message": row[7],
    }


def update_graph_build_run(run_id: int, status: str, summary_json: dict[str, Any] | None = None, error_message: str | None = None) -> None:
    now = dt.datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE graph_build_runs
                SET status = %s, finished_at = %s, summary_json = %s, error_message = %s
                WHERE run_id = %s
                """,
                (status, now, json.dumps(summary_json) if summary_json is not None else None, error_message, run_id),
            )
        conn.commit()


def mark_graph_build_run_running(run_id: int) -> None:
    now = dt.datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE graph_build_runs SET status = %s, started_at = %s, finished_at = NULL, error_message = NULL WHERE run_id = %s",
                ("running", now, run_id),
            )
        conn.commit()


def load_document_chunks(doc_id: str) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT chunk_id, chunk_order, chunk_index, source_kind, source_position, page, text, extra
                FROM document_chunks
                WHERE doc_id = %s
                ORDER BY chunk_order
                """,
                (doc_id,),
            )
            rows = cur.fetchall()
    return [
        {
            "chunk_id": int(row[0]),
            "chunk_order": int(row[1]),
            "chunk_index": int(row[2]),
            "source_kind": row[3],
            "source_position": row[4],
            "page": row[5],
            "text": row[6],
            "extra": row[7] if isinstance(row[7], dict) else {},
        }
        for row in rows
    ]


def list_documents_with_graph_status(status: str, limit: int) -> list[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT doc_id
                FROM documents
                WHERE graph_status = %s
                ORDER BY updated_at ASC
                LIMIT %s
                """,
                (status, limit),
            )
            rows = cur.fetchall()
    return [str(row[0]) for row in rows]


def replace_document_graph(
    doc_id: str,
    entities: list[dict[str, Any]],
    mentions: list[dict[str, Any]],
    relations: list[dict[str, Any]],
) -> dict[str, int]:
    now = dt.datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chunk_id, chunk_order FROM document_chunks WHERE doc_id = %s", (doc_id,))
            chunk_lookup = {int(row[1]): int(row[0]) for row in cur.fetchall()}

            cur.execute(
                "DELETE FROM graph_relation_evidence WHERE relation_id IN (SELECT relation_id FROM graph_relations WHERE doc_id = %s)",
                (doc_id,),
            )
            cur.execute("DELETE FROM graph_relations WHERE doc_id = %s", (doc_id,))
            cur.execute("DELETE FROM graph_entity_mentions WHERE doc_id = %s", (doc_id,))
            cur.execute("DELETE FROM graph_entities WHERE doc_id = %s", (doc_id,))

            entity_ids: dict[int, int] = {}
            for entity in entities:
                cur.execute(
                    """
                    INSERT INTO graph_entities (
                        doc_id, entity_type, canonical_name, normalized_name, aliases, attributes, confidence, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING entity_id
                    """,
                    (
                        doc_id,
                        entity["entity_type"],
                        entity["canonical_name"],
                        entity["normalized_name"],
                        json.dumps(entity.get("aliases", [])),
                        json.dumps(entity.get("attributes", {})),
                        entity.get("confidence"),
                        now,
                    ),
                )
                entity_ids[int(entity["temp_id"])] = int(cur.fetchone()[0])

            for mention in mentions:
                chunk_order = int(mention.get("chunk_order", 0))
                cur.execute(
                    """
                    INSERT INTO graph_entity_mentions (
                        doc_id, entity_id, chunk_id, chunk_order, mention_text, start_char, end_char,
                        confidence, evidence_text, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        doc_id,
                        entity_ids[int(mention["entity_temp_id"])],
                        chunk_lookup.get(chunk_order),
                        chunk_order,
                        mention["mention_text"],
                        mention.get("start_char"),
                        mention.get("end_char"),
                        mention.get("confidence"),
                        mention.get("evidence_text"),
                        now,
                    ),
                )

            relation_count = 0
            evidence_count = 0
            for relation in relations:
                cur.execute(
                    """
                    INSERT INTO graph_relations (
                        doc_id, subject_entity_id, object_entity_id, predicate_text, predicate_family,
                        confidence, is_aggregated, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING relation_id
                    """,
                    (
                        doc_id,
                        entity_ids[int(relation["subject_temp_id"])],
                        entity_ids[int(relation["object_temp_id"])],
                        relation["predicate_text"],
                        relation.get("predicate_family"),
                        relation.get("confidence"),
                        bool(relation.get("is_aggregated", False)),
                        now,
                    ),
                )
                relation_id = int(cur.fetchone()[0])
                relation_count += 1
                for evidence in relation.get("evidence", []):
                    chunk_order = evidence.get("chunk_order")
                    cur.execute(
                        """
                        INSERT INTO graph_relation_evidence (
                            relation_id, chunk_id, chunk_order, path_json, evidence_text, confidence, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            relation_id,
                            chunk_lookup.get(int(chunk_order)) if chunk_order is not None else None,
                            chunk_order,
                            json.dumps(relation.get("path_json")) if relation.get("path_json") is not None else None,
                            evidence.get("evidence_text"),
                            evidence.get("confidence"),
                            now,
                        ),
                    )
                    evidence_count += 1
        conn.commit()
    return {
        "entities": len(entities),
        "mentions": len(mentions),
        "relations": relation_count,
        "evidence": evidence_count,
    }


def get_document_graph_status(doc_id: str) -> str | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT graph_status FROM documents WHERE doc_id = %s", (doc_id,))
            row = cur.fetchone()
    return str(row[0]) if row and row[0] is not None else None
