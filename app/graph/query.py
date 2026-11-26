from __future__ import annotations

from collections import defaultdict
from typing import Any

from app.graph.repository import get_document_graph_status
from app.storage.db import get_conn


def _load_relations(
    doc_ids: list[str],
    frontier: list[int],
    relation_families: list[str] | None,
    excluded: list[int],
    limit: int,
) -> list[dict[str, Any]]:
    if not doc_ids or not frontier:
        return []
    predicate_filter = "AND r.predicate_family = ANY(%s)" if relation_families else ""
    params: list[Any] = [doc_ids, frontier, frontier, excluded]
    if relation_families:
        params.append(relation_families)
    params.append(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    r.relation_id,
                    r.doc_id,
                    r.subject_entity_id,
                    se.canonical_name,
                    r.object_entity_id,
                    oe.canonical_name,
                    r.predicate_text,
                    r.predicate_family,
                    r.confidence,
                    r.is_aggregated
                FROM graph_relations r
                JOIN graph_entities se ON se.entity_id = r.subject_entity_id
                JOIN graph_entities oe ON oe.entity_id = r.object_entity_id
                WHERE r.doc_id = ANY(%s)
                    AND (r.subject_entity_id = ANY(%s) OR r.object_entity_id = ANY(%s))
                    AND NOT (r.relation_id = ANY(%s))
                    {predicate_filter}
                ORDER BY r.confidence DESC NULLS LAST, r.relation_id ASC
                LIMIT %s
                """,
                params,
            )
            rows = cur.fetchall()
    return [
        {
            "relation_id": int(row[0]),
            "doc_id": row[1],
            "subject_entity_id": int(row[2]),
            "subject": row[3],
            "object_entity_id": int(row[4]),
            "object": row[5],
            "predicate_text": row[6],
            "predicate_family": row[7],
            "confidence": float(row[8]) if row[8] is not None else None,
            "is_aggregated": bool(row[9]),
        }
        for row in rows
    ]


def _load_evidence_map(relation_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not relation_ids:
        return {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.relation_id, e.chunk_order, e.path_json, e.evidence_text,
                       e.confidence, c.page
                FROM graph_relation_evidence e
                LEFT JOIN document_chunks c ON c.chunk_id = e.chunk_id
                WHERE e.relation_id = ANY(%s)
                ORDER BY e.evidence_id ASC
                """,
                (relation_ids,),
            )
            rows = cur.fetchall()
    evidence_map: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        evidence_map[int(row[0])].append(
            {
                "chunk_order": row[1],
                "path": row[2],
                "evidence_text": row[3],
                "confidence": float(row[4]) if row[4] is not None else None,
                "page": row[5],
            }
        )
    return evidence_map


def _load_entities_by_ids(entity_ids: list[int]) -> list[dict[str, Any]]:
    if not entity_ids:
        return []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT entity_id, doc_id, entity_type, canonical_name, normalized_name, aliases, confidence
                FROM graph_entities
                WHERE entity_id = ANY(%s)
                ORDER BY entity_id ASC
                """,
                (entity_ids,),
            )
            rows = cur.fetchall()
    return [
        {
            "entity_id": int(row[0]),
            "doc_id": row[1],
            "entity_type": row[2],
            "canonical_name": row[3],
            "normalized_name": row[4],
            "aliases": row[5] if isinstance(row[5], list) else [],
            "confidence": float(row[6]) if row[6] is not None else None,
        }
        for row in rows
    ]


def _find_matching_entities(query: str, doc_id: str | None, limit: int, entity_types: list[str] | None) -> list[dict[str, Any]]:
    like = f"%{query}%"
    sql = """
        SELECT entity_id, doc_id, entity_type, canonical_name, normalized_name, aliases, confidence
        FROM graph_entities
        WHERE (canonical_name ILIKE %s OR aliases::text ILIKE %s)
    """
    params: list[Any] = [like, like]
    if doc_id:
        sql += " AND doc_id = %s"
        params.append(doc_id)
    if entity_types:
        sql += " AND entity_type = ANY(%s)"
        params.append(entity_types)
    sql += " ORDER BY confidence DESC NULLS LAST, entity_id ASC LIMIT %s"
    params.append(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [
        {
            "entity_id": int(row[0]),
            "doc_id": row[1],
            "entity_type": row[2],
            "canonical_name": row[3],
            "normalized_name": row[4],
            "aliases": row[5] if isinstance(row[5], list) else [],
            "confidence": float(row[6]) if row[6] is not None else None,
        }
        for row in rows
    ]


def entity_subgraph(
    query: str,
    doc_id: str | None = None,
    depth: int = 1,
    limit: int = 20,
    entity_types: list[str] | None = None,
    relation_families: list[str] | None = None,
) -> dict[str, Any]:
    if doc_id:
        graph_status = get_document_graph_status(doc_id)
        if graph_status in {"pending", "running", "failed", "empty", "not_requested", None}:
            return {"status": graph_status or "not_found", "doc_id": doc_id, "matches": [], "nodes": [], "edges": []}

    matches = _find_matching_entities(query=query, doc_id=doc_id, limit=limit, entity_types=entity_types)
    if not matches:
        return {"status": "ready", "doc_id": doc_id, "matches": [], "nodes": [], "edges": []}

    doc_ids = [doc_id] if doc_id else sorted({match["doc_id"] for match in matches})
    frontier = {match["entity_id"] for match in matches}
    visited = set(frontier)
    kept_relations: list[dict[str, Any]] = []
    for _ in range(max(depth, 0)):
        remaining = limit - len(kept_relations)
        if not frontier or remaining <= 0:
            break
        edges = _load_relations(doc_ids, sorted(frontier), relation_families,
                                [edge["relation_id"] for edge in kept_relations], remaining)
        kept_relations.extend(edges)
        neighbors = {edge[key] for edge in edges for key in ("subject_entity_id", "object_entity_id")}
        frontier = neighbors - visited
        visited.update(frontier)

    evidence_map = _load_evidence_map([relation["relation_id"] for relation in kept_relations])
    nodes = _load_entities_by_ids(sorted(visited))
    for relation in kept_relations:
        relation["evidence"] = evidence_map.get(relation["relation_id"], [])
    return {
        "status": "ready",
        "doc_id": doc_id,
        "matches": matches,
        "nodes": nodes,
        "edges": kept_relations,
    }


def relation_search(
    subject: str,
    predicate: str | None = None,
    obj: str | None = None,
    doc_id: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    if doc_id:
        graph_status = get_document_graph_status(doc_id)
        if graph_status in {"pending", "running", "failed", "empty", "not_requested", None}:
            return {"status": graph_status or "not_found", "doc_id": doc_id, "edges": []}

    sql = """
        SELECT
            r.relation_id,
            r.doc_id,
            r.subject_entity_id,
            se.canonical_name,
            r.object_entity_id,
            oe.canonical_name,
            r.predicate_text,
            r.predicate_family,
            r.confidence,
            r.is_aggregated
        FROM graph_relations r
        JOIN graph_entities se ON se.entity_id = r.subject_entity_id
        JOIN graph_entities oe ON oe.entity_id = r.object_entity_id
        WHERE se.canonical_name ILIKE %s
    """
    params: list[Any] = [f"%{subject}%"]
    if predicate:
        sql += " AND r.predicate_text ILIKE %s"
        params.append(f"%{predicate}%")
    if obj:
        sql += " AND oe.canonical_name ILIKE %s"
        params.append(f"%{obj}%")
    if doc_id:
        sql += " AND r.doc_id = %s"
        params.append(doc_id)
    sql += " ORDER BY r.confidence DESC NULLS LAST, r.relation_id ASC LIMIT %s"
    params.append(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    edges = [
        {
            "relation_id": int(row[0]),
            "doc_id": row[1],
            "subject_entity_id": int(row[2]),
            "subject": row[3],
            "object_entity_id": int(row[4]),
            "object": row[5],
            "predicate_text": row[6],
            "predicate_family": row[7],
            "confidence": float(row[8]) if row[8] is not None else None,
            "is_aggregated": bool(row[9]),
        }
        for row in rows
    ]
    evidence_map = _load_evidence_map([edge["relation_id"] for edge in edges])
    for edge in edges:
        edge["evidence"] = evidence_map.get(edge["relation_id"], [])
    return {"status": "ready", "doc_id": doc_id, "edges": edges}


def run_graph_query(
    *,
    mode: str,
    query: str | None = None,
    subject: str | None = None,
    predicate: str | None = None,
    object_text: str | None = None,
    doc_id: str | None = None,
    depth: int = 1,
    limit: int = 20,
    entity_types: list[str] | None = None,
    relation_families: list[str] | None = None,
) -> dict[str, Any]:
    if mode == "entity_subgraph":
        return entity_subgraph(
            query=query or "",
            doc_id=doc_id,
            depth=depth,
            limit=limit,
            entity_types=entity_types,
            relation_families=relation_families,
        )
    if mode == "relation_search":
        return relation_search(
            subject=subject or "",
            predicate=predicate,
            obj=object_text,
            doc_id=doc_id,
            limit=limit,
        )
    raise ValueError(f"Unsupported graph query mode: {mode}")
