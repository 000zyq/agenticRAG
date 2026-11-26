from __future__ import annotations

import re
from collections import defaultdict
from typing import Any


ORG_SUFFIX_RE = re.compile(
    r"(有限责任公司|股份有限公司|有限公司|公司|集团|co\.?,?\s*ltd\.?|limited|inc\.?|corp\.?|corporation|company)$",
    re.IGNORECASE,
)
PUNCT_RE = re.compile(r"[\s\-_.,;:()（）\[\]{}<>《》“”\"'`]+")


def normalize_entity_name(name: str, entity_type: str) -> str:
    text = PUNCT_RE.sub("", (name or "").strip().lower())
    if entity_type == "organization":
        previous = None
        while previous != text:
            previous = text
            text = ORG_SUFFIX_RE.sub("", text).strip()
    return text


def _span_for_mention(text: str, mention: str) -> tuple[int | None, int | None]:
    if not mention:
        return None, None
    start = text.find(mention)
    if start < 0:
        return None, None
    return start, start + len(mention)


def _predicate_family(predicate: str) -> str | None:
    norm = predicate.lower()
    if norm in {"has_reported_subsidiary", "registered_in", "operates_in", "has_financial_fact",
                "has_subsidiary_candidate", "has_subsidiary_financial_disclosure",
                "has_major_subsidiary_financial_disclosure"}:
        return norm
    if any(token in norm for token in ("acquire", "收购", "并购")):
        return "acquisition"
    if any(token in norm for token in ("use", "采用", "使用", "依赖")):
        return "usage"
    if any(token in norm for token in ("belong", "属于", "隶属", "is")):
        return "membership"
    return None


def _merge_relations(relations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    for relation in relations:
        key = (
            relation["subject_temp_id"],
            relation["object_temp_id"],
            relation["predicate_text"],
            relation["is_aggregated"],
        )
        current = merged.get(key)
        if not current:
            merged[key] = {
                **relation,
                "evidence": list(relation.get("evidence", [])),
            }
            continue
        current["confidence"] = max(float(current.get("confidence", 0.0)), float(relation.get("confidence", 0.0)))
        current["evidence"].extend(relation.get("evidence", []))
    return list(merged.values())


def aggregate_relations(
    direct_relations: list[dict[str, Any]],
    hop_window: int,
    min_confidence: float,
) -> list[dict[str, Any]]:
    aggregated: list[dict[str, Any]] = []
    for left in direct_relations:
        for right in direct_relations:
            if left["object_temp_id"] != right["subject_temp_id"]:
                continue
            if left["subject_temp_id"] == right["object_temp_id"]:
                continue
            if abs(int(left["chunk_order"]) - int(right["chunk_order"])) > hop_window:
                continue
            confidence = round(min(float(left["confidence"]), float(right["confidence"])) * 0.8, 4)
            if confidence < min_confidence:
                continue
            aggregated.append(
                {
                    "subject_temp_id": left["subject_temp_id"],
                    "object_temp_id": right["object_temp_id"],
                    "predicate_text": f"{left['predicate_text']} => {right['predicate_text']}",
                    "predicate_family": "multi_hop",
                    "confidence": confidence,
                    "is_aggregated": True,
                    "chunk_order": min(int(left["chunk_order"]), int(right["chunk_order"])),
                    "evidence": [
                        {
                            "chunk_order": left["chunk_order"],
                            "evidence_text": left["evidence"][0]["evidence_text"] if left.get("evidence") else "",
                            "confidence": left["confidence"],
                        },
                        {
                            "chunk_order": right["chunk_order"],
                            "evidence_text": right["evidence"][0]["evidence_text"] if right.get("evidence") else "",
                            "confidence": right["confidence"],
                        },
                    ],
                    "path_json": {
                        "via_entity_temp_id": left["object_temp_id"],
                        "chunks": [left["chunk_order"], right["chunk_order"]],
                        "predicates": [left["predicate_text"], right["predicate_text"]],
                    },
                }
            )
    return _merge_relations(aggregated)


def canonicalize_chunk_graphs(
    chunk_graphs: list[dict[str, Any]],
    hop_window: int = 2,
    min_aggregate_confidence: float = 0.55,
) -> dict[str, list[dict[str, Any]]]:
    entities_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    mentions: list[dict[str, Any]] = []
    direct_relations: list[dict[str, Any]] = []
    entity_seq = 1

    def ensure_entity(name: str, entity_type: str, confidence: float, aliases: list[str] | None = None, attributes: dict[str, Any] | None = None) -> int:
        nonlocal entity_seq
        aliases = aliases or []
        attributes = attributes or {}
        normalized = normalize_entity_name(name, entity_type)
        if not normalized:
            normalized = normalize_entity_name(name, "business_term")
        key = (entity_type, normalized)
        current = entities_by_key.get(key)
        if not current:
            current = {
                "temp_id": entity_seq,
                "entity_type": entity_type,
                "canonical_name": name.strip(),
                "normalized_name": normalized,
                "aliases": sorted({name.strip(), *[alias.strip() for alias in aliases if alias.strip()]}),
                "attributes": attributes,
                "confidence": float(confidence or 0.0),
            }
            entities_by_key[key] = current
            entity_seq += 1
        else:
            current["aliases"] = sorted({*current["aliases"], name.strip(), *[alias.strip() for alias in aliases if alias.strip()]})
            current["confidence"] = max(float(current.get("confidence", 0.0)), float(confidence or 0.0))
        return int(current["temp_id"])

    for chunk_graph in chunk_graphs:
        chunk_text = str(chunk_graph.get("text") or "")
        chunk_order = int(chunk_graph.get("chunk_order", 0))
        local_lookup: dict[str, int] = {}
        for entity in chunk_graph.get("entities", []) or []:
            name = str(entity.get("name") or "").strip()
            entity_type = str(entity.get("type") or "business_term").strip()
            if not name or not entity_type:
                continue
            temp_id = ensure_entity(
                name=name,
                entity_type=entity_type,
                confidence=float(entity.get("confidence", 0.0) or 0.0),
                aliases=list(entity.get("aliases") or []),
                attributes=dict(entity.get("attributes") or {}),
            )
            for alias in [name, *[str(alias).strip() for alias in (entity.get("aliases") or []) if str(alias).strip()]]:
                normalized_alias = normalize_entity_name(alias, entity_type)
                if normalized_alias:
                    local_lookup[normalized_alias] = temp_id
                fallback_alias = normalize_entity_name(alias, "business_term")
                if fallback_alias:
                    local_lookup[fallback_alias] = temp_id
            start_char, end_char = _span_for_mention(chunk_text, name)
            mentions.append(
                {
                    "entity_temp_id": temp_id,
                    "chunk_order": chunk_order,
                    "mention_text": name,
                    "start_char": start_char,
                    "end_char": end_char,
                    "confidence": float(entity.get("confidence", 0.0) or 0.0),
                    "evidence_text": chunk_text[:280],
                }
            )

        for relation in chunk_graph.get("relations", []) or []:
            subject = str(relation.get("subject") or "").strip()
            predicate = str(relation.get("predicate") or "").strip()
            obj = str(relation.get("object") or "").strip()
            if not subject or not predicate or not obj:
                continue
            subject_key = normalize_entity_name(subject, "business_term")
            object_key = normalize_entity_name(obj, "business_term")
            subject_temp_id = local_lookup.get(subject_key)
            object_temp_id = local_lookup.get(object_key)
            if not subject_temp_id:
                subject_temp_id = ensure_entity(subject, "business_term", float(relation.get("confidence", 0.5) or 0.5))
            if not object_temp_id:
                object_temp_id = ensure_entity(obj, "business_term", float(relation.get("confidence", 0.5) or 0.5))
            direct_relations.append(
                {
                    "subject_temp_id": subject_temp_id,
                    "object_temp_id": object_temp_id,
                    "predicate_text": predicate,
                    "predicate_family": _predicate_family(predicate),
                    "confidence": float(relation.get("confidence", 0.0) or 0.0),
                    "is_aggregated": False,
                    "chunk_order": chunk_order,
                    "evidence": [
                        {
                            "chunk_order": chunk_order,
                            "evidence_text": str(relation.get("evidence_text") or chunk_text[:280]),
                            "confidence": float(relation.get("confidence", 0.0) or 0.0),
                        }
                    ],
                }
            )

    merged_direct = _merge_relations(direct_relations)
    aggregated = aggregate_relations(merged_direct, hop_window=hop_window, min_confidence=min_aggregate_confidence)
    return {
        "entities": list(entities_by_key.values()),
        "mentions": mentions,
        "relations": merged_direct + aggregated,
    }
