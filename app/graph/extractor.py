from __future__ import annotations

import json
import re
from typing import Any

from openai import OpenAI

from app.config import get_settings


ALLOWED_ENTITY_TYPES = {
    "organization",
    "person",
    "location",
    "time",
    "product_system",
    "document_concept",
    "business_term",
}
_ENTITY_TYPE_MAP = {
    "org": "organization",
    "company": "organization",
    "organisation": "organization",
    "organization": "organization",
    "person": "person",
    "people": "person",
    "human": "person",
    "location": "location",
    "place": "location",
    "time": "time",
    "date": "time",
    "product": "product_system",
    "system": "product_system",
    "platform": "product_system",
    "product_system": "product_system",
    "document_concept": "document_concept",
    "concept": "document_concept",
    "business_term": "business_term",
    "term": "business_term",
}

DATE_RE = re.compile(r"(20\d{2}[年\-/]\d{1,2}[月\-/]\d{1,2}日?|20\d{2}年|20\d{2}-\d{2}-\d{2})")
CN_ORG_RE = re.compile(r"[\u4e00-\u9fffA-Za-z0-9()（）·\-]{2,40}(?:公司|集团|银行|大学|学院|委员会|协会|研究院|研究所)")
EN_ORG_RE = re.compile(r"\b[A-Z][A-Za-z0-9&.,\- ]{1,40}(?:Inc|Corp|Corporation|Company|Group|Bank|University|Institute|Ltd)\b")
SYSTEM_RE = re.compile(r"[\u4e00-\u9fffA-Za-z0-9()（）·\-]{2,30}(?:系统|平台|产品)")
RELATION_PATTERNS = [
    re.compile(r"(?P<subject>[\u4e00-\u9fffA-Za-z0-9()（）·\-]{2,30})(?:是|属于)(?P<object>[\u4e00-\u9fffA-Za-z0-9()（）·\-]{2,30})"),
    re.compile(r"(?P<subject>[\u4e00-\u9fffA-Za-z0-9()（）·\-]{2,30})(?:使用|采用|依赖)(?P<object>[\u4e00-\u9fffA-Za-z0-9()（）·\-]{2,30})"),
    re.compile(r"(?P<subject>[A-Z][A-Za-z0-9&.,\- ]{1,30})\s+(?:uses|adopts|depends on|belongs to|is)\s+(?P<object>[A-Z][A-Za-z0-9&.,\- ]{1,30})", re.IGNORECASE),
]


def _strip_code_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_+-]*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```$", "", cleaned)
    return cleaned.strip()


def _normalize_entity_type(value: object) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    mapped = _ENTITY_TYPE_MAP.get(text, text)
    return mapped if mapped in ALLOWED_ENTITY_TYPES else None


def _sanitize_payload(payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    entities: list[dict[str, Any]] = []
    for entity in payload.get("entities", []) or []:
        if not isinstance(entity, dict):
            continue
        name = str(entity.get("name") or "").strip()
        entity_type = _normalize_entity_type(entity.get("type"))
        if not name or not entity_type:
            continue
        aliases = [str(alias).strip() for alias in (entity.get("aliases") or []) if str(alias).strip()]
        confidence = float(entity.get("confidence", 0.6) or 0.6)
        attributes = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        entities.append(
            {
                "name": name,
                "type": entity_type,
                "aliases": aliases,
                "confidence": max(0.0, min(confidence, 1.0)),
                "attributes": attributes,
            }
        )

    relations: list[dict[str, Any]] = []
    for relation in payload.get("relations", []) or []:
        if not isinstance(relation, dict):
            continue
        subject = str(relation.get("subject") or "").strip()
        predicate = str(relation.get("predicate") or "").strip()
        obj = str(relation.get("object") or "").strip()
        if not subject or not predicate or not obj:
            continue
        confidence = float(relation.get("confidence", 0.6) or 0.6)
        relations.append(
            {
                "subject": subject,
                "predicate": predicate,
                "object": obj,
                "confidence": max(0.0, min(confidence, 1.0)),
            }
        )
    return {"entities": entities, "relations": relations}


def _extract_with_llm(text: str) -> dict[str, Any]:
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key, base_url=settings.openai_base_url)
    prompt = (
        "Extract a compact document knowledge graph from the chunk. "
        "Return JSON only with keys entities and relations. "
        "Each entity must have name, type, aliases, confidence, attributes. "
        "Allowed entity types: organization, person, location, time, product_system, document_concept, business_term. "
        "Each relation must have subject, predicate, object, confidence. "
        "Keep only relations directly supported by the chunk."
    )
    resp = client.chat.completions.create(
        model=settings.graph_extraction_model,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": text[: settings.graph_extraction_max_chars]},
        ],
        temperature=0,
    )
    content = resp.choices[0].message.content or ""
    try:
        graph = _sanitize_payload(json.loads(_strip_code_fences(content)))
        mode = "llm"
    except (ValueError, TypeError, AttributeError):
        graph = _extract_with_rules(text)
        mode = "rules_after_invalid_response"
    return {**graph, "extraction_mode": mode, "usage": resp.usage.model_dump() if resp.usage else None}


def _extract_with_rules(text: str) -> dict[str, list[dict[str, Any]]]:
    entities: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def add_entity(name: str, entity_type: str, confidence: float = 0.55) -> None:
        key = (entity_type, name.strip())
        if not key[1] or key in seen:
            return
        seen.add(key)
        entities.append(
            {
                "name": key[1],
                "type": entity_type,
                "aliases": [],
                "confidence": confidence,
                "attributes": {},
            }
        )

    for match in DATE_RE.findall(text):
        add_entity(match, "time", 0.7)
    for match in CN_ORG_RE.findall(text):
        add_entity(match, "organization", 0.7)
    for match in EN_ORG_RE.findall(text):
        add_entity(match, "organization", 0.7)
    for match in SYSTEM_RE.findall(text):
        add_entity(match, "product_system", 0.6)
    for quoted in re.findall(r"[“\"]([^”\"]{2,30})[”\"]", text):
        add_entity(quoted, "business_term", 0.5)

    relations: list[dict[str, Any]] = []
    for pattern in RELATION_PATTERNS:
        for match in pattern.finditer(text):
            subject = match.group("subject").strip()
            obj = match.group("object").strip()
            relation_text = match.group(0).strip()
            predicate = relation_text.replace(subject, "", 1).replace(obj, "", 1).strip() or "related_to"
            relations.append(
                {
                    "subject": subject,
                    "predicate": predicate,
                    "object": obj,
                    "confidence": 0.55,
                }
            )
    return {"entities": entities, "relations": relations}


def extract_chunk_graph(text: str) -> dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {"entities": [], "relations": []}
    try:
        return _extract_with_llm(text)
    except Exception:
        return {**_extract_with_rules(text), "extraction_mode": "rules_after_request_failure", "usage": None}
