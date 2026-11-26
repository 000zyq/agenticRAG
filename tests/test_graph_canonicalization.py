from __future__ import annotations

from app.graph.canonicalize import canonicalize_chunk_graphs


def test_canonicalize_merges_document_local_org_aliases() -> None:
    graph = canonicalize_chunk_graphs(
        [
            {
                "chunk_order": 0,
                "text": "阿里巴巴集团有限公司发布了平台更新。",
                "entities": [
                    {"name": "阿里巴巴集团有限公司", "type": "organization", "aliases": ["阿里巴巴集团"], "confidence": 0.9},
                ],
                "relations": [],
            },
            {
                "chunk_order": 1,
                "text": "阿里巴巴集团继续推进系统建设。",
                "entities": [
                    {"name": "阿里巴巴集团", "type": "organization", "aliases": [], "confidence": 0.8},
                ],
                "relations": [],
            },
        ]
    )

    entities = graph["entities"]
    assert len(entities) == 1
    assert entities[0]["entity_type"] == "organization"
    assert "阿里巴巴集团" in entities[0]["aliases"]
    assert "阿里巴巴集团有限公司" in entities[0]["aliases"]


def test_canonicalize_does_not_merge_across_entity_types() -> None:
    graph = canonicalize_chunk_graphs(
        [
            {
                "chunk_order": 0,
                "text": "Phoenix 发布了产品。",
                "entities": [
                    {"name": "Phoenix", "type": "organization", "aliases": [], "confidence": 0.8},
                    {"name": "Phoenix", "type": "product_system", "aliases": [], "confidence": 0.7},
                ],
                "relations": [],
            }
        ]
    )

    entities = sorted((entity["entity_type"], entity["canonical_name"]) for entity in graph["entities"])
    assert entities == [("organization", "Phoenix"), ("product_system", "Phoenix")]
