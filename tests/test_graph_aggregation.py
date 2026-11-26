from __future__ import annotations

from app.graph.canonicalize import aggregate_relations


def test_aggregate_relations_keeps_direct_evidence_shape() -> None:
    direct = [
        {
            "subject_temp_id": 1,
            "object_temp_id": 2,
            "predicate_text": "uses",
            "predicate_family": "usage",
            "confidence": 0.9,
            "is_aggregated": False,
            "chunk_order": 0,
            "evidence": [{"chunk_order": 0, "evidence_text": "A uses B", "confidence": 0.9}],
        }
    ]

    aggregated = aggregate_relations(direct, hop_window=2, min_confidence=0.55)
    assert aggregated == []
    assert direct[0]["evidence"][0]["chunk_order"] == 0


def test_aggregate_relations_builds_two_hop_edge() -> None:
    direct = [
        {
            "subject_temp_id": 1,
            "object_temp_id": 2,
            "predicate_text": "uses",
            "predicate_family": "usage",
            "confidence": 0.9,
            "is_aggregated": False,
            "chunk_order": 0,
            "evidence": [{"chunk_order": 0, "evidence_text": "A uses B", "confidence": 0.9}],
        },
        {
            "subject_temp_id": 2,
            "object_temp_id": 3,
            "predicate_text": "belongs to",
            "predicate_family": "membership",
            "confidence": 0.8,
            "is_aggregated": False,
            "chunk_order": 1,
            "evidence": [{"chunk_order": 1, "evidence_text": "B belongs to C", "confidence": 0.8}],
        },
    ]

    aggregated = aggregate_relations(direct, hop_window=2, min_confidence=0.55)
    assert len(aggregated) == 1
    assert aggregated[0]["subject_temp_id"] == 1
    assert aggregated[0]["object_temp_id"] == 3
    assert aggregated[0]["is_aggregated"] is True
    assert aggregated[0]["predicate_family"] == "multi_hop"
    assert aggregated[0]["path_json"]["via_entity_temp_id"] == 2
