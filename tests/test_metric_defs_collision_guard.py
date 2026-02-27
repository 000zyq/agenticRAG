from collections import defaultdict

from app.ingest.metric_defs import METRIC_DEFS, normalize_label


def test_no_normalized_pattern_collision_within_statement_type() -> None:
    seen: dict[tuple[str, str], set[str]] = defaultdict(set)
    for metric in METRIC_DEFS:
        statement_type = metric["statement_type"]
        metric_code = metric["metric_code"]
        for pattern in list(metric.get("patterns", [])) + list(metric.get("patterns_en", [])):
            norm = normalize_label(pattern)
            if norm:
                seen[(statement_type, norm)].add(metric_code)

    collisions = {k: v for k, v in seen.items() if len(v) > 1}
    assert collisions == {}
