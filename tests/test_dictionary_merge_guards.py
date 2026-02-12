from __future__ import annotations

from app.ingest.metric_defs import _normalize_pattern_buckets
from scripts.merge_cas2020_toc_dictionary import _append_cn_pattern


def test_normalize_pattern_buckets_cn_short_and_generic() -> None:
    loose, exact = _normalize_pattern_buckets(
        patterns=["资产", "税", "营业收入", "费用"],
        patterns_exact=[],
        is_cn=True,
    )
    assert "营业收入" in loose
    assert "税" in exact
    assert "资产" not in loose
    assert "资产" not in exact
    assert "费用" not in loose
    assert "费用" not in exact


def test_normalize_pattern_buckets_en_short_to_exact() -> None:
    loose, exact = _normalize_pattern_buckets(
        patterns=["RO", "revenue"],
        patterns_exact=[],
        is_cn=False,
    )
    assert loose == ["revenue"]
    assert exact == ["RO"]


def test_append_cn_pattern_filters_generic_short() -> None:
    metric = {"patterns_cn": [], "patterns_cn_exact": []}
    assert _append_cn_pattern(metric, "资产") == 0
    assert _append_cn_pattern(metric, "合同资产", prefer_exact=True) == 1
    assert _append_cn_pattern(metric, "税") == 1
    assert _append_cn_pattern(metric, "税") == 0
    assert metric["patterns_cn"] == []
    assert metric["patterns_cn_exact"] == ["合同资产", "税"]
