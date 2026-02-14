from __future__ import annotations

from decimal import Decimal

from scripts.resolve_fact_candidates import FlowCandidate, _choose_candidate, _column_score


def _flow(candidate_id: int, value: str, column_label: str, version_id: int = 1) -> FlowCandidate:
    return FlowCandidate(
        candidate_id=candidate_id,
        version_id=version_id,
        metric_id=1,
        period_start_date=None,
        period_end_date=None,
        value=Decimal(value),
        unit="1",
        currency="CNY",
        consolidation_scope="consolidated",
        audit_flag=None,
        source_trace_id=None,
        column_label=column_label,
        quality_score=None,
    )


def test_column_score_prefers_current_period() -> None:
    assert _column_score("col_1") > _column_score("col_2")
    assert _column_score("2024") > _column_score("2023")
    assert _column_score("本期") > _column_score("上期")


def test_choose_candidate_prefers_col1_when_single_engine() -> None:
    groups = {
        "100": [_flow(1, "100", "col_2")],
        "90": [_flow(2, "90", "col_1")],
    }
    chosen, agree_count, group_size, status, method = _choose_candidate(groups, min_agree=1, version_to_engine={1: "pypdf"})
    assert chosen.candidate_id == 2
    assert agree_count == 1
    assert group_size == 1
    assert status == "auto"
    assert method == "single_engine"
