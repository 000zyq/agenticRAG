from hashlib import sha256

import pytest

from scripts.summarize_financial_rag import score, summarize_runs
from scripts.price_financial_rag import price_scenarios, replay_category_route


def test_score_accepts_explicit_grouped_citations_without_inventing_pages():
    gold = {"slots": [{"id": "num", "any_of": [r"133642人"]}],
            "evidence": [[r"第9页"], [r"第82页"]]}
    run = {"status": "ok", "answer": "133,642人（第９、８２页）"}
    assert score(run, gold)["joint_correct"]
    run["answer"] = "133,642人（第9–83页）"
    assert not score(run, gold)["citation_correct"]


def test_paired_cost_parity_and_joint_accuracy():
    question = {"id": "q", "report": "r", "category": "fact",
                "slots": [{"id": "num", "any_of": [r"(?<!\d)10人"]}],
                "evidence": [[r"第5页"]]}
    common = {"question_id": "q", "report": "r", "category": "fact", "status": "ok",
              "model": "model", "unreported_usage_calls": 0}
    runs = [
        {**common, "arm": "graphrag", "answer": "10人（第5页）", "elapsed_seconds": 1,
         "usage": {"input_tokens": 100, "cached_input_tokens": 10, "output_tokens": 10}},
        {**common, "arm": "search_tools", "answer": "10人（第6页）", "elapsed_seconds": 2,
         "usage": {"input_tokens": 200, "cached_input_tokens": 10, "output_tokens": 10}},
    ]
    pricing = {"currency": "CNY", "models": {"model": {
        "input_per_million": 1, "cached_input_per_million": 0.1, "output_per_million": 2}},
        "corpus_offline_cost": {"graphrag": 0.001, "search_tools": 0}}
    result = summarize_runs(runs, {"questions": [question]}, pricing)
    assert result["overall"]["graphrag"]["joint_correct"] == 1
    assert result["overall"]["search_tools"]["joint_correct"] == 0
    assert result["overall"]["search_tools"]["answer_correct"] == 1
    assert result["amortization"]["first_parity_queries"] == 10
    assert result["paired"]["graph_only_joint_correct"] == 1
    assert result["paired"]["median_paired_speedup_successful"] == 2
    assert result["overall"]["graphrag"]["cost_per_joint_correct"] == result["items"][0]["api_cost"]
    assert result["overall"]["search_tools"]["cost_per_joint_correct"] is None
    assert summarize_runs(runs, {"questions": [question]})["amortization"] is None
    grid = price_scenarios(runs, {"questions": [question]}, {
        "currency": "CNY", "model": "model", "input_per_million": 1, "output_per_million": 2,
        "cached_input_per_million_scenarios": [0, 1], "incremental_offline_cost_scenarios": [0, 0.001],
    })
    assert len(grid["scenarios"]) == 4
    assert grid["scenarios"][1]["amortization"]["first_parity_queries"] == 10
    assert grid["scenarios"][1]["amortization"]["first_parity_queries_when_both_joint_correct"] is None


def test_review_is_bound_to_answer_and_retains_automatic_grade():
    question = {"id": "q", "report": "r", "category": "fact",
                "slots": [{"id": "num", "any_of": [r"10人"]}], "evidence": [[r"第5页"]]}
    common = {"question_id": "q", "report": "r", "category": "fact", "status": "ok",
              "model": "model", "elapsed_seconds": 1, "usage": {}}
    runs = [{**common, "arm": arm, "answer": "表显示十人（第5页）"} for arm in ("graphrag", "search_tools")]
    review = {"question_id": "q", "arm": "graphrag", "answer_correct": True, "citation_correct": True,
              "answer_sha256": sha256(runs[0]["answer"].encode()).hexdigest(), "note": "十 is 10"}
    manifest = {"questions": [question]}
    result = summarize_runs(runs, manifest, reviews=[review])
    assert result["items"][0]["joint_correct"]
    assert not result["items"][0]["automatic_score"]["joint_correct"]
    assert result["items"][0]["review_note"] == "十 is 10"
    with pytest.raises(ValueError, match="stale review"):
        summarize_runs([{**runs[0], "answer": "different response"}, runs[1]], manifest, reviews=[review])


def test_category_route_never_uses_answer_quality_to_choose_arm():
    from decimal import Decimal

    categories = {"covered": {}, "unknown": {}}
    result = {"by_category": categories, "overall": {"search_tools": {"joint_correct": 2,
                                                                      "mean_api_cost": Decimal("0.2")}}}
    result["items"] = [
        {"question_id": cat, "category": cat, "arm": arm, "status": "ok", "elapsed_seconds": 1,
         "usage": {}, "answer_correct": correct, "citation_correct": True, "joint_correct": correct,
         "api_cost": Decimal("0.1") if arm == "graphrag" else Decimal("0.2")}
        for cat in categories for arm in ("graphrag", "search_tools")
        for correct in [not (cat == "covered" and arm == "graphrag")]
    ]
    replay = replay_category_route(result, ["covered"], Decimal("1"))
    assert replay["selection"] == {"covered": "graphrag", "unknown": "search_tools"}
    assert replay["joint_correct_delta_vs_search"] == -1
    assert replay["first_parity_queries"] == 20
    with pytest.raises(ValueError, match="Unknown routing"):
        replay_category_route(result, ["missing"], Decimal("1"))
