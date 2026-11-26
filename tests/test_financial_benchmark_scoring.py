from scripts.summarize_financial_rag import score, summarize_runs


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
    assert summarize_runs(runs, {"questions": [question]})["amortization"] is None
