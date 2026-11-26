from types import SimpleNamespace

from scripts.chat_financial_hybrid import hybrid_ask


def choice(content):
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))])


def run(question, plan, graph):
    calls, trace = [], []

    def complete(**_):
        calls.append("planner")
        return choice(plan)

    def search():
        calls.append("search")
        return "fallback answer"

    def record(event, **fields):
        trace.append((event, fields))

    return hybrid_ask(question, graph, 2024, complete, search, record), calls, trace


PLAN = '{"op":"facts","scope":"report_group","year":2023,"metrics":["revenue"],"money_unit":"千元"}'


def fact(**overrides):
    return {"subject": "issuer", "scope": "report_group", "year": 2023, "metric": "revenue",
            "value": "40", "unit": "万元", "currency": "CNY",
            "evidence": [{"page": 9, "evidence_text": "营业收入 | 40"}], **overrides}


def test_hybrid_validates_year_and_scope_and_returns_document_page():
    graph = SimpleNamespace(facts=lambda metrics, year: {"facts": [fact()]})
    answer, calls, trace = run("根据2024年年报，2023年营业收入是多少千元？", PLAN, graph)
    assert "400千元（第9页）" in answer and "2023" in answer
    assert calls == ["planner"] and trace[-1][1]["selected"] == "graphrag"
    for question, data in (
        ("2022年营业收入是多少？", [fact()]),
        ("2022年和2023年营业收入是多少？", [fact()]),
        ("2023年营业收入是多少？", [fact(scope="parent")]),
        ("2023年营业收入是多少？", [fact(evidence=[])]),
        ("2023年营业收入是多少？", [fact(), fact(value="50")]),
        ("2023年营业收入是多少？", []),
    ):
        answer, calls, trace = run(question, PLAN, SimpleNamespace(facts=lambda *_: {"facts": data}))
        assert answer == "fallback answer"
        assert calls == ["planner", "search"] and trace[-1][1]["selected"] == "search_tools"


def test_hybrid_does_not_turn_group_fact_into_parent_only_answer():
    answer, calls, _ = run("母公司在职员工多少？", PLAN, None)
    assert answer == "fallback answer" and calls == ["search"]


def test_hybrid_location_requires_location_edge_page_not_ownership_page():
    plan = ('{"op":"location","scope":"disclosed_roster","year":2024,'
            '"company":"甲有限公司","bases":["registered_in"]}')
    graph = SimpleNamespace(company_locations=lambda *_: {"matches": [
        {"company": "甲有限公司", "locations": ["北京"], "evidence_pages": [10], "location_evidence_pages": []},
    ]})
    answer, _, trace = run("甲有限公司注册地在哪里？", plan, graph)
    assert answer == "fallback answer" and trace[-1][1]["reason"] == "location_evidence_missing_or_conflicting"


def test_hybrid_does_not_skip_failed_rank_or_generalize_to_all_subsidiaries():
    plan = ('{"op":"rank","scope":"major_subsidiaries","year":2024,"k":5,'
            '"rank_by":"revenue","sum_metric":"parent_net_profit","unit":"万元","bases":[]}')
    graph = SimpleNamespace(rank_subsidiaries=lambda *_, **kw: {
        "status": "insufficient_comparable_disclosure", "reasons": ["fewer_companies_with_values_than_requested"],
    })
    answer, calls, trace = run("只在主要子公司、参股子公司情况表按收入取前五家。", plan, graph)
    assert answer == "fallback answer"
    assert calls == ["planner", "search"]
    assert "fewer_companies" in trace[-1][1]["reason"]
    assert run("整个集团利润最高的是哪家？", plan, graph)[0] == "fallback answer"


def test_comparison_prices_router_and_fallback_together(tmp_path):
    import json

    from scripts.summarize_financial_hybrid import compare

    q = {"id": "1", "report": "r", "category": "fact", "slots": [{"id": "n", "any_of": ["5人"]}],
         "evidence": [["第9页"]]}
    common = {"question_id": "1", "report": "r", "category": "fact", "model": "m", "status": "ok",
              "answer": "5人（第9页）", "elapsed_seconds": 3, "model_calls": 2, "unreported_usage_calls": 0,
              "usage": {"input_tokens": 4, "output_tokens": 6, "cached_input_tokens": 0}}
    controls = [{**common, "arm": arm} for arm in ("graphrag", "search_tools")]
    hybrid = [{**common, "arm": "hybrid", "trace": "trace.jsonl",
               "routing": {"selected": "search_tools", "reason": "missing_metric", "duration_ms": 100},
               "fallback_ms": 200}]
    events = [{"event": "model", "phase": phase, "usage": {"prompt_tokens": 2, "completion_tokens": 3}}
              for phase in ("router", "search_fallback")]
    (tmp_path / "trace.jsonl").write_text("\n".join(map(json.dumps, events)))
    rates = {"model": "m", "currency": "CNY", "input_per_million": 12, "output_per_million": 36,
             "cached_input_per_million_scenarios": [0]}
    result = compare(controls, hybrid, {"questions": [q]}, rates, trace_directory=tmp_path)
    scenario, = result["scenarios"]
    stage = scenario["mean_hybrid_api_cost_by_stage"]
    assert stage["router"] == stage["search_fallback"] > 0
    assert sum(stage.values()) == scenario["overall"]["hybrid"]["mean_api_cost"]
    assert result["selections"] == {"search_tools": 1}
