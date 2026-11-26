"""Compare a measured hybrid arm with the two frozen, reviewed controls."""
import argparse
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
from statistics import mean, median

from scripts.summarize_financial_rag import api_cost, score, summarize_arms, summarize_runs


def known_cost(row, prices):
    usage = row["usage"] or {"input_tokens": 0, "output_tokens": 0, "cached_input_tokens": 0}
    return api_cost({**row, "usage": usage, "status": "ok", "unreported_usage_calls": 0}, prices)


def stage_cost(rows, prices, directory):
    amounts = {"router": [], "search_fallback": []}
    for row in rows:
        path = directory / row["trace"]
        trace = path.read_text(encoding="utf-8") if path.exists() else ""
        events = [json.loads(line) for line in trace.splitlines() if line.strip()]
        model = [e for e in events if e["event"] == "model"]
        if len(model) != row["model_calls"] or any(e.get("phase") not in amounts for e in model):
            raise ValueError("Hybrid trace and recorded model calls differ")
        for phase in amounts:
            usage = [e["usage"] for e in model if e["phase"] == phase and e.get("usage")]
            totals = {
                "input_tokens": sum(e.get("prompt_tokens") or 0 for e in usage),
                "output_tokens": sum(e.get("completion_tokens") or 0 for e in usage),
                "cached_input_tokens": sum((e.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
                                           for e in usage),
            }
            amounts[phase].append(known_cost({**row, "usage": totals}, prices))
        if sum(values[-1] for values in amounts.values()) != known_cost(row, prices):
            raise ValueError("Hybrid stage costs do not reconcile with total usage")
    return {phase: mean(values) for phase, values in amounts.items()}


def compare(controls, hybrid, gold, rates, control_reviews=(), hybrid_reviews=(), trace_directory=None):
    baseline = summarize_runs(controls, gold, reviews=control_reviews)["items"]
    questions = {q["id"]: q for q in gold["questions"]}
    ids = Counter(row["question_id"] for row in hybrid)
    if ids != Counter(questions.keys()) or any(row["arm"] != "hybrid" for row in hybrid):
        raise ValueError("Expected exactly one measured hybrid answer per frozen question")
    if {r["model"] for r in [*baseline, *hybrid]} != {rates["model"]}:
        raise ValueError("All measured arms must use the same model")
    reviews = {item["question_id"]: item for item in hybrid_reviews}
    if len(reviews) != len(hybrid_reviews) or not reviews.keys() <= questions.keys():
        raise ValueError("Invalid hybrid review keys")
    judged = []
    for row in hybrid:
        expected = questions[row["question_id"]]
        if (row["report"], row["category"]) != (expected["report"], expected["category"]):
            raise ValueError("Hybrid question metadata differs from controls")
        grade = score(row, expected)
        if review := reviews.get(row["question_id"]):
            if (review["arm"] != "hybrid" or review["answer_sha256"] != sha256(row["answer"].encode()).hexdigest()
                    or not review["note"].strip() or row["status"] != "ok"
                    or any(type(review[k]) is not bool for k in ("answer_correct", "citation_correct"))):
                raise ValueError("Invalid or stale hybrid review")
            grade.update(review_note=review["note"], answer_correct=review["answer_correct"],
                         citation_correct=review["citation_correct"],
                         joint_correct=review["answer_correct"] and review["citation_correct"])
        judged.append({**row, **grade})
    selections = Counter((row.get("routing") or {}).get("selected", "missing") for row in hybrid)
    reasons = Counter((row.get("routing") or {}).get("reason", "missing") for row in hybrid)
    route_times = [r["routing"]["duration_ms"] for r in hybrid if r.get("routing")]
    planned_times = [r["routing"]["duration_ms"] for r in hybrid if r.get("routing")
                     and r["routing"]["reason"] != "unsupported_scope_period_or_metric"]
    evidence_times = [r["routing"]["evidence_ms"] for r in hybrid if r.get("routing")
                      and r["routing"].get("evidence_ms") is not None]
    fallback_times = [r["fallback_ms"] for r in hybrid if r.get("fallback_ms") is not None]
    all_rows = [*baseline, *judged]
    arms = ("search_tools", "graphrag", "hybrid")
    comparisons = []
    for cache_rate in rates["cached_input_per_million_scenarios"]:
        prices = {**rates, "cached_input_per_million": cache_rate}
        priced = [{**row, "api_cost": api_cost(row, prices)} for row in all_rows]
        comparisons.append({
            "cached_input_per_million": cache_rate,
            "mean_hybrid_known_cost_by_stage": stage_cost(hybrid, prices, trace_directory)
            if trace_directory else None,
            "mean_hybrid_known_cost_lower_bound": mean(known_cost(row, prices) for row in hybrid),
            "priced_pairs": _priced_pairs(priced),
            "overall": summarize_arms(priced, arms=arms),
            "by_category": {c: summarize_arms([row for row in priced if row["category"] == c], arms=arms)
                            for c in sorted({row["category"] for row in priced})},
        })
    return {"model": rates["model"], "currency": rates["currency"], "selections": selections, "reasons": reasons,
            "unreported_hybrid_calls": sum(row["unreported_usage_calls"] for row in hybrid),
            "p50_route_ms": median(route_times) if route_times else None,
            "p50_route_ms_when_model_used": median(planned_times) if planned_times else None,
            "p50_verified_graph_evidence_ms": median(evidence_times) if evidence_times else None,
            "p50_fallback_ms_conditional": median(fallback_times) if fallback_times else None,
            "scenarios": comparisons, "items": judged}


def _priced_pairs(rows):
    by_id = {}
    for row in rows:
        by_id.setdefault(row["question_id"], {})[row["arm"]] = row["api_cost"]
    full = [p for p in by_id.values() if all(value is not None for value in p.values())]
    return {"n": len(full), "mean_cost": {arm: mean(p[arm] for p in full)
                                         for arm in ("search_tools", "graphrag", "hybrid")} if full else None}


def verify_inputs(control, hybrid):
    for key in ("model", "baseline_commit", "reports"):
        if control[key] != hybrid[key]:
            raise ValueError(f"Frozen controls and measured hybrid differ: {key}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("controls", "hybrid", "gold", "rates"):
        parser.add_argument(arg, type=Path)
    parser.add_argument("--control-reviews", type=Path)
    parser.add_argument("--hybrid-reviews", type=Path)
    args = parser.parse_args()

    def load(path):
        return json.loads(path.read_text(encoding="utf-8")) if path else []

    control_experiment = load(args.controls.with_name("experiment.json"))
    hybrid_experiment = load(args.hybrid.with_name("experiment.json"))
    verify_inputs(control_experiment, hybrid_experiment)
    print(json.dumps(compare(load(args.controls), load(args.hybrid), load(args.gold), load(args.rates),
                             load(args.control_reviews), load(args.hybrid_reviews), args.hybrid.parent),
                     ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
