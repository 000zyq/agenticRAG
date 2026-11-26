"""Score paired financial RAG runs against evidence-annotated expectations."""
import argparse
from collections import defaultdict
from decimal import Decimal, ROUND_CEILING
import json
from math import ceil
from pathlib import Path
import re
from statistics import mean, median
import unicodedata

from scripts.benchmark_financial_rag import ARMS


def matches(text, alternatives):
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in alternatives)


def score(run, gold):
    text = unicodedata.normalize("NFKC", run["answer"]).replace(",", "")
    slots = {slot["id"]: matches(text, slot["any_of"]) for slot in gold["slots"]}
    forbidden = [pattern for pattern in gold.get("forbid", []) if matches(text, [pattern])]
    citation = all(matches(text, choices) for choices in gold["evidence"])
    succeeded = run["status"] == "ok"
    answer_correct = succeeded and all(slots.values()) and not forbidden
    return {
        "slots": slots,
        "forbidden": forbidden,
        "answer_correct": answer_correct,
        "citation_correct": succeeded and citation,
        "joint_correct": answer_correct and citation,
    }


def api_cost(run, prices):
    usage = run.get("usage", {})
    if not prices or run["status"] != "ok" or run.get("unreported_usage_calls") != 0 or not usage:
        return None
    input_rate, cached_rate, output_rate = (Decimal(str(prices[key])) for key in
                                           ("input_per_million", "cached_input_per_million", "output_per_million"))
    cached = usage["cached_input_tokens"]
    return ((usage["input_tokens"] - cached) * input_rate + cached * cached_rate
            + usage["output_tokens"] * output_rate) / Decimal(1_000_000)


def summarize_runs(runs, manifest, pricing=None):
    gold = {item["id"]: item for item in manifest["questions"]}
    groups = defaultdict(dict)
    for run in runs:
        question_id, arm = run["question_id"], run["arm"]
        if arm not in ARMS or arm in groups[question_id]:
            raise ValueError(f"Invalid or duplicate arm for {question_id}: {arm}")
        groups[question_id][arm] = run
    if not runs or set(groups) != set(gold) or any(set(pair) != set(ARMS) for pair in groups.values()):
        raise ValueError("A complete result for each question and arm is required")
    models = {run["model"] for run in runs}
    if len(models) != 1:
        raise ValueError("Paired comparison requires one model across the cohort")
    model = models.pop()
    prices = (pricing or {}).get("models", {}).get(model)
    scored = []
    for row in runs:
        expected = gold[row["question_id"]]
        if (row["report"], row["category"]) != (expected["report"], expected["category"]):
            raise ValueError(f"Run metadata differs from manifest for {row['question_id']}")
        scored.append({**row, **score(row, expected), "api_cost": api_cost(row, prices)})
    by_category = {category: summarize_arms([row for row in scored if row["category"] == category])
                   for category in sorted({row["category"] for row in scored})}
    aggregate = summarize_arms(scored)
    offline = (pricing or {}).get("corpus_offline_cost")
    curve = None
    if offline and all(aggregate[arm]["mean_api_cost"] is not None for arm in ARMS):
        fixed = {arm: Decimal(str(offline[arm])) for arm in ARMS}
        marginal = {arm: aggregate[arm]["mean_api_cost"] for arm in ARMS}
        delta = fixed["graphrag"] - fixed["search_tools"]
        saving = marginal["search_tools"] - marginal["graphrag"]
        parity = (int((delta / saving).to_integral_value(rounding=ROUND_CEILING))
                  if delta > 0 and saving > 0 else 0 if delta <= 0 and saving >= 0 else None)
        curve = {"first_parity_queries": parity,
                 "totals": {str(n): {arm: fixed[arm] + n * marginal[arm] for arm in ARMS}
                            for n in (10, 100, 1000)}}
    return {"model": model, "currency": (pricing or {}).get("currency"), "overall": aggregate,
            "by_category": by_category, "amortization": curve, "items": scored}


def summarize_arms(rows):
    summary = {}
    for arm in ARMS:
        group = [row for row in rows if row["arm"] == arm]
        elapsed = sorted(row["elapsed_seconds"] for row in group)
        costs = [row["api_cost"] for row in group]
        summary[arm] = {
            "n": len(group),
            "failed": sum(row["status"] != "ok" for row in group),
            "answer_correct": sum(row["answer_correct"] for row in group),
            "citation_correct": sum(row["citation_correct"] for row in group),
            "joint_correct": sum(row["joint_correct"] for row in group),
            "p50_seconds": median(elapsed),
            "p95_seconds": elapsed[ceil(0.95 * len(elapsed)) - 1],
            "mean_input_tokens": mean(row["usage"].get("input_tokens", 0) for row in group),
            "mean_output_tokens": mean(row["usage"].get("output_tokens", 0) for row in group),
            "mean_api_cost": mean(costs) if all(cost is not None for cost in costs) else None,
        }
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--pricing", type=Path)
    args = parser.parse_args()
    pricing = json.loads(args.pricing.read_text()) if args.pricing else None
    result = summarize_runs(json.loads(args.runs.read_text()), json.loads(args.manifest.read_text()), pricing)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
