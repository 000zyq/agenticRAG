"""Score paired financial RAG runs against evidence-annotated expectations."""
import argparse
from collections import defaultdict
from decimal import Decimal, ROUND_CEILING
from hashlib import sha256
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
    text = unicodedata.normalize("NFKC", run["answer"])
    values = re.sub(r"(?<=\d),(?=\d{3}(?!\d))", "", text)
    slots = {slot["id"]: matches(values, slot["any_of"]) for slot in gold["slots"]}
    forbidden = [pattern for pattern in gold.get("forbid", []) if matches(values, [pattern])]
    # Expand explicit page lists, not ranges or unqualified numbers.
    citations = re.sub(r"第\s*(\d+(?:\s*[,、和及]\s*\d+)+)\s*页",
                       lambda match: "、".join(f"第{n}页" for n in re.findall(r"\d+", match[1])),
                       text)
    citation = all(matches(citations, choices) for choices in gold["evidence"])
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


def break_even(incremental_cost, saving):
    if saving is None or saving < 0 or (incremental_cost > 0 and saving == 0):
        return None
    if incremental_cost <= 0:
        return 0
    return int((incremental_cost / saving).to_integral_value(rounding=ROUND_CEILING))


def summarize_runs(runs, manifest, pricing=None, reviews=()):
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
    keyed_reviews = {}
    for review in reviews:
        key = (review["question_id"], review["arm"])
        if key in keyed_reviews or key[1] not in groups.get(key[0], {}):
            raise ValueError(f"Duplicate or unknown review: {key}")
        row = groups[key[0]][key[1]]
        if (review["answer_sha256"] != sha256(row["answer"].encode()).hexdigest()
                or not isinstance(review["note"], str) or not review["note"].strip()
                or any(type(review[k]) is not bool for k in ("answer_correct", "citation_correct"))
                or row["status"] != "ok" and (review["answer_correct"] or review["citation_correct"])):
            raise ValueError(f"Invalid or stale review: {key}")
        keyed_reviews[key] = review
    scored = []
    for row in runs:
        expected = gold[row["question_id"]]
        if (row["report"], row["category"]) != (expected["report"], expected["category"]):
            raise ValueError(f"Run metadata differs from manifest for {row['question_id']}")
        grade = score(row, expected)
        review = keyed_reviews.get((row["question_id"], row["arm"]))
        if review:
            grade = {**grade, "automatic_score": grade, "review_note": review["note"],
                     "answer_correct": review["answer_correct"], "citation_correct": review["citation_correct"],
                     "joint_correct": review["answer_correct"] and review["citation_correct"]}
        scored.append({**row, **grade, "api_cost": api_cost(row, prices)})
    by_category = {category: summarize_arms([row for row in scored if row["category"] == category])
                   for category in sorted({row["category"] for row in scored})}
    aggregate = summarize_arms(scored)
    paired = paired_statistics(scored)
    offline = (pricing or {}).get("corpus_offline_cost")
    curve = None
    if offline and all(aggregate[arm]["mean_api_cost"] is not None for arm in ARMS):
        fixed = {arm: Decimal(str(offline[arm])) for arm in ARMS}
        marginal = {arm: aggregate[arm]["mean_api_cost"] for arm in ARMS}
        delta = fixed["graphrag"] - fixed["search_tools"]
        saving = marginal["search_tools"] - marginal["graphrag"]
        curve = {"first_parity_queries": break_even(delta, saving),
                 "first_parity_queries_when_both_joint_correct": break_even(
                     delta, paired["mean_saving_when_both_joint_correct"]),
                 "incremental_offline_cost": delta, "savings_per_query": saving,
                 "totals": {str(n): {arm: fixed[arm] + n * marginal[arm] for arm in ARMS}
                            for n in (10, 100, 1000)}}
    return {"model": model, "currency": (pricing or {}).get("currency"), "overall": aggregate,
            "by_category": by_category, "paired": paired,
            "amortization": curve, "items": scored}


def paired_statistics(rows):
    pairs = defaultdict(dict)
    for row in rows:
        pairs[row["question_id"]][row["arm"]] = row
    paired = [(p["search_tools"], p["graphrag"]) for p in pairs.values()]
    joint_delta = [int(g["joint_correct"]) - int(b["joint_correct"]) for b, g in paired]
    successful = [(b, g) for b, g in paired if b["status"] == g["status"] == "ok"]
    speedups = [b["elapsed_seconds"] / g["elapsed_seconds"] for b, g in successful if g["elapsed_seconds"] > 0]
    both_correct_benchmark = [(b, g) for b, g in paired if b["joint_correct"] and g["joint_correct"]]
    comparable_savings = [b["api_cost"] - g["api_cost"] for b, g in both_correct_benchmark
                          if b["api_cost"] is not None and g["api_cost"] is not None]
    return {
        "both_joint_correct": len(both_correct_benchmark),
        "graph_only_joint_correct": joint_delta.count(1), "search_only_joint_correct": joint_delta.count(-1),
        "neither_joint_correct": joint_delta.count(0) - len(both_correct_benchmark),
        "median_paired_speedup_successful": median(speedups) if speedups else None,
        "mean_saving_when_both_joint_correct": mean(comparable_savings) if comparable_savings else None,
        "n_priced_both_joint_correct": len(comparable_savings),
    }


def summarize_arms(rows, *, arms=ARMS):
    summary = {}
    for arm in arms:
        group = [row for row in rows if row["arm"] == arm]
        elapsed = sorted(row["elapsed_seconds"] for row in group)
        costs = [row["api_cost"] for row in group]
        priced = all(cost is not None for cost in costs)
        correct = sum(row["joint_correct"] for row in group)
        model_times = [row["model_ms"] / 1000 for row in group if "model_ms" in row]
        summary[arm] = {
            "n": len(group),
            "failed": sum(row["status"] != "ok" for row in group),
            "answer_correct": sum(row["answer_correct"] for row in group),
            "citation_correct": sum(row["citation_correct"] for row in group),
            "joint_correct": correct,
            "reviewed": sum("review_note" in row for row in group),
            "p50_seconds": median(elapsed),
            "p95_seconds": elapsed[ceil(0.95 * len(elapsed)) - 1],
            "mean_input_tokens": mean(row["usage"].get("input_tokens", 0) for row in group),
            "mean_output_tokens": mean(row["usage"].get("output_tokens", 0) for row in group),
            "p50_model_seconds": median(model_times) if len(model_times) == len(group) else None,
            "mean_api_cost": mean(costs) if priced else None,
            "cost_per_joint_correct": sum(costs) / correct if priced and correct else None,
        }
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--pricing", type=Path)
    parser.add_argument("--reviews", type=Path, help="Human adjudications bound to answer hashes")
    args = parser.parse_args()
    pricing = json.loads(args.pricing.read_text()) if args.pricing else None
    reviews = json.loads(args.reviews.read_text()) if args.reviews else []
    result = summarize_runs(json.loads(args.runs.read_text()), json.loads(args.manifest.read_text()), pricing, reviews)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
