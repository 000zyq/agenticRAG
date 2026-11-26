"""Replay paired financial RAG measurements over a published-price sensitivity grid."""
import argparse
import json
from pathlib import Path

from scripts.summarize_financial_rag import break_even, summarize_arms, summarize_runs


def replay_category_route(summary, graph_categories, increment):
    """Replay a fixed category rule, not a live classifier or measured hybrid run."""
    categories = set(graph_categories)
    unknown = categories - summary["by_category"].keys()
    if unknown:
        raise ValueError(f"Unknown routing categories: {sorted(unknown)}")
    selected = [item for item in summary["items"]
                if item["arm"] == ("graphrag" if item["category"] in categories else "search_tools")]
    graph_selected = sum(item["arm"] == "graphrag" for item in selected)
    # Relabel the replay as a single arm only for aggregation; preserve the
    # original arm in the selection manifest.
    metrics = summarize_arms([{**item, "arm": "replay"} for item in selected], arms=("replay",))["replay"]
    baseline = summary["overall"]["search_tools"]
    paid = metrics["mean_api_cost"] is not None and baseline["mean_api_cost"] is not None
    saving = baseline["mean_api_cost"] - metrics["mean_api_cost"] if paid else None
    return {
        "method": "category-labeled offline replay; classification, retries and routing overhead excluded",
        "graph_categories": sorted(categories),
        "graph_selected": graph_selected,
        "selection": {item["question_id"]: item["arm"] for item in selected},
        "metrics": metrics,
        "joint_correct_delta_vs_search": metrics["joint_correct"] - baseline["joint_correct"],
        "mean_savings_vs_search": saving,
        "first_parity_queries": break_even(increment if graph_selected else 0, saving),
    }


def price_scenarios(runs, gold, rates, reviews=(), graph_categories=()):
    if {row["model"] for row in runs} != {rates["model"]}:
        raise ValueError("Rates must match the model used for both arms")
    results = []
    for cached in rates["cached_input_per_million_scenarios"]:
        for increment in rates["incremental_offline_cost_scenarios"]:
            pricing = {
                "currency": rates["currency"],
                "models": {rates["model"]: {"cached_input_per_million": cached,
                                           "input_per_million": rates["input_per_million"],
                                           "output_per_million": rates["output_per_million"]}},
                "corpus_offline_cost": {"graphrag": increment, "search_tools": 0},
            }
            summary = summarize_runs(runs, gold, pricing, reviews=reviews)
            result = {key: summary[key] for key in ("overall", "by_category", "paired", "amortization")}
            result.update(cached_input_per_million=cached, incremental_offline_cost=increment)
            if graph_categories:
                result["category_route_replay"] = replay_category_route(summary, graph_categories, increment)
            results.append(result)
    return {"rates": rates, "scenarios": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("rates", type=Path)
    parser.add_argument("--reviews", type=Path)
    parser.add_argument("--route-graph-category", action="append", default=[],
                        help="Replay graph for this labeled category and search for all others (repeatable)")
    args = parser.parse_args()
    reviews = json.loads(args.reviews.read_text()) if args.reviews else []
    result = price_scenarios(*(json.loads(path.read_text()) for path in (args.runs, args.manifest, args.rates)),
                             reviews=reviews, graph_categories=args.route_graph_category)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
