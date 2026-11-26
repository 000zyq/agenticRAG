"""Replay paired financial RAG measurements over a published-price sensitivity grid."""
import argparse
import json
from pathlib import Path

from scripts.summarize_financial_rag import summarize_runs


def price_scenarios(runs, gold, rates, reviews=()):
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
            results.append({key: summary[key] for key in ("overall", "by_category", "paired", "amortization")}
                           | {"cached_input_per_million": cached, "incremental_offline_cost": increment})
    return {"rates": rates, "scenarios": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("rates", type=Path)
    parser.add_argument("--reviews", type=Path)
    args = parser.parse_args()
    reviews = json.loads(args.reviews.read_text()) if args.reviews else []
    result = price_scenarios(*(json.loads(path.read_text()) for path in (args.runs, args.manifest, args.rates)),
                             reviews=reviews)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
