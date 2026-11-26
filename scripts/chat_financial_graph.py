"""Answer a report question using the PostgreSQL financial knowledge graph."""
import argparse
from contextlib import nullcontext
from functools import partial
from pathlib import Path

from openai import OpenAI

from app.config import get_settings
from app.graph.financial import METRICS
from app.graph.financial_query import FinancialGraph, PER_PERSON_NUMERATORS, PERSON_METRICS, RANK_METRICS, UNIT_SCALE
from scripts.chat_markdown import STRING, ask, tool


def graph_ask(question, doc_id, complete, *, trace=None, deterministic_ratios=False):
    graph = FinancialGraph(doc_id)
    tools = [
        tool("financial_facts", "Read typed, page-cited facts. Request all inputs of a calculation together. "
             + "Metric labels and codes: " + ", ".join(f"{label}={code}" for label, code in METRICS.items()), {
            "metrics": {"type": "array", "items": {"type": "string", "enum": sorted(set(METRICS.values()))}},
            "year": {"type": "integer"},
        }),
        tool("reported_subsidiaries", "Count and list distinct companies from disclosed subsidiary tables. "
             "An empty location list returns all indexed subsidiaries. Scope may be incomplete.", {
            "location_terms": {"type": "array", "items": STRING},
            "location_basis": {"type": "string", "enum": ["registered_in", "operates_in"]},
        }),
        tool("company_locations", "Follow ownership to the stated locations of named subsidiaries.", {
            "company_names": {"type": "array", "items": STRING},
            "location_basis": {"type": "string", "enum": ["registered_in", "operates_in"]},
        }),
        tool("rank_subsidiaries", "Rank annual revenue or profit, return rank_total and the sum of the requested "
             "sum_metric over exactly the same companies. "
             "material_non_wholly_owned = 重要非全资子公司主要财务信息 (net_profit); "
             "major_subsidiaries = 主要子公司、参股子公司情况中的子公司 (parent_net_profit, excludes associates). "
             "Each supports revenue. Neither is all group subsidiaries.", {
            "year": {"type": "integer"}, "top_k": {"type": "integer", "minimum": 1, "maximum": 20},
            "scope": {"type": "string", "enum": list(RANK_METRICS)},
            "rank_by": {"type": "string", "enum": sorted(set.union(*RANK_METRICS.values()))},
            "sum_metric": {"type": "string", "enum": sorted(set.union(*RANK_METRICS.values()))},
        }),
    ]
    guidance = (
        "Query the report graph for typed evidence. Batch requested facts. Ask for all inputs of derived metrics. "
        "For subsidiary counts, identify the location basis and include known literal geographic variants. "
        "A count describes only the declared tool scope. Report incomplete coverage and conflicting factual "
        "observations, and do not infer a location from a company name alone. Use rank_subsidiaries for "
        "financial rankings or sums; if the user asks about all subsidiaries, never generalize from this subset."
    )
    dispatch = {"financial_facts": graph.facts, "reported_subsidiaries": graph.subsidiaries,
                "company_locations": graph.company_locations, "rank_subsidiaries": graph.rank_subsidiaries}
    if deterministic_ratios:
        tools.append(tool("financial_per_person", "Compute a report-group money metric per person with exact unit "
                          "conversion. Cite the two input observations. Reuse the expression and rounded value, "
                          "rather than deriving another unit conversion.", {
            "year": {"type": "integer"},
            "numerator_metric": {"type": "string", "enum": list(PER_PERSON_NUMERATORS)},
            "people_metric": {"type": "string", "enum": list(PERSON_METRICS)},
            "output_unit": {"type": "string", "enum": list(UNIT_SCALE)},
            "decimal_places": {"type": "integer", "minimum": 0, "maximum": 6},
        }))
        dispatch["financial_per_person"] = graph.per_person
        guidance += (" For a requested per-person monetary ratio, use financial_per_person directly. "
                     "Do not add other conversion formulas, and do not calculate through missing or conflicting inputs.")
    return ask(question, None, complete, trace=trace, toolset=(tools, dispatch, guidance))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("doc_id")
    parser.add_argument("question")
    parser.add_argument("--trace", type=Path)
    parser.add_argument("--deterministic-ratios", action="store_true", help="Enable the experimental exact ratio tool")
    args = parser.parse_args()
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key, base_url=settings.openai_base_url)
    complete = partial(client.chat.completions.create, model=settings.openai_chat_model)
    with (args.trace.open("x", encoding="utf-8") if args.trace else nullcontext()) as trace:
        print(graph_ask(args.question, args.doc_id, complete, trace=trace,
                        deterministic_ratios=args.deterministic_ratios))


if __name__ == "__main__":
    main()
