"""Answer a report question using the PostgreSQL financial knowledge graph."""
import argparse
from contextlib import nullcontext
from functools import partial
from pathlib import Path

from openai import OpenAI

from app.config import get_settings
from app.graph.financial import METRICS
from app.graph.financial_query import FinancialGraph
from scripts.chat_markdown import STRING, ask, tool


def graph_ask(question, doc_id, complete, *, trace=None):
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
        tool("rank_subsidiaries", "Rank the disclosed material non-wholly-owned subsidiaries by annual "
             "revenue or net profit and sum one metric over the selected companies. Never represents all "
             "group subsidiaries. Missing values, insufficient K and cutoff ties are reported.", {
            "year": {"type": "integer"}, "top_k": {"type": "integer", "minimum": 1, "maximum": 20},
            "rank_by": {"type": "string", "enum": ["revenue", "net_profit"]},
            "sum_metric": {"type": "string", "enum": ["revenue", "net_profit"]},
        }),
    ]
    guidance = (
        "Query the report graph for typed evidence. Batch requested facts. Ask for all inputs of derived metrics. "
        "For subsidiary counts, identify the location basis and include known literal geographic variants. "
        "A count describes only the declared tool scope. Report incomplete coverage and conflicting factual "
        "observations, and do not infer a location from a company name alone. Use rank_subsidiaries for "
        "financial rankings or sums; if the user asks about all subsidiaries, never generalize from this subset."
    )
    return ask(question, None, complete, trace=trace, toolset=(
        tools, {"financial_facts": graph.facts, "reported_subsidiaries": graph.subsidiaries,
                "company_locations": graph.company_locations, "rank_subsidiaries": graph.rank_subsidiaries}, guidance,
    ))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("doc_id")
    parser.add_argument("question")
    parser.add_argument("--trace", type=Path)
    args = parser.parse_args()
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key, base_url=settings.openai_base_url)
    complete = partial(client.chat.completions.create, model=settings.openai_chat_model)
    with (args.trace.open("x", encoding="utf-8") if args.trace else nullcontext()) as trace:
        print(graph_ask(args.question, args.doc_id, complete, trace=trace))


if __name__ == "__main__":
    main()
