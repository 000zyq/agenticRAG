# Financial GraphRAG benchmark

Baseline is Search Tools Agentic RAG at `c024721`. Both systems must use the
same versioned PDF-to-Markdown pages, the same questions, and the same answer
model and answer budget. Measure warm queries separately from offline indexing.
Use a separate PostgreSQL schema for evaluation and an isolated Milvus index
for fixture tests.

## Work remaining

- Extend graph extraction for typed ownership and locations, and numeric facts
  with subject, period, currency, unit, scope, and page provenance. Keep report
  extraction coverage and conflicts available for query-time decisions.
- Add graph-only answer tools with typed traversal and exact aggregation.
  Apply scope filters before counting or ranking. Use a separate benchmark
  arm for any Search Tools fallback.
- Label paired questions from multiple reports across direct lookup,
  calculation, multi-hop, aggregation, and insufficient-disclosure cases.
  Retain evidence pages and define acceptable units and error tolerances.
  Hold out questions used for final evaluation from prompt tuning.
- Run interleaved paired evaluations with JSONL tool traces. Report answer and
  citation accuracy per question and category, and P50/P95 end-to-end times.
  Account separately for extraction, embedding, query, and answer-stage costs.
- Apply an explicit price table to recorded token usage and report unpriced
  requests separately. Report local resource use separately from API charges.
  Compare offline-plus-online cost at several query volumes. Report cost
  parity only where the query savings can offset incremental offline costs.

## Current build path

`python -m scripts.index_graph_report report.pdf --engine mineru` uses the same
Markdown conversion and passage boundaries as the Search Tools baseline. It
stores those passages in PostgreSQL with source pages, calls the existing
graph builder, and emits a graph identifier, counts, timing, and extraction
usage. The same content and extraction configuration reuse a ready graph.
The current extractor is generic and its build summary records fallback
counts and unavailable usage. Its outputs are not a complete financial index.

## Typed extraction and first paired run

Pass `--issuer`, `--fiscal-year`, and optionally `--currency` to
`index_graph_report` for deterministic financial-table extraction. This mode
keeps full page tables, emits typed group-level facts, reported subsidiary
edges, and distinct registration and operating locations. Build records have
zero extraction model calls. Coverage depends on recognized disclosures, and
subsidiary profit rankings need additional subject-level metric extraction.

Use `scripts.chat_financial_graph` with the resulting graph ID for graph-only
QA. It shares the Search Tools answer loop and round limit. The paired runner
expects a manifest containing report paths and graph IDs plus question IDs,
categories and text. Supply the unchanged `c024721` checkout as the baseline.
The scoring command uses the gold JSON from `data/benchmarks/`, evaluates
number and citation presence separately, and requires one model across both
arms. Pattern scores are a first pass and disputed cases need manual review.

The development pilot ran one Midea cross-page R&D question with
`qwen3.8-max-0902`. Both answers passed the number and citation check.

| Arm | Seconds | Input tokens | Cached input tokens | Output tokens |
| --- | ---: | ---: | ---: | ---: |
| Search Tools | 30.92 | 11,585 | 3,200 | 1,005 |
| GraphRAG | 15.14 | 2,616 | 0 | 461 |

Alibaba Cloud's published Beijing prices are RMB 12 per million standard input
tokens and RMB 36 per million output tokens for this snapshot. The
[pricing page](https://help.aliyun.com/zh/model-studio/model-pricing)
and [cache documentation](https://help.aliyun.com/zh/model-studio/context-cache)
leave the exact implicit cache rate for this model to the console. At an
assumed cache rate between zero and the standard input rate, the pilot's
list-price estimates are RMB 0.1368–0.1752 and RMB 0.0480 respectively, before
account credits. These are estimates for one question, with local resource
cost excluded. A cohort result and offline measurements are needed for a
break-even estimate.
