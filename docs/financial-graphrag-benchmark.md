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
