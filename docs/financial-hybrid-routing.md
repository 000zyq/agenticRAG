# Measured financial hybrid routing

The hybrid is an independent third arm, not the earlier category replay.
It receives only the question, report graph ID, PDF path and report fiscal
year. Evaluation categories and gold answers are never passed to the router.

1. Obvious unsupported scope, period or metric goes straight to Search Tools.
2. Otherwise the same answer model produces one JSON query plan. A narrow
   planner supports annual group facts, money per person, exact-company
   locations, and scoped subsidiary rank or rank-to-location.
3. PostgreSQL verifies the plan against actual observations. Missing or
   conflicting values, period/scope mismatch, missing original-page evidence,
   and incomplete ranking cause Search Tools fallback. A roster edge without
   a cited location edge and a membership page without metric evidence do
   not pass the gate.
4. A successful graph branch renders the values, calculation, scope and
   citations directly from verified observations. It does not pay for a
   second synthesis call or ask the model to perform the arithmetic.
5. Fallback runs the unmodified Search Tools checkout at c024721, from the
   same warm Milvus corpus and using the same answering model.

The CLI currently requires trusted report fiscal-year metadata and the paired
PDF/graph ID; do not expose those as arbitrary user selections. The planner
is not a complete language parser and unsupported cases remain Search Tools
territory. This is a report-scoped experiment, not a general knowledge router.

## Reproduction

Supply the same benchmark manifest, with an added integer
`reports.<key>.report_year`, and the same parsed-page cache used by the
controls. Run `scripts.benchmark_financial_rag` with `--hybrid-only`.
The runner stores separate route and fallback events, timing and token
usage. Then compare with the frozen pure-run receipts:

```sh
uv run --no-sync python -m scripts.summarize_financial_hybrid \
  control/runs.json hybrid/runs.json gold.json \
  data/benchmarks/financial_graphrag_qwen_beijing_pricing.json \
  --control-reviews control-adjudications.json
```

The reported wall time covers process startup, model planning, graph
validation, and the entire fallback where applicable. Stage API costs come
from the recorded token usage, including both router and fallback. Use the
same list-price cache sensitivity as the original evaluation. Costs exclude
local host usage and are not account invoices. The controls are archived
warm runs, not a newly interleaved rerun; model service latency can vary
between the measurement windows.
