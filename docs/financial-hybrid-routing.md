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

To run a single question, configure the existing PostgreSQL, Milvus, embedding
and model environment and provide the matching parsed PDF cache:

```sh
uv run --no-sync python -m scripts.chat_financial_hybrid report.pdf GRAPH_DOC_ID \
  '按年报，2023年度营业收入是多少？' --report-year 2024 \
  --baseline ../search-tools-baseline --trace hybrid.trace.jsonl
```

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

If a route call times out without usage, its possible cost is not silently
set to zero. Report the sum of known charges as a lower bound and report
cost comparisons on the subset of questions priced in all three arms.

## Actual results with the frozen Qwen model

All original 28 questions were submitted using the same manifests and page
hashes. Qwen returned `403 insufficient_quota` partway through: the independent
holdout completed 12/12, the development cohort 4/12, and the Lianchuang cohort
0/4. The 12 rejected requests are availability failures, not graph/search
judgments. The benchmark now stops on the first quota rejection, and an
exhausted planner does not retry the same model through Search Tools.

| Independent holdout, 12 paired questions | Search Tools | GraphRAG | Measured hybrid |
| --- | ---: | ---: | ---: |
| Answer and physical page both correct | 12/12 | 7/12 | 12/12 |
| P50 end-to-end seconds | 19.05 | 13.90 | 19.06 |
| P95 end-to-end seconds | 51.02 | 51.39 | 69.92 |
| Mean API cost on 11 fully priced pairs (CNY) | 0.1040–0.1681 | 0.0401–0.0672 | 0.0644–0.0902 |

Six holdout answers passed the graph evidence gate; six used Search Tools
(five known unsupported requests and one planner timeout). The model-using
routing stage was 19.71 s median, versus 45.5 ms for successful PostgreSQL
evidence checks and direct formatting. Conditional fallback median was
20.85 s. The timed-out router waited 40 s before fallback; the maximum
end-to-end time was a separate long search fallback. Reported-token spend
averaged CNY 0.0651–0.0898 across the 12, but this is a lower bound: the
timed-out request did not report usage. Across the 11 fully priced pairs,
hybrid saved approximately 38–46% of Search Tools model cost without a
median latency gain. The actual route was slower than the earlier labeled
replay, which omitted model planning and live fallback variation.

The four completed development queries were all graph answers and all
joint-correct, versus Search Tools 4/4 and pure GraphRAG 3/4 on those
same questions. The direct PostgreSQL check also verified Lianchuang's
five-company sum and the missing-sixth-company gate, but is not a live
hybrid Top-K result. No paid-tier setting was changed and no replacement
model was substituted into the Qwen measurements.

`data/benchmarks/financial_hybrid_run_receipts.json` records the attempt
counts, rejected question IDs and run/experiment hashes.
`data/benchmarks/financial_hybrid_holdout_adjudications.json` records a
reviewed equivalent refusal phrase with the answer hash. Resume the
uncompleted question IDs using the same model in a new output directory;
if changing models, rerun all compared arms on the selected subset and
report that as a different experiment.
