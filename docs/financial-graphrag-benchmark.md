# Financial GraphRAG benchmark

Baseline is Search Tools Agentic RAG at `c024721`. Both systems must use the
same versioned PDF-to-Markdown pages, the same questions, and the same answer
model and answer budget. Measure warm queries separately from offline indexing.
Use a separate PostgreSQL schema for evaluation and an isolated Milvus index
for fixture tests.

## Work remaining

- Evaluate the page-faithful input with the same interleaved runner, then run
  the separate holdout. It includes facts intentionally outside the graph
  schema; do not extend the schema using those questions before evaluating it.
- Evaluate the third issuer's positive Top-5 and separate it from the two-report
  MinerU cohort because the native PDF parser and graph extractor differ.
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
zero extraction model calls. Version 2 adds year-specific subsidiary revenue
and net profit for supported material non-wholly-owned subsidiary tables.
Ranking selects companies first and sums a metric over the same cohort using
decimal arithmetic. Insufficient K, conflicting or absent values, mixed
currencies and cutoff ties are surfaced instead of making up a ranking.

Use `scripts.chat_financial_graph` with the resulting graph ID for graph-only
QA. It shares the Search Tools answer loop and round limit. The paired runner
expects a manifest containing report paths and graph IDs plus question IDs,
categories and text. Supply the unchanged `c024721` checkout as the baseline.
The scoring command uses the gold JSON from `data/benchmarks/`, evaluates
number and citation presence separately, and requires one model across both
arms. Pattern scores are a first pass and disputed cases need manual review.
New runs capture the baseline/graph revisions and file digests in
`experiment.json`; keep that file with the run JSON and JSONL traces.

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

The eight-question development cohort uses the Midea and Muyuan 2024 reports.
On the original Muyuan PDF, the Shangcai entity's registration row is on
physical page 227. The shared MinerU content list merges the table into page
224. Grade against the physical PDF. The same cache's middle JSON retains the
unmerged table in page 227's `preproc_blocks`, which can support a later
provenance correction. Keep results for the frozen input separate from any
rerun with corrected input.

`scripts.restore_mineru_table_pages` now prepares a separate content list from
the matching middle JSON without changing the original cache. The script
validates table ordering against the final HTML, restores the original
fragments by page and table bounding box, and refuses to overwrite an existing
output. Put its output under a separate MinerU output root, preserving the
report subdirectory and content-list filename, then use that root for **both**
arms and rebuild each index. This restored 54 Midea and 178 Muyuan table
fragments in the current cache. In the corrected Muyuan input the extractor
returns the Shangcai registration row on page 227 instead of page 224. The
corrected graph builds took 0.29 and 0.91 seconds respectively, excluding
initial PDF parsing; this confirms the locator correction, not QA accuracy
across the entire corpus.

### Frozen original-input development cohort

The paired `qwen3.8-max-0902` runs used two reports and eight questions.

| Arm | Answer correct | Joint correct | P50 s | P95 s | Mean input tokens | Mean output tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Search Tools | 8/8 | 7/8 | 26.83 | 69.15 | 12,565 | 1,276 |
| GraphRAG | 8/8 | 7/8 | 11.62 | 22.58 | 2,717 | 458 |

Both evidence failures were the same wrong PDF page on the Muyuan
registration lookup. Median end-to-end time on the two multi-hop questions
was 19.29 versus 6.02 seconds; on the two cross-page calculations it was
24.99 versus 14.64 seconds. These very small development strata establish
observed differences, not reusable performance estimates.

Using the public Beijing list rates and the cache-rate sensitivity above,
the original cohort's mean model cost was RMB 0.1357–0.1967 for Search
Tools and RMB 0.0476–0.0491 for the graph. The corresponding paired saving
is RMB 0.0881–0.1476 per question. At exchangeable workload mix and
quality, an incremental fixed graph deployment cost of RMB 1, 10, or 50
would be recovered in 7–12, 68–114, or 339–568 queries respectively. Those
fixed costs are scenarios, not measured labor or infrastructure bills. Both
offline builders in the current experiment make zero paid model calls (BGE
embeddings run locally). PDF extraction is shared, and cancels when taking
the difference. Keep local cold build times separate from billed costs.

### Positive multi-company Top-K on a third issuer

The [Lianchuang Optoelectronic 2024 report](https://static.cninfo.com.cn/finalpage/2025-04-29/1223383548.PDF)
shows five subsidiaries and three associates in separate row groups on physical
page 26. The registration table for the profit leader is on physical page 181.
The graph has a separate `major_subsidiaries` scope: use the report's row group,
not an equity percentage threshold. It excludes associates before ranking and
does not treat subsidiary attributable net profit as unqualified net profit.

Build the shared cache without downloading OCR model weights:

```sh
uv run --no-project --with pdfplumber==0.11.11 python scripts/native_pdf_content_list.py \
  lianchuang-2024.pdf native-cache/lianchuang-2024/native/lianchuang-2024_content_list.json
```

This is a native-text/ruled-table input for both arms, not MinerU output. It uses
the same content-list interchange format the fixed Search Tools baseline can
read. Keep native extraction cost and results separate from the MinerU cohort.
The native parser does not cover scanned or overlapping-table PDFs.
The actual 211-page extraction produced 819 items in 31.45 seconds. The
PostgreSQL query independently matched the paper table: total revenue
302,910.94 ten-thousand yuan and total attributable net profit negative
9,064.12 ten-thousand yuan. Asking for six companies returns incomplete
coverage rather than substituting an associate. The paired model evaluation
uses `data/benchmarks/financial_graphrag_lianchuang_gold.json`.

### Cold indexing and amortization replay

Measure from a prepared page cache to a queryable index. Both models reuse the
page extraction; do not include it on just one side. Cold page-faithful
indexes on the local evaluation machine:

| Report | Search Tools (BGE + Milvus) s | Typed graph (PostgreSQL) s |
| --- | ---: | ---: |
| Midea | 366.48 | 0.66 |
| Muyuan | 394.02 | 3.96 |
| Total | 760.49 | 4.62 |

These wall times exclude shared PDF extraction and are not a CPU-hour or
invoice comparison. The measured graph extractor does not call an LLM; both
index builds spend zero billed model tokens. There is no incremental
graph-build API cost to recover in this setup. Treat engineer time, ongoing
schema maintenance and concurrent database/embedding infrastructure as
separate deployment inputs.

```sh
uv run python -m scripts.price_financial_rag runs.json gold.json \
  data/benchmarks/financial_graphrag_qwen_beijing_pricing.json
```

The price grid computes per-attempt cost, model cost divided by the number of
joint-correct answers, and total cost at 10, 100 and 1,000 questions. Use
`first_parity_queries_when_both_joint_correct` to describe a **matched-answer
subset**: it is conditional on these particular questions being answered and
cited correctly by both arms, not a service-wide guarantee. If correctness
differs, raw per-attempt parity by itself does not justify replacing Search
Tools. Record human adjudications with `--reviews` (question ID, arm, boolean
answer/citation grades, answer SHA-256 and reason); the original regex outcome
is retained alongside each override. Failed or unpriced runs are never treated
as free requests.

### Independent MinerU holdout: reviewed results

Twelve previously unseen questions on the two page-faithful reports, with
the frozen Search Tools commit `c024721` and graph commit `9ae0154`. Both
arms used `qwen3.8-max-0902`, warm indexes, and the same tool-round cap.

| Question type | Search Tools joint correct | GraphRAG joint correct | Search / graph P50 s |
| --- | ---: | ---: | ---: |
| Direct typed facts | 3/3 | 3/3 | 18.53 / 12.86 |
| Cross-page arithmetic | 2/2 | 2/2 | 27.78 / 16.17 |
| Entity location | 1/1 | 1/1 | 12.81 / 8.09 |
| Scoped financial rank | 1/1 | 1/1 | 18.34 / 10.79 |
| Outside the graph schema | 4/4 | 0/4 | 20.36 / 17.46 |
| Incomplete disclosure | 1/1 | 0/1 | 51.02 / 51.39 |
| **Total** | **12/12** | **7/12** | **19.05 / 13.90** |

Overall P95 was 51.02 s versus 51.39 s; at this sample size the
nearest-rank P95 is the maximum. The faster median does not mean a faster
tail. The paired median speedup was 1.27x. Median time in model calls was
17.84 s versus 13.22 s; most of the end-to-end median difference is consistent
with faster model turns, not simply database versus vector lookup.

The graph missed the indirect stake, operating cash flow, capitalized R&D
share and parent-only staff count. These facts are disclosed in the PDFs;
they were not in the selected graph schema. On the incomplete-disclosure
question, the graph correctly said a group-wide rank was not identifiable but
then made a material unsupported inference about where most consolidated
profit came from. This was scored wrong. Search used an equivalent coverage
phrase the regex missed; manual review accepted it. The hash-bound
adjudications are in `data/benchmarks/financial_graphrag_holdout_adjudications.json`.

| Model cost, CNY | Search Tools | GraphRAG |
| --- | ---: | ---: |
| Per attempted question | 0.1029–0.1658 | 0.0404–0.0673 |
| Per joint-correct answer | 0.1029–0.1658 | 0.0693–0.1154 |

Raw attempt costs would offset a hypothetical RMB 10 incremental build
expense after 102–161 attempts, but this trades away coverage. On the seven
questions correct in both arms, mean savings were RMB 0.0532–0.0936, implying
107–189 **matching-quality queries** for the same hypothetical expense.
The latter is a conditional sample estimate: it is not a guarantee that a
runtime router can select only winning cases. The data supports graph-first
evaluation for well-covered typed questions, not replacing Search Tools for
the full workload.

#### Category-labeled route replay (not a deployed third arm)

Replay a fixed rule on the same twelve paired answers: use graph for direct
typed questions, calculation, multi-hop and scoped aggregation; use Search
Tools for outside-schema and insufficient-coverage questions. The rule sees
only the manifest's category, not the answer or grade. It selected graph
for seven questions and Search Tools for five, with 12/12 joint-correct in
this replay. Simulated P50 was 16.19 seconds versus 19.05 for all-search;
P95 was unchanged at 51.02 seconds. Estimated model cost was
RMB 0.0718–0.1113 per question, saving RMB 0.0310–0.0546 versus all-search.
At a hypothetical RMB 10 incremental expense the mixed traffic takes
**184–323 attempts** to reach cost parity. This denominator is all routed
traffic, not seven preselected successes. It excludes the time and cost of
classifying, retrying or falling back in production. Broad category names are
not a sufficient production router; verify that the report has the required
period, scope, metrics and edges before selecting graph.

Reproduce by adding the following flags to the price-grid command above:

```sh
--reviews data/benchmarks/financial_graphrag_holdout_adjudications.json \
--route-graph-category direct --route-graph-category calculation \
--route-graph-category multi_hop --route-graph-category aggregation
```
