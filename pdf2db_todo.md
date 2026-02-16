## PDF2DB TODO (Prioritized)

### P0
1. Done: stabilized dictionary after CAS2020 import.
   - Removed auto-created `cas2020_*` metrics from core dictionary.
   - Kept CAS2020 integration in alias-only mode (no bulk metric creation).
2. Done: built `cas2020_sub_code -> metric_code` mapping artifact and use it before fallback `raw_*`.
   - artifact: `data/taxonomy/cas2020_metric_mapping.json`
   - runtime hook: `app/ingest/metric_defs.py::match_metric`
3. Done: integrated CAS2020 background rules (`ELR` and `element types`) into parser routing/validation.
   - parser now maps `ELR code -> statement_type` via `cas2020_background_rules.json`
   - MinerU content-list parsing now validates item `type` against background `element_types`
4. Done (phase-1): calibrated `match_metric` with labeled cases.
   - added labeled set: `tests/fixtures/metric_match_cases.json` (`required` + `optional`)
   - added eval script: `scripts/eval_metric_match_cases.py`
   - added regression test: `tests/test_metric_match_cases.py`
   - current baseline:
     - `required` exact match: `1.0`
     - `all_cases` exact match: `1.0`
     - `core_metric_match_rate` improved from `0.2189` -> `0.2568`

### P1
4. Done: MinerU cache env is now fixed to `/tmp` at runtime for consistent reuse.
   - enforced envs: `XDG_CACHE_HOME`, `HF_HOME`, `HUGGINGFACE_HUB_CACHE`, `TRANSFORMERS_CACHE`, `MPLCONFIGDIR`
5. Done: `MINERU_OUTPUT_DIR` references are persisted in `report_versions.summary_json`.
   - keys: `mineru_output_dir`, `mineru_output_report_dir`, `mineru_content_list_path` (when discovered)
6. Done: parser now prioritizes MinerU HTML `<table>` extraction path and skips whitespace-only fallback when HTML tables exist.
   - covered by regression test in `tests/test_table_detection.py`
7. Done: taxonomy label merge tightened to reduce generic-term collisions.
   - CAS2020 merge path now applies stop-label/short-deny filters before adding aliases
8. Done: short labels (<=2 chars) are normalized to exact-match bucket at dictionary load time; generic short labels are dropped.
9. Done: MinerU output root auto-detection now supports static `MINERU_CMD -o <path>` (without `{output}` placeholder).
   - prevents false `MinerU extraction failed` retries when command writes to a fixed directory (e.g. `tmp/mineru_output`)
   - covered by regression test `tests/test_mineru_extract.py::test_mineru_extract_reads_static_output_path_in_cmd`

### P2
1. Done: make candidate resolution column-aware to stabilize current-period selection in single-engine runs.
   - `resolve_fact_candidates` now prefers `col_1/current/本期/最新年份` over prior columns when agreement counts are tied.
   - cash consistency check for `2024-12-31` now passes for both consolidated and parent scope on the sample report.
2. Done: increase multi-engine agreement coverage (`multi_engine_groups_with_multi`) by running stable dual-engine ingest (`pypdf + mineru`) into separate versions.
   - fixed MinerU false-negative path: even if MinerU CLI exits non-zero, parser now consumes generated `*_content_list.json` / markdown artifacts when present.
   - validated on sample report with cached MinerU output:
     - `multi_engine_groups_with_multi`: `0 -> 135`
     - `multi_engine_agreement_rate`: `1.0`
3. Done: stabilize cashflow identity `net_increase_eq_sum_cashflows` under multi-engine consensus.
   - added explicit metric mapping for `fx_effect_on_cash` (`汇率变动对现金及现金等价物的影响`).
   - consistency formula now uses `经营 + 投资 + 筹资 + 汇率影响 = 现金净增加额` when FX metric is available.
   - latest run (`report_id=2`): all cashflow consistency checks pass for both scopes and both periods.
4. In progress: reduce single-engine-only groups (coverage gap between `pypdf` and `mineru`).
   - current breakdown on `report_id=2` latest dual-engine run:
     - total groups: `461`
     - multi-engine groups: `192`
     - single-engine groups: `269` = `mineru-only 267` + `pypdf-only 2`
   - flow: `pypdf=109`, `mineru=258`, overlap=`108`
   - stock: `pypdf=85`, `mineru=201`, overlap=`84`
   - latest consensus stats (`version_id=89`):
     - `multi_engine_groups_with_multi`: `192`
     - `multi_engine_groups_agreed`: `191`
     - `multi_engine_agreement_rate`: `0.9948`
   - primary hypothesis: `pypdf` recall is lower on complex OCR/table pages; secondary cause is key split by metadata (`unit/currency/scope`) and residual `raw_*` metric fragmentation.
   - next step:
     - add coverage diff report by metric/date/scope and track top missing metrics per engine
     - normalize grouping metadata defaults before resolver compare
     - improve `pypdf` table/column extraction or lower weight in consensus for low-confidence groups
   - progress:
     - fixed `unit` pollution from pypdf fallback (`"单位"` no longer copies full header text into `unit`)
     - added HTML header fiscal-year parsing for labels like `2024年度/2023年度`
     - fixed MinerU fixed-output-dir detection to avoid false extraction failure when `MINERU_CMD` hardcodes `-o tmp/mineru_output`
5. Done (phase-1): merged-cell-aware HTML table normalization for MinerU path.
   - parser now expands `rowspan/colspan` into a logical grid before row/column extraction.
   - multi-header rows are merged into stable column labels, supporting patterns like `2024年度 本期` / `2023年度 上期`.
   - validation:
     - added synthetic rowspan/colspan test
     - no regression on table-detection and non-integration test suite
6. Done (phase-2): reduced `mineru raw_*` by fixing metric normalization + dictionary merge + row-level statement fallback.
   - root causes identified:
     - metric matching: missing common aliases/metrics + noisy label wrappers (`一、`, `其中：`, `（损失以...号填列）`)
     - merge bug: when dictionary existed, base patterns for same `metric_code` were ignored
     - parser/type coupling: some rows in mixed tables were forced into table-level statement type
   - fixes shipped:
     - stronger `normalize_label` cleanup (prefix/annotation removal)
     - added high-frequency metrics/aliases (e.g. `investment_income`, `lease_liability`, `minority_interest`, `cash_paid_dividends_interest`)
     - merged base+dictionary patterns for same `metric_code` (union instead of replace)
     - row-level statement fallback when table-level type mismatches row metric
     - skip low-quality unmatched labels to avoid noisy `raw_*`
   - measured impact on sample report (`report_id=2`):
     - `mineru flow raw`: `133/315 (42.22%) -> 3/321 (0.93%)`
     - `mineru stock raw`: `39/228 (17.11%) -> 0/229 (0.00%)`
     - `pypdf flow raw`: `14/161 (8.70%) -> 6/189 (3.17%)`
     - `pypdf stock raw`: `8/136 (5.88%) -> 1/162 (0.62%)`
   - current residual raw labels are only long-tail items (e.g. `收到的税费返还`, `3.其他权益工具投资公允价值变动`).

### Done
9. append-candidates can now write/update `report_pages` via `--write-pages`; MinerU uses `_content_list.json` to split pages.
10. parser now uses post-table context for unit/currency detection and infers `statement_type` from row labels when missing.
11. extended stop-list for overly generic tokens in taxonomy merge and dictionary cleanup.
12. consistency checks now group by `consolidation_scope` + `currency` + `unit` + date.
13. added `deferred_tax_assets` metric to avoid “递延所得税资产 -> 资产总计” mis-match.
14. multi-engine agreement KPI now reported in consensus summary.
15. ingestion now gates table write by quality:
   - default `INGEST_CORE_STATEMENTS_ONLY=true` (only core statement tables)
   - require minimum matched metric rows per table (`MIN_METRIC_ROWS_PER_TABLE`, default `2`)
16. pattern matching no longer uses broad substring fallback; switched to exact/controlled prefix-suffix rules to avoid collapsing detailed rows into one metric.
17. done: locked suspicious-label mapping with regression tests and ambiguity audit.
   - added regression test: `tests/test_metric_suspicious_label_mappings.py` (13 high-risk labels)
   - added latest-version ambiguity audit artifact: `tmp/metric_ambiguity_latest_report2.csv`
   - audit result on latest `pypdf/mineru` versions for `report_id=2`: `ambiguous_rows = 0`
18. Done: added repeatable engine-gap analysis script and refreshed latest coverage report.
   - script: `scripts/analyze_engine_gap.py`
   - artifacts (report_id=2):
     - `tmp/engine_gap_summary_report2.txt`
     - `tmp/engine_gap_flow_mineru_only_report2.csv`
     - `tmp/engine_gap_flow_pypdf_only_report2.csv`
     - `tmp/engine_gap_stock_mineru_only_report2.csv`
     - `tmp/engine_gap_stock_pypdf_only_report2.csv`
   - latest finding (`pypdf=109`, `mineru=110`):
     - flow keys: `pypdf=177`, `mineru=283`, overlap=`177`, `pypdf_only=0`, `mineru_only=106`
     - stock keys: `pypdf=161`, `mineru=201`, overlap=`158`, `pypdf_only=3`, `mineru_only=43`
     - overlap value conflicts: flow=`7`, stock=`9`
19. Done: improved discrepancy review UX navigation stability.
   - `Next Conflict` now cycles through unresolved groups with wrap-around.
   - active discrepancy card now auto-scrolls into view.
   - fiscal year input default is empty (no implicit year filter on first load).
   - added `Clear Year` button to explicitly reset year filter during review.
   - verified in browser on `http://127.0.0.1:3001` with report `2`: active index and PDF page stay in sync during repeated next-click operations.
