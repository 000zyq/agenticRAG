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
     - total groups: `386`
     - multi-engine groups: `158`
     - single-engine groups: `228` = `mineru-only 215` + `pypdf-only 13`
   - flow: `pypdf=99`, `mineru=273`, overlap=`90`
   - stock: `pypdf=72`, `mineru=100`, overlap=`68`
   - primary hypothesis: `pypdf` recall is lower on complex OCR/table pages; secondary cause is key split by metadata (`unit/currency/scope`) and residual `raw_*` metric fragmentation.
   - next step:
     - add coverage diff report by metric/date/scope and track top missing metrics per engine
     - normalize grouping metadata defaults before resolver compare
     - improve `pypdf` table/column extraction or lower weight in consensus for low-confidence groups
   - progress:
     - fixed `unit` pollution from pypdf fallback (`"单位"` no longer copies full header text into `unit`)
     - added HTML header fiscal-year parsing for labels like `2024年度/2023年度`
     - latest run (`pypdf=52`, `mineru=53`):
       - `pypdf` bad-unit rows: `0`
       - `multi_engine_groups_with_multi`: `158`
5. Done (phase-1): merged-cell-aware HTML table normalization for MinerU path.
   - parser now expands `rowspan/colspan` into a logical grid before row/column extraction.
   - multi-header rows are merged into stable column labels, supporting patterns like `2024年度 本期` / `2023年度 上期`.
   - validation:
     - added synthetic rowspan/colspan test
     - no regression on table-detection and non-integration test suite

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
