from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from app.ingest.financial_report import extract_financial_report
from app.ingest.metric_defs import match_metric


STATEMENT_TYPE_MAP = {
    "balance_sheet": "balance",
    "income_statement": "income",
    "cash_flow": "cashflow",
}


def load_manifest(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("reports", [])


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = max(0, min(len(values) - 1, int(round((p / 100) * (len(values) - 1)))))
    return values[k]


def evaluate(manifest_path: Path) -> dict:
    reports = load_manifest(manifest_path)
    total = len(reports)
    success = 0
    parse_times: list[float] = []

    tables_total = 0
    tables_valid = 0
    tables_statement = 0
    tables_with_units = 0
    tables_with_currency = 0

    rows_total = 0
    cells_total = 0
    numeric_cells = 0

    meta_currency = 0
    meta_units = 0
    meta_period_end = 0
    metric_rows_total = 0
    metric_rows_matched = 0

    errors: list[str] = []

    for item in reports:
        path = Path(item["path"])
        if not path.exists():
            errors.append(f"missing:{path}")
            continue

        start = time.time()
        try:
            pages, meta, tables, _ = extract_financial_report(str(path))
        except Exception as exc:
            errors.append(f"error:{path}:{exc}")
            continue
        finally:
            parse_times.append(time.time() - start)

        success += 1
        if meta.currency:
            meta_currency += 1
        if meta.units:
            meta_units += 1
        if meta.period_end:
            meta_period_end += 1

        tables_total += len(tables)
        for table in tables:
            mapped_statement = STATEMENT_TYPE_MAP.get(table.statement_type or "")
            if table.statement_type:
                tables_statement += 1
            if table.units:
                tables_with_units += 1
            if table.currency:
                tables_with_currency += 1

            if len(table.rows) >= 2 and len(table.columns) >= 2:
                tables_valid += 1

            rows_total += len(table.rows)
            for row in table.rows:
                cells_total += len(row.cells)
                numeric_cells += sum(1 for cell in row.cells if cell.value is not None)
                metric_rows_total += 1
                if mapped_statement:
                    if match_metric(row.label, mapped_statement):
                        metric_rows_matched += 1
                else:
                    for st in ("income", "balance", "cashflow"):
                        if match_metric(row.label, st):
                            metric_rows_matched += 1
                            break

    doc_success_rate = success / total if total else 0.0
    table_success_rate = tables_valid / tables_total if tables_total else 0.0
    statement_table_rate = tables_statement / tables_total if tables_total else 0.0
    unit_rate = tables_with_units / tables_total if tables_total else 0.0
    currency_rate = tables_with_currency / tables_total if tables_total else 0.0
    numeric_cell_rate = numeric_cells / cells_total if cells_total else 0.0
    metric_match_rate = metric_rows_matched / metric_rows_total if metric_rows_total else 0.0

    metrics = {
        "doc_success_rate": doc_success_rate,
        "tables_total": tables_total,
        "table_success_rate": table_success_rate,
        "statement_table_rate": statement_table_rate,
        "table_unit_rate": unit_rate,
        "table_currency_rate": currency_rate,
        "rows_total": rows_total,
        "cells_total": cells_total,
        "numeric_cell_rate": numeric_cell_rate,
        "core_metric_match_rate": metric_match_rate,
        "meta_currency_rate": meta_currency / success if success else 0.0,
        "meta_units_rate": meta_units / success if success else 0.0,
        "meta_period_end_rate": meta_period_end / success if success else 0.0,
        "p95_parse_time": percentile(parse_times, 95),
        "errors": errors,
    }
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="tests/fixtures/manifest.json")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    metrics = evaluate(Path(args.manifest))
    output = json.dumps(metrics, ensure_ascii=False, indent=2)
    print(output)
    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")


if __name__ == "__main__":
    main()
