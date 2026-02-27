from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg
from dotenv import load_dotenv


@dataclass
class GTRow:
    report_id: int
    statement_type: str
    value_nature: str
    metric_code: str
    expected_value: Decimal
    period_start_date: date | None = None
    period_end_date: date | None = None
    as_of_date: date | None = None
    consolidation_scope: str | None = None
    unit: str | None = None
    currency: str | None = None
    abs_tol: Decimal = Decimal("0")
    rel_tol: Decimal = Decimal("0")
    note: str | None = None


def _to_date(raw: str | None) -> date | None:
    text = (raw or "").strip()
    if not text:
        return None
    return date.fromisoformat(text)


def _to_decimal(raw: str | None, default: str = "0") -> Decimal:
    text = (raw or "").strip()
    if not text:
        text = default
    return Decimal(text)


def _to_opt_text(raw: str | None) -> str | None:
    text = (raw or "").strip()
    return text or None


def _row_from_dict(item: dict[str, str], default_report_id: int | None) -> GTRow:
    report_text = (item.get("report_id") or "").strip()
    if report_text:
        report_id = int(report_text)
    elif default_report_id is not None:
        report_id = default_report_id
    else:
        raise ValueError("report_id is required (or pass --report-id)")

    value_nature = (item.get("value_nature") or "").strip().lower()
    if value_nature not in {"flow", "stock"}:
        if (item.get("as_of_date") or "").strip():
            value_nature = "stock"
        else:
            value_nature = "flow"

    metric_code = (item.get("metric_code") or "").strip()
    if not metric_code:
        raise ValueError("metric_code is required")

    expected_value = _to_decimal(item.get("expected_value"))

    return GTRow(
        report_id=report_id,
        statement_type=(item.get("statement_type") or "unknown").strip() or "unknown",
        value_nature=value_nature,
        metric_code=metric_code,
        expected_value=expected_value,
        period_start_date=_to_date(item.get("period_start_date")),
        period_end_date=_to_date(item.get("period_end_date")),
        as_of_date=_to_date(item.get("as_of_date")),
        consolidation_scope=_to_opt_text(item.get("consolidation_scope")),
        unit=_to_opt_text(item.get("unit")),
        currency=_to_opt_text(item.get("currency")),
        abs_tol=_to_decimal(item.get("abs_tol"), default="0"),
        rel_tol=_to_decimal(item.get("rel_tol"), default="0"),
        note=_to_opt_text(item.get("note")),
    )


def load_gt_rows(path: Path, default_report_id: int | None = None) -> list[GTRow]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        items = payload if isinstance(payload, list) else payload.get("rows", [])
        if not isinstance(items, list):
            raise ValueError("json gt must be a list or {'rows': [...]} ")
        rows = [_row_from_dict(dict(item), default_report_id) for item in items]
        return rows

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = [_row_from_dict(dict(item), default_report_id) for item in reader]
    return rows


def _within_tolerance(actual: Decimal, expected: Decimal, abs_tol: Decimal, rel_tol: Decimal) -> bool:
    diff = abs(actual - expected)
    rel_bound = abs(expected) * rel_tol
    bound = abs_tol if abs_tol > rel_bound else rel_bound
    return diff <= bound


def _fetch_flow_candidates(cur, row: GTRow) -> list[tuple[str, Decimal]]:
    clauses = ["c.report_id = %s", "m.metric_code = %s"]
    params: list[object] = [row.report_id, row.metric_code]
    if row.period_end_date is not None:
        clauses.append("c.period_end_date = %s")
        params.append(row.period_end_date)
    if row.period_start_date is not None:
        clauses.append("c.period_start_date = %s")
        params.append(row.period_start_date)
    if row.consolidation_scope is not None:
        clauses.append("c.consolidation_scope = %s")
        params.append(row.consolidation_scope)
    if row.unit is not None:
        clauses.append("c.unit = %s")
        params.append(row.unit)
    if row.currency is not None:
        clauses.append("c.currency = %s")
        params.append(row.currency)

    sql = f"""
    WITH latest AS (
      SELECT parse_method, MAX(version_id) AS version_id
      FROM report_versions
      WHERE report_id = %s AND status = 'ready' AND parse_method IN ('pypdf', 'mineru')
      GROUP BY parse_method
    )
    SELECT rv.parse_method, c.value
    FROM financial_flow_candidate c
    JOIN metric m ON m.metric_id = c.metric_id
    JOIN latest l ON l.version_id = c.version_id
    JOIN report_versions rv ON rv.version_id = c.version_id
    WHERE {' AND '.join(clauses)}
    """
    cur.execute(sql, [row.report_id, *params])
    return [(str(r[0]), Decimal(str(r[1]))) for r in cur.fetchall() if r[1] is not None]


def _fetch_stock_candidates(cur, row: GTRow) -> list[tuple[str, Decimal]]:
    clauses = ["c.report_id = %s", "m.metric_code = %s"]
    params: list[object] = [row.report_id, row.metric_code]
    if row.as_of_date is not None:
        clauses.append("c.as_of_date = %s")
        params.append(row.as_of_date)
    if row.consolidation_scope is not None:
        clauses.append("c.consolidation_scope = %s")
        params.append(row.consolidation_scope)
    if row.unit is not None:
        clauses.append("c.unit = %s")
        params.append(row.unit)
    if row.currency is not None:
        clauses.append("c.currency = %s")
        params.append(row.currency)

    sql = f"""
    WITH latest AS (
      SELECT parse_method, MAX(version_id) AS version_id
      FROM report_versions
      WHERE report_id = %s AND status = 'ready' AND parse_method IN ('pypdf', 'mineru')
      GROUP BY parse_method
    )
    SELECT rv.parse_method, c.value
    FROM financial_stock_candidate c
    JOIN metric m ON m.metric_id = c.metric_id
    JOIN latest l ON l.version_id = c.version_id
    JOIN report_versions rv ON rv.version_id = c.version_id
    WHERE {' AND '.join(clauses)}
    """
    cur.execute(sql, [row.report_id, *params])
    return [(str(r[0]), Decimal(str(r[1]))) for r in cur.fetchall() if r[1] is not None]


def _load_consistency_score(cur, report_id: int) -> float | None:
    cur.execute(
        """
        SELECT summary_json
        FROM report_versions
        WHERE report_id = %s AND status = 'ready'
        ORDER BY version_id DESC
        """,
        (report_id,),
    )
    for (summary_json,) in cur.fetchall():
        if not summary_json:
            continue
        checks = summary_json.get("consistency_checks")
        if not isinstance(checks, list) or not checks:
            continue
        total = len(checks)
        passed = sum(1 for item in checks if isinstance(item, dict) and item.get("status") == "pass")
        return passed / total if total else None
    return None


def evaluate(rows: list[GTRow], dsn: str) -> tuple[dict, list[dict[str, object]]]:
    if not rows:
        return {
            "gt_rows": 0,
            "reports": 0,
            "field_accuracy": 0.0,
            "numeric_accuracy": 0.0,
            "key_hit_rate": 0.0,
            "table_success_rate": 0.0,
            "consistency_score_by_report": {},
        }, []

    per_statement_total: Counter[str] = Counter()
    per_statement_match: Counter[str] = Counter()
    per_engine_hit: Counter[str] = Counter()
    per_engine_total: Counter[str] = Counter()
    detail_rows: list[dict[str, object]] = []

    gt_total = len(rows)
    key_hit = 0
    value_match = 0
    missing = 0
    wrong = 0

    report_ids = sorted({row.report_id for row in rows})

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            consistency = {str(rid): _load_consistency_score(cur, rid) for rid in report_ids}

            for row in rows:
                per_statement_total[row.statement_type] += 1
                if row.value_nature == "stock":
                    candidates = _fetch_stock_candidates(cur, row)
                else:
                    candidates = _fetch_flow_candidates(cur, row)

                engines_seen = {engine for engine, _ in candidates}
                for engine in engines_seen:
                    per_engine_total[engine] += 1

                engine_values: dict[str, list[Decimal]] = defaultdict(list)
                for engine, value in candidates:
                    engine_values[engine].append(value)

                matched_engines: set[str] = set()
                matched_values: list[str] = []
                for engine, values in engine_values.items():
                    if any(_within_tolerance(v, row.expected_value, row.abs_tol, row.rel_tol) for v in values):
                        matched_engines.add(engine)
                        matched_values.extend(str(v) for v in values)

                has_candidates = len(candidates) > 0
                is_matched = len(matched_engines) > 0

                if has_candidates:
                    key_hit += 1
                if is_matched:
                    value_match += 1
                    per_statement_match[row.statement_type] += 1
                    for engine in matched_engines:
                        per_engine_hit[engine] += 1
                elif not has_candidates:
                    missing += 1
                else:
                    wrong += 1

                detail_rows.append(
                    {
                        "report_id": row.report_id,
                        "statement_type": row.statement_type,
                        "value_nature": row.value_nature,
                        "metric_code": row.metric_code,
                        "period_start_date": str(row.period_start_date or ""),
                        "period_end_date": str(row.period_end_date or ""),
                        "as_of_date": str(row.as_of_date or ""),
                        "consolidation_scope": row.consolidation_scope or "",
                        "unit": row.unit or "",
                        "currency": row.currency or "",
                        "expected_value": str(row.expected_value),
                        "status": "matched" if is_matched else ("missing" if not has_candidates else "wrong"),
                        "matched_engines": "|".join(sorted(matched_engines)),
                        "candidate_values": json.dumps({k: [str(v) for v in vals] for k, vals in engine_values.items()}, ensure_ascii=False),
                        "note": row.note or "",
                    }
                )

    statement_completeness = {
        st: (per_statement_match[st] / total if total else 0.0)
        for st, total in per_statement_total.items()
    }
    complete_statements = sum(1 for st, total in per_statement_total.items() if total and per_statement_match[st] == total)
    table_success_rate = complete_statements / len(per_statement_total) if per_statement_total else 0.0

    engine_hit_rate = {
        engine: (per_engine_hit[engine] / total if total else 0.0)
        for engine, total in per_engine_total.items()
    }

    summary = {
        "gt_rows": gt_total,
        "reports": len(report_ids),
        "field_accuracy": value_match / gt_total if gt_total else 0.0,
        "numeric_accuracy": value_match / gt_total if gt_total else 0.0,
        "key_hit_rate": key_hit / gt_total if gt_total else 0.0,
        "missing_rows": missing,
        "wrong_value_rows": wrong,
        "table_completeness": statement_completeness,
        "table_success_rate": table_success_rate,
        "engine_hit_rate": engine_hit_rate,
        "consistency_score_by_report": consistency,
    }
    return summary, detail_rows


def write_details_csv(path: Path, rows: list[dict[str, object]]) -> None:
    headers = [
        "report_id",
        "statement_type",
        "value_nature",
        "metric_code",
        "period_start_date",
        "period_end_date",
        "as_of_date",
        "consolidation_scope",
        "unit",
        "currency",
        "expected_value",
        "status",
        "matched_engines",
        "candidate_values",
        "note",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate pdf2db extraction against ground truth rows.")
    parser.add_argument("--gt", required=True, help="Path to GT file (csv/json)")
    parser.add_argument("--report-id", type=int, default=None, help="Default report_id for GT rows without report_id")
    parser.add_argument("--output", default="", help="Optional summary json output path")
    parser.add_argument("--details-output", default="", help="Optional detail csv output path")
    args = parser.parse_args()

    load_dotenv(Path(".env"))
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is not set")

    gt_path = Path(args.gt)
    rows = load_gt_rows(gt_path, default_report_id=args.report_id)
    summary, details = evaluate(rows, dsn)

    payload = json.dumps(summary, ensure_ascii=False, indent=2)
    print(payload)

    if args.output:
        Path(args.output).write_text(payload, encoding="utf-8")
    if args.details_output:
        write_details_csv(Path(args.details_output), details)


if __name__ == "__main__":
    main()
