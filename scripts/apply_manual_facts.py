from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from app.storage.db import get_conn


def _parse_decimal(value: str | None) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _load_rows(path: Path) -> list[dict]:
    if path.suffix.lower() in {".json"}:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data.get("rows", [])
        return data
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def _fetch_metric_ids(cur, codes: list[str]) -> dict[str, int]:
    if not codes:
        return {}
    cur.execute(
        "SELECT metric_code, metric_id FROM metric WHERE metric_code = ANY(%s)",
        (codes,),
    )
    return {row[0]: int(row[1]) for row in cur.fetchall()}


def _update_flow_fact(cur, row: dict, metric_id: int, report_id: int, reviewed_by: str | None) -> None:
    period_end = row.get("period_end_date") or row.get("as_of_date")
    period_start = row.get("period_start_date")
    consolidation_scope = row.get("consolidation_scope")
    value = _parse_decimal(row.get("value"))
    unit = row.get("unit")
    currency = row.get("currency")
    audit_flag = row.get("audit_flag")
    notes = row.get("review_notes")

    cur.execute(
        """
        SELECT fact_id FROM financial_flow_fact
        WHERE report_id = %s
          AND metric_id = %s
          AND period_end_date IS NOT DISTINCT FROM %s
          AND period_start_date IS NOT DISTINCT FROM %s
          AND consolidation_scope IS NOT DISTINCT FROM %s
        """,
        (report_id, metric_id, period_end, period_start, consolidation_scope),
    )
    existing = cur.fetchone()
    if existing:
        cur.execute(
            """
            UPDATE financial_flow_fact
            SET value = %s,
                unit = COALESCE(%s, unit),
                currency = COALESCE(%s, currency),
                audit_flag = COALESCE(%s, audit_flag),
                selected_candidate_id = NULL,
                resolution_status = 'verified',
                resolution_method = 'manual',
                reviewed_by = %s,
                reviewed_at = %s,
                review_notes = %s
            WHERE fact_id = %s
            """,
            (value, unit, currency, audit_flag, reviewed_by, datetime.utcnow(), notes, int(existing[0])),
        )
        return

    cur.execute(
        """
        INSERT INTO financial_flow_fact (
            report_id, metric_id, period_start_date, period_end_date, value, unit, currency,
            consolidation_scope, audit_flag, source_trace_id, quality_score, created_at,
            selected_candidate_id, resolution_status, resolution_method,
            reviewed_by, reviewed_at, review_notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            report_id,
            metric_id,
            period_start,
            period_end,
            value,
            unit,
            currency,
            consolidation_scope,
            audit_flag,
            None,
            None,
            datetime.utcnow(),
            None,
            "verified",
            "manual",
            reviewed_by,
            datetime.utcnow(),
            notes,
        ),
    )


def _update_stock_fact(cur, row: dict, metric_id: int, report_id: int, reviewed_by: str | None) -> None:
    as_of_date = row.get("as_of_date") or row.get("period_end_date")
    consolidation_scope = row.get("consolidation_scope")
    value = _parse_decimal(row.get("value"))
    unit = row.get("unit")
    currency = row.get("currency")
    notes = row.get("review_notes")

    cur.execute(
        """
        SELECT fact_id FROM financial_stock_fact
        WHERE report_id = %s
          AND metric_id = %s
          AND as_of_date IS NOT DISTINCT FROM %s
          AND consolidation_scope IS NOT DISTINCT FROM %s
        """,
        (report_id, metric_id, as_of_date, consolidation_scope),
    )
    existing = cur.fetchone()
    if existing:
        cur.execute(
            """
            UPDATE financial_stock_fact
            SET value = %s,
                unit = COALESCE(%s, unit),
                currency = COALESCE(%s, currency),
                selected_candidate_id = NULL,
                resolution_status = 'verified',
                resolution_method = 'manual',
                reviewed_by = %s,
                reviewed_at = %s,
                review_notes = %s
            WHERE fact_id = %s
            """,
            (value, unit, currency, reviewed_by, datetime.utcnow(), notes, int(existing[0])),
        )
        return

    cur.execute(
        """
        INSERT INTO financial_stock_fact (
            report_id, metric_id, as_of_date, value, unit, currency,
            consolidation_scope, source_trace_id, quality_score, created_at,
            selected_candidate_id, resolution_status, resolution_method,
            reviewed_by, reviewed_at, review_notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            report_id,
            metric_id,
            as_of_date,
            value,
            unit,
            currency,
            consolidation_scope,
            None,
            None,
            datetime.utcnow(),
            None,
            "verified",
            "manual",
            reviewed_by,
            datetime.utcnow(),
            notes,
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply manual fact overrides.")
    parser.add_argument("--input", required=True, help="CSV or JSON with manual facts.")
    parser.add_argument("--report-id", type=int, help="Default report_id if not in rows.")
    parser.add_argument("--reviewed-by", help="Reviewer name/email.")
    args = parser.parse_args()

    rows = _load_rows(Path(args.input))
    if not rows:
        raise SystemExit("No rows found.")

    metric_codes = [row.get("metric_code") for row in rows if row.get("metric_code")]
    with get_conn() as conn:
        with conn.cursor() as cur:
            metric_ids = _fetch_metric_ids(cur, metric_codes)
            for row in rows:
                metric_code = row.get("metric_code")
                if not metric_code or metric_code not in metric_ids:
                    raise SystemExit(f"Unknown metric_code: {metric_code}")
                report_id = row.get("report_id") or args.report_id
                if not report_id:
                    raise SystemExit("report_id missing.")
                fact_type = (row.get("fact_type") or "").lower()
                reviewed_by = row.get("reviewed_by") or args.reviewed_by
                if fact_type == "stock":
                    _update_stock_fact(cur, row, metric_ids[metric_code], int(report_id), reviewed_by)
                elif fact_type == "flow":
                    _update_flow_fact(cur, row, metric_ids[metric_code], int(report_id), reviewed_by)
                else:
                    raise SystemExit(f"Invalid fact_type for metric {metric_code}: {fact_type}")
        conn.commit()


if __name__ == "__main__":
    main()
