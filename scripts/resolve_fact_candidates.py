from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from app.storage.db import get_conn


@dataclass(frozen=True)
class FlowCandidate:
    candidate_id: int
    version_id: int | None
    metric_id: int
    period_start_date: object
    period_end_date: object
    value: Decimal | None
    unit: str | None
    currency: str | None
    consolidation_scope: str | None
    audit_flag: str | None
    source_trace_id: int | None
    quality_score: Decimal | None


@dataclass(frozen=True)
class StockCandidate:
    candidate_id: int
    version_id: int | None
    metric_id: int
    as_of_date: object
    value: Decimal | None
    unit: str | None
    currency: str | None
    consolidation_scope: str | None
    source_trace_id: int | None
    quality_score: Decimal | None


def _value_key(value: Decimal | None, tolerance: Decimal) -> str:
    if value is None:
        return "null"
    if tolerance <= 0:
        return str(value)
    return str(value.quantize(tolerance, rounding=ROUND_HALF_UP))


def _avg_quality(values: list[Decimal | None]) -> float:
    numeric = [float(v) for v in values if v is not None]
    if not numeric:
        return 0.0
    return sum(numeric) / len(numeric)


def _choose_candidate(groups: dict[str, list], min_agree: int) -> tuple[object, int, int, str, str]:
    ranked = []
    for value_key, candidates in groups.items():
        version_ids = {c.version_id or c.candidate_id for c in candidates}
        quality = _avg_quality([c.quality_score for c in candidates])
        ranked.append((len(version_ids), len(candidates), quality, value_key, candidates))
    ranked.sort(reverse=True)
    best = ranked[0]
    agree_count = best[0]
    chosen_candidates = best[4]
    chosen = sorted(
        chosen_candidates,
        key=lambda c: (c.quality_score is not None, c.quality_score or Decimal("0"), c.candidate_id),
        reverse=True,
    )[0]
    if agree_count >= min_agree:
        status = "auto"
        method = "consensus" if agree_count > 1 else "single_engine"
    else:
        status = "needs_review"
        method = "insufficient_agreement"
    return chosen, agree_count, len(chosen_candidates), status, method


def _insert_flow_fact(cur, report_id: int, candidate: FlowCandidate, status: str, method: str, now: datetime) -> None:
    cur.execute(
        """
        INSERT INTO financial_flow_fact (
            report_id, metric_id, period_start_date, period_end_date, value, unit, currency,
            consolidation_scope, audit_flag, source_trace_id, quality_score, created_at,
            selected_candidate_id, resolution_status, resolution_method
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            report_id,
            candidate.metric_id,
            candidate.period_start_date,
            candidate.period_end_date,
            candidate.value,
            candidate.unit,
            candidate.currency,
            candidate.consolidation_scope,
            candidate.audit_flag,
            candidate.source_trace_id,
            candidate.quality_score,
            now,
            candidate.candidate_id,
            status,
            method,
        ),
    )


def _insert_stock_fact(cur, report_id: int, candidate: StockCandidate, status: str, method: str, now: datetime) -> None:
    cur.execute(
        """
        INSERT INTO financial_stock_fact (
            report_id, metric_id, as_of_date, value, unit, currency,
            consolidation_scope, source_trace_id, quality_score, created_at,
            selected_candidate_id, resolution_status, resolution_method
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            report_id,
            candidate.metric_id,
            candidate.as_of_date,
            candidate.value,
            candidate.unit,
            candidate.currency,
            candidate.consolidation_scope,
            candidate.source_trace_id,
            candidate.quality_score,
            now,
            candidate.candidate_id,
            status,
            method,
        ),
    )


def _load_flow_candidates(cur, report_id: int) -> list[FlowCandidate]:
    cur.execute(
        """
        SELECT candidate_id, version_id, metric_id, period_start_date, period_end_date, value, unit, currency,
               consolidation_scope, audit_flag, source_trace_id, quality_score
        FROM financial_flow_candidate
        WHERE report_id = %s
        """,
        (report_id,),
    )
    return [
        FlowCandidate(
            candidate_id=int(row[0]),
            version_id=row[1],
            metric_id=int(row[2]),
            period_start_date=row[3],
            period_end_date=row[4],
            value=row[5],
            unit=row[6],
            currency=row[7],
            consolidation_scope=row[8],
            audit_flag=row[9],
            source_trace_id=row[10],
            quality_score=row[11],
        )
        for row in cur.fetchall()
    ]


def _load_stock_candidates(cur, report_id: int) -> list[StockCandidate]:
    cur.execute(
        """
        SELECT candidate_id, version_id, metric_id, as_of_date, value, unit, currency,
               consolidation_scope, source_trace_id, quality_score
        FROM financial_stock_candidate
        WHERE report_id = %s
        """,
        (report_id,),
    )
    return [
        StockCandidate(
            candidate_id=int(row[0]),
            version_id=row[1],
            metric_id=int(row[2]),
            as_of_date=row[3],
            value=row[4],
            unit=row[5],
            currency=row[6],
            consolidation_scope=row[7],
            source_trace_id=row[8],
            quality_score=row[9],
        )
        for row in cur.fetchall()
    ]


def _fetch_metric_ids(cur, codes: list[str]) -> dict[str, int]:
    if not codes:
        return {}
    cur.execute(
        "SELECT metric_code, metric_id FROM metric WHERE metric_code = ANY(%s)",
        (codes,),
    )
    return {row[0]: int(row[1]) for row in cur.fetchall()}


def _within_tolerance(lhs: Decimal, rhs: Decimal, abs_tol: Decimal, rel_tol: Decimal) -> bool:
    diff = abs(lhs - rhs)
    if diff <= abs_tol:
        return True
    scale = max(abs(lhs), abs(rhs))
    return diff <= (scale * rel_tol)


def _check_balance_consistency(cur, report_id: int, abs_tol: Decimal, rel_tol: Decimal) -> list[dict]:
    metric_ids = _fetch_metric_ids(
        cur,
        [
            "total_assets",
            "total_liabilities",
            "total_equity",
            "total_equity_parent",
            "total_liabilities_equity",
        ],
    )
    if not metric_ids:
        return []
    cur.execute(
        """
        SELECT metric_id, as_of_date, value
        FROM financial_stock_fact
        WHERE report_id = %s AND metric_id = ANY(%s)
        """,
        (report_id, list(metric_ids.values())),
    )
    by_date: dict[object, dict[int, Decimal]] = defaultdict(dict)
    for metric_id, as_of_date, value in cur.fetchall():
        if value is None:
            continue
        by_date[as_of_date][int(metric_id)] = value

    checks = []
    assets_id = metric_ids.get("total_assets")
    liabilities_id = metric_ids.get("total_liabilities")
    equity_id = metric_ids.get("total_equity") or metric_ids.get("total_equity_parent")
    liab_equity_id = metric_ids.get("total_liabilities_equity")
    for as_of_date, values in by_date.items():
        assets = values.get(assets_id) if assets_id else None
        liabilities = values.get(liabilities_id) if liabilities_id else None
        equity = values.get(equity_id) if equity_id else None
        liab_equity = values.get(liab_equity_id) if liab_equity_id else None
        if assets is not None and liabilities is not None and equity is not None:
            diff = assets - (liabilities + equity)
            checks.append(
                {
                    "name": "assets_eq_liab_plus_equity",
                    "as_of_date": str(as_of_date),
                    "lhs": float(assets),
                    "rhs": float(liabilities + equity),
                    "diff": float(diff),
                    "status": "pass" if _within_tolerance(assets, liabilities + equity, abs_tol, rel_tol) else "fail",
                }
            )
        if assets is not None and liab_equity is not None:
            diff = assets - liab_equity
            checks.append(
                {
                    "name": "assets_eq_liab_equity_total",
                    "as_of_date": str(as_of_date),
                    "lhs": float(assets),
                    "rhs": float(liab_equity),
                    "diff": float(diff),
                    "status": "pass" if _within_tolerance(assets, liab_equity, abs_tol, rel_tol) else "fail",
                }
            )
    return checks


def _check_cashflow_consistency(cur, report_id: int, abs_tol: Decimal, rel_tol: Decimal) -> list[dict]:
    metric_ids = _fetch_metric_ids(
        cur,
        [
            "net_cash_flow_operating",
            "net_cash_flow_investing",
            "net_cash_flow_financing",
            "net_increase_cash",
            "cash_begin",
            "cash_end",
        ],
    )
    if not metric_ids:
        return []

    cur.execute(
        """
        SELECT metric_id, period_end_date, value
        FROM financial_flow_fact
        WHERE report_id = %s AND metric_id = ANY(%s)
        """,
        (report_id, list(metric_ids.values())),
    )
    by_date: dict[object, dict[int, Decimal]] = defaultdict(dict)
    for metric_id, period_end_date, value in cur.fetchall():
        if value is None:
            continue
        by_date[period_end_date][int(metric_id)] = value

    checks = []
    op_id = metric_ids.get("net_cash_flow_operating")
    inv_id = metric_ids.get("net_cash_flow_investing")
    fin_id = metric_ids.get("net_cash_flow_financing")
    inc_id = metric_ids.get("net_increase_cash")
    begin_id = metric_ids.get("cash_begin")
    end_id = metric_ids.get("cash_end")

    for period_end, values in by_date.items():
        operating = values.get(op_id) if op_id else None
        investing = values.get(inv_id) if inv_id else None
        financing = values.get(fin_id) if fin_id else None
        net_increase = values.get(inc_id) if inc_id else None
        cash_begin = values.get(begin_id) if begin_id else None
        cash_end = values.get(end_id) if end_id else None

        if operating is not None and investing is not None and financing is not None and net_increase is not None:
            rhs = operating + investing + financing
            diff = net_increase - rhs
            checks.append(
                {
                    "name": "net_increase_eq_sum_cashflows",
                    "period_end_date": str(period_end),
                    "lhs": float(net_increase),
                    "rhs": float(rhs),
                    "diff": float(diff),
                    "status": "pass" if _within_tolerance(net_increase, rhs, abs_tol, rel_tol) else "fail",
                }
            )

        if cash_begin is not None and net_increase is not None and cash_end is not None:
            rhs = cash_begin + net_increase
            diff = cash_end - rhs
            checks.append(
                {
                    "name": "cash_end_eq_cash_begin_plus_increase",
                    "period_end_date": str(period_end),
                    "lhs": float(cash_end),
                    "rhs": float(rhs),
                    "diff": float(diff),
                    "status": "pass" if _within_tolerance(cash_end, rhs, abs_tol, rel_tol) else "fail",
                }
            )

    return checks


def resolve_report(
    report_id: int,
    min_agree: int = 1,
    tolerance: Decimal = Decimal("0.01"),
    consistency_abs_tol: Decimal = Decimal("1"),
    consistency_rel_tol: Decimal = Decimal("0.000001"),
    replace_existing: bool = True,
    dry_run: bool = False,
) -> dict:
    now = datetime.utcnow()
    summary: dict = {
        "report_id": report_id,
        "flow_candidates": 0,
        "stock_candidates": 0,
        "flow_facts": 0,
        "stock_facts": 0,
        "min_agree": min_agree,
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO report_versions (
                    report_id, parse_method, parser_version, started_at, status
                )
                VALUES (%s, %s, %s, %s, %s)
                RETURNING version_id
                """,
                (report_id, "consensus", "v1", now, "running"),
            )
            version_id = int(cur.fetchone()[0])

            flow_candidates = _load_flow_candidates(cur, report_id)
            stock_candidates = _load_stock_candidates(cur, report_id)
            summary["flow_candidates"] = len(flow_candidates)
            summary["stock_candidates"] = len(stock_candidates)

            if replace_existing:
                if not dry_run:
                    cur.execute("DELETE FROM financial_flow_fact WHERE report_id = %s", (report_id,))
                    cur.execute("DELETE FROM financial_stock_fact WHERE report_id = %s", (report_id,))

            if not dry_run:
                grouped_flow: dict[tuple, dict[str, list[FlowCandidate]]] = defaultdict(lambda: defaultdict(list))
                for candidate in flow_candidates:
                    key = (
                        candidate.metric_id,
                        candidate.period_start_date,
                        candidate.period_end_date,
                        candidate.unit,
                        candidate.currency,
                        candidate.consolidation_scope,
                    )
                    grouped_flow[key][_value_key(candidate.value, tolerance)].append(candidate)

                for groups in grouped_flow.values():
                    chosen, _, _, status, method = _choose_candidate(groups, min_agree)
                    _insert_flow_fact(cur, report_id, chosen, status, method, now)
                    summary["flow_facts"] += 1

                grouped_stock: dict[tuple, dict[str, list[StockCandidate]]] = defaultdict(lambda: defaultdict(list))
                for candidate in stock_candidates:
                    key = (
                        candidate.metric_id,
                        candidate.as_of_date,
                        candidate.unit,
                        candidate.currency,
                        candidate.consolidation_scope,
                    )
                    grouped_stock[key][_value_key(candidate.value, tolerance)].append(candidate)

                for groups in grouped_stock.values():
                    chosen, _, _, status, method = _choose_candidate(groups, min_agree)
                    _insert_stock_fact(cur, report_id, chosen, status, method, now)
                    summary["stock_facts"] += 1

                summary["consistency_checks"] = (
                    _check_balance_consistency(cur, report_id, consistency_abs_tol, consistency_rel_tol)
                    + _check_cashflow_consistency(cur, report_id, consistency_abs_tol, consistency_rel_tol)
                )

            finished = datetime.utcnow()
            cur.execute(
                """
                UPDATE report_versions
                SET finished_at = %s, status = %s, summary_json = %s
                WHERE version_id = %s
                """,
                (finished, "ready", json.dumps(summary), version_id),
            )
            conn.commit()

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve candidate facts into canonical facts.")
    parser.add_argument("--report-id", type=int, required=True)
    parser.add_argument("--min-agree", type=int, default=1)
    parser.add_argument("--tolerance", type=str, default="0.01")
    parser.add_argument("--consistency-abs-tol", type=str, default="1")
    parser.add_argument("--consistency-rel-tol", type=str, default="0.000001")
    parser.add_argument("--no-replace", action="store_true", help="Do not delete existing facts before resolution.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    tolerance = Decimal(args.tolerance)
    summary = resolve_report(
        report_id=args.report_id,
        min_agree=args.min_agree,
        tolerance=tolerance,
        consistency_abs_tol=Decimal(args.consistency_abs_tol),
        consistency_rel_tol=Decimal(args.consistency_rel_tol),
        replace_existing=not args.no_replace,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
