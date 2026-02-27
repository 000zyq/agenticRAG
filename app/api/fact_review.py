from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.storage.db import get_conn

router = APIRouter(prefix="/fact-review", tags=["fact-review"])


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _flow_group_id(
    metric_id: int,
    period_start_date: object,
    period_end_date: object,
    unit: str | None,
    currency: str | None,
    consolidation_scope: str | None,
    audit_flag: str | None,
) -> str:
    return "|".join(
        [
            "flow",
            str(metric_id),
            str(period_start_date or ""),
            str(period_end_date or ""),
            str(unit or ""),
            str(currency or ""),
            str(consolidation_scope or ""),
            str(audit_flag or ""),
        ]
    )


def _stock_group_id(
    metric_id: int,
    as_of_date: object,
    unit: str | None,
    currency: str | None,
    consolidation_scope: str | None,
) -> str:
    return "|".join(
        [
            "stock",
            str(metric_id),
            str(as_of_date or ""),
            str(unit or ""),
            str(currency or ""),
            str(consolidation_scope or ""),
        ]
    )


def _date_matches_period(d: date | None, period: str) -> bool:
    if d is None:
        return False
    month_day = (d.month, d.day)
    if period == "all":
        return True
    if period == "annual":
        return month_day == (12, 31)
    if period == "q1":
        return month_day == (3, 31)
    if period == "q2":
        return month_day == (6, 30)
    if period == "q3":
        return month_day == (9, 30)
    if period == "q4":
        return month_day == (12, 31)
    return True


def _matches_flow_period(
    period_start_date: date | None,
    period_end_date: date | None,
    fiscal_year: int | None,
    period: str,
) -> bool:
    if period_end_date is None:
        return False
    if fiscal_year is not None and period_end_date.year != fiscal_year:
        return False
    if period == "annual":
        if period_start_date is None:
            return False
        if not (period_start_date.month == 1 and period_start_date.day == 1):
            return False
    return _date_matches_period(period_end_date, period)


def _matches_stock_period(as_of_date: date | None, fiscal_year: int | None, period: str) -> bool:
    if as_of_date is None:
        return False
    if fiscal_year is not None and as_of_date.year != fiscal_year:
        return False
    return _date_matches_period(as_of_date, period)


def _fetch_flow_facts(cur, report_id: int) -> dict[tuple, dict[str, Any]]:
    cur.execute(
        """
        SELECT
            metric_id, period_start_date, period_end_date, unit, currency, consolidation_scope, audit_flag,
            fact_id, value, selected_candidate_id, resolution_status, resolution_method,
            reviewed_by, reviewed_at, review_notes
        FROM financial_flow_fact
        WHERE report_id = %s
        """,
        (report_id,),
    )
    facts: dict[tuple, dict[str, Any]] = {}
    for row in cur.fetchall():
        key = (int(row[0]), row[1], row[2], row[3], row[4], row[5], row[6])
        facts[key] = {
            "fact_id": int(row[7]),
            "value": _decimal_text(row[8]),
            "selected_candidate_id": row[9],
            "resolution_status": row[10],
            "resolution_method": row[11],
            "reviewed_by": row[12],
            "reviewed_at": row[13].isoformat() if row[13] else None,
            "review_notes": row[14],
        }
    return facts


def _fetch_stock_facts(cur, report_id: int) -> dict[tuple, dict[str, Any]]:
    cur.execute(
        """
        SELECT
            metric_id, as_of_date, unit, currency, consolidation_scope,
            fact_id, value, selected_candidate_id, resolution_status, resolution_method,
            reviewed_by, reviewed_at, review_notes
        FROM financial_stock_fact
        WHERE report_id = %s
        """,
        (report_id,),
    )
    facts: dict[tuple, dict[str, Any]] = {}
    for row in cur.fetchall():
        key = (int(row[0]), row[1], row[2], row[3], row[4])
        facts[key] = {
            "fact_id": int(row[5]),
            "value": _decimal_text(row[6]),
            "selected_candidate_id": row[7],
            "resolution_status": row[8],
            "resolution_method": row[9],
            "reviewed_by": row[10],
            "reviewed_at": row[11].isoformat() if row[11] else None,
            "review_notes": row[12],
        }
    return facts


def _load_flow_discrepancies(
    cur,
    report_id: int,
    limit: int,
    fiscal_year: int | None,
    period: str,
) -> list[dict[str, Any]]:
    facts = _fetch_flow_facts(cur, report_id)
    cur.execute(
        """
        WITH latest_versions AS (
            SELECT parse_method, MAX(version_id) AS version_id
            FROM report_versions
            WHERE report_id = %s
            GROUP BY parse_method
        )
        SELECT
            c.candidate_id,
            c.metric_id, m.metric_code, m.metric_name_cn,
            c.period_start_date, c.period_end_date,
            c.value, c.unit, c.currency, c.consolidation_scope, c.audit_flag,
            c.quality_score,
            COALESCE(v.parse_method, CONCAT('version_', c.version_id::text)) AS engine,
            st.source_page, st.raw_label, st.raw_value, st.column_label
        FROM financial_flow_candidate c
        JOIN latest_versions lv ON lv.version_id = c.version_id
        JOIN metric m ON m.metric_id = c.metric_id
        LEFT JOIN report_versions v ON v.version_id = c.version_id
        LEFT JOIN source_trace st ON st.trace_id = c.source_trace_id
        WHERE c.report_id = %s
        ORDER BY c.metric_id, c.period_end_date, c.candidate_id
        """,
        (report_id, report_id),
    )

    grouped: dict[tuple, dict[str, Any]] = {}
    for row in cur.fetchall():
        if not _matches_flow_period(row[4], row[5], fiscal_year, period):
            continue
        key = (int(row[1]), row[4], row[5], row[7], row[8], row[9], row[10])
        group = grouped.get(key)
        if group is None:
            fact = facts.get(key)
            group = {
                "fact_type": "flow",
                "group_id": _flow_group_id(int(row[1]), row[4], row[5], row[7], row[8], row[9], row[10]),
                "key": {
                    "metric_id": int(row[1]),
                    "metric_code": row[2],
                    "metric_name_cn": row[3],
                    "period_start_date": str(row[4]) if row[4] else None,
                    "period_end_date": str(row[5]) if row[5] else None,
                    "unit": row[7],
                    "currency": row[8],
                    "consolidation_scope": row[9],
                    "audit_flag": row[10],
                },
                "engine_values": defaultdict(set),
                "candidates": [],
                "resolution": fact
                or {
                    "fact_id": None,
                    "value": None,
                    "selected_candidate_id": None,
                    "resolution_status": None,
                    "resolution_method": None,
                    "reviewed_by": None,
                    "reviewed_at": None,
                    "review_notes": None,
                },
            }
            grouped[key] = group

        value_text = _decimal_text(row[6])
        group["engine_values"][row[12]].add(value_text)
        group["candidates"].append(
            {
                "candidate_id": int(row[0]),
                "engine": row[12],
                "value": value_text,
                "quality_score": _decimal_text(row[11]),
                "source_page": row[13],
                "raw_label": row[14],
                "raw_value": row[15],
                "column_label": row[16],
            }
        )

    items: list[dict[str, Any]] = []
    for group in grouped.values():
        engines = list(group["engine_values"].keys())
        all_values = set()
        pages = set()
        for values in group["engine_values"].values():
            all_values.update(values)
        for candidate in group["candidates"]:
            page = candidate.get("source_page")
            if isinstance(page, int) and page > 0:
                pages.add(page)
        if len(engines) < 2 or len(all_values) < 2:
            continue
        group["engine_values"] = {engine: sorted(list(values)) for engine, values in group["engine_values"].items()}
        group["pages"] = sorted(pages)
        items.append(group)

    items.sort(
        key=lambda x: (
            x["resolution"].get("resolution_method") == "manual",
            x["key"]["metric_code"] or "",
            x["key"]["period_end_date"] or "",
        )
    )
    return items[:limit]


def _load_stock_discrepancies(
    cur,
    report_id: int,
    limit: int,
    fiscal_year: int | None,
    period: str,
) -> list[dict[str, Any]]:
    facts = _fetch_stock_facts(cur, report_id)
    cur.execute(
        """
        WITH latest_versions AS (
            SELECT parse_method, MAX(version_id) AS version_id
            FROM report_versions
            WHERE report_id = %s
            GROUP BY parse_method
        )
        SELECT
            c.candidate_id,
            c.metric_id, m.metric_code, m.metric_name_cn,
            c.as_of_date,
            c.value, c.unit, c.currency, c.consolidation_scope,
            c.quality_score,
            COALESCE(v.parse_method, CONCAT('version_', c.version_id::text)) AS engine,
            st.source_page, st.raw_label, st.raw_value, st.column_label
        FROM financial_stock_candidate c
        JOIN latest_versions lv ON lv.version_id = c.version_id
        JOIN metric m ON m.metric_id = c.metric_id
        LEFT JOIN report_versions v ON v.version_id = c.version_id
        LEFT JOIN source_trace st ON st.trace_id = c.source_trace_id
        WHERE c.report_id = %s
        ORDER BY c.metric_id, c.as_of_date, c.candidate_id
        """,
        (report_id, report_id),
    )

    grouped: dict[tuple, dict[str, Any]] = {}
    for row in cur.fetchall():
        if not _matches_stock_period(row[4], fiscal_year, period):
            continue
        key = (int(row[1]), row[4], row[6], row[7], row[8])
        group = grouped.get(key)
        if group is None:
            fact = facts.get(key)
            group = {
                "fact_type": "stock",
                "group_id": _stock_group_id(int(row[1]), row[4], row[6], row[7], row[8]),
                "key": {
                    "metric_id": int(row[1]),
                    "metric_code": row[2],
                    "metric_name_cn": row[3],
                    "as_of_date": str(row[4]) if row[4] else None,
                    "unit": row[6],
                    "currency": row[7],
                    "consolidation_scope": row[8],
                },
                "engine_values": defaultdict(set),
                "candidates": [],
                "resolution": fact
                or {
                    "fact_id": None,
                    "value": None,
                    "selected_candidate_id": None,
                    "resolution_status": None,
                    "resolution_method": None,
                    "reviewed_by": None,
                    "reviewed_at": None,
                    "review_notes": None,
                },
            }
            grouped[key] = group

        value_text = _decimal_text(row[5])
        group["engine_values"][row[10]].add(value_text)
        group["candidates"].append(
            {
                "candidate_id": int(row[0]),
                "engine": row[10],
                "value": value_text,
                "quality_score": _decimal_text(row[9]),
                "source_page": row[11],
                "raw_label": row[12],
                "raw_value": row[13],
                "column_label": row[14],
            }
        )

    items: list[dict[str, Any]] = []
    for group in grouped.values():
        engines = list(group["engine_values"].keys())
        all_values = set()
        pages = set()
        for values in group["engine_values"].values():
            all_values.update(values)
        for candidate in group["candidates"]:
            page = candidate.get("source_page")
            if isinstance(page, int) and page > 0:
                pages.add(page)
        if len(engines) < 2 or len(all_values) < 2:
            continue
        group["engine_values"] = {engine: sorted(list(values)) for engine, values in group["engine_values"].items()}
        group["pages"] = sorted(pages)
        items.append(group)

    items.sort(
        key=lambda x: (
            x["resolution"].get("resolution_method") == "manual",
            x["key"]["metric_code"] or "",
            x["key"]["as_of_date"] or "",
        )
    )
    return items[:limit]


@router.get("/discrepancies")
def list_discrepancies(
    report_id: int = Query(..., ge=1),
    fact_type: str = Query("all", pattern="^(all|flow|stock)$"),
    fiscal_year: int | None = Query(None, ge=1900, le=2100),
    period: str = Query("all", pattern="^(all|annual|q1|q2|q3|q4)$"),
    limit: int = Query(200, ge=1, le=2000),
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            items: list[dict[str, Any]] = []
            if fact_type in {"all", "flow"}:
                items.extend(_load_flow_discrepancies(cur, report_id, limit, fiscal_year, period))
            if fact_type in {"all", "stock"}:
                items.extend(_load_stock_discrepancies(cur, report_id, limit, fiscal_year, period))
    if fact_type == "all":
        items = items[:limit]
    return {
        "report_id": report_id,
        "fact_type": fact_type,
        "fiscal_year": fiscal_year,
        "period": period,
        "count": len(items),
        "items": items,
    }


@router.get("/report-file/{report_id}")
def get_report_file(report_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT source_path FROM financial_reports WHERE report_id = %s", (report_id,))
            row = cur.fetchone()
    if not row or not row[0]:
        raise HTTPException(status_code=404, detail="report source_path not found")

    file_path = Path(row[0]).expanduser()
    if not file_path.is_absolute():
        file_path = (Path.cwd() / file_path).resolve()
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail=f"report file not found: {file_path}")
    return FileResponse(
        path=str(file_path),
        media_type="application/pdf",
    )


class ResolveRequest(BaseModel):
    fact_type: str
    candidate_id: int
    reviewed_by: str | None = None
    review_notes: str | None = None
    report_id: int | None = None


@router.post("/resolve")
def resolve_discrepancy(req: ResolveRequest):
    if req.fact_type not in {"flow", "stock"}:
        raise HTTPException(status_code=400, detail="fact_type must be flow or stock")
    now = datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            if req.fact_type == "flow":
                cur.execute(
                    """
                    SELECT report_id, metric_id, period_start_date, period_end_date, value, unit, currency,
                           consolidation_scope, audit_flag, source_trace_id, quality_score
                    FROM financial_flow_candidate
                    WHERE candidate_id = %s
                    """,
                    (req.candidate_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="flow candidate not found")
                if req.report_id is not None and int(row[0]) != req.report_id:
                    raise HTTPException(status_code=400, detail="report_id does not match candidate")
                report_id = int(row[0])
                metric_id = int(row[1])
                period_start_date = row[2]
                period_end_date = row[3]
                value = row[4]
                unit = row[5]
                currency = row[6]
                consolidation_scope = row[7]
                audit_flag = row[8]
                source_trace_id = row[9]
                quality_score = row[10]

                cur.execute(
                    """
                    UPDATE financial_flow_fact
                    SET value = %s,
                        unit = %s,
                        currency = %s,
                        consolidation_scope = %s,
                        audit_flag = %s,
                        source_trace_id = %s,
                        quality_score = %s,
                        selected_candidate_id = %s,
                        resolution_status = 'verified',
                        resolution_method = 'manual',
                        reviewed_by = %s,
                        reviewed_at = %s,
                        review_notes = %s
                    WHERE report_id = %s
                      AND metric_id = %s
                      AND period_start_date IS NOT DISTINCT FROM %s
                      AND period_end_date IS NOT DISTINCT FROM %s
                      AND unit IS NOT DISTINCT FROM %s
                      AND currency IS NOT DISTINCT FROM %s
                      AND consolidation_scope IS NOT DISTINCT FROM %s
                      AND audit_flag IS NOT DISTINCT FROM %s
                    """,
                    (
                        value,
                        unit,
                        currency,
                        consolidation_scope,
                        audit_flag,
                        source_trace_id,
                        quality_score,
                        req.candidate_id,
                        req.reviewed_by,
                        now,
                        req.review_notes,
                        report_id,
                        metric_id,
                        period_start_date,
                        period_end_date,
                        unit,
                        currency,
                        consolidation_scope,
                        audit_flag,
                    ),
                )
                if cur.rowcount == 0:
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
                            period_start_date,
                            period_end_date,
                            value,
                            unit,
                            currency,
                            consolidation_scope,
                            audit_flag,
                            source_trace_id,
                            quality_score,
                            now,
                            req.candidate_id,
                            "verified",
                            "manual",
                            req.reviewed_by,
                            now,
                            req.review_notes,
                        ),
                    )

                payload = {
                    "fact_type": "flow",
                    "report_id": report_id,
                    "metric_id": metric_id,
                    "period_start_date": str(period_start_date) if period_start_date else None,
                    "period_end_date": str(period_end_date) if period_end_date else None,
                    "value": _decimal_text(value),
                    "unit": unit,
                    "currency": currency,
                    "consolidation_scope": consolidation_scope,
                    "audit_flag": audit_flag,
                    "selected_candidate_id": req.candidate_id,
                    "resolution_status": "verified",
                    "resolution_method": "manual",
                }
            else:
                cur.execute(
                    """
                    SELECT report_id, metric_id, as_of_date, value, unit, currency,
                           consolidation_scope, source_trace_id, quality_score
                    FROM financial_stock_candidate
                    WHERE candidate_id = %s
                    """,
                    (req.candidate_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="stock candidate not found")
                if req.report_id is not None and int(row[0]) != req.report_id:
                    raise HTTPException(status_code=400, detail="report_id does not match candidate")
                report_id = int(row[0])
                metric_id = int(row[1])
                as_of_date = row[2]
                value = row[3]
                unit = row[4]
                currency = row[5]
                consolidation_scope = row[6]
                source_trace_id = row[7]
                quality_score = row[8]

                cur.execute(
                    """
                    UPDATE financial_stock_fact
                    SET value = %s,
                        unit = %s,
                        currency = %s,
                        consolidation_scope = %s,
                        source_trace_id = %s,
                        quality_score = %s,
                        selected_candidate_id = %s,
                        resolution_status = 'verified',
                        resolution_method = 'manual',
                        reviewed_by = %s,
                        reviewed_at = %s,
                        review_notes = %s
                    WHERE report_id = %s
                      AND metric_id = %s
                      AND as_of_date IS NOT DISTINCT FROM %s
                      AND unit IS NOT DISTINCT FROM %s
                      AND currency IS NOT DISTINCT FROM %s
                      AND consolidation_scope IS NOT DISTINCT FROM %s
                    """,
                    (
                        value,
                        unit,
                        currency,
                        consolidation_scope,
                        source_trace_id,
                        quality_score,
                        req.candidate_id,
                        req.reviewed_by,
                        now,
                        req.review_notes,
                        report_id,
                        metric_id,
                        as_of_date,
                        unit,
                        currency,
                        consolidation_scope,
                    ),
                )
                if cur.rowcount == 0:
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
                            source_trace_id,
                            quality_score,
                            now,
                            req.candidate_id,
                            "verified",
                            "manual",
                            req.reviewed_by,
                            now,
                            req.review_notes,
                        ),
                    )
                payload = {
                    "fact_type": "stock",
                    "report_id": report_id,
                    "metric_id": metric_id,
                    "as_of_date": str(as_of_date) if as_of_date else None,
                    "value": _decimal_text(value),
                    "unit": unit,
                    "currency": currency,
                    "consolidation_scope": consolidation_scope,
                    "selected_candidate_id": req.candidate_id,
                    "resolution_status": "verified",
                    "resolution_method": "manual",
                }
        conn.commit()
    return payload
