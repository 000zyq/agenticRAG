from __future__ import annotations

import argparse
from datetime import datetime, date
import json
from pathlib import Path

from app.ingest.financial_report import extract_financial_report, sha256_file
from app.ingest.metric_defs import (
    infer_statement_type_from_rows,
    match_metric,
    metric_code_from_label,
)
from app.storage.db import get_conn


def _record_error(
    source_path: Path,
    report_id: int | None,
    page_number: int | None,
    stage: str,
    exc: Exception,
) -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingest_errors (
                        source_path, report_id, page_number, stage, error_type, error_message, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(source_path),
                        report_id,
                        page_number,
                        stage,
                        type(exc).__name__,
                        str(exc),
                        datetime.utcnow(),
                    ),
                )
            conn.commit()
    except Exception:
        return


STATEMENT_TYPE_MAP = {
    "balance_sheet": "balance",
    "income_statement": "income",
    "cash_flow": "cashflow",
}


def _match_metric(label: str, statement_type: str) -> dict | None:
    return match_metric(label, statement_type)


def _get_or_create_company(cur, name: str | None, ticker: str | None, now: datetime) -> int | None:
    if not name:
        return None
    cur.execute(
        "SELECT company_id FROM company WHERE name = %s AND ticker IS NOT DISTINCT FROM %s",
        (name, ticker),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])
    cur.execute(
        """
        INSERT INTO company (name, ticker, created_at)
        VALUES (%s, %s, %s)
        RETURNING company_id
        """,
        (name, ticker, now),
    )
    return int(cur.fetchone()[0])


def _get_or_create_metric(cur, metric: dict, label: str, statement_type: str, unit_default: str | None, now: datetime, cache: dict[str, int]) -> int:
    if metric:
        metric_code = metric["metric_code"]
        metric_name = metric["metric_name_cn"]
        value_nature = metric["value_nature"]
    else:
        metric_code = metric_code_from_label(label, statement_type)
        metric_name = label
        value_nature = "flow" if statement_type != "balance" else "stock"

    cached = cache.get(metric_code)
    if cached:
        return cached
    cur.execute("SELECT metric_id FROM metric WHERE metric_code = %s", (metric_code,))
    row = cur.fetchone()
    if row:
        metric_id = int(row[0])
        cache[metric_code] = metric_id
        return metric_id
    cur.execute(
        """
        INSERT INTO metric (
            metric_code, metric_name_cn, statement_type, value_nature,
            unit_default, sign_rule, extra, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING metric_id
        """,
        (metric_code, metric_name, statement_type, value_nature, unit_default, "normal", None, now),
    )
    metric_id = int(cur.fetchone()[0])
    cache[metric_code] = metric_id
    return metric_id


def _load_existing_table_row_map(cur, report_id: int) -> tuple[list[int], dict[int, dict[int, int]]]:
    cur.execute(
        "SELECT table_id FROM report_tables WHERE report_id = %s ORDER BY table_id",
        (report_id,),
    )
    table_ids = [int(row[0]) for row in cur.fetchall()]
    if not table_ids:
        return [], {}
    cur.execute(
        """
        SELECT table_id, row_index, row_id
        FROM report_table_rows
        WHERE table_id = ANY(%s)
        ORDER BY table_id, row_index
        """,
        (table_ids,),
    )
    row_map: dict[int, dict[int, int]] = {}
    for table_id, row_index, row_id in cur.fetchall():
        row_map.setdefault(int(table_id), {})[int(row_index)] = int(row_id)
    return table_ids, row_map


def _infer_period_end(col, meta) -> date | None:
    if col.period_end:
        return col.period_end
    if col.label == "current_period" and meta.period_end:
        return meta.period_end
    if col.label == "prior_period" and meta.period_end:
        return date(meta.period_end.year - 1, meta.period_end.month, meta.period_end.day)
    if col.fiscal_year:
        return date(col.fiscal_year, 12, 31)
    return meta.period_end


def _infer_period_start(report_type: str | None, period_end: date | None) -> date | None:
    if report_type == "annual" and period_end:
        return date(period_end.year, 1, 1)
    return None


def _consolidation_scope(is_consolidated: bool | None) -> str | None:
    if is_consolidated is True:
        return "consolidated"
    if is_consolidated is False:
        return "parent"
    return None


def _insert_facts_for_table(
    cur,
    report_id: int,
    version_id: int | None,
    meta,
    table,
    table_id: int | None,
    row_ids: list[int] | None,
    now: datetime,
    metric_cache: dict[str, int],
    write_facts: bool = True,
) -> tuple[int, int]:
    mapped_statement = STATEMENT_TYPE_MAP.get(table.statement_type or "")
    if not mapped_statement:
        mapped_statement = infer_statement_type_from_rows(table.rows)
    if not mapped_statement:
        return 0, 0

    flow_fact_count = 0
    stock_fact_count = 0
    table_currency = table.currency or meta.currency
    table_units = table.units or meta.units
    consolidation_scope = _consolidation_scope(table.is_consolidated)

    for row_idx, row in enumerate(table.rows):
        metric_def = _match_metric(row.label, mapped_statement)
        metric_id = _get_or_create_metric(
            cur,
            metric_def,
            row.label,
            mapped_statement,
            table_units,
            now,
            metric_cache,
        )
        row_id = row_ids[row_idx] if row_ids else None
        for col, cell in zip(table.columns, row.cells):
            if cell.value is None:
                continue
            period_end = _infer_period_end(col, meta)
            period_start = _infer_period_start(meta.report_type, period_end)

            cur.execute(
                """
                INSERT INTO source_trace (
                    report_id, source_table_id, source_row_id, source_page,
                    raw_label, raw_value, column_label, extra, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING trace_id
                """,
                (
                    report_id,
                    table_id,
                    row_id,
                    row.page_number,
                    row.label,
                    cell.raw_text,
                    col.label,
                    None,
                    now,
                ),
            )
            trace_id = int(cur.fetchone()[0])

            if mapped_statement == "balance":
                cur.execute(
                    """
                    INSERT INTO financial_stock_candidate (
                        report_id, version_id, metric_id, as_of_date, value, unit, currency,
                        consolidation_scope, source_trace_id, quality_score, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING candidate_id
                    """,
                    (
                        report_id,
                        version_id,
                        metric_id,
                        period_end,
                        cell.value,
                        table_units,
                        table_currency,
                        consolidation_scope,
                        trace_id,
                        None,
                        now,
                    ),
                )
                candidate_id = int(cur.fetchone()[0])
                if write_facts:
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
                            metric_id,
                            period_end,
                            cell.value,
                            table_units,
                            table_currency,
                            consolidation_scope,
                            trace_id,
                            None,
                            now,
                            candidate_id,
                            "auto",
                            "single_engine",
                        ),
                    )
                    stock_fact_count += 1
            else:
                cur.execute(
                    """
                    INSERT INTO financial_flow_candidate (
                        report_id, version_id, metric_id, period_start_date, period_end_date, value, unit, currency,
                        consolidation_scope, audit_flag, source_trace_id, quality_score, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING candidate_id
                    """,
                    (
                        report_id,
                        version_id,
                        metric_id,
                        period_start,
                        period_end,
                        cell.value,
                        table_units,
                        table_currency,
                        consolidation_scope,
                        None,
                        trace_id,
                        None,
                        now,
                    ),
                )
                candidate_id = int(cur.fetchone()[0])
                if write_facts:
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
                            metric_id,
                            period_start,
                            period_end,
                            cell.value,
                            table_units,
                            table_currency,
                            consolidation_scope,
                            None,
                            trace_id,
                            None,
                            now,
                            candidate_id,
                            "auto",
                            "single_engine",
                        ),
                    )
                    flow_fact_count += 1

    return flow_fact_count, stock_fact_count


def insert_report(
    path: Path,
    recompute_facts: bool = False,
    parse_method_override: str | None = None,
    candidates_only: bool = False,
    allow_existing: bool = False,
) -> int:
    source_hash = sha256_file(path)
    now = datetime.utcnow()

    try:
        pages, meta, tables, parse_method = extract_financial_report(str(path))
    except Exception as exc:
        _record_error(path, None, None, "parse", exc)
        raise

    if parse_method_override:
        parse_method = parse_method_override

    currency_status = "detected" if meta.currency else "missing"
    units_status = "detected" if meta.units else "missing"
    period_status = "detected" if meta.period_end else "missing"
    report_status = "ready" if currency_status == "detected" and units_status == "detected" and period_status == "detected" else "draft"

    report_id: int | None = None
    version_id: int | None = None
    stage = "init"

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT report_id FROM financial_reports WHERE source_hash = %s", (source_hash,))
                existing = cur.fetchone()
                if existing:
                    report_id = int(existing[0])
                    if not recompute_facts and not allow_existing:
                        cur.execute(
                            """
                            INSERT INTO report_versions (
                                report_id, parse_method, parser_version, started_at, finished_at, status, summary_json
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (report_id, parse_method, "v1", now, now, "skipped", json.dumps({"reason": "duplicate"})),
                        )
                        conn.commit()
                        return report_id

                    if allow_existing and not recompute_facts:
                        cur.execute(
                            """
                            INSERT INTO report_versions (
                                report_id, parse_method, parser_version, started_at, status
                            )
                            VALUES (%s, %s, %s, %s, %s)
                            RETURNING version_id
                            """,
                            (report_id, parse_method, "v1", now, "running"),
                        )
                        version_id = int(cur.fetchone()[0])

                        stage = "append_candidates"
                        metric_cache: dict[str, int] = {}
                        flow_fact_count = 0
                        stock_fact_count = 0
                        for table in tables:
                            flow_inc, stock_inc = _insert_facts_for_table(
                                cur,
                                report_id,
                                version_id,
                                meta,
                                table,
                                None,
                                None,
                                now,
                                metric_cache,
                                write_facts=False,
                            )
                            flow_fact_count += flow_inc
                            stock_fact_count += stock_inc

                        finished = datetime.utcnow()
                        summary = {
                            "flow_facts": flow_fact_count,
                            "stock_facts": stock_fact_count,
                            "mode": "append_candidates",
                        }
                        cur.execute(
                            """
                            UPDATE report_versions
                            SET finished_at = %s, status = %s, summary_json = %s
                            WHERE version_id = %s
                            """,
                            (finished, "ready", json.dumps(summary), version_id),
                        )

                        conn.commit()
                        return report_id

                    cur.execute(
                        """
                        INSERT INTO report_versions (
                            report_id, parse_method, parser_version, started_at, status
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING version_id
                        """,
                        (report_id, parse_method, "v1", now, "running"),
                    )
                    version_id = int(cur.fetchone()[0])

                    stage = "recompute_facts_cleanup"
                    cur.execute("DELETE FROM financial_flow_fact WHERE report_id = %s", (report_id,))
                    cur.execute("DELETE FROM financial_stock_fact WHERE report_id = %s", (report_id,))
                    cur.execute("DELETE FROM financial_flow_candidate WHERE report_id = %s", (report_id,))
                    cur.execute("DELETE FROM financial_stock_candidate WHERE report_id = %s", (report_id,))
                    cur.execute("DELETE FROM source_trace WHERE report_id = %s", (report_id,))

                    stage = "recompute_facts_insert"
                    existing_table_ids, existing_row_map = _load_existing_table_row_map(cur, report_id)
                    metric_cache: dict[str, int] = {}
                    flow_fact_count = 0
                    stock_fact_count = 0
                    for table_idx, table in enumerate(tables):
                        table_id = existing_table_ids[table_idx] if table_idx < len(existing_table_ids) else None
                        row_ids = None
                        if table_id is not None:
                            row_index_map = existing_row_map.get(table_id, {})
                            row_ids = [row_index_map.get(row_idx) for row_idx in range(len(table.rows))]
                        flow_inc, stock_inc = _insert_facts_for_table(
                            cur,
                            report_id,
                            version_id,
                            meta,
                            table,
                            table_id,
                            row_ids,
                            now,
                            metric_cache,
                            write_facts=not candidates_only,
                        )
                        flow_fact_count += flow_inc
                        stock_fact_count += stock_inc

                    finished = datetime.utcnow()
                    summary = {
                        "flow_facts": flow_fact_count,
                        "stock_facts": stock_fact_count,
                        "mode": "recompute_facts",
                    }
                    cur.execute(
                        """
                        UPDATE report_versions
                        SET finished_at = %s, status = %s, summary_json = %s
                        WHERE version_id = %s
                        """,
                        (finished, "ready", json.dumps(summary), version_id),
                    )

                    conn.commit()
                    return report_id

                company_id = _get_or_create_company(cur, meta.company_name, meta.ticker, now)

                stage = "insert_report"
                cur.execute(
                    """
                    INSERT INTO financial_reports (
                        doc_id, source_path, source_hash, report_title, company_name, ticker, company_id,
                        report_type, fiscal_year, period_start, period_end, currency, units,
                        parse_method, extra, status, currency_status, units_status, period_status,
                        announce_date, source_url, version_no, is_restated,
                        created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING report_id
                    """,
                    (
                        None,
                        str(path),
                        source_hash,
                        meta.report_title,
                        meta.company_name,
                        meta.ticker,
                        company_id,
                        meta.report_type,
                        meta.fiscal_year,
                        meta.period_start,
                        meta.period_end,
                        meta.currency,
                        meta.units,
                        parse_method,
                        json.dumps(meta.extra),
                        report_status,
                        currency_status,
                        units_status,
                        period_status,
                        None,
                        None,
                        1,
                        False,
                        now,
                        now,
                    ),
                )
                report_id = int(cur.fetchone()[0])

                stage = "version_start"
                cur.execute(
                    """
                    INSERT INTO report_versions (
                        report_id, parse_method, parser_version, started_at, status
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING version_id
                    """,
                    (report_id, parse_method, "v1", now, "running"),
                )
                version_id = int(cur.fetchone()[0])

                stage = "insert_pages"
                page_rows = [
                    (report_id, page.page, page.text_md, page.text_raw, now)
                    for page in pages
                ]
                cur.executemany(
                    """
                    INSERT INTO report_pages (report_id, page_number, text_md, text_raw, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    page_rows,
                )
                cur.execute(
                    "UPDATE report_pages SET tsv = to_tsvector('simple', coalesce(text_md, '')) WHERE report_id = %s",
                    (report_id,),
                )

                stage = "insert_tables"
                metric_cache: dict[str, int] = {}
                flow_fact_count = 0
                stock_fact_count = 0
                for table in tables:
                    table_currency = table.currency or meta.currency
                    table_units = table.units or meta.units
                    table_currency_status = "detected" if table_currency else "missing"
                    table_units_status = "detected" if table_units else "missing"

                    cur.execute(
                        """
                        INSERT INTO report_tables (
                            report_id, section_title, statement_type, title, page_start, page_end,
                            currency, units, is_consolidated, currency_status, units_status,
                            extra, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING table_id
                        """,
                        (
                            report_id,
                            table.section_title,
                            table.statement_type,
                            table.title,
                            table.page_start,
                            table.page_end,
                            table_currency,
                            table_units,
                            table.is_consolidated,
                            table_currency_status,
                            table_units_status,
                            None,
                            now,
                        ),
                    )
                    table_id = int(cur.fetchone()[0])

                    column_ids: list[int] = []
                    for idx, col in enumerate(table.columns):
                        cur.execute(
                            """
                            INSERT INTO report_table_columns (
                                table_id, column_index, label, period_start, period_end,
                                fiscal_year, fiscal_period, extra, created_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING column_id
                            """,
                            (
                                table_id,
                                idx,
                                col.label,
                                col.period_start,
                                col.period_end,
                                col.fiscal_year,
                                col.fiscal_period,
                                None,
                                now,
                            ),
                        )
                        column_ids.append(int(cur.fetchone()[0]))

                    row_ids: list[int] = []
                    for idx, row in enumerate(table.rows):
                        is_total = "合计" in row.label or "total" in row.label.lower()
                        cur.execute(
                            """
                            INSERT INTO report_table_rows (
                                table_id, row_index, label, level, is_total, page_number, extra, created_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING row_id
                            """,
                            (table_id, idx, row.label, None, is_total, row.page_number, None, now),
                        )
                        row_ids.append(int(cur.fetchone()[0]))

                    stage = "insert_cells"
                    for row_id, row in zip(row_ids, table.rows):
                        for col_id, cell in zip(column_ids, row.cells):
                            if cell.value is None and not cell.raw_text:
                                continue
                            cur.execute(
                                """
                                INSERT INTO report_table_cells (
                                    row_id, column_id, value, raw_text, unit, extra, created_at
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """,
                                (row_id, col_id, cell.value, cell.raw_text, table_units, None, now),
                            )

                    stage = "insert_facts"
                    flow_inc, stock_inc = _insert_facts_for_table(
                        cur,
                        report_id,
                        version_id,
                        meta,
                        table,
                        table_id,
                        row_ids,
                        now,
                        metric_cache,
                        write_facts=not candidates_only,
                    )
                    flow_fact_count += flow_inc
                    stock_fact_count += stock_inc

                finished = datetime.utcnow()
                summary = {
                    "pages": len(pages),
                    "tables": len(tables),
                    "rows": sum(len(t.rows) for t in tables),
                    "cells": sum(len(t.rows) * len(t.columns) for t in tables),
                    "flow_facts": flow_fact_count,
                    "stock_facts": stock_fact_count,
                }
                cur.execute(
                    """
                    UPDATE report_versions
                    SET finished_at = %s, status = %s, summary_json = %s
                    WHERE version_id = %s
                    """,
                    (finished, "ready", json.dumps(summary), version_id),
                )

            conn.commit()
            return report_id
    except Exception as exc:
        _record_error(path, report_id, None, stage, exc)
        if version_id is not None:
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE report_versions SET finished_at = %s, status = %s WHERE version_id = %s",
                            (datetime.utcnow(), "failed", version_id),
                        )
                    conn.commit()
            except Exception:
                pass
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest financial report PDF into Postgres.")
    parser.add_argument("path", nargs="?", default="tmp/ingest/2024年报.pdf", help="Path to report PDF")
    parser.add_argument("--recompute-facts", action="store_true", help="Recompute facts for existing report")
    parser.add_argument("--parse-method", help="Override parse method label stored in report_versions")
    parser.add_argument(
        "--candidates-only",
        action="store_true",
        help="Only insert candidate facts (skip canonical fact write).",
    )
    parser.add_argument(
        "--allow-existing",
        action="store_true",
        help="Allow appending candidates for an existing report without recompute.",
    )
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    report_id = insert_report(
        path,
        recompute_facts=args.recompute_facts,
        parse_method_override=args.parse_method,
        candidates_only=args.candidates_only,
        allow_existing=args.allow_existing,
    )
    print(f"report_id={report_id}")


if __name__ == "__main__":
    main()
