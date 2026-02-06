from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

from app.ingest.financial_report import extract_financial_report, sha256_file
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


def insert_report(path: Path) -> int:
    source_hash = sha256_file(path)
    now = datetime.utcnow()

    try:
        pages, meta, tables, parse_method = extract_financial_report(str(path))
    except Exception as exc:
        _record_error(path, None, None, "parse", exc)
        raise

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

                stage = "insert_report"
                cur.execute(
                    """
                    INSERT INTO financial_reports (
                        doc_id, source_path, source_hash, report_title, company_name, ticker,
                        report_type, fiscal_year, period_start, period_end, currency, units,
                        parse_method, extra, status, currency_status, units_status, period_status,
                        created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING report_id
                    """,
                    (
                        None,
                        str(path),
                        source_hash,
                        meta.report_title,
                        meta.company_name,
                        meta.ticker,
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

                finished = datetime.utcnow()
                summary = {
                    "pages": len(pages),
                    "tables": len(tables),
                    "rows": sum(len(t.rows) for t in tables),
                    "cells": sum(len(t.rows) * len(t.columns) for t in tables),
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
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    report_id = insert_report(path)
    print(f"report_id={report_id}")


if __name__ == "__main__":
    main()
