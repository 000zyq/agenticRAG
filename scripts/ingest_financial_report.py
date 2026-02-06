from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

from app.ingest.financial_report import extract_financial_report, sha256_file
from app.storage.db import get_conn


def insert_report(path: Path) -> int:
    pages, meta, tables, parse_method = extract_financial_report(str(path))
    source_hash = sha256_file(path)
    now = datetime.utcnow()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT report_id FROM financial_reports WHERE source_hash = %s", (source_hash,))
            existing = cur.fetchone()
            if existing:
                return int(existing[0])

            cur.execute(
                """
                INSERT INTO financial_reports (
                    doc_id, source_path, source_hash, report_title, company_name, ticker,
                    report_type, fiscal_year, period_start, period_end, currency, units,
                    parse_method, extra, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    now,
                    now,
                ),
            )
            report_id = int(cur.fetchone()[0])

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

            for table in tables:
                cur.execute(
                    """
                    INSERT INTO report_tables (
                        report_id, section_title, statement_type, title, page_start, page_end,
                        currency, units, is_consolidated, extra, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING table_id
                    """,
                    (
                        report_id,
                        table.section_title,
                        table.statement_type,
                        table.title,
                        table.page_start,
                        table.page_end,
                        table.currency or meta.currency,
                        table.units or meta.units,
                        table.is_consolidated,
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
                            table_id, row_index, label, level, is_total, extra, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING row_id
                        """,
                        (table_id, idx, row.label, None, is_total, None, now),
                    )
                    row_ids.append(int(cur.fetchone()[0]))

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
                            (row_id, col_id, cell.value, cell.raw_text, table.units, None, now),
                        )

        conn.commit()
        return report_id


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
