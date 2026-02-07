from __future__ import annotations

import os
from pathlib import Path
import pytest

from app.ingest.financial_report import sha256_file


@pytest.mark.integration
def test_ingest_to_postgres() -> None:
    pytest.importorskip("psycopg")
    if os.getenv("RUN_DB_TESTS") != "1":
        pytest.skip("RUN_DB_TESTS not enabled")
    if not os.getenv("POSTGRES_DSN"):
        pytest.skip("POSTGRES_DSN not set")

    from app.storage.db import get_conn
    from scripts.ingest_financial_report import insert_report

    path = Path("tmp/ingest/2024年报.pdf")
    if not path.exists():
        pytest.skip("sample report missing")

    source_hash = sha256_file(path)
    preexisting = False
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT report_id FROM financial_reports WHERE source_hash = %s", (source_hash,))
            row = cur.fetchone()
            if row:
                preexisting = True
                report_id = int(row[0])
            else:
                report_id = None

    report_id = insert_report(path)
    assert report_id is not None

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM report_pages WHERE report_id = %s", (report_id,))
            assert cur.fetchone()[0] > 0
            cur.execute("SELECT COUNT(*) FROM report_tables WHERE report_id = %s", (report_id,))
            assert cur.fetchone()[0] >= 1

    if not preexisting:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM report_table_cells
                    WHERE row_id IN (
                        SELECT row_id FROM report_table_rows WHERE table_id IN (
                            SELECT table_id FROM report_tables WHERE report_id = %s
                        )
                    )
                    """,
                    (report_id,),
                )
                cur.execute(
                    """
                    DELETE FROM report_table_rows
                    WHERE table_id IN (
                        SELECT table_id FROM report_tables WHERE report_id = %s
                    )
                    """,
                    (report_id,),
                )
                cur.execute(
                    """
                    DELETE FROM report_table_columns
                    WHERE table_id IN (
                        SELECT table_id FROM report_tables WHERE report_id = %s
                    )
                    """,
                    (report_id,),
                )
                cur.execute("DELETE FROM report_tables WHERE report_id = %s", (report_id,))
                cur.execute("DELETE FROM report_pages WHERE report_id = %s", (report_id,))
                cur.execute("DELETE FROM report_versions WHERE report_id = %s", (report_id,))
                cur.execute("DELETE FROM financial_reports WHERE report_id = %s", (report_id,))
            conn.commit()
