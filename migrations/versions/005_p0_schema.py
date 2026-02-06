"""p0 schema additions

Revision ID: 005_p0_schema
Revises: 004_units_text
Create Date: 2026-02-06
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "005_p0_schema"
down_revision = "004_units_text"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # financial_reports status markers
    op.add_column("financial_reports", sa.Column("status", sa.String(length=16), nullable=True))
    op.add_column("financial_reports", sa.Column("currency_status", sa.String(length=16), nullable=True))
    op.add_column("financial_reports", sa.Column("units_status", sa.String(length=16), nullable=True))
    op.add_column("financial_reports", sa.Column("period_status", sa.String(length=16), nullable=True))

    # report_tables status markers
    op.add_column("report_tables", sa.Column("currency_status", sa.String(length=16), nullable=True))
    op.add_column("report_tables", sa.Column("units_status", sa.String(length=16), nullable=True))

    # evidence chain
    op.add_column("report_table_rows", sa.Column("page_number", sa.Integer(), nullable=True))

    # report_versions
    op.create_table(
        "report_versions",
        sa.Column("version_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("report_id", sa.BigInteger(), sa.ForeignKey("financial_reports.report_id"), nullable=True),
        sa.Column("parse_method", sa.String(length=32), nullable=True),
        sa.Column("parser_version", sa.String(length=32), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("summary_json", sa.JSON(), nullable=True),
    )
    op.create_index("ix_report_versions_report_id", "report_versions", ["report_id"])

    # ingest_errors
    op.create_table(
        "ingest_errors",
        sa.Column("error_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source_path", sa.Text(), nullable=False),
        sa.Column("report_id", sa.BigInteger(), nullable=True),
        sa.Column("page_number", sa.Integer(), nullable=True),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("error_type", sa.String(length=64), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_ingest_errors_report_id", "ingest_errors", ["report_id"])

    # full-text search on report_pages
    op.add_column("report_pages", sa.Column("tsv", postgresql.TSVECTOR(), nullable=True))
    op.execute("UPDATE report_pages SET tsv = to_tsvector('simple', coalesce(text_md, ''))")
    op.create_index("ix_report_pages_tsv", "report_pages", ["tsv"], postgresql_using="gin")
    op.create_index("ux_report_pages_report_id_page", "report_pages", ["report_id", "page_number"], unique=True)

    # backfill status markers
    op.execute("UPDATE financial_reports SET status = 'ready' WHERE status IS NULL")
    op.execute(
        """
        UPDATE financial_reports
        SET currency_status = CASE WHEN currency IS NULL THEN 'missing' ELSE 'detected' END,
            units_status = CASE WHEN units IS NULL THEN 'missing' ELSE 'detected' END,
            period_status = CASE WHEN period_end IS NULL THEN 'missing' ELSE 'detected' END
        WHERE currency_status IS NULL OR units_status IS NULL OR period_status IS NULL
        """
    )
    op.execute(
        """
        UPDATE report_tables
        SET currency_status = CASE WHEN currency IS NULL THEN 'missing' ELSE 'detected' END,
            units_status = CASE WHEN units IS NULL THEN 'missing' ELSE 'detected' END
        WHERE currency_status IS NULL OR units_status IS NULL
        """
    )


def downgrade() -> None:
    op.drop_index("ux_report_pages_report_id_page", table_name="report_pages")
    op.drop_index("ix_report_pages_tsv", table_name="report_pages")
    op.drop_column("report_pages", "tsv")

    op.drop_index("ix_ingest_errors_report_id", table_name="ingest_errors")
    op.drop_table("ingest_errors")

    op.drop_index("ix_report_versions_report_id", table_name="report_versions")
    op.drop_table("report_versions")

    op.drop_column("report_table_rows", "page_number")

    op.drop_column("report_tables", "units_status")
    op.drop_column("report_tables", "currency_status")

    op.drop_column("financial_reports", "period_status")
    op.drop_column("financial_reports", "units_status")
    op.drop_column("financial_reports", "currency_status")
    op.drop_column("financial_reports", "status")
