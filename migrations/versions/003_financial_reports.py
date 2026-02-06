"""financial report tables

Revision ID: 003_financial_reports
Revises: 002_finqa_qa
Create Date: 2026-02-06
"""

from alembic import op
import sqlalchemy as sa

revision = "003_financial_reports"
down_revision = "002_finqa_qa"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "financial_reports",
        sa.Column("report_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("doc_id", sa.String(length=64), nullable=True),
        sa.Column("source_path", sa.Text(), nullable=False),
        sa.Column("source_hash", sa.String(length=64), nullable=False),
        sa.Column("report_title", sa.Text(), nullable=True),
        sa.Column("company_name", sa.Text(), nullable=True),
        sa.Column("ticker", sa.String(length=32), nullable=True),
        sa.Column("report_type", sa.String(length=32), nullable=True),
        sa.Column("fiscal_year", sa.Integer(), nullable=True),
        sa.Column("period_start", sa.Date(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=True),
        sa.Column("currency", sa.String(length=16), nullable=True),
        sa.Column("units", sa.String(length=32), nullable=True),
        sa.Column("parse_method", sa.String(length=32), nullable=False),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_financial_reports_source_hash", "financial_reports", ["source_hash"], unique=True)
    op.create_index("ix_financial_reports_company_name", "financial_reports", ["company_name"])
    op.create_index("ix_financial_reports_fiscal_year", "financial_reports", ["fiscal_year"])

    op.create_table(
        "report_pages",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("report_id", sa.BigInteger(), sa.ForeignKey("financial_reports.report_id"), nullable=False),
        sa.Column("page_number", sa.Integer(), nullable=False),
        sa.Column("text_md", sa.Text(), nullable=True),
        sa.Column("text_raw", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_report_pages_report_id", "report_pages", ["report_id"])
    op.create_index("ix_report_pages_page_number", "report_pages", ["page_number"])

    op.create_table(
        "report_tables",
        sa.Column("table_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("report_id", sa.BigInteger(), sa.ForeignKey("financial_reports.report_id"), nullable=False),
        sa.Column("section_title", sa.Text(), nullable=True),
        sa.Column("statement_type", sa.String(length=32), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("page_start", sa.Integer(), nullable=True),
        sa.Column("page_end", sa.Integer(), nullable=True),
        sa.Column("currency", sa.String(length=16), nullable=True),
        sa.Column("units", sa.String(length=32), nullable=True),
        sa.Column("is_consolidated", sa.Boolean(), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_report_tables_report_id", "report_tables", ["report_id"])
    op.create_index("ix_report_tables_statement_type", "report_tables", ["statement_type"])

    op.create_table(
        "report_table_columns",
        sa.Column("column_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("table_id", sa.BigInteger(), sa.ForeignKey("report_tables.table_id"), nullable=False),
        sa.Column("column_index", sa.Integer(), nullable=False),
        sa.Column("label", sa.Text(), nullable=True),
        sa.Column("period_start", sa.Date(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=True),
        sa.Column("fiscal_year", sa.Integer(), nullable=True),
        sa.Column("fiscal_period", sa.String(length=16), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_report_table_columns_table_id", "report_table_columns", ["table_id"])

    op.create_table(
        "report_table_rows",
        sa.Column("row_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("table_id", sa.BigInteger(), sa.ForeignKey("report_tables.table_id"), nullable=False),
        sa.Column("row_index", sa.Integer(), nullable=False),
        sa.Column("label", sa.Text(), nullable=True),
        sa.Column("level", sa.Integer(), nullable=True),
        sa.Column("is_total", sa.Boolean(), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_report_table_rows_table_id", "report_table_rows", ["table_id"])

    op.create_table(
        "report_table_cells",
        sa.Column("cell_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("row_id", sa.BigInteger(), sa.ForeignKey("report_table_rows.row_id"), nullable=False),
        sa.Column("column_id", sa.BigInteger(), sa.ForeignKey("report_table_columns.column_id"), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("raw_text", sa.Text(), nullable=True),
        sa.Column("unit", sa.String(length=32), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_report_table_cells_row_id", "report_table_cells", ["row_id"])
    op.create_index("ix_report_table_cells_column_id", "report_table_cells", ["column_id"])


def downgrade() -> None:
    op.drop_index("ix_report_table_cells_column_id", table_name="report_table_cells")
    op.drop_index("ix_report_table_cells_row_id", table_name="report_table_cells")
    op.drop_table("report_table_cells")

    op.drop_index("ix_report_table_rows_table_id", table_name="report_table_rows")
    op.drop_table("report_table_rows")

    op.drop_index("ix_report_table_columns_table_id", table_name="report_table_columns")
    op.drop_table("report_table_columns")

    op.drop_index("ix_report_tables_statement_type", table_name="report_tables")
    op.drop_index("ix_report_tables_report_id", table_name="report_tables")
    op.drop_table("report_tables")

    op.drop_index("ix_report_pages_page_number", table_name="report_pages")
    op.drop_index("ix_report_pages_report_id", table_name="report_pages")
    op.drop_table("report_pages")

    op.drop_index("ix_financial_reports_fiscal_year", table_name="financial_reports")
    op.drop_index("ix_financial_reports_company_name", table_name="financial_reports")
    op.drop_index("ix_financial_reports_source_hash", table_name="financial_reports")
    op.drop_table("financial_reports")
