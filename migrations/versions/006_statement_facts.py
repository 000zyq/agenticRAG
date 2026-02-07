"""statement facts schema

Revision ID: 006_statement_facts
Revises: 005_p0_schema
Create Date: 2026-02-07
"""

from alembic import op
import sqlalchemy as sa

revision = "006_statement_facts"
down_revision = "005_p0_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "company",
        sa.Column("company_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("ticker", sa.String(length=32), nullable=True),
        sa.Column("exchange", sa.String(length=32), nullable=True),
        sa.Column("industry", sa.String(length=64), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ux_company_name_ticker", "company", ["name", "ticker"], unique=True)

    op.add_column("financial_reports", sa.Column("company_id", sa.BigInteger(), nullable=True))
    op.add_column("financial_reports", sa.Column("announce_date", sa.Date(), nullable=True))
    op.add_column("financial_reports", sa.Column("source_url", sa.Text(), nullable=True))
    op.add_column("financial_reports", sa.Column("version_no", sa.Integer(), nullable=True))
    op.add_column("financial_reports", sa.Column("is_restated", sa.Boolean(), nullable=True))
    op.create_index("ix_financial_reports_company_id", "financial_reports", ["company_id"])
    op.create_foreign_key("fk_financial_reports_company", "financial_reports", "company", ["company_id"], ["company_id"])
    op.execute("UPDATE financial_reports SET version_no = 1 WHERE version_no IS NULL")
    op.execute("UPDATE financial_reports SET is_restated = FALSE WHERE is_restated IS NULL")

    op.create_table(
        "metric",
        sa.Column("metric_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("metric_code", sa.String(length=64), nullable=False),
        sa.Column("metric_name_cn", sa.Text(), nullable=False),
        sa.Column("statement_type", sa.String(length=16), nullable=False),
        sa.Column("value_nature", sa.String(length=16), nullable=False),
        sa.Column("unit_default", sa.String(length=16), nullable=True),
        sa.Column("sign_rule", sa.String(length=16), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ux_metric_code", "metric", ["metric_code"], unique=True)
    op.create_index("ix_metric_statement", "metric", ["statement_type"])

    op.create_table(
        "policy",
        sa.Column("policy_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("accounting_policy", sa.Text(), nullable=True),
        sa.Column("consolidation_scope", sa.String(length=32), nullable=True),
        sa.Column("currency", sa.String(length=16), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "source_trace",
        sa.Column("trace_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("report_id", sa.BigInteger(), sa.ForeignKey("financial_reports.report_id"), nullable=False),
        sa.Column("source_table_id", sa.BigInteger(), sa.ForeignKey("report_tables.table_id"), nullable=True),
        sa.Column("source_row_id", sa.BigInteger(), sa.ForeignKey("report_table_rows.row_id"), nullable=True),
        sa.Column("source_page", sa.Integer(), nullable=True),
        sa.Column("raw_label", sa.Text(), nullable=True),
        sa.Column("raw_value", sa.Text(), nullable=True),
        sa.Column("column_label", sa.Text(), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_source_trace_report_id", "source_trace", ["report_id"])

    op.create_table(
        "financial_flow_fact",
        sa.Column("fact_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("report_id", sa.BigInteger(), sa.ForeignKey("financial_reports.report_id"), nullable=False),
        sa.Column("metric_id", sa.BigInteger(), sa.ForeignKey("metric.metric_id"), nullable=False),
        sa.Column("period_start_date", sa.Date(), nullable=True),
        sa.Column("period_end_date", sa.Date(), nullable=True),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("unit", sa.String(length=16), nullable=True),
        sa.Column("currency", sa.String(length=16), nullable=True),
        sa.Column("consolidation_scope", sa.String(length=32), nullable=True),
        sa.Column("audit_flag", sa.String(length=16), nullable=True),
        sa.Column("source_trace_id", sa.BigInteger(), sa.ForeignKey("source_trace.trace_id"), nullable=True),
        sa.Column("quality_score", sa.Numeric(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_flow_fact_report_id", "financial_flow_fact", ["report_id"])
    op.create_index("ix_flow_fact_metric_id", "financial_flow_fact", ["metric_id"])
    op.create_index("ix_flow_fact_period_end", "financial_flow_fact", ["period_end_date"])

    op.create_table(
        "financial_stock_fact",
        sa.Column("fact_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("report_id", sa.BigInteger(), sa.ForeignKey("financial_reports.report_id"), nullable=False),
        sa.Column("metric_id", sa.BigInteger(), sa.ForeignKey("metric.metric_id"), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=True),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("unit", sa.String(length=16), nullable=True),
        sa.Column("currency", sa.String(length=16), nullable=True),
        sa.Column("consolidation_scope", sa.String(length=32), nullable=True),
        sa.Column("source_trace_id", sa.BigInteger(), sa.ForeignKey("source_trace.trace_id"), nullable=True),
        sa.Column("quality_score", sa.Numeric(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_stock_fact_report_id", "financial_stock_fact", ["report_id"])
    op.create_index("ix_stock_fact_metric_id", "financial_stock_fact", ["metric_id"])
    op.create_index("ix_stock_fact_as_of", "financial_stock_fact", ["as_of_date"])


def downgrade() -> None:
    op.drop_index("ix_stock_fact_as_of", table_name="financial_stock_fact")
    op.drop_index("ix_stock_fact_metric_id", table_name="financial_stock_fact")
    op.drop_index("ix_stock_fact_report_id", table_name="financial_stock_fact")
    op.drop_table("financial_stock_fact")

    op.drop_index("ix_flow_fact_period_end", table_name="financial_flow_fact")
    op.drop_index("ix_flow_fact_metric_id", table_name="financial_flow_fact")
    op.drop_index("ix_flow_fact_report_id", table_name="financial_flow_fact")
    op.drop_table("financial_flow_fact")

    op.drop_index("ix_source_trace_report_id", table_name="source_trace")
    op.drop_table("source_trace")

    op.drop_table("policy")

    op.drop_index("ix_metric_statement", table_name="metric")
    op.drop_index("ux_metric_code", table_name="metric")
    op.drop_table("metric")

    op.drop_constraint("fk_financial_reports_company", "financial_reports", type_="foreignkey")
    op.drop_index("ix_financial_reports_company_id", table_name="financial_reports")
    op.drop_column("financial_reports", "is_restated")
    op.drop_column("financial_reports", "version_no")
    op.drop_column("financial_reports", "source_url")
    op.drop_column("financial_reports", "announce_date")
    op.drop_column("financial_reports", "company_id")

    op.drop_index("ux_company_name_ticker", table_name="company")
    op.drop_table("company")
