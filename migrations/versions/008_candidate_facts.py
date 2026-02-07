"""candidate facts and resolutions

Revision ID: 008_candidate_facts
Revises: 007_widen_unit_fields
Create Date: 2026-02-07
"""

from alembic import op
import sqlalchemy as sa

revision = "008_candidate_facts"
down_revision = "007_widen_unit_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "financial_flow_candidate",
        sa.Column("candidate_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("report_id", sa.BigInteger(), sa.ForeignKey("financial_reports.report_id"), nullable=False),
        sa.Column("version_id", sa.BigInteger(), sa.ForeignKey("report_versions.version_id"), nullable=True),
        sa.Column("metric_id", sa.BigInteger(), sa.ForeignKey("metric.metric_id"), nullable=False),
        sa.Column("period_start_date", sa.Date(), nullable=True),
        sa.Column("period_end_date", sa.Date(), nullable=True),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("unit", sa.Text(), nullable=True),
        sa.Column("currency", sa.String(length=16), nullable=True),
        sa.Column("consolidation_scope", sa.String(length=32), nullable=True),
        sa.Column("audit_flag", sa.String(length=16), nullable=True),
        sa.Column("source_trace_id", sa.BigInteger(), sa.ForeignKey("source_trace.trace_id"), nullable=True),
        sa.Column("quality_score", sa.Numeric(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_flow_candidate_report_id", "financial_flow_candidate", ["report_id"])
    op.create_index("ix_flow_candidate_metric_id", "financial_flow_candidate", ["metric_id"])
    op.create_index("ix_flow_candidate_period_end", "financial_flow_candidate", ["period_end_date"])
    op.create_index("ix_flow_candidate_version_id", "financial_flow_candidate", ["version_id"])

    op.create_table(
        "financial_stock_candidate",
        sa.Column("candidate_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("report_id", sa.BigInteger(), sa.ForeignKey("financial_reports.report_id"), nullable=False),
        sa.Column("version_id", sa.BigInteger(), sa.ForeignKey("report_versions.version_id"), nullable=True),
        sa.Column("metric_id", sa.BigInteger(), sa.ForeignKey("metric.metric_id"), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=True),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("unit", sa.Text(), nullable=True),
        sa.Column("currency", sa.String(length=16), nullable=True),
        sa.Column("consolidation_scope", sa.String(length=32), nullable=True),
        sa.Column("source_trace_id", sa.BigInteger(), sa.ForeignKey("source_trace.trace_id"), nullable=True),
        sa.Column("quality_score", sa.Numeric(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_stock_candidate_report_id", "financial_stock_candidate", ["report_id"])
    op.create_index("ix_stock_candidate_metric_id", "financial_stock_candidate", ["metric_id"])
    op.create_index("ix_stock_candidate_as_of", "financial_stock_candidate", ["as_of_date"])
    op.create_index("ix_stock_candidate_version_id", "financial_stock_candidate", ["version_id"])

    op.add_column("financial_flow_fact", sa.Column("selected_candidate_id", sa.BigInteger(), nullable=True))
    op.add_column("financial_flow_fact", sa.Column("resolution_status", sa.String(length=16), nullable=True))
    op.add_column("financial_flow_fact", sa.Column("resolution_method", sa.String(length=32), nullable=True))
    op.add_column("financial_flow_fact", sa.Column("reviewed_by", sa.Text(), nullable=True))
    op.add_column("financial_flow_fact", sa.Column("reviewed_at", sa.DateTime(), nullable=True))
    op.add_column("financial_flow_fact", sa.Column("review_notes", sa.Text(), nullable=True))
    op.create_foreign_key(
        "fk_flow_fact_selected_candidate",
        "financial_flow_fact",
        "financial_flow_candidate",
        ["selected_candidate_id"],
        ["candidate_id"],
    )
    op.execute(
        "UPDATE financial_flow_fact SET resolution_status = 'auto', resolution_method = 'legacy' "
        "WHERE resolution_status IS NULL"
    )

    op.add_column("financial_stock_fact", sa.Column("selected_candidate_id", sa.BigInteger(), nullable=True))
    op.add_column("financial_stock_fact", sa.Column("resolution_status", sa.String(length=16), nullable=True))
    op.add_column("financial_stock_fact", sa.Column("resolution_method", sa.String(length=32), nullable=True))
    op.add_column("financial_stock_fact", sa.Column("reviewed_by", sa.Text(), nullable=True))
    op.add_column("financial_stock_fact", sa.Column("reviewed_at", sa.DateTime(), nullable=True))
    op.add_column("financial_stock_fact", sa.Column("review_notes", sa.Text(), nullable=True))
    op.create_foreign_key(
        "fk_stock_fact_selected_candidate",
        "financial_stock_fact",
        "financial_stock_candidate",
        ["selected_candidate_id"],
        ["candidate_id"],
    )
    op.execute(
        "UPDATE financial_stock_fact SET resolution_status = 'auto', resolution_method = 'legacy' "
        "WHERE resolution_status IS NULL"
    )


def downgrade() -> None:
    op.drop_constraint("fk_stock_fact_selected_candidate", "financial_stock_fact", type_="foreignkey")
    op.drop_column("financial_stock_fact", "review_notes")
    op.drop_column("financial_stock_fact", "reviewed_at")
    op.drop_column("financial_stock_fact", "reviewed_by")
    op.drop_column("financial_stock_fact", "resolution_method")
    op.drop_column("financial_stock_fact", "resolution_status")
    op.drop_column("financial_stock_fact", "selected_candidate_id")

    op.drop_constraint("fk_flow_fact_selected_candidate", "financial_flow_fact", type_="foreignkey")
    op.drop_column("financial_flow_fact", "review_notes")
    op.drop_column("financial_flow_fact", "reviewed_at")
    op.drop_column("financial_flow_fact", "reviewed_by")
    op.drop_column("financial_flow_fact", "resolution_method")
    op.drop_column("financial_flow_fact", "resolution_status")
    op.drop_column("financial_flow_fact", "selected_candidate_id")

    op.drop_index("ix_stock_candidate_version_id", table_name="financial_stock_candidate")
    op.drop_index("ix_stock_candidate_as_of", table_name="financial_stock_candidate")
    op.drop_index("ix_stock_candidate_metric_id", table_name="financial_stock_candidate")
    op.drop_index("ix_stock_candidate_report_id", table_name="financial_stock_candidate")
    op.drop_table("financial_stock_candidate")

    op.drop_index("ix_flow_candidate_version_id", table_name="financial_flow_candidate")
    op.drop_index("ix_flow_candidate_period_end", table_name="financial_flow_candidate")
    op.drop_index("ix_flow_candidate_metric_id", table_name="financial_flow_candidate")
    op.drop_index("ix_flow_candidate_report_id", table_name="financial_flow_candidate")
    op.drop_table("financial_flow_candidate")
