"""metric dictionary tables

Revision ID: 009_metric_dictionary
Revises: 008_candidate_facts
Create Date: 2026-02-10
"""

from alembic import op
import sqlalchemy as sa

revision = "009_metric_dictionary"
down_revision = "008_candidate_facts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("metric", sa.Column("metric_name_en", sa.Text(), nullable=True))
    op.add_column("metric", sa.Column("parent_metric_id", sa.BigInteger(), nullable=True))
    op.create_foreign_key(
        "fk_metric_parent",
        "metric",
        "metric",
        ["parent_metric_id"],
        ["metric_id"],
    )

    op.create_table(
        "metric_alias",
        sa.Column("alias_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("metric_id", sa.BigInteger(), sa.ForeignKey("metric.metric_id"), nullable=False),
        sa.Column("alias_text", sa.Text(), nullable=False),
        sa.Column("language", sa.String(length=8), nullable=False),
        sa.Column("match_mode", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_metric_alias_metric_id", "metric_alias", ["metric_id"])
    op.create_index("ix_metric_alias_language", "metric_alias", ["language"])
    op.create_index("ix_metric_alias_match_mode", "metric_alias", ["match_mode"])

    op.create_table(
        "metric_dictionary_state",
        sa.Column("state_id", sa.Integer(), primary_key=True),
        sa.Column("file_hash", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("metric_dictionary_state")
    op.drop_index("ix_metric_alias_match_mode", table_name="metric_alias")
    op.drop_index("ix_metric_alias_language", table_name="metric_alias")
    op.drop_index("ix_metric_alias_metric_id", table_name="metric_alias")
    op.drop_table("metric_alias")
    op.drop_constraint("fk_metric_parent", "metric", type_="foreignkey")
    op.drop_column("metric", "parent_metric_id")
    op.drop_column("metric", "metric_name_en")
