"""widen unit fields for facts

Revision ID: 007_widen_unit_fields
Revises: 006_statement_facts
Create Date: 2026-02-07
"""

from alembic import op
import sqlalchemy as sa

revision = "007_widen_unit_fields"
down_revision = "006_statement_facts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("metric", "unit_default", type_=sa.Text())
    op.alter_column("financial_flow_fact", "unit", type_=sa.Text())
    op.alter_column("financial_stock_fact", "unit", type_=sa.Text())


def downgrade() -> None:
    op.alter_column("financial_stock_fact", "unit", type_=sa.String(length=16))
    op.alter_column("financial_flow_fact", "unit", type_=sa.String(length=16))
    op.alter_column("metric", "unit_default", type_=sa.String(length=16))
