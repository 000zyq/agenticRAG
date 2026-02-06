"""widen unit fields

Revision ID: 004_units_text
Revises: 003_financial_reports
Create Date: 2026-02-06
"""

from alembic import op
import sqlalchemy as sa

revision = "004_units_text"
down_revision = "003_financial_reports"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("financial_reports", "units", type_=sa.Text())
    op.alter_column("report_tables", "units", type_=sa.Text())
    op.alter_column("report_table_cells", "unit", type_=sa.Text())


def downgrade() -> None:
    op.alter_column("report_table_cells", "unit", type_=sa.String(length=32))
    op.alter_column("report_tables", "units", type_=sa.String(length=32))
    op.alter_column("financial_reports", "units", type_=sa.String(length=32))
