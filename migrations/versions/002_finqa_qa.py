"""finqa qa table

Revision ID: 002_finqa_qa
Revises: 001_init
Create Date: 2026-02-04
"""

from alembic import op
import sqlalchemy as sa

revision = "002_finqa_qa"
down_revision = "001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "finqa_qa",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("qa_id", sa.String(length=128), nullable=False),
        sa.Column("doc_id", sa.String(length=64), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("answer", sa.Text(), nullable=True),
        sa.Column("raw_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_finqa_qa_qa_id", "finqa_qa", ["qa_id"], unique=True)
    op.create_index("ix_finqa_qa_doc_id", "finqa_qa", ["doc_id"])


def downgrade() -> None:
    op.drop_index("ix_finqa_qa_doc_id", table_name="finqa_qa")
    op.drop_index("ix_finqa_qa_qa_id", table_name="finqa_qa")
    op.drop_table("finqa_qa")
