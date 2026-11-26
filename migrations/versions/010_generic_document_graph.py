"""generic document graph schema

Revision ID: 010_generic_document_graph
Revises: 009_metric_dictionary
Create Date: 2026-03-23
"""

from alembic import op
import sqlalchemy as sa

revision = "010_generic_document_graph"
down_revision = "009_metric_dictionary"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "documents",
        sa.Column("graph_status", sa.String(length=32), nullable=False, server_default="not_requested"),
    )
    op.add_column("documents", sa.Column("graph_updated_at", sa.DateTime(), nullable=True))
    op.create_index("ix_documents_graph_status", "documents", ["graph_status"])

    op.create_table(
        "document_chunks",
        sa.Column("chunk_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("doc_id", sa.String(length=64), sa.ForeignKey("documents.doc_id"), nullable=False),
        sa.Column("chunk_order", sa.Integer(), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("source_position", sa.Integer(), nullable=True),
        sa.Column("page", sa.Integer(), nullable=True),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_document_chunks_doc_id", "document_chunks", ["doc_id"])
    op.create_index("ux_document_chunks_doc_order", "document_chunks", ["doc_id", "chunk_order"], unique=True)

    op.create_table(
        "graph_build_runs",
        sa.Column("run_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("doc_id", sa.String(length=64), sa.ForeignKey("documents.doc_id"), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("extractor_version", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("summary_json", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_index("ix_graph_build_runs_doc_id", "graph_build_runs", ["doc_id"])
    op.create_index("ix_graph_build_runs_status", "graph_build_runs", ["status"])

    op.create_table(
        "graph_entities",
        sa.Column("entity_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("doc_id", sa.String(length=64), sa.ForeignKey("documents.doc_id"), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column("normalized_name", sa.Text(), nullable=False),
        sa.Column("aliases", sa.JSON(), nullable=True),
        sa.Column("attributes", sa.JSON(), nullable=True),
        sa.Column("confidence", sa.Numeric(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_graph_entities_doc_id", "graph_entities", ["doc_id"])
    op.create_index("ix_graph_entities_doc_type", "graph_entities", ["doc_id", "entity_type"])
    op.create_index("ix_graph_entities_doc_norm", "graph_entities", ["doc_id", "normalized_name"])

    op.create_table(
        "graph_entity_mentions",
        sa.Column("mention_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("doc_id", sa.String(length=64), sa.ForeignKey("documents.doc_id"), nullable=False),
        sa.Column("entity_id", sa.BigInteger(), sa.ForeignKey("graph_entities.entity_id"), nullable=False),
        sa.Column("chunk_id", sa.BigInteger(), sa.ForeignKey("document_chunks.chunk_id"), nullable=True),
        sa.Column("chunk_order", sa.Integer(), nullable=True),
        sa.Column("mention_text", sa.Text(), nullable=False),
        sa.Column("start_char", sa.Integer(), nullable=True),
        sa.Column("end_char", sa.Integer(), nullable=True),
        sa.Column("confidence", sa.Numeric(), nullable=True),
        sa.Column("evidence_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_graph_entity_mentions_doc_id", "graph_entity_mentions", ["doc_id"])
    op.create_index("ix_graph_entity_mentions_entity_id", "graph_entity_mentions", ["entity_id"])

    op.create_table(
        "graph_relations",
        sa.Column("relation_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("doc_id", sa.String(length=64), sa.ForeignKey("documents.doc_id"), nullable=False),
        sa.Column("subject_entity_id", sa.BigInteger(), sa.ForeignKey("graph_entities.entity_id"), nullable=False),
        sa.Column("object_entity_id", sa.BigInteger(), sa.ForeignKey("graph_entities.entity_id"), nullable=False),
        sa.Column("predicate_text", sa.Text(), nullable=False),
        sa.Column("predicate_family", sa.String(length=64), nullable=True),
        sa.Column("confidence", sa.Numeric(), nullable=True),
        sa.Column("is_aggregated", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_graph_relations_doc_id", "graph_relations", ["doc_id"])
    op.create_index("ix_graph_relations_subject", "graph_relations", ["subject_entity_id"])
    op.create_index("ix_graph_relations_object", "graph_relations", ["object_entity_id"])

    op.create_table(
        "graph_relation_evidence",
        sa.Column("evidence_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("relation_id", sa.BigInteger(), sa.ForeignKey("graph_relations.relation_id"), nullable=False),
        sa.Column("chunk_id", sa.BigInteger(), sa.ForeignKey("document_chunks.chunk_id"), nullable=True),
        sa.Column("chunk_order", sa.Integer(), nullable=True),
        sa.Column("path_json", sa.JSON(), nullable=True),
        sa.Column("evidence_text", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Numeric(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_graph_relation_evidence_relation_id", "graph_relation_evidence", ["relation_id"])


def downgrade() -> None:
    op.drop_index("ix_graph_relation_evidence_relation_id", table_name="graph_relation_evidence")
    op.drop_table("graph_relation_evidence")

    op.drop_index("ix_graph_relations_object", table_name="graph_relations")
    op.drop_index("ix_graph_relations_subject", table_name="graph_relations")
    op.drop_index("ix_graph_relations_doc_id", table_name="graph_relations")
    op.drop_table("graph_relations")

    op.drop_index("ix_graph_entity_mentions_entity_id", table_name="graph_entity_mentions")
    op.drop_index("ix_graph_entity_mentions_doc_id", table_name="graph_entity_mentions")
    op.drop_table("graph_entity_mentions")

    op.drop_index("ix_graph_entities_doc_norm", table_name="graph_entities")
    op.drop_index("ix_graph_entities_doc_type", table_name="graph_entities")
    op.drop_index("ix_graph_entities_doc_id", table_name="graph_entities")
    op.drop_table("graph_entities")

    op.drop_index("ix_graph_build_runs_status", table_name="graph_build_runs")
    op.drop_index("ix_graph_build_runs_doc_id", table_name="graph_build_runs")
    op.drop_table("graph_build_runs")

    op.drop_index("ux_document_chunks_doc_order", table_name="document_chunks")
    op.drop_index("ix_document_chunks_doc_id", table_name="document_chunks")
    op.drop_table("document_chunks")

    op.drop_index("ix_documents_graph_status", table_name="documents")
    op.drop_column("documents", "graph_updated_at")
    op.drop_column("documents", "graph_status")
