from __future__ import annotations

import datetime as dt
from typing import Iterable

from app.storage.db import get_conn


def upsert_document(doc_id: str, path: str, file_hash: str, status: str, num_chunks: int) -> None:
    now = dt.datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO documents (doc_id, path, hash, status, num_chunks, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_id) DO UPDATE SET
                    path = EXCLUDED.path,
                    hash = EXCLUDED.hash,
                    status = EXCLUDED.status,
                    num_chunks = EXCLUDED.num_chunks,
                    updated_at = EXCLUDED.updated_at
                """,
                (doc_id, path, file_hash, status, num_chunks, now),
            )
        conn.commit()


def mark_document_status(doc_id: str, status: str) -> None:
    now = dt.datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE documents SET status = %s, updated_at = %s WHERE doc_id = %s",
                (status, now, doc_id),
            )
        conn.commit()


def get_document_by_hash(file_hash: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT doc_id, path, hash, status, num_chunks FROM documents WHERE hash = %s", (file_hash,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "doc_id": row[0],
                "path": row[1],
                "hash": row[2],
                "status": row[3],
                "num_chunks": row[4],
            }


def ensure_session(session_id: str) -> None:
    now = dt.datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sessions (session_id, created_at, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (session_id) DO UPDATE SET updated_at = EXCLUDED.updated_at
                """,
                (session_id, now, now),
            )
        conn.commit()


def append_message(session_id: str, role: str, content: str) -> None:
    now = dt.datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES (%s, %s, %s, %s)",
                (session_id, role, content, now),
            )
            cur.execute("UPDATE sessions SET updated_at = %s WHERE session_id = %s", (now, session_id))
        conn.commit()


def get_session_messages(session_id: str, limit: int = 20) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT role, content, timestamp
                FROM messages
                WHERE session_id = %s
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (session_id, limit),
            )
            rows = cur.fetchall()
    return [
        {"role": row[0], "content": row[1], "timestamp": row[2].isoformat() if row[2] else None}
        for row in reversed(rows)
    ]


def upsert_finqa_qa(qa_id: str, doc_id: str, question: str, answer: str | None, raw_json: str | None) -> None:
    now = dt.datetime.utcnow()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO finqa_qa (qa_id, doc_id, question, answer, raw_json, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (qa_id) DO UPDATE SET
                    doc_id = EXCLUDED.doc_id,
                    question = EXCLUDED.question,
                    answer = EXCLUDED.answer,
                    raw_json = EXCLUDED.raw_json
                """,
                (qa_id, doc_id, question, answer, raw_json, now),
            )
        conn.commit()

