from __future__ import annotations

from contextlib import contextmanager
import psycopg

from app.config import get_settings


@contextmanager
def get_conn():
    settings = get_settings()
    conn = psycopg.connect(settings.postgres_dsn)
    try:
        yield conn
    finally:
        conn.close()
