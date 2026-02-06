from __future__ import annotations

import json
import os
import sys
import urllib.request

import psycopg
from pymilvus import connections, utility

from app.config import get_settings


def check_postgres():
    settings = get_settings()
    print("[Postgres] connecting...")
    conn = psycopg.connect(settings.postgres_dsn)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM documents")
        count = cur.fetchone()[0]
        cur.execute("SELECT path, status, num_chunks FROM documents ORDER BY updated_at DESC LIMIT 5")
        rows = cur.fetchall()
    conn.close()
    print(f"[Postgres] documents count: {count}")
    for r in rows:
        print(f"[Postgres] recent: {r}")


def check_milvus():
    settings = get_settings()
    print("[Milvus] connecting...")
    connections.connect(uri=settings.milvus_uri, token=settings.milvus_token or "")
    exists = utility.has_collection(settings.milvus_collection)
    print(f"[Milvus] collection exists: {exists}")
    if exists:
        from pymilvus import Collection

        col = Collection(settings.milvus_collection)
        col.load()
        print(f"[Milvus] entity count: {col.num_entities}")


def call_chat():
    url = os.getenv("QA_URL", "http://127.0.0.1:8000/chat")
    api_key = os.getenv("API_KEY", "change-me")
    message = os.getenv("QA_MESSAGE", "这份2024年年报的主要经营情况有哪些？")

    payload = {"message": message}
    req = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-API-Key": api_key},
    )

    print(f"[QA] POST {url}")
    with urllib.request.urlopen(req, timeout=120) as resp:
        print(f"[QA] status: {resp.status}")
        print(resp.read().decode("utf-8"))


def main():
    try:
        check_postgres()
    except Exception as e:
        print("[Postgres] ERROR:", e)

    try:
        check_milvus()
    except Exception as e:
        print("[Milvus] ERROR:", e)

    try:
        call_chat()
    except Exception as e:
        print("[QA] ERROR:", e)


if __name__ == "__main__":
    sys.exit(main())
