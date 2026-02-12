from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

from app.ingest.metric_defs import DICTIONARY_PATH, metric_name_en_from_code
from app.storage.db import get_conn


def _load_dictionary(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    items = data.get("metrics") if isinstance(data, dict) else data
    if not isinstance(items, list):
        raise ValueError("Dictionary file must contain a list of metrics or a metrics field.")

    normalized: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        metric_code = item.get("metric_code")
        metric_name_cn = item.get("metric_name_cn")
        statement_type = item.get("statement_type")
        value_nature = item.get("value_nature")
        if not metric_code or not metric_name_cn or not statement_type or not value_nature:
            continue
        normalized.append(
            {
                "metric_code": metric_code,
                "metric_name_cn": metric_name_cn,
                "metric_name_en": item.get("metric_name_en") or metric_name_en_from_code(metric_code),
                "statement_type": statement_type,
                "value_nature": value_nature,
                "parent_metric_code": item.get("parent_metric_code"),
                "patterns": list(item.get("patterns") or item.get("patterns_cn") or []),
                "patterns_exact": list(item.get("patterns_exact") or item.get("patterns_cn_exact") or []),
                "patterns_en": list(item.get("patterns_en") or []),
                "patterns_en_exact": list(item.get("patterns_en_exact") or []),
            }
        )
    if not normalized:
        raise ValueError("No valid metrics found in dictionary file.")
    return normalized


def _file_hash(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _upsert_metrics(cur, metrics: list[dict], now: datetime) -> dict[str, int]:
    codes = [metric["metric_code"] for metric in metrics]
    cur.execute("SELECT metric_id, metric_code FROM metric WHERE metric_code = ANY(%s)", (codes,))
    existing = {row[1]: int(row[0]) for row in cur.fetchall()}

    for metric in metrics:
        code = metric["metric_code"]
        if code in existing:
            cur.execute(
                """
                UPDATE metric
                SET metric_name_cn = %s,
                    metric_name_en = %s,
                    statement_type = %s,
                    value_nature = %s
                WHERE metric_code = %s
                """,
                (
                    metric["metric_name_cn"],
                    metric["metric_name_en"],
                    metric["statement_type"],
                    metric["value_nature"],
                    code,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO metric (
                    metric_code, metric_name_cn, metric_name_en, statement_type, value_nature,
                    unit_default, sign_rule, extra, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING metric_id
                """,
                (
                    code,
                    metric["metric_name_cn"],
                    metric["metric_name_en"],
                    metric["statement_type"],
                    metric["value_nature"],
                    None,
                    "normal",
                    None,
                    now,
                ),
            )
            existing[code] = int(cur.fetchone()[0])

    return existing


def _update_parents(cur, metrics: list[dict], code_to_id: dict[str, int]) -> None:
    for metric in metrics:
        parent_code = metric.get("parent_metric_code")
        parent_id = code_to_id.get(parent_code) if parent_code else None
        cur.execute(
            "UPDATE metric SET parent_metric_id = %s WHERE metric_code = %s",
            (parent_id, metric["metric_code"]),
        )


def _sync_aliases(cur, metrics: list[dict], code_to_id: dict[str, int], now: datetime) -> None:
    metric_ids = [code_to_id[metric["metric_code"]] for metric in metrics]
    cur.execute("DELETE FROM metric_alias WHERE metric_id = ANY(%s)", (metric_ids,))

    for metric in metrics:
        metric_id = code_to_id[metric["metric_code"]]
        for pattern in metric.get("patterns", []):
            cur.execute(
                """
                INSERT INTO metric_alias (metric_id, alias_text, language, match_mode, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (metric_id, pattern, "cn", "phrase", now),
            )
        for pattern in metric.get("patterns_exact", []):
            cur.execute(
                """
                INSERT INTO metric_alias (metric_id, alias_text, language, match_mode, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (metric_id, pattern, "cn", "exact", now),
            )
        for pattern in metric.get("patterns_en", []):
            cur.execute(
                """
                INSERT INTO metric_alias (metric_id, alias_text, language, match_mode, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (metric_id, pattern, "en", "phrase", now),
            )
        for pattern in metric.get("patterns_en_exact", []):
            cur.execute(
                """
                INSERT INTO metric_alias (metric_id, alias_text, language, match_mode, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (metric_id, pattern, "en", "exact", now),
            )


def _state_matches(cur, file_hash: str) -> bool:
    cur.execute("SELECT file_hash FROM metric_dictionary_state WHERE state_id = 1")
    row = cur.fetchone()
    if not row:
        return False
    return row[0] == file_hash


def _write_state(cur, file_hash: str, now: datetime) -> None:
    cur.execute(
        """
        INSERT INTO metric_dictionary_state (state_id, file_hash, updated_at)
        VALUES (1, %s, %s)
        ON CONFLICT (state_id)
        DO UPDATE SET file_hash = EXCLUDED.file_hash, updated_at = EXCLUDED.updated_at
        """,
        (file_hash, now),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync financial dictionary file into Postgres.")
    parser.add_argument("--path", default=str(DICTIONARY_PATH), help="Path to dictionary JSON file.")
    parser.add_argument("--force", action="store_true", help="Force sync even if hash unchanged.")
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        raise SystemExit(f"Dictionary file not found: {path}")

    metrics = _load_dictionary(path)
    file_hash = _file_hash(path)
    now = datetime.utcnow()

    with get_conn() as conn:
        with conn.cursor() as cur:
            if not args.force and _state_matches(cur, file_hash):
                print("Dictionary unchanged; skipping sync.")
                return

            code_to_id = _upsert_metrics(cur, metrics, now)
            _update_parents(cur, metrics, code_to_id)
            _sync_aliases(cur, metrics, code_to_id, now)
            _write_state(cur, file_hash, now)
        conn.commit()

    print(f"Synced {len(metrics)} metrics from {path}")


if __name__ == "__main__":
    main()
