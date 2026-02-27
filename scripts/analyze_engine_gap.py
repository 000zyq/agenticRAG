from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
import os

import psycopg
from dotenv import load_dotenv


@dataclass(frozen=True)
class FlowKey:
    metric_code: str
    period_start_date: object
    period_end_date: object
    unit: str | None
    currency: str | None
    consolidation_scope: str | None


@dataclass(frozen=True)
class StockKey:
    metric_code: str
    as_of_date: object
    unit: str | None
    currency: str | None
    consolidation_scope: str | None


def _fmt_pages(values: list[object]) -> str:
    pages = sorted({int(v) for v in values if v is not None})
    return "|".join(str(v) for v in pages)


def _fmt_values(values: list[object]) -> str:
    uniq = []
    seen = set()
    for v in values:
        if v is None:
            continue
        s = str(v)
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    return "|".join(uniq[:10])


def _fmt_text(values: list[object], limit: int = 6) -> str:
    uniq = []
    seen = set()
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    return "|".join(uniq[:limit])


def _load_latest_versions(cur, report_id: int) -> dict[str, int]:
    cur.execute(
        """
        SELECT DISTINCT ON (parse_method) parse_method, version_id
        FROM report_versions
        WHERE report_id = %s AND parse_method IN ('pypdf', 'mineru')
        ORDER BY parse_method, version_id DESC
        """,
        (report_id,),
    )
    return {str(row[0]): int(row[1]) for row in cur.fetchall()}


def _load_flow_rows(cur, report_id: int, version_ids: list[int]) -> list[tuple]:
    cur.execute(
        """
        SELECT c.version_id, m.metric_code, c.period_start_date, c.period_end_date,
               c.unit, c.currency, c.consolidation_scope, c.value,
               st.source_page, st.raw_label, st.column_label
        FROM financial_flow_candidate c
        JOIN metric m ON m.metric_id = c.metric_id
        LEFT JOIN source_trace st ON st.trace_id = c.source_trace_id
        WHERE c.report_id = %s AND c.version_id = ANY(%s)
        """,
        (report_id, version_ids),
    )
    return cur.fetchall()


def _load_stock_rows(cur, report_id: int, version_ids: list[int]) -> list[tuple]:
    cur.execute(
        """
        SELECT c.version_id, m.metric_code, c.as_of_date,
               c.unit, c.currency, c.consolidation_scope, c.value,
               st.source_page, st.raw_label, st.column_label
        FROM financial_stock_candidate c
        JOIN metric m ON m.metric_id = c.metric_id
        LEFT JOIN source_trace st ON st.trace_id = c.source_trace_id
        WHERE c.report_id = %s AND c.version_id = ANY(%s)
        """,
        (report_id, version_ids),
    )
    return cur.fetchall()


def _write_flow_csv(path: Path, items: list[tuple[FlowKey, list[tuple]]]) -> None:
    path.write_text(
        "metric_code,period_start_date,period_end_date,unit,currency,consolidation_scope,row_count,pages,values,raw_labels,column_labels\n",
        encoding="utf-8",
    )
    with path.open("a", encoding="utf-8") as f:
        for key, rows in items:
            pages = _fmt_pages([r[8] for r in rows])
            values = _fmt_values([r[7] for r in rows])
            labels = _fmt_text([r[9] for r in rows])
            cols = _fmt_text([r[10] for r in rows])
            line = [
                key.metric_code,
                str(key.period_start_date or ""),
                str(key.period_end_date or ""),
                str(key.unit or ""),
                str(key.currency or ""),
                str(key.consolidation_scope or ""),
                str(len(rows)),
                pages,
                values,
                labels,
                cols,
            ]
            f.write(",".join(f'\"{v.replace(chr(34), chr(34) * 2)}\"' for v in line) + "\n")


def _write_stock_csv(path: Path, items: list[tuple[StockKey, list[tuple]]]) -> None:
    path.write_text(
        "metric_code,as_of_date,unit,currency,consolidation_scope,row_count,pages,values,raw_labels,column_labels\n",
        encoding="utf-8",
    )
    with path.open("a", encoding="utf-8") as f:
        for key, rows in items:
            pages = _fmt_pages([r[7] for r in rows])
            values = _fmt_values([r[6] for r in rows])
            labels = _fmt_text([r[8] for r in rows])
            cols = _fmt_text([r[9] for r in rows])
            line = [
                key.metric_code,
                str(key.as_of_date or ""),
                str(key.unit or ""),
                str(key.currency or ""),
                str(key.consolidation_scope or ""),
                str(len(rows)),
                pages,
                values,
                labels,
                cols,
            ]
            f.write(",".join(f'\"{v.replace(chr(34), chr(34) * 2)}\"' for v in line) + "\n")


def _top_metric_counts(keys: list[object]) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter(getattr(key, "metric_code") for key in keys)
    return counter.most_common(20)


def _value_conflicts(flow_by_engine: dict[str, dict[FlowKey, list[tuple]]], stock_by_engine: dict[str, dict[StockKey, list[tuple]]]) -> tuple[int, int, list[tuple[str, int]], list[tuple[str, int]]]:
    flow_overlap = set(flow_by_engine["pypdf"]).intersection(flow_by_engine["mineru"])
    stock_overlap = set(stock_by_engine["pypdf"]).intersection(stock_by_engine["mineru"])

    flow_conf_metric: Counter[str] = Counter()
    stock_conf_metric: Counter[str] = Counter()

    flow_conf = 0
    for key in flow_overlap:
        a = {str(row[7]) for row in flow_by_engine["pypdf"][key] if row[7] is not None}
        b = {str(row[7]) for row in flow_by_engine["mineru"][key] if row[7] is not None}
        if a != b:
            flow_conf += 1
            flow_conf_metric[key.metric_code] += 1

    stock_conf = 0
    for key in stock_overlap:
        a = {str(row[6]) for row in stock_by_engine["pypdf"][key] if row[6] is not None}
        b = {str(row[6]) for row in stock_by_engine["mineru"][key] if row[6] is not None}
        if a != b:
            stock_conf += 1
            stock_conf_metric[key.metric_code] += 1

    return flow_conf, stock_conf, flow_conf_metric.most_common(20), stock_conf_metric.most_common(20)


def analyze(report_id: int, output_dir: Path) -> dict:
    load_dotenv(Path(".env"))
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is not set")

    output_dir.mkdir(parents=True, exist_ok=True)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            latest = _load_latest_versions(cur, report_id)
            if {"pypdf", "mineru"} - set(latest):
                raise RuntimeError(f"missing latest versions for engines: {latest}")
            version_to_engine = {latest["pypdf"]: "pypdf", latest["mineru"]: "mineru"}

            flow_rows = _load_flow_rows(cur, report_id, list(version_to_engine))
            stock_rows = _load_stock_rows(cur, report_id, list(version_to_engine))

    flow_by_engine: dict[str, dict[FlowKey, list[tuple]]] = {"pypdf": defaultdict(list), "mineru": defaultdict(list)}
    for row in flow_rows:
        engine = version_to_engine[int(row[0])]
        key = FlowKey(
            metric_code=str(row[1]),
            period_start_date=row[2],
            period_end_date=row[3],
            unit=row[4],
            currency=row[5],
            consolidation_scope=row[6],
        )
        flow_by_engine[engine][key].append(row)

    stock_by_engine: dict[str, dict[StockKey, list[tuple]]] = {"pypdf": defaultdict(list), "mineru": defaultdict(list)}
    for row in stock_rows:
        engine = version_to_engine[int(row[0])]
        key = StockKey(
            metric_code=str(row[1]),
            as_of_date=row[2],
            unit=row[3],
            currency=row[4],
            consolidation_scope=row[5],
        )
        stock_by_engine[engine][key].append(row)

    flow_pypdf = set(flow_by_engine["pypdf"])
    flow_mineru = set(flow_by_engine["mineru"])
    stock_pypdf = set(stock_by_engine["pypdf"])
    stock_mineru = set(stock_by_engine["mineru"])

    flow_pypdf_only = sorted(flow_pypdf - flow_mineru, key=lambda k: (k.metric_code, str(k.period_end_date), str(k.consolidation_scope or "")))
    flow_mineru_only = sorted(flow_mineru - flow_pypdf, key=lambda k: (k.metric_code, str(k.period_end_date), str(k.consolidation_scope or "")))
    stock_pypdf_only = sorted(stock_pypdf - stock_mineru, key=lambda k: (k.metric_code, str(k.as_of_date), str(k.consolidation_scope or "")))
    stock_mineru_only = sorted(stock_mineru - stock_pypdf, key=lambda k: (k.metric_code, str(k.as_of_date), str(k.consolidation_scope or "")))

    _write_flow_csv(
        output_dir / f"engine_gap_flow_pypdf_only_report{report_id}.csv",
        [(key, flow_by_engine["pypdf"][key]) for key in flow_pypdf_only],
    )
    _write_flow_csv(
        output_dir / f"engine_gap_flow_mineru_only_report{report_id}.csv",
        [(key, flow_by_engine["mineru"][key]) for key in flow_mineru_only],
    )
    _write_stock_csv(
        output_dir / f"engine_gap_stock_pypdf_only_report{report_id}.csv",
        [(key, stock_by_engine["pypdf"][key]) for key in stock_pypdf_only],
    )
    _write_stock_csv(
        output_dir / f"engine_gap_stock_mineru_only_report{report_id}.csv",
        [(key, stock_by_engine["mineru"][key]) for key in stock_mineru_only],
    )

    flow_conf, stock_conf, flow_conf_top, stock_conf_top = _value_conflicts(flow_by_engine, stock_by_engine)

    summary = {
        "report_id": report_id,
        "latest_versions": latest,
        "flow": {
            "pypdf_keys": len(flow_pypdf),
            "mineru_keys": len(flow_mineru),
            "overlap_keys": len(flow_pypdf & flow_mineru),
            "pypdf_only": len(flow_pypdf_only),
            "mineru_only": len(flow_mineru_only),
            "top_pypdf_only_metrics": _top_metric_counts(flow_pypdf_only),
            "top_mineru_only_metrics": _top_metric_counts(flow_mineru_only),
            "overlap_value_conflicts": flow_conf,
            "top_value_conflict_metrics": flow_conf_top,
        },
        "stock": {
            "pypdf_keys": len(stock_pypdf),
            "mineru_keys": len(stock_mineru),
            "overlap_keys": len(stock_pypdf & stock_mineru),
            "pypdf_only": len(stock_pypdf_only),
            "mineru_only": len(stock_mineru_only),
            "top_pypdf_only_metrics": _top_metric_counts(stock_pypdf_only),
            "top_mineru_only_metrics": _top_metric_counts(stock_mineru_only),
            "overlap_value_conflicts": stock_conf,
            "top_value_conflict_metrics": stock_conf_top,
        },
    }

    summary_path = output_dir / f"engine_gap_summary_report{report_id}.txt"
    lines = [
        f"report_id={report_id}",
        f"pypdf_version={latest['pypdf']} mineru_version={latest['mineru']}",
        "",
        "[flow]",
        f"keys: pypdf={summary['flow']['pypdf_keys']} mineru={summary['flow']['mineru_keys']} overlap={summary['flow']['overlap_keys']}",
        f"only: pypdf={summary['flow']['pypdf_only']} mineru={summary['flow']['mineru_only']}",
        f"overlap_value_conflicts={summary['flow']['overlap_value_conflicts']}",
        "top_pypdf_only_metrics=" + " ".join(f"{k}:{v}" for k, v in summary['flow']['top_pypdf_only_metrics'][:10]),
        "top_mineru_only_metrics=" + " ".join(f"{k}:{v}" for k, v in summary['flow']['top_mineru_only_metrics'][:10]),
        "top_flow_value_conflict_metrics=" + " ".join(f"{k}:{v}" for k, v in summary['flow']['top_value_conflict_metrics'][:10]),
        "",
        "[stock]",
        f"keys: pypdf={summary['stock']['pypdf_keys']} mineru={summary['stock']['mineru_keys']} overlap={summary['stock']['overlap_keys']}",
        f"only: pypdf={summary['stock']['pypdf_only']} mineru={summary['stock']['mineru_only']}",
        f"overlap_value_conflicts={summary['stock']['overlap_value_conflicts']}",
        "top_pypdf_only_metrics=" + " ".join(f"{k}:{v}" for k, v in summary['stock']['top_pypdf_only_metrics'][:10]),
        "top_mineru_only_metrics=" + " ".join(f"{k}:{v}" for k, v in summary['stock']['top_mineru_only_metrics'][:10]),
        "top_stock_value_conflict_metrics=" + " ".join(f"{k}:{v}" for k, v in summary['stock']['top_value_conflict_metrics'][:10]),
        "",
    ]
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze pypdf/mineru coverage gap on candidate facts.")
    parser.add_argument("report_id", type=int, help="report_id in database")
    parser.add_argument("--output-dir", default="tmp", help="output directory for csv/txt artifacts")
    args = parser.parse_args()

    summary = analyze(args.report_id, Path(args.output_dir))
    print(summary)


if __name__ == "__main__":
    main()
