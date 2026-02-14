from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

from scripts.ingest_financial_report import insert_report


def _run_resolver(report_id: int, min_agree: int, tolerance: str) -> None:
    cmd = [
        sys.executable,
        "scripts/resolve_fact_candidates.py",
        "--report-id",
        str(report_id),
        "--min-agree",
        str(min_agree),
        "--tolerance",
        tolerance,
    ]
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest a report with multiple engines.")
    parser.add_argument("path", help="Path to report PDF")
    parser.add_argument(
        "--engines",
        default="auto",
        help="Comma-separated engines to run (auto,pypdf,mineru). Order matters.",
    )
    parser.add_argument("--recompute", action="store_true", help="Recompute facts for existing report.")
    parser.add_argument("--min-agree", type=int, default=2, help="Minimum agreeing engines for consensus.")
    parser.add_argument("--tolerance", default="0.01", help="Value rounding tolerance for consensus.")
    parser.add_argument("--no-resolve", action="store_true", help="Skip consensus resolution.")
    parser.add_argument("--write-pages", action="store_true", help="Write report_pages when appending candidates.")
    parser.add_argument(
        "--engine-retries",
        type=int,
        default=2,
        help="Retry attempts per engine when ingestion fails (default: 2).",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=2.0,
        help="Delay between retry attempts.",
    )
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    engines = [item.strip() for item in args.engines.split(",") if item.strip()]
    if not engines:
        raise SystemExit("No engines specified.")

    report_id: int | None = None
    for idx, engine in enumerate(engines):
        engine_value = None if engine == "auto" else engine
        last_exc: Exception | None = None
        for attempt in range(1, max(args.engine_retries, 1) + 1):
            try:
                report_id = insert_report(
                    path,
                    recompute_facts=args.recompute and idx == 0,
                    candidates_only=True,
                    allow_existing=idx > 0,
                    write_pages=args.write_pages and idx > 0,
                    engine=engine_value,
                    parse_method_override=engine,
                )
                args.recompute = False
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if attempt < max(args.engine_retries, 1):
                    print(f"[warn] engine {engine} attempt {attempt} failed: {exc}; retrying...")
                    time.sleep(max(args.retry_delay_seconds, 0.0))
                else:
                    print(f"[warn] engine {engine} failed: {exc}")
        if last_exc is not None:
            continue

    if report_id is None:
        raise SystemExit("No engines succeeded.")

    if not args.no_resolve:
        _run_resolver(report_id, args.min_agree, args.tolerance)

    print(f"report_id={report_id}")


if __name__ == "__main__":
    main()
