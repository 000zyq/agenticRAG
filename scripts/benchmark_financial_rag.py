"""Run paired GraphRAG and frozen Search Tools queries with identical model settings."""
import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys
from time import perf_counter

from dotenv import dotenv_values


BASELINE = "c024721"
ARMS = ("search_tools", "graphrag")
MODELS = ("qwen3.8-max-0902", "deepseek-v4.1-flash",
          "deepseek-v4.1-flash-1017", "kimi-k3.1-pro")


def git(worktree, *args):
    return subprocess.check_output(["git", "-C", str(worktree), *args], text=True).strip()


def file_digest(path):
    with path.open("rb") as source:
        digest = sha256()
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def trace_metrics(path):
    if not path.exists():
        return {"model_calls": 0, "tool_calls": 0, "unreported_usage_calls": 0, "usage": {}}
    with path.open(encoding="utf-8") as stream:
        events = [json.loads(line) for line in stream if line.endswith("\n") and line.strip()]
    model = [row for row in events if row["event"] == "model"]
    usage = [row["usage"] for row in model if row.get("usage")]
    return {
        "model_calls": len(model),
        "tool_calls": sum(row["event"] == "tool_call" for row in events),
        "model_ms": sum(row["duration_ms"] for row in model),
        "unreported_usage_calls": len(model) - len(usage),
        "usage": {
            "input_tokens": sum(row.get("prompt_tokens") or 0 for row in usage),
            "cached_input_tokens": sum((row.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
                                       for row in usage),
            "output_tokens": sum(row.get("completion_tokens") or 0 for row in usage),
        },
    }


def run_arm(arm, question, source, doc_id, baseline, project, env, out, timeout, report_year=None):
    trace = out.with_suffix(".trace.jsonl")
    module = "scripts.chat_markdown" if arm == "search_tools" else "scripts.chat_financial_graph"
    args = ([str(source), question, "--engine", "mineru", "--backend", "milvus"] if arm == "search_tools"
            else [doc_id, question])
    if arm == "hybrid":
        module = "scripts.chat_financial_hybrid"
        args = [str(source), doc_id, question, "--baseline", str(baseline), "--report-year", str(report_year)]
    started = perf_counter()
    try:
        response = subprocess.run(
            [sys.executable, "-m", module, *args, "--trace", str(trace)],
            cwd=baseline if arm == "search_tools" else project, env=env,
            capture_output=True, text=True, timeout=timeout, check=False,
        )
        result = {"status": "ok" if response.returncode == 0 else "failed",
                  "answer": response.stdout.strip(), "stderr": response.stderr.strip()[-2000:]}
    except subprocess.TimeoutExpired:
        result = {"status": "timeout", "answer": "", "stderr": "question exceeded deadline"}
    result.update(arm=arm, elapsed_seconds=perf_counter() - started, trace=trace.name, **trace_metrics(trace))
    if arm == "hybrid" and trace.exists():
        events = [json.loads(line) for line in trace.read_text().splitlines() if line.strip()]
        result["routing"] = next((row for row in events if row["event"] == "route"), None)
        result["fallback_ms"] = next((row["duration_ms"] for row in events
                                      if row["event"] == "fallback_duration"), None)
        result["unreported_usage_calls"] += sum(row["event"] == "unreported_route_call" for row in events)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--mineru-output", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", default=MODELS[0], choices=MODELS)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--hybrid-only", action="store_true")
    args = parser.parse_args()
    if not git(args.baseline, "rev-parse", "HEAD").startswith(BASELINE) or git(
        args.baseline, "status", "--porcelain", "--untracked-files=no"
    ):
        parser.error(f"Baseline must be a clean tracked checkout of {BASELINE}")
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    reports = manifest["reports"]
    env = {**os.environ, **{k: v for k, v in dotenv_values(args.env_file).items() if v is not None},
           "OPENAI_CHAT_MODEL": args.model, "MINERU_OUTPUT_DIR": str(args.mineru_output.resolve()),
           "MINERU_CMD": "false"}
    project = Path(__file__).resolve().parents[1]
    freeze = {}
    for name, report in reports.items():
        if args.hybrid_only and type(report.get("report_year")) is not int:
            parser.error(f"Hybrid requires report_year metadata for {name}")
        pdf = (args.manifest.parent / report["path"]).resolve()
        candidates = sorted((args.mineru_output / pdf.stem).rglob("*_content_list.json"))
        if len(candidates) != 1:
            parser.error(f"Expected exactly one cached content list for report {name}")
        freeze[name] = {"pdf_sha256": file_digest(pdf), "content_list_sha256": file_digest(candidates[0]),
                        "graph_doc_id": report["graph_doc_id"]}
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    (out / "experiment.json").write_text(json.dumps({
        "model": args.model, "baseline_commit": git(args.baseline, "rev-parse", "HEAD"),
        "graph_commit": git(project, "rev-parse", "HEAD"),
        "graph_tracked_patch_sha256": sha256(git(project, "diff", "HEAD").encode()).hexdigest(),
        "manifest_sha256": file_digest(args.manifest), "reports": freeze, "timeout_seconds": args.timeout,
        "arms": ["hybrid"] if args.hybrid_only else ARMS,
    }, indent=2), encoding="utf-8")
    rows = []
    for index, item in enumerate(manifest["questions"]):
        report = reports[item["report"]]
        source = (args.manifest.parent / report["path"]).resolve()
        arms = ("hybrid",) if args.hybrid_only else ARMS if index % 2 == 0 else ARMS[::-1]
        for arm in arms:
            print(item["id"], arm, flush=True)
            result = run_arm(arm, item["question"], source, report["graph_doc_id"],
                             args.baseline.resolve(), project, env, out / f"{index:03d}-{arm}.json", args.timeout,
                             report.get("report_year"))
            rows.append({"question_id": item["id"], "category": item["category"], "model": args.model,
                         "report": item["report"], **result})
            (out / "runs.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
            if result["status"] == "failed" and "insufficient_quota" in result["stderr"]:
                raise SystemExit("Model quota exhausted; partial runs saved. Resume in a new output directory.")


if __name__ == "__main__":
    main()
