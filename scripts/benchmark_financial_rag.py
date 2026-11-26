"""Run paired GraphRAG and frozen Search Tools queries with identical model settings."""
import argparse
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


def run_arm(arm, question, source, doc_id, baseline, project, env, out, timeout):
    trace = out.with_suffix(".trace.jsonl")
    module = "scripts.chat_markdown" if arm == "search_tools" else "scripts.chat_financial_graph"
    args = ([str(source), question, "--engine", "mineru", "--backend", "milvus"] if arm == "search_tools"
            else [doc_id, question])
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
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    project = Path(__file__).resolve().parents[1]
    rows = []
    for index, item in enumerate(manifest["questions"]):
        report = reports[item["report"]]
        source = (args.manifest.parent / report["path"]).resolve()
        arms = ARMS if index % 2 == 0 else ARMS[::-1]
        for arm in arms:
            print(item["id"], arm, flush=True)
            result = run_arm(arm, item["question"], source, report["graph_doc_id"],
                             args.baseline.resolve(), project, env, out / f"{index:03d}-{arm}.json", args.timeout)
            rows.append({"question_id": item["id"], "category": item["category"], "model": args.model,
                         "report": item["report"], **result})
            (out / "runs.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
