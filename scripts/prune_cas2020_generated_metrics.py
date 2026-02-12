from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Prune auto-generated CAS2020 metrics from dictionary.")
    parser.add_argument("--dictionary", default="data/financial_dictionary.json")
    parser.add_argument("--prefix", default="cas2020_")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    dictionary_path = Path(args.dictionary)
    data = json.loads(dictionary_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "metrics" not in data:
        raise SystemExit("Dictionary must be an object with 'metrics'.")

    metrics = data["metrics"]
    before = len(metrics)
    filtered = [m for m in metrics if not str(m.get("metric_code", "")).startswith(args.prefix)]
    removed = before - len(filtered)
    data["metrics"] = filtered

    output_path = Path(args.output) if args.output else dictionary_path
    output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"before": before, "after": len(filtered), "removed": removed, "output": str(output_path)}))


if __name__ == "__main__":
    main()
