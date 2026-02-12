from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.ingest.metric_defs import match_metric


def _safe_div(a: int, b: int) -> float:
    if b == 0:
        return 0.0
    return a / b


def evaluate_cases(cases: list[dict], required_only: bool) -> dict:
    selected = [c for c in cases if (not required_only) or bool(c.get("required", True))]
    total = len(selected)
    exact = 0
    tp = 0
    fp = 0
    fn = 0
    tn = 0
    errors: list[dict] = []

    for case in selected:
        label = str(case["label"])
        statement_type = str(case["statement_type"])
        expected = case.get("expected_metric_code")
        pred = match_metric(label, statement_type)
        pred_code = pred["metric_code"] if pred else None

        if pred_code == expected:
            exact += 1
        else:
            errors.append(
                {
                    "id": case.get("id"),
                    "label": label,
                    "statement_type": statement_type,
                    "expected": expected,
                    "predicted": pred_code,
                }
            )

        if expected is None and pred_code is None:
            tn += 1
        elif expected is None and pred_code is not None:
            fp += 1
        elif expected is not None and pred_code == expected:
            tp += 1
        else:
            fn += 1

    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _safe_div(2 * precision * recall, precision + recall) if precision + recall else 0.0

    return {
        "total": total,
        "exact_match": exact,
        "exact_match_rate": _safe_div(exact, total),
        "positive_precision": precision,
        "positive_recall": recall,
        "positive_f1": f1,
        "false_positive_rate_on_negatives": _safe_div(fp, fp + tn),
        "errors": errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate metric matching with labeled cases.")
    parser.add_argument("--cases", default="tests/fixtures/metric_match_cases.json")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    payload = json.loads(Path(args.cases).read_text(encoding="utf-8"))
    cases = payload.get("cases", [])

    required = evaluate_cases(cases, required_only=True)
    all_cases = evaluate_cases(cases, required_only=False)
    optional_only = evaluate_cases([c for c in cases if not bool(c.get("required", True))], required_only=False)

    result = {
        "required": required,
        "all_cases": all_cases,
        "optional_only": optional_only,
    }

    text = json.dumps(result, ensure_ascii=False, indent=2)
    print(text)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
