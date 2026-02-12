from __future__ import annotations

import json
from pathlib import Path

from app.ingest.metric_defs import match_metric


def test_metric_match_required_cases() -> None:
    path = Path("tests/fixtures/metric_match_cases.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    cases = [case for case in payload.get("cases", []) if bool(case.get("required", True))]

    total = len(cases)
    exact = 0
    fp = 0
    neg = 0
    for case in cases:
        expected = case.get("expected_metric_code")
        predicted = match_metric(case["label"], case["statement_type"])
        predicted_code = predicted["metric_code"] if predicted else None
        if predicted_code == expected:
            exact += 1
        if expected is None:
            neg += 1
            if predicted_code is not None:
                fp += 1

    exact_rate = exact / total if total else 0.0
    fp_rate = fp / neg if neg else 0.0

    assert exact_rate >= 0.90
    assert fp_rate <= 0.10
