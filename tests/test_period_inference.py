from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from scripts.ingest_financial_report import _infer_period_end, _infer_period_start


def _col(label: str, period_end: date | None = None, fiscal_year: int | None = None):
    return SimpleNamespace(label=label, period_end=period_end, fiscal_year=fiscal_year)


def _meta(report_type: str, period_end: date):
    return SimpleNamespace(report_type=report_type, period_end=period_end)


def test_annual_current_prior_period_dates() -> None:
    meta = _meta("annual", date(2024, 12, 31))

    current_end = _infer_period_end(_col("本期"), meta)
    prior_end = _infer_period_end(_col("上期"), meta)

    assert current_end == date(2024, 12, 31)
    assert prior_end == date(2023, 12, 31)
    assert _infer_period_start(meta.report_type, current_end) == date(2024, 1, 1)
    assert _infer_period_start(meta.report_type, prior_end) == date(2023, 1, 1)


def test_quarter_q2_current_prior_period_dates() -> None:
    meta = _meta("q2", date(2024, 6, 30))

    current_end = _infer_period_end(_col("本期"), meta)
    prior_end = _infer_period_end(_col("上期"), meta)

    assert current_end == date(2024, 6, 30)
    assert prior_end == date(2024, 3, 31)
    assert _infer_period_start(meta.report_type, current_end) == date(2024, 4, 1)
    assert _infer_period_start(meta.report_type, prior_end) == date(2024, 1, 1)


def test_quarter_q1_prior_is_previous_q4() -> None:
    meta = _meta("q1", date(2024, 3, 31))

    prior_end = _infer_period_end(_col("上期"), meta)

    assert prior_end == date(2023, 12, 31)
    assert _infer_period_start(meta.report_type, prior_end) == date(2023, 10, 1)


def test_prior_label_overrides_shared_column_period_end() -> None:
    meta = _meta("annual", date(2024, 12, 31))

    # Some parsers assign the same end-date to both 本期/上期 columns.
    prior_end = _infer_period_end(_col("上期", period_end=date(2024, 12, 31)), meta)

    assert prior_end == date(2023, 12, 31)
