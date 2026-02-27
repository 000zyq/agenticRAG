from __future__ import annotations

from datetime import date

from app.api.fact_review import _matches_flow_period, _matches_stock_period


def test_matches_flow_period_annual() -> None:
    assert _matches_flow_period(date(2024, 1, 1), date(2024, 12, 31), 2024, "annual") is True
    assert _matches_flow_period(date(2024, 4, 1), date(2024, 12, 31), 2024, "annual") is False


def test_matches_flow_period_quarter() -> None:
    assert _matches_flow_period(date(2024, 1, 1), date(2024, 3, 31), 2024, "q1") is True
    assert _matches_flow_period(date(2024, 4, 1), date(2024, 6, 30), 2024, "q1") is False


def test_matches_stock_period() -> None:
    assert _matches_stock_period(date(2024, 3, 31), 2024, "q1") is True
    assert _matches_stock_period(date(2024, 12, 31), 2024, "q1") is False
    assert _matches_stock_period(date(2023, 12, 31), 2024, "annual") is False
