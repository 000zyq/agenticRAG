from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.ingest.financial_report import (
    _extract_numbers,
    _strip_numbers,
    _detect_units,
    _parse_date_from_text,
    _guess_column_labels,
)


def test_extract_numbers_parses_commas_and_parentheses() -> None:
    line = "Revenue 1,234 (5,678) 9"
    cells = _extract_numbers(line)
    values = [c.value for c in cells]
    assert values == [Decimal("1234"), Decimal("-5678"), Decimal("9")]


def test_strip_numbers_removes_numeric_tokens() -> None:
    line = "Operating Income 12,345"
    assert _strip_numbers(line) == "Operating Income"


def test_detect_units_usd() -> None:
    currency, units = _detect_units("Amounts in USD")
    assert currency == "USD"
    assert units is None


def test_parse_date_from_text() -> None:
    assert _parse_date_from_text("As of 2024-12-31") == date(2024, 12, 31)
    assert _parse_date_from_text("截至 2024 年 12 月 31 日") == date(2024, 12, 31)


def test_guess_column_labels_years() -> None:
    cols = _guess_column_labels(["For 2024 and 2023"], 2)
    labels = [c.label for c in cols]
    assert labels == ["2024", "2023"]
