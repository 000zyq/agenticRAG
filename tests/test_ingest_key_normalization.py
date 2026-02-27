from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from scripts.ingest_financial_report import (
    _infer_column_scopes,
    _infer_period_end_for_cell,
    _infer_period_start,
    _normalize_unit_and_value,
)


def _col(label: str, period_end: date | None = None, fiscal_year: int | None = None):
    return SimpleNamespace(label=label, period_end=period_end, fiscal_year=fiscal_year)


def _table(title: str, section_title: str, is_consolidated: bool | None, columns: list[SimpleNamespace]):
    return SimpleNamespace(title=title, section_title=section_title, is_consolidated=is_consolidated, columns=columns)


def test_normalize_unit_and_value_scales_1k_to_base_unit() -> None:
    unit, value = _normalize_unit_and_value("1k", Decimal("12.5"))
    assert unit == "1"
    assert value == Decimal("12.5")


def test_infer_period_start_from_year_label_when_report_type_unknown() -> None:
    start = _infer_period_start("unknown", date(2024, 12, 31), "2024年度 合并")
    assert start == date(2024, 1, 1)


def test_infer_column_scopes_for_mixed_consolidated_and_company_table() -> None:
    table = _table(
        "2024 年度合并及公司现金流量表",
        "一、经营活动产生的现金流量",
        True,
        [
            _col("2024"),
            _col("2023"),
            _col("2024"),
            _col("2023"),
        ],
    )
    assert _infer_column_scopes(table) == ["consolidated", "consolidated", "parent", "parent"]


def test_infer_column_scopes_uses_explicit_column_scope_first() -> None:
    table = _table(
        "2024 年度合并及公司现金流量表",
        "",
        True,
        [
            _col("2024年度 合并"),
            _col("2024年度 公司"),
        ],
    )
    assert _infer_column_scopes(table) == ["consolidated", "parent"]


def _meta(report_type: str, period_end: date):
    return SimpleNamespace(report_type=report_type, period_end=period_end)


def test_infer_period_end_for_balance_generic_columns_alternates_current_prior() -> None:
    meta = _meta("annual", date(2024, 12, 31))
    table = _table(
        "合并及公司资产负债表",
        "",
        True,
        [
            _col("col_2", period_end=date(2024, 12, 31)),
            _col("col_3", period_end=date(2024, 12, 31)),
            _col("col_4", period_end=date(2024, 12, 31)),
            _col("col_5", period_end=date(2024, 12, 31)),
        ],
    )
    actual = [
        _infer_period_end_for_cell(table, col, idx, meta, "balance")
        for idx, col in enumerate(table.columns)
    ]
    assert actual == [
        date(2024, 12, 31),
        date(2023, 12, 31),
        date(2024, 12, 31),
        date(2023, 12, 31),
    ]


def test_infer_period_end_for_flow_does_not_override_generic_columns() -> None:
    meta = _meta("annual", date(2024, 12, 31))
    table = _table(
        "合并及公司现金流量表",
        "",
        True,
        [
            _col("col_2", period_end=date(2024, 12, 31)),
            _col("col_3", period_end=date(2024, 12, 31)),
        ],
    )
    actual = [
        _infer_period_end_for_cell(table, col, idx, meta, "cashflow")
        for idx, col in enumerate(table.columns)
    ]
    assert actual == [date(2024, 12, 31), date(2024, 12, 31)]
