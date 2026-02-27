from __future__ import annotations

from decimal import Decimal

from scripts.eval_pdf2db_gt import _within_tolerance, load_gt_rows


def test_within_tolerance_abs_and_rel() -> None:
    assert _within_tolerance(Decimal("100.5"), Decimal("100"), Decimal("1"), Decimal("0"))
    assert _within_tolerance(Decimal("105"), Decimal("100"), Decimal("0"), Decimal("0.05"))
    assert not _within_tolerance(Decimal("106"), Decimal("100"), Decimal("0"), Decimal("0.05"))


def test_load_gt_rows_uses_default_report_id(tmp_path) -> None:
    gt = tmp_path / "gt.csv"
    gt.write_text(
        "statement_type,value_nature,metric_code,period_start_date,period_end_date,as_of_date,consolidation_scope,unit,currency,expected_value,abs_tol,rel_tol,note\n"
        "cashflow,flow,cash_received_from_sales,2024-01-01,2024-12-31,,consolidated,1,CNY,388661338,0,0,sample\n",
        encoding="utf-8",
    )

    rows = load_gt_rows(gt, default_report_id=3)
    assert len(rows) == 1
    row = rows[0]
    assert row.report_id == 3
    assert row.metric_code == "cash_received_from_sales"
    assert row.expected_value == Decimal("388661338")
