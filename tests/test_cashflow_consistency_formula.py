from __future__ import annotations

from decimal import Decimal

from scripts.resolve_fact_candidates import _cashflow_net_increase_rhs


def test_cashflow_rhs_without_fx_effect() -> None:
    rhs = _cashflow_net_increase_rhs(
        Decimal("100"),
        Decimal("-40"),
        Decimal("15"),
        None,
    )
    assert rhs == Decimal("75")


def test_cashflow_rhs_with_fx_effect() -> None:
    rhs = _cashflow_net_increase_rhs(
        Decimal("100"),
        Decimal("-40"),
        Decimal("15"),
        Decimal("-2.5"),
    )
    assert rhs == Decimal("72.5")
