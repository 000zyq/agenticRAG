from app.ingest.metric_defs import match_metric


def test_cash_begin_matches_year_start_aliases() -> None:
    m1 = match_metric("加：年初现金及现金等价物余额", "cashflow")
    assert m1 is not None
    assert m1["metric_code"] == "cash_begin"

    m2 = match_metric("减：现金及现金等价物的年初余额", "cashflow")
    assert m2 is not None
    assert m2["metric_code"] == "cash_begin"


def test_cash_begin_aliases_not_bound_to_balance_cash_metric() -> None:
    m = match_metric("加：年初现金及现金等价物余额", "balance")
    assert m is None
