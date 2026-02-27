from app.ingest.metric_defs import match_metric


def test_minor_dividend_detail_not_mapped_to_other_financing() -> None:
    metric = match_metric("其中：子公司支付给少数股东的股利、利润", "cashflow")
    assert metric is not None
    assert metric["metric_code"] == "cash_paid_dividends_to_minority"


def test_other_financing_parent_still_maps() -> None:
    metric = match_metric("支付其他与筹资活动有关的现金", "cashflow")
    assert metric is not None
    assert metric["metric_code"] == "cash_paid_other_financing"
