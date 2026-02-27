from app.ingest.metric_defs import match_metric


def test_main_business_revenue_prefers_specific_metric() -> None:
    metric = match_metric("主营业务收入", "income")
    assert metric is not None
    assert metric["metric_code"] == "main_business_revenue"


def test_operating_total_cost_prefers_specific_metric() -> None:
    metric = match_metric("营业总成本", "income")
    assert metric is not None
    assert metric["metric_code"] == "operating_total_cost"


def test_generic_balance_end_label_not_mapped() -> None:
    assert match_metric("期末账面余额", "balance") is None
    assert match_metric("Balance at end of period", "balance") is None
