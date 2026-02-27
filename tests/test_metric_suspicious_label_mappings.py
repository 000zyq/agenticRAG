from app.ingest.metric_defs import match_metric


def test_suspicious_labels_map_to_expected_metrics() -> None:
    cases = [
        ("balance", "少数股东权益", "minority_interest"),
        ("balance", "少数股东权益合计", "minority_interest"),
        ("balance", "减：库存股", "treasury_stock"),
        ("cashflow", "其中：子公司支付给少数股东的股利、利润", "cash_paid_dividends_to_minority"),
        ("cashflow", "子公司支付给少数股东的股利、利润", "cash_paid_dividends_to_minority"),
        ("cashflow", "子公司吸收少数股东投资收到的现金", "cash_received_other_financing"),
        ("income", "减：所得税费用", "income_tax"),
        ("income", "减：利息收入", "interest_income"),
        ("income", "少数股东损益", "net_profit_minority"),
        ("income", "减：营业外支出", "non_operating_expense"),
        ("income", "加：营业外收入", "non_operating_income"),
        ("income", "其中：营业成本", "operating_cost"),
        ("income", "归属于少数股东的综合收益总额", "total_comprehensive_income_minority"),
    ]
    for statement_type, label, expected in cases:
        metric = match_metric(label, statement_type)
        assert metric is not None, f"no match for {statement_type}:{label}"
        assert metric["metric_code"] == expected, f"unexpected match for {statement_type}:{label}"
