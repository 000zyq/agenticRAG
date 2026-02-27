from app.ingest.metric_defs import match_metric


def _metric_code(label: str, statement_type: str) -> str | None:
    metric = match_metric(label, statement_type)
    return metric["metric_code"] if metric else None


def test_other_income_parent_and_children_are_separated() -> None:
    assert _metric_code("其他收益", "income") == "other_income"
    assert _metric_code("保险赔偿", "income") == "other_income_insurance_compensation"
    assert _metric_code("废品销售", "income") == "other_income_scrap_sales"


def test_depreciation_parent_and_children_are_separated() -> None:
    assert _metric_code("折旧及摊销", "cashflow") == "depreciation_amortization"
    assert _metric_code("固定资产折旧、油气资产折耗、生产性生物资产折旧", "cashflow") == "fixed_assets_depreciation"
    assert _metric_code("耗、生产性生物资产折旧", "cashflow") == "fixed_assets_depreciation"
    assert _metric_code("投资性房地产折旧", "cashflow") == "investment_property_depreciation"
    assert _metric_code("使用权资产折旧", "cashflow") == "right_of_use_assets_depreciation"
    assert _metric_code("无形资产摊销", "cashflow") == "intangible_assets_amortization"
    assert _metric_code("长期待摊费用摊销", "cashflow") == "long_term_prepaid_expenses_amortization"
