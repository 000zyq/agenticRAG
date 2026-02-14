from app.ingest import metric_defs as md


def _metric(metric_code: str, statement_type: str) -> dict:
    return {
        "metric_code": metric_code,
        "metric_name_cn": metric_code,
        "statement_type": statement_type,
        "value_nature": "stock" if statement_type == "balance" else "flow",
        "patterns": [],
        "patterns_exact": [],
        "patterns_en": [],
        "patterns_en_exact": [],
    }


def test_match_metric_uses_cas_sub_code_mapping(monkeypatch):
    metric = _metric("right_of_use_assets", "balance")
    monkeypatch.setattr(md, "METRIC_DEFS", [metric])
    monkeypatch.setattr(md, "METRIC_BY_CODE", {"right_of_use_assets": metric})
    monkeypatch.setattr(
        md,
        "CAS2020_MAPPING",
        {
            "by_sub_code": {"821110": ["right_of_use_assets"]},
            "by_sub_name": {"使用权资产": ["right_of_use_assets"]},
        },
    )

    matched = md.match_metric("[821110] 附注_使用权资产（已执行新准则）", "balance")
    assert matched is not None
    assert matched["metric_code"] == "right_of_use_assets"


def test_match_metric_cas_mapping_respects_statement_type(monkeypatch):
    balance_metric = _metric("contract_assets", "balance")
    income_metric = _metric("contract_assets_turnover", "income")
    monkeypatch.setattr(md, "METRIC_DEFS", [balance_metric, income_metric])
    monkeypatch.setattr(
        md,
        "METRIC_BY_CODE",
        {
            "contract_assets": balance_metric,
            "contract_assets_turnover": income_metric,
        },
    )
    monkeypatch.setattr(
        md,
        "CAS2020_MAPPING",
        {
            "by_sub_code": {"837270": ["contract_assets", "contract_assets_turnover"]},
            "by_sub_name": {"合同资产": ["contract_assets", "contract_assets_turnover"]},
        },
    )

    matched = md.match_metric("[837270] 附注_合同资产", "balance")
    assert matched is not None
    assert matched["metric_code"] == "contract_assets"


def test_match_metric_cas_mapping_skips_ambiguous(monkeypatch):
    metric_a = _metric("metric_a", "balance")
    metric_b = _metric("metric_b", "balance")
    monkeypatch.setattr(md, "METRIC_DEFS", [metric_a, metric_b])
    monkeypatch.setattr(
        md,
        "METRIC_BY_CODE",
        {
            "metric_a": metric_a,
            "metric_b": metric_b,
        },
    )
    monkeypatch.setattr(
        md,
        "CAS2020_MAPPING",
        {
            "by_sub_code": {"999999": ["metric_a", "metric_b"]},
            "by_sub_name": {},
        },
    )

    matched = md.match_metric("[999999] 示例科目", "balance")
    assert matched is None


def test_match_metric_cas_mapping_unmapped_falls_back_to_cas_code(monkeypatch):
    monkeypatch.setattr(md, "METRIC_DEFS", [])
    monkeypatch.setattr(md, "METRIC_BY_CODE", {})
    monkeypatch.setattr(
        md,
        "CAS2020_MAPPING",
        {
            "by_sub_code": {},
            "by_sub_name": {},
            "by_sub_code_unmapped": {"837460": "租赁负债"},
            "by_sub_name_unmapped": {"租赁负债": "837460"},
        },
    )

    matched = md.match_metric("租赁负债", "balance")
    assert matched is not None
    assert matched["metric_code"] == "cas2020_837460"
    assert matched["value_nature"] == "stock"
