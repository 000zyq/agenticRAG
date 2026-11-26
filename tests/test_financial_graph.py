from pathlib import Path

from app.graph.canonicalize import canonicalize_chunk_graphs
from app.graph.financial import FinancialExtractor
from app.graph.financial_query import rank_disclosure
from scripts.native_pdf_content_list import html_table


def test_financial_graph_separates_periods_and_legal_location_types():
    text = """
<table><tr><td>指标</td><td>2024年</td><td>2023年</td><td>同比</td></tr>
<tr><td>营业收入（千元）</td><td>4,000</td><td>3,000</td><td>33%</td></tr>
<tr><td>归属于上市公司股东的净利润（千元）</td><td>500</td><td>(300)</td><td>-</td></tr>
</table>
<table><tr><td>子公司名称</td><td>主要经营地</td><td>注册地</td></tr>
<tr><td>示例有限公司</td><td>郑州市</td><td>深圳市</td></tr></table>
<table><tr><td>报告期末在职员工的数量合计（人）</td><td>100</td></tr></table>
"""
    graph = FinancialExtractor("甲股份有限公司", 2024, "CNY")(text)
    facts = [row["attributes"] for row in graph["entities"] if row["type"] == "financial_fact"]
    assert {(f["metric"], f["year"], f["value"], f["unit"]) for f in facts} == {
        ("revenue", 2024, "4000", "千元"),
        ("revenue", 2023, "3000", "千元"),
        ("parent_net_profit", 2024, "500", "千元"),
        ("parent_net_profit", 2023, "-300", "千元"),
        ("employee_count", 2024, "100", "人"),
    }
    assert {(r["predicate"], r["object"]) for r in graph["relations"] if r["subject"] == "示例有限公司"} == {
        ("operates_in", "郑州市"), ("registered_in", "深圳市"),
    }
    canonical = canonicalize_chunk_graphs([{"chunk_order": 0, "text": text, **graph}])
    record = next(edge for edge in canonical["relations"] if edge["predicate_family"] == "registered_in")
    assert "示例有限公司 | 郑州市 | 深圳市" in record["evidence"][0]["evidence_text"]


def test_typed_extractor_does_not_guess_a_period_for_revenue():
    html = "<table><tr><td>营业收入（千元）</td><td>100</td></tr>" \
           "<tr><td>归属于上市公司股东的净利润（千元）</td><td>10</td></tr></table>"
    assert FinancialExtractor("甲股份有限公司", 2024, "CNY")(html)["relations"] == []


def test_subsidiary_metrics_require_scope_and_separate_annual_periods():
    extract = FinancialExtractor("甲股份有限公司", 2024, "CNY")
    text = Path(__file__).with_name("fixtures").joinpath("subsidiary_financials.md").read_text(encoding="utf-8")
    graph = extract(text)
    facts = {row["name"]: row["attributes"] for row in graph["entities"] if row["type"] == "financial_fact"}
    assert len(facts) == 8
    assert {f["scope"] for f in facts.values()} == {"material_non_wholly_owned"}
    assert {(f["year"], f["value"]) for edge in graph["relations"] if edge["subject"] == "甲子公司"
            if (f := facts[edge["object"]])["metric"] == "net_profit"} == {(2024, "8"), (2023, "3")}
    assert not extract(text.replace("重要非全资子公司的主要财务信息", "其他信息"))["relations"]


def test_rank_selects_by_revenue_sums_same_companies_and_refuses_incomplete_rank():
    data = [
        ("甲", "a", "a1", {"metric": "revenue", "value": "300", "unit": "千元", "currency": "CNY"}),
        ("甲", "a", "a2", {"metric": "net_profit", "value": "8", "unit": "元", "currency": "CNY"}),
        ("乙", "b", "b1", {"metric": "revenue", "value": "20", "unit": "万元", "currency": "CNY"}),
        ("乙", "b", "b2", {"metric": "net_profit", "value": "40", "unit": "元", "currency": "CNY"}),
    ]
    evidence = {"a": [{"page": 1}], "b": [{"page": 2}]}
    args = {"rank_by": "revenue", "sum_metric": "net_profit"}
    result = rank_disclosure(data, evidence, top_k=1, **args)
    assert result["status"] == "ok" and result["aggregate"] == "8"
    assert result["ranked"][0]["company"] == "甲" and not result["full_group_coverage"]
    assert rank_disclosure(data, evidence, top_k=2, **args)["aggregate"] == "48"
    assert rank_disclosure(data, evidence, top_k=3, **args)["aggregate"] is None
    assert rank_disclosure(data[1:], evidence, top_k=1, **args)["status"] == "insufficient_comparable_disclosure"
    tied = {**data[2][3], "value": "30"}
    data[2] = (*data[2][:3], tied)
    assert rank_disclosure(data, evidence, top_k=1, **args)["status"] == "tie_at_cutoff"


def test_major_subsidiaries_use_section_labels_not_equity_cutoff():
    rows = [
        ["公司名称", "持股比例", "2024年度营业\n收入", "2024年度归属于母公司的净利润"],
        ["子公司：", None, None, None],
        ["甲子公司", "47.67", "100", "-5.1"],
        ["乙子公司", "60", "80", "2"],
        ["参股子公司：", None, None, None],
        ["参股公司", "20", "10000", "5000"],
    ]
    text = "主要子公司、参股子公司情况\n单位：万元\n" + html_table(rows)
    graph = FinancialExtractor("报告公司", 2024, "CNY")(text)
    members = [r["object"] for r in graph["relations"] if r["predicate"] == "has_major_subsidiary_financial_disclosure"]
    assert members == ["甲子公司", "乙子公司"]
    facts = [e["attributes"] for e in graph["entities"] if e["type"] == "financial_fact"]
    assert {(row["metric"], row["scope"], row["year"]) for row in facts} == {
        ("revenue", "major_subsidiaries", 2024), ("parent_net_profit", "major_subsidiaries", 2024),
    }
    assert len(facts) == 4
