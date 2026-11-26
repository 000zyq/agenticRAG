from app.graph.canonicalize import canonicalize_chunk_graphs
from app.graph.financial import FinancialExtractor


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
