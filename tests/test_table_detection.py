from __future__ import annotations

from app.ingest.financial_report import PageContent, _detect_is_consolidated, _detect_table_blocks


def test_detect_table_blocks_simple() -> None:
    text = """
合并资产负债表
项目 本期 上期
货币资金 1,000 900
存货 2,000 1,800
""".strip()
    pages = [PageContent(page=1, text_raw=text, text_md=text)]
    tables = _detect_table_blocks(pages)
    assert len(tables) == 1
    table = tables[0]
    assert table.statement_type == "balance_sheet"
    assert len(table.columns) == 2
    assert len(table.rows) == 2


def test_detect_table_blocks_prefers_mineru_html_table() -> None:
    md_text = """
# 合并资产负债表

<table>
  <tr><th>项目</th><th>2024年12月31日</th><th>2023年12月31日</th></tr>
  <tr><td>货币资金</td><td>1,000</td><td>900</td></tr>
  <tr><td>存货</td><td>2,000</td><td>1,800</td></tr>
  <tr><td>应收账款</td><td>3,000</td><td>2,700</td></tr>
</table>
""".strip()
    raw_text = """
这是一段包含数字 123 456 789 的文本，不是报表行
另一段文本 100 200 300
""".strip()
    pages = [PageContent(page=1, text_raw=raw_text, text_md=md_text)]
    tables = _detect_table_blocks(pages)
    assert len(tables) == 1
    table = tables[0]
    assert table.statement_type == "balance_sheet"
    assert len(table.columns) == 2
    assert [row.label for row in table.rows] == ["货币资金", "存货", "应收账款"]


def test_detect_table_blocks_parses_year_labels_from_html_header() -> None:
    md_text = """
# 合并现金流量表

<table>
  <tr><th>项目</th><th>2024年度</th><th>2023年度</th></tr>
  <tr><td>经营活动产生的现金流量净额</td><td>1,000</td><td>900</td></tr>
  <tr><td>投资活动产生的现金流量净额</td><td>-2,000</td><td>-1,800</td></tr>
  <tr><td>筹资活动产生的现金流量净额</td><td>3,000</td><td>2,700</td></tr>
</table>
""".strip()
    pages = [PageContent(page=1, text_raw="placeholder", text_md=md_text)]
    tables = _detect_table_blocks(pages)
    assert len(tables) == 1
    years = [col.fiscal_year for col in tables[0].columns]
    assert years == [2024, 2023]


def test_detect_table_blocks_handles_rowspan_colspan_header() -> None:
    md_text = """
# 合并现金流量表

<table>
  <tr><th rowspan="2">项目</th><th colspan="2">2024年度</th><th colspan="2">2023年度</th></tr>
  <tr><th>本期</th><th>上期</th><th>本期</th><th>上期</th></tr>
  <tr><td>经营活动产生的现金流量净额</td><td>1,000</td><td>900</td><td>800</td><td>700</td></tr>
  <tr><td>投资活动产生的现金流量净额</td><td>-2,000</td><td>-1,800</td><td>-1,600</td><td>-1,400</td></tr>
  <tr><td>筹资活动产生的现金流量净额</td><td>3,000</td><td>2,700</td><td>2,400</td><td>2,100</td></tr>
</table>
""".strip()
    pages = [PageContent(page=1, text_raw="placeholder", text_md=md_text)]
    tables = _detect_table_blocks(pages)
    assert len(tables) == 1
    table = tables[0]
    assert len(table.columns) == 4
    assert table.columns[0].fiscal_year == 2024
    assert table.columns[2].fiscal_year == 2023
    assert table.columns[0].label.startswith("2024")
    assert table.columns[2].label.startswith("2023")
    assert [row.label for row in table.rows][:3] == [
        "经营活动产生的现金流量净额",
        "投资活动产生的现金流量净额",
        "筹资活动产生的现金流量净额",
    ]


def test_detect_table_blocks_plaintext_year_columns_get_distinct_period_end() -> None:
    text = """
合并现金流量表
2024年12月31日
项目 2024年度 2023年度
经营活动产生的现金流量净额 1,000 900
投资活动产生的现金流量净额 -2,000 -1,800
筹资活动产生的现金流量净额 3,000 2,700
""".strip()
    pages = [PageContent(page=1, text_raw=text, text_md=text)]
    tables = _detect_table_blocks(pages)
    assert len(tables) == 1
    cols = tables[0].columns
    assert [c.fiscal_year for c in cols] == [2024, 2023]
    assert [c.period_end.isoformat() if c.period_end else None for c in cols] == [
        "2024-12-31",
        "2023-12-31",
    ]


def test_detect_is_consolidated_explicit_only() -> None:
    assert _detect_is_consolidated("合并现金流量表") is True
    assert _detect_is_consolidated("母公司现金流量表") is False
    assert _detect_is_consolidated("现金流量表") is None


def test_detect_table_blocks_plaintext_two_columns_default_to_current_prior() -> None:
    text = """
合并现金流量表
项目 金额A 金额B
经营活动产生的现金流量净额 1,000 900
投资活动产生的现金流量净额 -2,000 -1,800
筹资活动产生的现金流量净额 3,000 2,700
""".strip()
    pages = [PageContent(page=1, text_raw=text, text_md=text)]
    tables = _detect_table_blocks(pages)
    assert len(tables) == 1
    assert [c.label for c in tables[0].columns] == ["current_period", "prior_period"]


def test_detect_table_blocks_plaintext_keeps_rows_across_non_numeric_lines() -> None:
    text = """
合并现金流量表
项目 2024 年度 2023 年度
销售商品、提供劳务收到的现金 1,000 900
客户存款和同业存放款项净增加额
向中央银行借款净增加额
收到其他与经营活动有关的现金 500 450
经营活动现金流入小计 1,500 1,350
""".strip()
    pages = [PageContent(page=1, text_raw=text, text_md=text)]
    tables = _detect_table_blocks(pages)
    assert len(tables) == 1
    labels = [row.label for row in tables[0].rows]
    assert "销售商品、提供劳务收到的现金" in labels
    assert "收到其他与经营活动有关的现金" in labels
    assert "经营活动现金流入小计" in labels


def test_detect_table_blocks_plaintext_keeps_single_value_metric_rows() -> None:
    text = """
合并现金流量表
项目 2024 年度 2023 年度
销售商品、提供劳务收到的现金 1,000 900
收到的税费返还 10
收到其他与经营活动有关的现金 500 450
""".strip()
    pages = [PageContent(page=1, text_raw=text, text_md=text)]
    tables = _detect_table_blocks(pages)
    assert len(tables) == 1
    labels = [row.label for row in tables[0].rows]
    assert "收到的税费返还" in labels


def test_detect_table_blocks_plaintext_ignores_single_value_non_metric_rows() -> None:
    text = """
合并现金流量表
项目 2024 年度 2023 年度
销售商品、提供劳务收到的现金 1,000 900
注释说明 123
收到其他与经营活动有关的现金 500 450
""".strip()
    pages = [PageContent(page=1, text_raw=text, text_md=text)]
    tables = _detect_table_blocks(pages)
    assert len(tables) == 1
    labels = [row.label for row in tables[0].rows]
    assert "注释说明" not in labels
