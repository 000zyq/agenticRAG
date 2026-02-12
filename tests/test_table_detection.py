from __future__ import annotations

from app.ingest.financial_report import PageContent, _detect_table_blocks


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
