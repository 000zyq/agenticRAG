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
