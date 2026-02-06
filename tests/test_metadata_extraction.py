from __future__ import annotations

from app.ingest.financial_report import PageContent, _extract_metadata


def test_extract_metadata_from_head() -> None:
    pages = [
        PageContent(
            page=1,
            text_raw="公司名称：测试股份有限公司\n2024年年度报告\n股票代码：123456\n截至2024年12月31日",
            text_md="",
        )
    ]
    meta = _extract_metadata(pages)
    assert meta.company_name == "测试股份有限公司"
    assert meta.ticker == "123456"
    assert meta.report_type == "annual"
    assert meta.fiscal_year == 2024
    assert meta.period_end is not None
