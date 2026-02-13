from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.ingest.financial_report import (
    _build_mineru_env,
    _extract_numbers,
    _strip_numbers,
    _detect_units,
    _parse_date_from_text,
    _guess_column_labels,
)
from app.ingest import financial_report as fr


def test_extract_numbers_parses_commas_and_parentheses() -> None:
    line = "Revenue 1,234 (5,678) 9"
    cells = _extract_numbers(line)
    values = [c.value for c in cells]
    assert values == [Decimal("1234"), Decimal("-5678"), Decimal("9")]


def test_strip_numbers_removes_numeric_tokens() -> None:
    line = "Operating Income 12,345"
    assert _strip_numbers(line) == "Operating Income"


def test_detect_units_usd() -> None:
    currency, units = _detect_units("Amounts in USD")
    assert currency == "USD"
    assert units is None


def test_detect_units_does_not_fallback_to_long_text() -> None:
    text = "取得子公司及其他营业单位支付的现金净额 支付其他与投资活动有关的现金 13,586,560.00"
    currency, units = _detect_units(text)
    assert currency is None
    assert units is None


def test_parse_date_from_text() -> None:
    assert _parse_date_from_text("As of 2024-12-31") == date(2024, 12, 31)
    assert _parse_date_from_text("截至 2024 年 12 月 31 日") == date(2024, 12, 31)


def test_guess_column_labels_years() -> None:
    cols = _guess_column_labels(["For 2024 and 2023"], 2)
    labels = [c.label for c in cols]
    assert labels == ["2024", "2023"]


def test_detect_statement_type_from_elr_code(monkeypatch) -> None:
    monkeypatch.setattr(fr, "ELR_STATEMENT_MAP", {"230005a": "balance_sheet"})
    assert fr._detect_statement_type("ELR [230005a]") == "balance_sheet"


def test_mineru_content_list_unknown_type_filtered(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(fr, "KNOWN_ELEMENT_TYPES", {"text", "table"})
    content_path = tmp_path / "content.json"
    content_path.write_text(
        '[{"page_idx": 0, "type": "foo", "text": "x"}, {"page_idx": 0, "type": "text", "text": "ok"}]',
        encoding="utf-8",
    )
    pages = fr._mineru_pages_from_content_list(content_path)
    assert len(pages) == 1
    assert "ok" in pages[0].text_md


def test_build_mineru_env_uses_tmp_cache() -> None:
    env = _build_mineru_env()
    assert env["XDG_CACHE_HOME"] == "/tmp"
    assert env["HF_HOME"] == "/tmp"
    assert env["HUGGINGFACE_HUB_CACHE"] == "/tmp"
    assert env["TRANSFORMERS_CACHE"] == "/tmp"
    assert env["MPLCONFIGDIR"] == "/tmp/mplconfig"
