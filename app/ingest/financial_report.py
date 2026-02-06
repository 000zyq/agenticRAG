from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
import os
import hashlib
import re
import subprocess
import tempfile

from app.ingest.parser_pdf import parse_pdf


STATEMENT_KEYWORDS = {
    "balance_sheet": ["资产负债表", "合并资产负债表", "balance sheet", "statement of financial position"],
    "income_statement": ["利润表", "合并利润表", "income statement", "statement of profit", "statement of operations"],
    "cash_flow": ["现金流量表", "合并现金流量表", "cash flow"],
    "changes_in_equity": ["所有者权益变动表", "股东权益变动表", "changes in equity"],
}

UNIT_PATTERNS = [
    ("万元", "CNY", "10k"),
    ("千元", "CNY", "1k"),
    ("元", "CNY", "1"),
    ("人民币", "CNY", None),
    ("USD", "USD", None),
    ("美元", "USD", None),
]

NUMBER_RE = re.compile(r"(?<![\w.%])[\(]?-?\d{1,3}(?:,\d{3})*(?:\.\d+)?[\)]?(?![\w.%])")
DATE_RE = re.compile(r"(20\d{2})[年\-/](\d{1,2})[月\-/](\d{1,2})")
YEAR_RE = re.compile(r"(20\d{2})")


@dataclass
class PageContent:
    page: int
    text_raw: str
    text_md: str


@dataclass
class TableCell:
    value: Decimal | None
    raw_text: str | None


@dataclass
class TableRow:
    label: str
    cells: list[TableCell]
    page_number: int | None = None


@dataclass
class TableColumn:
    label: str
    period_start: date | None = None
    period_end: date | None = None
    fiscal_year: int | None = None
    fiscal_period: str | None = None


@dataclass
class TableBlock:
    title: str | None
    section_title: str | None
    statement_type: str | None
    page_start: int
    page_end: int
    currency: str | None
    units: str | None
    is_consolidated: bool | None
    columns: list[TableColumn]
    rows: list[TableRow]


@dataclass
class ReportMeta:
    report_title: str | None
    company_name: str | None
    ticker: str | None
    report_type: str | None
    fiscal_year: int | None
    period_start: date | None
    period_end: date | None
    currency: str | None
    units: str | None
    extra: dict


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _mineru_extract(path: Path) -> list[PageContent] | None:
    cmd_template = os.getenv("MINERU_CMD")
    if not cmd_template:
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        cmd = cmd_template.format(input=str(path), output=tmpdir)
        try:
            subprocess.run(cmd, shell=True, check=True)
        except subprocess.CalledProcessError:
            return None
        md_files = sorted(Path(tmpdir).rglob("*.md"))
        if not md_files:
            return None
        results: list[PageContent] = []
        for idx, md_path in enumerate(md_files, start=1):
            text_md = md_path.read_text(encoding="utf-8", errors="ignore")
            text_raw = text_md
            results.append(PageContent(page=idx, text_raw=text_raw, text_md=text_md))
        return results


def extract_pdf_to_markdown(path: Path) -> tuple[list[PageContent], str]:
    mineru = _mineru_extract(path)
    if mineru:
        return mineru, "mineru"

    pages = parse_pdf(str(path))
    results: list[PageContent] = []
    for item in pages:
        text = item["text"]
        page = item["page"]
        md = f"## Page {page}\n\n{text}\n"
        results.append(PageContent(page=page, text_raw=text, text_md=md))
    return results, "pypdf"


def _detect_statement_type(text: str) -> str | None:
    lowered = text.lower()
    for statement, keys in STATEMENT_KEYWORDS.items():
        for key in keys:
            if key.lower() in lowered:
                return statement
    return None


def _detect_units(text: str) -> tuple[str | None, str | None]:
    currency = None
    units = None
    for needle, cur, unit in UNIT_PATTERNS:
        if needle in text:
            if cur:
                currency = currency or cur
            if unit:
                units = units or unit
    if "单位" in text and not units:
        units = text.strip()
    return currency, units


def _parse_date_from_text(text: str) -> date | None:
    match = DATE_RE.search(text)
    if not match:
        return None
    year, month, day = match.groups()
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        return None


def _extract_numbers(line: str) -> list[TableCell]:
    cells: list[TableCell] = []
    for match in NUMBER_RE.finditer(line):
        raw = match.group(0)
        val_text = raw.replace(",", "")
        negative = False
        if val_text.startswith("(") and val_text.endswith(")"):
            negative = True
            val_text = val_text[1:-1]
        try:
            val = Decimal(val_text)
            if negative:
                val = -val
        except (InvalidOperation, ValueError):
            val = None
        cells.append(TableCell(value=val, raw_text=raw))
    return cells


def _strip_numbers(line: str) -> str:
    cleaned = NUMBER_RE.sub(" ", line)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()


def _guess_column_labels(header_lines: list[str], num_cols: int) -> list[TableColumn]:
    header_text = " ".join(header_lines)
    years = YEAR_RE.findall(header_text)
    date_match = _parse_date_from_text(header_text)
    columns: list[TableColumn] = []

    labels: list[str] = []
    if num_cols <= 0:
        return columns

    if "本期" in header_text and "上期" in header_text and num_cols >= 2:
        labels = ["current_period", "prior_period"]
    elif len(years) >= num_cols:
        labels = years[-num_cols:]
    elif len(years) == 1 and num_cols == 2:
        try:
            prior = str(int(years[0]) - 1)
            labels = [years[0], prior]
        except ValueError:
            labels = []

    if not labels:
        labels = [f"col_{i + 1}" for i in range(num_cols)]

    for label in labels:
        fiscal_year = None
        period_end = None
        if label.isdigit() and len(label) == 4:
            fiscal_year = int(label)
        if date_match:
            period_end = date_match
        columns.append(TableColumn(label=label, fiscal_year=fiscal_year, period_end=period_end))

    return columns


def _detect_table_blocks(pages: list[PageContent]) -> list[TableBlock]:
    blocks: list[TableBlock] = []
    header_buffer: list[tuple[int, str]] = []

    current_rows: list[tuple[int, str]] = []
    current_header: list[str] = []
    current_page_start: int | None = None
    current_page_end: int | None = None

    def flush_current():
        nonlocal current_rows, current_header, current_page_start, current_page_end
        if not current_rows:
            return
        rows_cells = [_extract_numbers(line) for _, line in current_rows]
        max_cols = max((len(cells) for cells in rows_cells), default=0)
        if max_cols == 0:
            current_rows = []
            current_header = []
            current_page_start = None
            current_page_end = None
            return

        columns = _guess_column_labels(current_header, max_cols)
        table_rows: list[TableRow] = []
        for (row_page, line), cells in zip(current_rows, rows_cells):
            label = _strip_numbers(line)
            if not label:
                label = "(blank)"
            if len(cells) < max_cols:
                cells = [TableCell(value=None, raw_text=None)] * (max_cols - len(cells)) + cells
            table_rows.append(TableRow(label=label, cells=cells, page_number=row_page))

        header_text = " ".join(current_header)
        statement_type = _detect_statement_type(header_text)
        currency, units = _detect_units(header_text)
        title = current_header[0] if current_header else None
        section_title = current_header[-1] if current_header else None
        is_consolidated = "合并" in header_text if header_text else None

        blocks.append(
            TableBlock(
                title=title,
                section_title=section_title,
                statement_type=statement_type,
                page_start=current_page_start or 1,
                page_end=current_page_end or (current_page_start or 1),
                currency=currency,
                units=units,
                is_consolidated=is_consolidated,
                columns=columns,
                rows=table_rows,
            )
        )

        current_rows = []
        current_header = []
        current_page_start = None
        current_page_end = None

    for page in pages:
        lines = [line.strip() for line in page.text_raw.splitlines()]
        for line in lines:
            if not line:
                if current_rows:
                    flush_current()
                continue

            cells = _extract_numbers(line)
            is_row = len(cells) >= 1 and bool(_strip_numbers(line))
            if is_row:
                if not current_rows:
                    current_header = [text for _, text in header_buffer]
                    current_page_start = page.page
                current_rows.append((page.page, line))
                current_page_end = page.page
            else:
                if current_rows:
                    flush_current()
                header_buffer.append((page.page, line))
                if len(header_buffer) > 3:
                    header_buffer.pop(0)

    if current_rows:
        flush_current()

    return blocks


def _extract_metadata(pages: list[PageContent]) -> ReportMeta:
    head_text = "\n".join(page.text_raw for page in pages[:3])
    report_title = None
    company_name = None
    ticker = None
    report_type = None
    fiscal_year = None
    period_start = None
    period_end = None
    currency, units = _detect_units(head_text)

    for line in head_text.splitlines():
        if not report_title and ("年度报告" in line or "年报" in line or "annual report" in line.lower()):
            report_title = line.strip()
        if not company_name and "公司名称" in line:
            company_name = line.split("：", 1)[-1].strip()
        if not ticker and ("股票代码" in line or "证券代码" in line):
            ticker = line.split("：", 1)[-1].strip()
        if not report_type and ("年度报告" in line or "年报" in line):
            report_type = "annual"

    years = YEAR_RE.findall(head_text)
    if years:
        try:
            fiscal_year = int(years[0])
        except ValueError:
            fiscal_year = None

    date_match = _parse_date_from_text(head_text)
    if date_match:
        period_end = date_match

    extra = {"raw_head": head_text[:2000]}
    return ReportMeta(
        report_title=report_title,
        company_name=company_name,
        ticker=ticker,
        report_type=report_type,
        fiscal_year=fiscal_year,
        period_start=period_start,
        period_end=period_end,
        currency=currency,
        units=units,
        extra=extra,
    )


def extract_financial_report(path: str) -> tuple[list[PageContent], ReportMeta, list[TableBlock], str]:
    pdf_path = Path(path)
    pages, parse_method = extract_pdf_to_markdown(pdf_path)
    meta = _extract_metadata(pages)
    tables = _detect_table_blocks(pages)
    return pages, meta, tables, parse_method
