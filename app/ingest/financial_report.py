from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
import os
import hashlib
import html
import json
import re
import subprocess
import tempfile

from app.ingest.metric_defs import infer_statement_type_from_rows
from app.ingest.parser_pdf import parse_pdf


STATEMENT_KEYWORDS = {
    "balance_sheet": ["资产负债表", "合并资产负债表", "balance sheet", "statement of financial position"],
    "income_statement": ["利润表", "合并利润表", "income statement", "statement of profit", "statement of operations"],
    "cash_flow": ["现金流量表", "合并现金流量表", "cash flow"],
    "changes_in_equity": ["所有者权益变动表", "股东权益变动表", "changes in equity"],
}
CAS_BACKGROUND_RULES_PATH = Path(__file__).resolve().parents[2] / "data" / "taxonomy" / "cas2020_background_rules.json"

UNIT_PATTERNS = [
    ("万元", "CNY", "10k"),
    ("千元", "CNY", "1k"),
    ("元", "CNY", "1"),
    ("人民币", "CNY", None),
    ("USD", "USD", None),
    ("美元", "USD", None),
]

NUMBER_RE = re.compile(r"(?<![\w.%])[\(]?-?\d{1,3}(?:,\d{3})*(?:\.\d+)?[\)]?(?![\w.%])")
DATE_RE = re.compile(r"(20\d{2})\s*[年\-/]\s*(\d{1,2})\s*[月\-/]\s*(\d{1,2})")
YEAR_RE = re.compile(r"(20\d{2})")
HTML_TABLE_RE = re.compile(r"<table\b.*?>.*?</table>", re.IGNORECASE | re.DOTALL)
HTML_ROW_RE = re.compile(r"<tr\b.*?>.*?</tr>", re.IGNORECASE | re.DOTALL)
HTML_CELL_RE = re.compile(r"<t[dh]\b.*?>.*?</t[dh]>", re.IGNORECASE | re.DOTALL)
ELR_CODE_RE = re.compile(r"[\[【]\s*([0-9]{6}[a-z]?)\s*[\]】]", re.IGNORECASE)


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


def _infer_statement_type_from_elr_name(name: str) -> str | None:
    if "资产负债表" in name:
        return "balance_sheet"
    if "利润表" in name or "损益表" in name:
        return "income_statement"
    if "现金流量表" in name:
        return "cash_flow"
    if "所有者权益变动表" in name or "股东权益变动表" in name:
        return "changes_in_equity"
    return None


def _load_background_validation(path: Path) -> tuple[dict[str, str], set[str]]:
    if not path.exists():
        return {}, set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}, set()

    elr_map: dict[str, str] = {}
    for item in data.get("elr_changes", []):
        if not isinstance(item, dict):
            continue
        statement_type = _infer_statement_type_from_elr_name(str(item.get("ELR名称") or ""))
        if not statement_type:
            continue
        for code in item.get("elr_codes", []) or []:
            code_text = str(code).strip().lower()
            if code_text:
                elr_map[code_text] = statement_type

    element_types: set[str] = set()
    for item in data.get("element_types", []):
        if not isinstance(item, dict):
            continue
        value = str(item.get("元素类型") or "").strip().lower()
        if value:
            element_types.add(value)

    return elr_map, element_types


ELR_STATEMENT_MAP, KNOWN_ELEMENT_TYPES = _load_background_validation(CAS_BACKGROUND_RULES_PATH)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _find_mineru_markdown(output_root: Path, source_path: Path) -> list[Path]:
    preferred_root = output_root / source_path.stem
    roots = [preferred_root, output_root] if preferred_root != output_root else [output_root]
    for root in roots:
        if root.exists():
            md_files = sorted(root.rglob("*.md"))
            if md_files:
                return md_files
    return []


def _find_mineru_content_list(output_root: Path, source_path: Path) -> Path | None:
    preferred_root = output_root / source_path.stem
    roots = [preferred_root, output_root] if preferred_root != output_root else [output_root]
    for root in roots:
        if root.exists():
            candidates = sorted(root.rglob("*_content_list.json"))
            if candidates:
                return candidates[0]
    return None


def _normalize_caption(value: object) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _mineru_pages_from_content_list(path: Path) -> list[PageContent]:
    items = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    if not isinstance(items, list):
        return []

    pages_md: dict[int, list[str]] = {}
    pages_raw: dict[int, list[str]] = {}

    for item in items:
        if not isinstance(item, dict):
            continue
        page_idx = item.get("page_idx")
        if page_idx is None:
            continue
        try:
            page_number = int(page_idx) + 1
        except (TypeError, ValueError):
            continue
        md_parts = pages_md.setdefault(page_number, [])
        raw_parts = pages_raw.setdefault(page_number, [])

        item_type = item.get("type")
        if item_type:
            item_type = str(item_type).strip().lower()
            if KNOWN_ELEMENT_TYPES and item_type not in KNOWN_ELEMENT_TYPES:
                continue
        if item_type == "text":
            text = (item.get("text") or "").strip()
            if not text:
                continue
            level = item.get("text_level")
            if isinstance(level, int) and level > 0:
                md_parts.append(f"{'#' * min(level, 6)} {text}")
            else:
                md_parts.append(text)
            raw_parts.append(text)
            continue

        if item_type == "table":
            captions = _normalize_caption(item.get("table_caption"))
            footnotes = _normalize_caption(item.get("table_footnote"))
            for caption in captions:
                md_parts.append(f"### {caption}")
                raw_parts.append(caption)
            table_body = (item.get("table_body") or "").strip()
            if table_body:
                md_parts.append(table_body)
            for footnote in footnotes:
                md_parts.append(footnote)
                raw_parts.append(footnote)
            continue

    results: list[PageContent] = []
    for page_number in sorted(pages_md.keys()):
        md_text = "\n\n".join(pages_md.get(page_number, [])).strip()
        raw_text = "\n".join(pages_raw.get(page_number, [])).strip()
        if not md_text and not raw_text:
            continue
        results.append(PageContent(page=page_number, text_raw=raw_text, text_md=md_text))
    return results


def _build_mineru_env() -> dict[str, str]:
    env = os.environ.copy()
    env["XDG_CACHE_HOME"] = "/tmp"
    env["HF_HOME"] = "/tmp"
    env["HUGGINGFACE_HUB_CACHE"] = "/tmp"
    env["TRANSFORMERS_CACHE"] = "/tmp"
    env["MPLCONFIGDIR"] = "/tmp/mplconfig"
    Path(env["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)
    return env


def _mineru_extract(path: Path) -> list[PageContent] | None:
    cmd_template = os.getenv("MINERU_CMD")
    if not cmd_template:
        return None

    output_override = os.getenv("MINERU_OUTPUT_DIR")
    if output_override:
        output_root = Path(output_override).expanduser()
        output_root.mkdir(parents=True, exist_ok=True)
        tmp_context = None
    else:
        tmp_context = tempfile.TemporaryDirectory()
        output_root = Path(tmp_context.name)

    try:
        cmd = cmd_template.format(input=str(path), output=str(output_root))
        mineru_env = _build_mineru_env()
        try:
            subprocess.run(cmd, shell=True, check=True, env=mineru_env)
        except subprocess.CalledProcessError:
            return None
        content_list = _find_mineru_content_list(output_root, path)
        if content_list:
            pages = _mineru_pages_from_content_list(content_list)
            if pages:
                return pages
        md_files = _find_mineru_markdown(output_root, path)
        if not md_files:
            return None
        results: list[PageContent] = []
        for idx, md_path in enumerate(md_files, start=1):
            text_md = md_path.read_text(encoding="utf-8", errors="ignore")
            text_raw = text_md
            results.append(PageContent(page=idx, text_raw=text_raw, text_md=text_md))
        return results
    finally:
        if tmp_context is not None:
            tmp_context.cleanup()


def extract_pdf_to_markdown(path: Path, engine: str | None = None) -> tuple[list[PageContent], str]:
    if engine == "mineru":
        mineru = _mineru_extract(path)
        if not mineru:
            raise RuntimeError("MinerU extraction failed or MINERU_CMD not set.")
        return mineru, "mineru"

    if engine == "pypdf":
        pages = parse_pdf(str(path))
        results: list[PageContent] = []
        for item in pages:
            text = item["text"]
            page = item["page"]
            md = f"## Page {page}\n\n{text}\n"
            results.append(PageContent(page=page, text_raw=text, text_md=md))
        return results, "pypdf"

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
    for match in ELR_CODE_RE.finditer(text):
        code = match.group(1).lower()
        mapped = ELR_STATEMENT_MAP.get(code)
        if mapped:
            return mapped
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


def _html_cell_text(cell_html: str) -> str:
    cleaned = re.sub(r"<br\\s*/?>", " ", cell_html, flags=re.IGNORECASE)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = html.unescape(cleaned)
    cleaned = cleaned.replace("\u00a0", " ")
    cleaned = re.sub(r"\\s{2,}", " ", cleaned)
    return cleaned.strip()


def _parse_number(text: str) -> Decimal | None:
    match = NUMBER_RE.search(text)
    if not match:
        return None
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
        return val
    except (InvalidOperation, ValueError):
        return None


def _is_header_row(cells: list[str]) -> bool:
    joined = "".join(cells)
    if "项目" in joined or "期末" in joined or "期初" in joined or "本期" in joined or "上期" in joined:
        return True
    if YEAR_RE.search(joined):
        return True
    if all(_parse_number(cell) is None for cell in cells):
        return True
    return False


def _extract_last_heading(context: str) -> str | None:
    lines = [line.strip() for line in context.splitlines() if line.strip().startswith("#")]
    if not lines:
        return None
    return lines[-1].lstrip("# ").strip() or None


def _parse_html_tables(md_text: str, page_number: int) -> list[TableBlock]:
    blocks: list[TableBlock] = []
    for match in HTML_TABLE_RE.finditer(md_text):
        table_html = match.group(0)
        start = match.start()
        end = match.end()
        context_before = md_text[max(0, start - 1200) : start]
        context_after = md_text[end : end + 800]
        context = f"{context_before}\n{context_after}"
        statement_type = _detect_statement_type(context)
        title = _extract_last_heading(context_before)
        currency, units = _detect_units(context)
        is_consolidated = "合并" in context if context else None

        row_htmls = HTML_ROW_RE.findall(table_html)
        if not row_htmls:
            continue
        rows: list[list[str]] = []
        for row_html in row_htmls:
            cell_htmls = HTML_CELL_RE.findall(row_html)
            if not cell_htmls:
                continue
            cells = [_html_cell_text(cell_html) for cell_html in cell_htmls]
            if any(cells):
                rows.append(cells)

        if not rows:
            continue

        header_row = rows[0]
        data_rows = rows[1:] if _is_header_row(header_row) and len(rows) > 1 else rows
        if not data_rows:
            continue

        num_cols = max((max(len(r) - 1, 0) for r in data_rows), default=0)
        if num_cols < 2:
            continue

        col_labels: list[str] = []
        if _is_header_row(header_row):
            col_labels = header_row[1:]
        if len(col_labels) < num_cols:
            col_labels += [f"col_{i + 1}" for i in range(len(col_labels), num_cols)]

        columns: list[TableColumn] = []
        header_text = " ".join(col_labels)
        header_has_period = bool(YEAR_RE.findall(header_text)) or ("本期" in header_text) or ("上期" in header_text)
        for label in col_labels:
            fiscal_year = int(label) if label.isdigit() and len(label) == 4 else None
            period_end = _parse_date_from_text(label) or _parse_date_from_text(context)
            columns.append(TableColumn(label=label, fiscal_year=fiscal_year, period_end=period_end))

        table_rows: list[TableRow] = []
        rows_with_numbers = 0
        for row in data_rows:
            label = row[0].strip() if row else ""
            if not label:
                continue
            values = row[1:]
            cells: list[TableCell] = []
            for value_text in values:
                val = _parse_number(value_text)
                cells.append(TableCell(value=val, raw_text=value_text or None))
            if len(cells) < num_cols:
                cells.extend([TableCell(value=None, raw_text=None)] * (num_cols - len(cells)))
            if any(cell.value is not None for cell in cells):
                rows_with_numbers += 1
            table_rows.append(TableRow(label=label, cells=cells, page_number=page_number))

        if rows_with_numbers < 3:
            continue
        if not statement_type:
            statement_type = infer_statement_type_from_rows(table_rows)
        if not header_has_period and not statement_type and (not title or "表" not in title):
            continue

        blocks.append(
            TableBlock(
                title=title,
                section_title=title,
                statement_type=statement_type,
                page_start=page_number,
                page_end=page_number,
                currency=currency,
                units=units,
                is_consolidated=is_consolidated,
                columns=columns,
                rows=table_rows,
            )
        )
    return blocks


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

    html_blocks: list[TableBlock] = []
    for page in pages:
        if "<table" in page.text_md:
            html_blocks.extend(_parse_html_tables(page.text_md, page.page))
    if html_blocks:
        return html_blocks
    header_buffer: list[tuple[int, str]] = []
    last_statement_header: tuple[int, str] | None = None

    current_rows: list[tuple[int, str]] = []
    current_header: list[str] = []
    current_page_start: int | None = None
    current_page_end: int | None = None

    def flush_current():
        nonlocal current_rows, current_header, current_page_start, current_page_end
        if not current_rows:
            return
        filtered_rows: list[tuple[int, str]] = []
        for row_page, line in current_rows:
            label = _strip_numbers(line)
            if not label:
                continue
            if len(label) > 60:
                continue
            if "。" in label or "，" in label:
                continue
            if "公司" in label and len(label) > 30:
                continue
            filtered_rows.append((row_page, line))

        rows_cells = [_extract_numbers(line) for _, line in filtered_rows]
        row_labels = [_strip_numbers(line) for _, line in filtered_rows]
        max_cols = max((len(cells) for cells in rows_cells), default=0)
        if max_cols == 0:
            current_rows = []
            current_header = []
            current_page_start = None
            current_page_end = None
            return

        rows_total = len(rows_cells)
        rows_with_two = sum(1 for cells in rows_cells if len(cells) >= 2)
        short_label_rows = sum(1 for label in row_labels if len(label) <= 40)
        header_text = " ".join(current_header)
        header_has_period = bool(YEAR_RE.findall(header_text)) or ("本期" in header_text) or ("上期" in header_text)
        statement_hint = _detect_statement_type(header_text)

        # Basic table quality filters to avoid treating narrative paragraphs as tables.
        if max_cols < 2 or rows_total < 2:
            current_rows = []
            current_header = []
            current_page_start = None
            current_page_end = None
            return
        if rows_with_two < 2 or rows_with_two / rows_total < 0.5:
            current_rows = []
            current_header = []
            current_page_start = None
            current_page_end = None
            return
        if short_label_rows / rows_total < 0.5:
            current_rows = []
            current_header = []
            current_page_start = None
            current_page_end = None
            return
        if not header_has_period and not statement_hint and rows_total < 5:
            current_rows = []
            current_header = []
            current_page_start = None
            current_page_end = None
            return

        columns = _guess_column_labels(current_header, max_cols)
        table_rows: list[TableRow] = []
        for (row_page, line), cells in zip(filtered_rows, rows_cells):
            label = _strip_numbers(line)
            if not label:
                label = "(blank)"
            if len(cells) < max_cols:
                cells = [TableCell(value=None, raw_text=None)] * (max_cols - len(cells)) + cells
            table_rows.append(TableRow(label=label, cells=cells, page_number=row_page))

        statement_type = statement_hint
        currency, units = _detect_units(header_text)
        title = current_header[0] if current_header else None
        section_title = current_header[-1] if current_header else None
        is_consolidated = "合并" in header_text if header_text else None

        if not statement_type:
            statement_type = infer_statement_type_from_rows(table_rows)

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

            if _detect_statement_type(line):
                last_statement_header = (page.page, line)

            cells = _extract_numbers(line)
            has_label = bool(_strip_numbers(line))
            if not current_rows:
                is_row = len(cells) >= 2 and has_label
            else:
                is_row = len(cells) >= 1 and has_label
            if is_row:
                if not current_rows:
                    current_header = [text for _, text in header_buffer]
                    if not _detect_statement_type(" ".join(current_header)) and last_statement_header:
                        if page.page - last_statement_header[0] <= 2:
                            if last_statement_header[1] not in current_header:
                                current_header = [last_statement_header[1]] + current_header
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
    elif fiscal_year and report_type == "annual":
        period_end = date(fiscal_year, 12, 31)

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


def extract_financial_report(path: str, engine: str | None = None) -> tuple[list[PageContent], ReportMeta, list[TableBlock], str]:
    pdf_path = Path(path)
    pages, parse_method = extract_pdf_to_markdown(pdf_path, engine=engine)
    meta = _extract_metadata(pages)
    tables = _detect_table_blocks(pages)
    return pages, meta, tables, parse_method
