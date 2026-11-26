"""Conservative financial observations and legal-entity edges from report tables."""
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
import json
import re
import unicodedata

from app.ingest.financial_report import (
    HTML_CELL_RE, HTML_ROW_RE, HTML_TABLE_RE, _expand_html_rows, _html_cell_spans, _html_cell_text,
)


FINANCIAL_EXTRACTOR_VERSION = "financial_tables_v1"
YEAR = re.compile(r"(20\d{2})\s*年(?:度)?(?:\s*\([^)]*\))?")
MONEY = re.compile(r"(?:人民币)?(亿元|万元|千元|元)")
NUMBER = re.compile(r"-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?")
METRICS = {
    "营业收入": "revenue",
    "营业总收入": "total_revenue",
    "归属于上市公司股东的净利润": "parent_net_profit",
    "归属于母公司股东的净利润": "parent_net_profit",
    "研发人员数量": "rd_staff",
    "研发人员数量占比": "rd_staff_share",
    "研发投入金额": "rd_investment",
    "研发投入占营业收入比例": "rd_investment_share",
    "报告期末在职员工的数量合计": "employee_count",
}
UNIT_BY_METRIC = {"rd_staff": "人", "employee_count": "人",
                  "rd_staff_share": "%", "rd_investment_share": "%"}


def clean(text):
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", text))


def metric_label(label):
    return re.sub(r"\((?:人民币)?(?:亿元|万元|千元|元|人|%)\)$", "", clean(label))


def table_rows(table):
    rows = [[(_html_cell_text(cell), *_html_cell_spans(cell)) for cell in HTML_CELL_RE.findall(row)]
            for row in HTML_ROW_RE.findall(table)]
    return _expand_html_rows(rows)


def value_from_cell(cell, unit):
    value = clean(cell)
    if unit == "%":
        if not value.endswith("%"):
            return None
        value = value[:-1]
    if value.startswith("(") and value.endswith(")"):
        value = "-" + value[1:-1]
    if not NUMBER.fullmatch(value):
        return None
    return str(Decimal(value.replace(",", "")))


@dataclass(frozen=True)
class FinancialExtractor:
    issuer: str
    fiscal_year: int
    currency: str

    def __call__(self, text):
        entities = {}
        relations = []

        def entity(name, kind, attributes=None):
            entities.setdefault(name, {"name": name, "type": kind, "confidence": 1.0,
                                       "attributes": attributes or {}})

        def edge(subject, predicate, obj, row):
            relations.append({"subject": subject, "predicate": predicate, "object": obj, "confidence": 1.0,
                              "evidence_text": " | ".join(row)})

        def subsidiaries(rows):
            for idx, header in enumerate(rows[:8]):
                columns = {clean(value): col for col, value in enumerate(header)}
                if "子公司名称" not in columns or not {"注册地", "主要经营地"} & columns.keys():
                    continue
                for row in rows[idx + 1:]:
                    name = row[columns["子公司名称"]].strip()
                    locations = [(predicate, row[columns[label]].strip())
                                 for label, predicate in (("注册地", "registered_in"), ("主要经营地", "operates_in"))
                                 if label in columns]
                    locations = [(predicate, place) for predicate, place in locations
                                 if place and place not in {"-", "—", "同上", "不适用"} and len(place) <= 40]
                    if not name or len(name) > 110 or clean(name) in {"合计", "子公司名称", "直接", "间接"} or not locations:
                        continue
                    entity(name, "financial_company")
                    edge(self.issuer, "has_reported_subsidiary", name, row)
                    for predicate, place in locations:
                        entity(place, "location")
                        edge(name, predicate, place, row)
                break

        entity(self.issuer, "financial_company")
        for table in HTML_TABLE_RE.finditer(text):
            rows = table_rows(table[0])
            if not rows:
                continue
            subsidiaries(rows)
            years = {}
            labels = {METRICS.get(metric_label(row[0])) for row in rows if row}
            context = clean(text[max(0, table.start() - 160):table.start()])
            for row in rows:
                header = {i: int(match[1]) for i, cell in enumerate(row)
                          if i and (match := YEAR.fullmatch(clean(cell)))}
                if header:
                    years = header
                    continue
                metric = METRICS.get(metric_label(row[0])) if row else None
                if not metric:
                    continue
                if metric in {"revenue", "total_revenue"} and "parent_net_profit" not in labels:
                    continue
                unit_match = MONEY.search(clean(row[0])) or MONEY.search(context)
                unit = UNIT_BY_METRIC.get(metric) or (unit_match[1] if unit_match else None)
                if unit is None:
                    continue
                year_columns = {1: self.fiscal_year} if metric == "employee_count" else years
                for col, year in year_columns.items():
                    value = value_from_cell(row[col], unit) if col < len(row) else None
                    if value is None:
                        continue
                    attrs = {"metric": metric, "label": row[0], "year": year, "unit": unit,
                             "currency": self.currency if unit in {"亿元", "万元", "千元", "元"} else None,
                             "scope": "report_group", "value": value,
                             "period_kind": "year_end" if metric == "employee_count" else "fiscal_year"}
                    identity = [self.issuer, metric, year, unit, value]
                    fact = "fact_" + sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()
                    entity(fact, "financial_fact", attrs)
                    edge(self.issuer, "has_financial_fact", fact, row)
        return {"entities": list(entities.values()) if relations else [], "relations": relations,
                "extraction_mode": "financial_rules"}
