from __future__ import annotations

import hashlib
import re


METRIC_DEFS = [
    {
        "metric_code": "revenue",
        "metric_name_cn": "营业收入",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["营业收入", "主营业务收入", "营业总收入", "revenue"],
    },
    {
        "metric_code": "operating_cost",
        "metric_name_cn": "营业成本",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["营业成本", "主营业务成本"],
    },
    {
        "metric_code": "operating_total_cost",
        "metric_name_cn": "营业总成本",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["营业总成本"],
    },
    {
        "metric_code": "selling_expense",
        "metric_name_cn": "销售费用",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["销售费用"],
    },
    {
        "metric_code": "admin_expense",
        "metric_name_cn": "管理费用",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["管理费用"],
    },
    {
        "metric_code": "rd_expense",
        "metric_name_cn": "研发费用",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["研发费用"],
    },
    {
        "metric_code": "finance_expense",
        "metric_name_cn": "财务费用",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["财务费用"],
    },
    {
        "metric_code": "operating_profit",
        "metric_name_cn": "营业利润",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["营业利润"],
    },
    {
        "metric_code": "total_profit",
        "metric_name_cn": "利润总额",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["利润总额"],
    },
    {
        "metric_code": "income_tax",
        "metric_name_cn": "所得税费用",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["所得税费用", "所得税"],
    },
    {
        "metric_code": "net_profit",
        "metric_name_cn": "净利润",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["净利润", "净收益"],
    },
    {
        "metric_code": "net_profit_parent",
        "metric_name_cn": "归属于母公司股东的净利润",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["归属于母公司股东的净利润", "归母净利润", "归属于母公司所有者的净利润"],
    },
    {
        "metric_code": "eps_basic",
        "metric_name_cn": "基本每股收益",
        "statement_type": "income",
        "value_nature": "ratio",
        "patterns": ["基本每股收益"],
    },
    {
        "metric_code": "eps_diluted",
        "metric_name_cn": "稀释每股收益",
        "statement_type": "income",
        "value_nature": "ratio",
        "patterns": ["稀释每股收益"],
    },
    {
        "metric_code": "cash_and_cash_equivalents",
        "metric_name_cn": "货币资金",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["货币资金", "现金及现金等价物"],
    },
    {
        "metric_code": "notes_receivable",
        "metric_name_cn": "应收票据",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["应收票据"],
    },
    {
        "metric_code": "accounts_receivable",
        "metric_name_cn": "应收账款",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["应收账款"],
    },
    {
        "metric_code": "prepayments",
        "metric_name_cn": "预付款项",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["预付款项"],
    },
    {
        "metric_code": "other_receivables",
        "metric_name_cn": "其他应收款",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["其他应收款"],
    },
    {
        "metric_code": "inventory",
        "metric_name_cn": "存货",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["存货"],
    },
    {
        "metric_code": "other_current_assets",
        "metric_name_cn": "其他流动资产",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["其他流动资产"],
    },
    {
        "metric_code": "long_term_investments",
        "metric_name_cn": "长期股权投资",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["长期股权投资", "长期投资"],
    },
    {
        "metric_code": "fixed_assets",
        "metric_name_cn": "固定资产",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["固定资产"],
    },
    {
        "metric_code": "construction_in_progress",
        "metric_name_cn": "在建工程",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["在建工程"],
    },
    {
        "metric_code": "intangible_assets",
        "metric_name_cn": "无形资产",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["无形资产"],
    },
    {
        "metric_code": "goodwill",
        "metric_name_cn": "商誉",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["商誉"],
    },
    {
        "metric_code": "total_assets",
        "metric_name_cn": "资产总计",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["资产总计", "资产总额"],
    },
    {
        "metric_code": "short_term_borrowings",
        "metric_name_cn": "短期借款",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["短期借款"],
    },
    {
        "metric_code": "notes_payable",
        "metric_name_cn": "应付票据",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["应付票据"],
    },
    {
        "metric_code": "accounts_payable",
        "metric_name_cn": "应付账款",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["应付账款"],
    },
    {
        "metric_code": "contract_liabilities",
        "metric_name_cn": "合同负债",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["合同负债", "预收款项"],
    },
    {
        "metric_code": "payroll_payable",
        "metric_name_cn": "应付职工薪酬",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["应付职工薪酬"],
    },
    {
        "metric_code": "taxes_payable",
        "metric_name_cn": "应交税费",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["应交税费"],
    },
    {
        "metric_code": "other_payables",
        "metric_name_cn": "其他应付款",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["其他应付款"],
    },
    {
        "metric_code": "long_term_borrowings",
        "metric_name_cn": "长期借款",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["长期借款"],
    },
    {
        "metric_code": "bonds_payable",
        "metric_name_cn": "应付债券",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["应付债券"],
    },
    {
        "metric_code": "total_liabilities",
        "metric_name_cn": "负债合计",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["负债合计", "负债总计"],
    },
    {
        "metric_code": "paid_in_capital",
        "metric_name_cn": "实收资本",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["实收资本", "股本"],
    },
    {
        "metric_code": "capital_reserve",
        "metric_name_cn": "资本公积",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["资本公积"],
    },
    {
        "metric_code": "retained_earnings",
        "metric_name_cn": "未分配利润",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["未分配利润"],
    },
    {
        "metric_code": "total_equity_parent",
        "metric_name_cn": "归属于母公司股东权益合计",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["归属于母公司股东权益合计", "归属于母公司所有者权益合计"],
    },
    {
        "metric_code": "total_equity",
        "metric_name_cn": "所有者权益合计",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["所有者权益合计", "股东权益合计", "权益合计"],
    },
    {
        "metric_code": "net_cash_flow_operating",
        "metric_name_cn": "经营活动产生的现金流量净额",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["经营活动产生的现金流量净额", "经营活动现金流量净额"],
    },
    {
        "metric_code": "cash_received_from_sales",
        "metric_name_cn": "销售商品、提供劳务收到的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["销售商品、提供劳务收到的现金"],
    },
    {
        "metric_code": "cash_paid_for_goods",
        "metric_name_cn": "购买商品、接受劳务支付的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["购买商品、接受劳务支付的现金"],
    },
    {
        "metric_code": "net_cash_flow_investing",
        "metric_name_cn": "投资活动产生的现金流量净额",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["投资活动产生的现金流量净额"],
    },
    {
        "metric_code": "net_cash_flow_financing",
        "metric_name_cn": "筹资活动产生的现金流量净额",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["筹资活动产生的现金流量净额"],
    },
    {
        "metric_code": "net_increase_cash",
        "metric_name_cn": "现金及现金等价物净增加额",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["现金及现金等价物净增加额"],
    },
    {
        "metric_code": "cash_begin",
        "metric_name_cn": "期初现金及现金等价物余额",
        "statement_type": "cashflow",
        "value_nature": "stock",
        "patterns": ["期初现金及现金等价物余额"],
    },
    {
        "metric_code": "cash_end",
        "metric_name_cn": "期末现金及现金等价物余额",
        "statement_type": "cashflow",
        "value_nature": "stock",
        "patterns": ["期末现金及现金等价物余额"],
    },
]


def normalize_label(label: str) -> str:
    cleaned = re.sub(r"[\s\u3000]+", "", label)
    cleaned = re.sub(r"[：:（）()，,．.。;；-]+", "", cleaned)
    return cleaned.lower()


def metric_code_from_label(label: str, statement_type: str) -> str:
    norm = normalize_label(label)
    digest = hashlib.sha1(f"{statement_type}:{norm}".encode("utf-8")).hexdigest()[:12]
    return f"raw_{digest}"


def match_metric(label: str, statement_type: str) -> dict | None:
    norm_label = normalize_label(label)
    label_has_ratio = ("率" in label) or ("%" in label)
    for metric in METRIC_DEFS:
        if metric["statement_type"] != statement_type:
            continue
        if label_has_ratio and metric["value_nature"] != "ratio":
            continue
        for pattern in metric["patterns"]:
            if normalize_label(pattern) in norm_label:
                return metric
    return None


def infer_statement_type_from_rows(rows) -> str | None:
    scores: dict[str, int] = {"income": 0, "balance": 0, "cashflow": 0}
    for row in rows:
        for metric in METRIC_DEFS:
            for pattern in metric["patterns"]:
                if normalize_label(pattern) in normalize_label(row.label):
                    scores[metric["statement_type"]] += 1
    best = max(scores, key=scores.get)
    if scores[best] == 0:
        return None
    return best
