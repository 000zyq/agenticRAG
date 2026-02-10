from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re


BASE_METRIC_DEFS = [
    {
        "metric_code": "revenue",
        "metric_name_cn": "营业收入",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["营业收入", "主营业务收入", "营业总收入", "revenue"],
    },
    {
        "metric_code": "main_business_revenue",
        "metric_name_cn": "主营业务收入",
        "metric_name_en": "Main Business Revenue",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["主营业务收入"],
        "patterns_exact": ["主营业务"],
        "parent_metric_code": "revenue",
    },
    {
        "metric_code": "other_business_revenue",
        "metric_name_cn": "其他业务收入",
        "metric_name_en": "Other Business Revenue",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["其他业务收入"],
        "patterns_exact": ["其他业务"],
        "parent_metric_code": "revenue",
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
        "metric_code": "office_expense",
        "metric_name_cn": "办公费",
        "metric_name_en": "Office Expense",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["办公费"],
        "patterns_exact": ["办公费"],
        "parent_metric_code": "admin_expense",
    },
    {
        "metric_code": "entertainment_expense",
        "metric_name_cn": "业务招待费",
        "metric_name_en": "Entertainment Expense",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["业务招待费"],
        "patterns_exact": ["业务招待费"],
        "parent_metric_code": "admin_expense",
    },
    {
        "metric_code": "service_fee",
        "metric_name_cn": "服务费",
        "metric_name_en": "Service Fee",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["服务费"],
        "patterns_exact": ["服务费"],
        "parent_metric_code": "admin_expense",
    },
    {
        "metric_code": "employee_compensation",
        "metric_name_cn": "职工薪酬",
        "metric_name_en": "Employee Compensation",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["职工薪酬"],
    },
    {
        "metric_code": "rd_expense",
        "metric_name_cn": "研发费用",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["研发费用"],
    },
    {
        "metric_code": "travel_expense",
        "metric_name_cn": "差旅费",
        "metric_name_en": "Travel Expense",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["差旅费"],
    },
    {
        "metric_code": "vehicle_expense",
        "metric_name_cn": "车辆费用",
        "metric_name_en": "Vehicle Expense",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["车辆费用"],
    },
    {
        "metric_code": "insurance_expense",
        "metric_name_cn": "保险费",
        "metric_name_en": "Insurance Expense",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["保险费"],
    },
    {
        "metric_code": "lease_expense",
        "metric_name_cn": "租赁费",
        "metric_name_en": "Lease Expense",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["租赁费", "资产租赁费用"],
    },
    {
        "metric_code": "finance_expense",
        "metric_name_cn": "财务费用",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["财务费用"],
    },
    {
        "metric_code": "taxes_and_surcharges",
        "metric_name_cn": "税金及附加",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": [
            "税金及附加",
            "印花税",
            "房产税",
            "城市维护建设税",
            "土地使用税",
            "资源税",
            "环境保护税",
        ],
    },
    {
        "metric_code": "interest_income",
        "metric_name_cn": "利息收入",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["利息收入"],
    },
    {
        "metric_code": "interest_expense",
        "metric_name_cn": "利息费用",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["利息费用"],
    },
    {
        "metric_code": "other_income",
        "metric_name_cn": "其他收益",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["其他收益"],
    },
    {
        "metric_code": "non_operating_income",
        "metric_name_cn": "营业外收入",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["营业外收入"],
    },
    {
        "metric_code": "non_operating_expense",
        "metric_name_cn": "营业外支出",
        "statement_type": "income",
        "value_nature": "flow",
        "patterns": ["营业外支出"],
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
        "metric_code": "security_deposit",
        "metric_name_cn": "保证金",
        "metric_name_en": "Security Deposit",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["保证金"],
        "patterns_exact": ["保证金"],
        "parent_metric_code": "other_receivables",
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
        "metric_code": "trading_financial_assets",
        "metric_name_cn": "交易性金融资产",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["交易性金融资产"],
    },
    {
        "metric_code": "current_assets_total",
        "metric_name_cn": "流动资产合计",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["流动资产合计"],
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
        "metric_code": "biological_assets",
        "metric_name_cn": "生产性生物资产",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["生产性生物资产"],
    },
    {
        "metric_code": "right_of_use_assets",
        "metric_name_cn": "使用权资产",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["使用权资产"],
    },
    {
        "metric_code": "long_term_prepaid_expenses",
        "metric_name_cn": "长期待摊费用",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["长期待摊费用"],
    },
    {
        "metric_code": "other_noncurrent_assets",
        "metric_name_cn": "其他非流动资产",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["其他非流动资产"],
    },
    {
        "metric_code": "noncurrent_assets_total",
        "metric_name_cn": "非流动资产合计",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["非流动资产合计"],
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
        "metric_code": "mortgage_loan",
        "metric_name_cn": "抵押借款",
        "metric_name_en": "Mortgage Loan",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["抵押借款"],
        "patterns_exact": ["抵押借款"],
        "parent_metric_code": "short_term_borrowings",
    },
    {
        "metric_code": "guaranteed_loan",
        "metric_name_cn": "保证借款",
        "metric_name_en": "Guaranteed Loan",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["保证借款"],
        "patterns_exact": ["保证借款"],
        "parent_metric_code": "short_term_borrowings",
    },
    {
        "metric_code": "credit_loan",
        "metric_name_cn": "信用借款",
        "metric_name_en": "Credit Loan",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["信用借款"],
        "patterns_exact": ["信用借款"],
        "parent_metric_code": "short_term_borrowings",
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
        "metric_code": "current_portion_noncurrent_liabilities",
        "metric_name_cn": "一年内到期的非流动负债",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["一年内到期的非流动负债"],
    },
    {
        "metric_code": "other_current_liabilities",
        "metric_name_cn": "其他流动负债",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["其他流动负债"],
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
        "patterns_exact": ["股本"],
    },
    {
        "metric_code": "capital_reserve",
        "metric_name_cn": "资本公积",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["资本公积"],
    },
    {
        "metric_code": "share_premium",
        "metric_name_cn": "股本溢价",
        "metric_name_en": "Share Premium",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["股本溢价"],
        "parent_metric_code": "capital_reserve",
    },
    {
        "metric_code": "retained_earnings",
        "metric_name_cn": "未分配利润",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["未分配利润"],
    },
    {
        "metric_code": "other_equity_instruments",
        "metric_name_cn": "其他权益工具",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["其他权益工具"],
    },
    {
        "metric_code": "treasury_stock",
        "metric_name_cn": "库存股",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["库存股"],
    },
    {
        "metric_code": "other_comprehensive_income",
        "metric_name_cn": "其他综合收益",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["其他综合收益"],
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
        "metric_code": "total_liabilities_equity",
        "metric_name_cn": "负债和所有者权益总计",
        "statement_type": "balance",
        "value_nature": "stock",
        "patterns": ["负债和所有者权益总计"],
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
        "metric_code": "cash_received_other_operating",
        "metric_name_cn": "收到其他与经营活动有关的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["收到其他与经营活动有关的现金"],
    },
    {
        "metric_code": "cash_paid_to_employees",
        "metric_name_cn": "支付给职工以及为职工支付的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["支付给职工以及为职工支付的现金"],
    },
    {
        "metric_code": "taxes_paid",
        "metric_name_cn": "支付的各项税费",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["支付的各项税费"],
    },
    {
        "metric_code": "cash_paid_other_operating",
        "metric_name_cn": "支付其他与经营活动有关的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["支付其他与经营活动有关的现金"],
    },
    {
        "metric_code": "operating_cash_inflows_subtotal",
        "metric_name_cn": "经营活动现金流入小计",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["经营活动现金流入小计"],
    },
    {
        "metric_code": "operating_cash_outflows_subtotal",
        "metric_name_cn": "经营活动现金流出小计",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["经营活动现金流出小计"],
    },
    {
        "metric_code": "net_cash_flow_investing",
        "metric_name_cn": "投资活动产生的现金流量净额",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["投资活动产生的现金流量净额"],
    },
    {
        "metric_code": "cash_received_from_investments",
        "metric_name_cn": "收回投资收到的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["收回投资收到的现金"],
    },
    {
        "metric_code": "cash_received_from_investment_income",
        "metric_name_cn": "取得投资收益收到的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["取得投资收益收到的现金"],
    },
    {
        "metric_code": "cash_received_from_disposal_long_term_assets",
        "metric_name_cn": "处置长期资产收回的现金净额",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["处置固定资产", "处置无形资产", "资产收回的现金净额"],
    },
    {
        "metric_code": "investing_cash_inflows_subtotal",
        "metric_name_cn": "投资活动现金流入小计",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["投资活动现金流入小计"],
    },
    {
        "metric_code": "cash_paid_for_long_term_assets",
        "metric_name_cn": "购建长期资产支付的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["购建固定资产", "购建无形资产", "资产支付的现金"],
    },
    {
        "metric_code": "cash_paid_for_investments",
        "metric_name_cn": "投资支付的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["投资支付的现金"],
    },
    {
        "metric_code": "investing_cash_outflows_subtotal",
        "metric_name_cn": "投资活动现金流出小计",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["投资活动现金流出小计"],
    },
    {
        "metric_code": "net_cash_flow_financing",
        "metric_name_cn": "筹资活动产生的现金流量净额",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["筹资活动产生的现金流量净额"],
    },
    {
        "metric_code": "cash_received_from_borrowings",
        "metric_name_cn": "取得借款收到的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["取得借款收到的现金"],
    },
    {
        "metric_code": "cash_received_other_financing",
        "metric_name_cn": "收到其他与筹资活动有关的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["收到其他与筹资活动有关的现金"],
    },
    {
        "metric_code": "financing_cash_inflows_subtotal",
        "metric_name_cn": "筹资活动现金流入小计",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["筹资活动现金流入小计"],
    },
    {
        "metric_code": "cash_paid_for_debt",
        "metric_name_cn": "偿还债务支付的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["偿还债务支付的现金"],
    },
    {
        "metric_code": "cash_paid_other_financing",
        "metric_name_cn": "支付其他与筹资活动有关的现金",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["支付其他与筹资活动有关的现金"],
    },
    {
        "metric_code": "financing_cash_outflows_subtotal",
        "metric_name_cn": "筹资活动现金流出小计",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["筹资活动现金流出小计"],
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
    {
        "metric_code": "depreciation_amortization",
        "metric_name_cn": "折旧及摊销",
        "statement_type": "cashflow",
        "value_nature": "flow",
        "patterns": ["折旧及摊销"],
    },
]


DICTIONARY_PATH = Path(__file__).resolve().parents[2] / "data" / "financial_dictionary.json"


def _load_dictionary_file(path: Path) -> list[dict] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    if isinstance(data, dict):
        items = data.get("metrics")
    else:
        items = data

    if not isinstance(items, list) or not items:
        return None

    normalized: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        metric_code = item.get("metric_code")
        metric_name_cn = item.get("metric_name_cn")
        statement_type = item.get("statement_type")
        value_nature = item.get("value_nature")
        if not metric_code or not metric_name_cn or not statement_type or not value_nature:
            continue
        normalized.append(
            {
                "metric_code": metric_code,
                "metric_name_cn": metric_name_cn,
                "metric_name_en": item.get("metric_name_en"),
                "statement_type": statement_type,
                "value_nature": value_nature,
                "parent_metric_code": item.get("parent_metric_code"),
                "patterns": list(item.get("patterns") or item.get("patterns_cn") or []),
                "patterns_exact": list(item.get("patterns_exact") or item.get("patterns_cn_exact") or []),
                "patterns_en": list(item.get("patterns_en") or []),
                "patterns_en_exact": list(item.get("patterns_en_exact") or []),
            }
        )

    return normalized or None


METRIC_DEFS = _load_dictionary_file(DICTIONARY_PATH) or BASE_METRIC_DEFS


def normalize_label(label: str) -> str:
    cleaned = re.sub(r"[\s\u3000]+", "", label)
    cleaned = re.sub(r"[：:（）()，,．.。;；-]+", "", cleaned)
    return cleaned.lower()


def metric_name_en_from_code(metric_code: str) -> str:
    return metric_code.replace("_", " ").title()


def get_metric_dictionary(use_base: bool = False) -> list[dict]:
    dictionary: list[dict] = []
    source = BASE_METRIC_DEFS if use_base else METRIC_DEFS
    for metric in source:
        metric_name_en = metric.get("metric_name_en") or metric_name_en_from_code(metric["metric_code"])
        dictionary.append(
            {
                "metric_code": metric["metric_code"],
                "metric_name_cn": metric["metric_name_cn"],
                "metric_name_en": metric_name_en,
                "statement_type": metric["statement_type"],
                "value_nature": metric["value_nature"],
                "parent_metric_code": metric.get("parent_metric_code"),
                "patterns_cn": list(metric.get("patterns", [])),
                "patterns_cn_exact": list(metric.get("patterns_exact", [])),
                "patterns_en": list(metric.get("patterns_en", [])),
                "patterns_en_exact": list(metric.get("patterns_en_exact", [])),
            }
        )
    return dictionary


def metric_code_from_label(label: str, statement_type: str) -> str:
    norm = normalize_label(label)
    digest = hashlib.sha1(f"{statement_type}:{norm}".encode("utf-8")).hexdigest()[:12]
    return f"raw_{digest}"


def _metric_patterns(metric: dict) -> list[str]:
    patterns = list(metric.get("patterns", []))
    patterns += list(metric.get("patterns_en", []))
    return patterns


def _metric_exact_patterns(metric: dict) -> set[str]:
    patterns = set(metric.get("patterns_exact", []))
    patterns.update(metric.get("patterns_en_exact", []))
    return {normalize_label(pattern) for pattern in patterns}


def match_metric(label: str, statement_type: str) -> dict | None:
    norm_label = normalize_label(label)
    label_has_ratio = ("率" in label) or ("%" in label)
    for metric in METRIC_DEFS:
        if metric["statement_type"] != statement_type:
            continue
        if label_has_ratio and metric["value_nature"] != "ratio":
            continue
        exact_patterns = _metric_exact_patterns(metric)
        for norm_pattern in exact_patterns:
            if norm_label == norm_pattern:
                return metric
        for pattern in _metric_patterns(metric):
            norm_pattern = normalize_label(pattern)
            if norm_pattern in norm_label:
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
