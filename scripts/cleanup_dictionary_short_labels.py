from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.ingest.metric_defs import normalize_label


SHORT_LABEL_MAX = 2
SHORT_LABEL_DENYLIST = {
    "资产",
    "负债",
    "权益",
    "现金",
    "成本",
    "费用",
}

STOP_LABELS = {
    "合计",
    "小计",
    "其他",
    "金额",
    "项目",
    "单位",
    "币种",
    "余额",
    "本期",
    "上期",
    "本年",
    "上年",
    "本年度",
    "上年度",
    "期末",
    "期初",
    "年末",
    "年初",
    "期末余额",
    "期初余额",
    "年末余额",
    "年初余额",
    "其中",
    "合计,",
    "合计：",
    "小计：",
    "金额（元）",
    "金额(元)",
    "金额（人民币元）",
    "人民币",
    "元",
    "千元",
    "万元",
    "亿元",
    "total",
    "subtotal",
    "amount",
    "balance",
    "currency",
    "unit",
    "current",
    "prior",
    "year",
}


def _load_dictionary(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "metrics" not in data:
        raise ValueError("Dictionary file must contain a 'metrics' list.")
    return data


def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _cleanup_metric(metric: dict) -> bool:
    changed = False
    cn = list(metric.get("patterns_cn") or [])
    cn_exact = list(metric.get("patterns_cn_exact") or [])
    en = list(metric.get("patterns_en") or [])
    en_exact = list(metric.get("patterns_en_exact") or [])
    stop_norm = {normalize_label(item) for item in STOP_LABELS}
    short_deny_norm = {normalize_label(item) for item in SHORT_LABEL_DENYLIST}

    cleaned_cn: list[str] = []
    for label in cn:
        norm = normalize_label(label)
        if norm in stop_norm:
            changed = True
            continue
        if len(norm) <= SHORT_LABEL_MAX:
            if norm in short_deny_norm:
                changed = True
                continue
            if label not in cn_exact:
                cn_exact.append(label)
                changed = True
            changed = True
            continue
        cleaned_cn.append(label)
    if cleaned_cn != cn:
        cn = cleaned_cn

    cleaned_en: list[str] = []
    for label in en:
        norm = normalize_label(label)
        if norm in stop_norm:
            changed = True
            continue
        if len(norm) <= SHORT_LABEL_MAX:
            if label not in en_exact:
                en_exact.append(label)
                changed = True
            changed = True
            continue
        cleaned_en.append(label)
    if cleaned_en != en:
        en = cleaned_en

    metric["patterns_cn"] = _dedupe(cn)
    metric["patterns_cn_exact"] = _dedupe(cn_exact)
    metric["patterns_en"] = _dedupe(en)
    metric["patterns_en_exact"] = _dedupe(en_exact)
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize short labels in metric dictionary.")
    parser.add_argument("--dictionary", default="data/financial_dictionary.json")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    path = Path(args.dictionary)
    data = _load_dictionary(path)
    metrics = data["metrics"]

    changed = 0
    for metric in metrics:
        if _cleanup_metric(metric):
            changed += 1

    output_path = Path(args.output) if args.output else path
    output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Updated {changed} metrics. Output: {output_path}")


if __name__ == "__main__":
    main()
