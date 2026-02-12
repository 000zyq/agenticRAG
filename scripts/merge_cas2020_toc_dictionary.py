from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from app.ingest.metric_defs import normalize_label


OVERRIDE_BY_SUB_NAME = {
    "合同资产": "contract_assets",
}
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
    "其中",
    "人民币",
    "元",
    "千元",
    "万元",
    "亿元",
}


def _load_dictionary(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "metrics" not in data:
        raise ValueError("Dictionary file must contain a 'metrics' field.")
    return data


def _load_toc(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "sub_categories" not in data:
        raise ValueError("TOC file must contain a 'sub_categories' field.")
    return data


def _infer_statement_type(cas_name: str, sub_name: str) -> str:
    text = f"{cas_name} {sub_name}"
    if re.search(r"(现金流量)", text):
        return "cashflow"
    if re.search(r"(资产|负债|权益|财务报表|金融工具|合并财务报表|主体中权益|公允价值|持有待售|租赁)", text):
        return "balance"
    return "income"


def _infer_value_nature(statement_type: str, sub_name: str) -> str:
    if re.search(r"(率|每股|%|比率)", sub_name):
        return "ratio"
    if statement_type == "balance":
        return "stock"
    return "flow"


def _metric_name_en_from_code(metric_code: str) -> str:
    return metric_code.replace("_", " ").title()


def _build_index(metrics: list[dict]) -> dict[str, list[dict]]:
    # De-duplicate by metric_code, otherwise one metric may appear multiple times
    # (metric_name + aliases) and be mistaken as ambiguous.
    index: dict[str, dict[str, dict]] = {}
    for metric in metrics:
        labels = [metric.get("metric_name_cn")]
        labels += metric.get("patterns_cn", [])
        labels += metric.get("patterns_cn_exact", [])
        for label in labels:
            if not label:
                continue
            norm = normalize_label(label)
            if not norm:
                continue
            by_code = index.setdefault(norm, {})
            by_code[metric["metric_code"]] = metric
    return {norm: list(by_code.values()) for norm, by_code in index.items()}


def _ensure_metric_fields(metric: dict) -> None:
    metric.setdefault("metric_name_en", _metric_name_en_from_code(metric["metric_code"]))
    metric.setdefault("parent_metric_code", None)
    metric.setdefault("patterns_cn", [])
    metric.setdefault("patterns_cn_exact", [])
    metric.setdefault("patterns_en", [])
    metric.setdefault("patterns_en_exact", [])


def _append_cn_pattern(metric: dict, label: str, prefer_exact: bool = False) -> int:
    norm = normalize_label(label)
    if not norm:
        return 0
    stop_norm = {normalize_label(item) for item in STOP_LABELS}
    short_deny_norm = {normalize_label(item) for item in SHORT_LABEL_DENYLIST}
    if norm in stop_norm:
        return 0
    if len(norm) <= SHORT_LABEL_MAX and norm in short_deny_norm:
        return 0
    bucket = "patterns_cn_exact" if prefer_exact or len(norm) <= SHORT_LABEL_MAX else "patterns_cn"
    if label in metric[bucket]:
        return 0
    metric[bucket].append(label)
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge CAS2020 TOC sub-categories into metric dictionary.")
    parser.add_argument("--dictionary", default="data/financial_dictionary.json")
    parser.add_argument("--toc", default="data/taxonomy/cas2020_toc.json")
    parser.add_argument("--output", default="")
    parser.add_argument("--mapping-output", default="data/taxonomy/cas2020_metric_mapping.json")
    parser.add_argument("--create-missing", action="store_true", help="Create metric if sub-category is unmatched.")
    args = parser.parse_args()

    dictionary_path = Path(args.dictionary)
    toc_path = Path(args.toc)
    output_path = Path(args.output) if args.output else dictionary_path

    dictionary = _load_dictionary(dictionary_path)
    metrics: list[dict] = dictionary["metrics"]
    sub_categories = _load_toc(toc_path)["sub_categories"]

    metric_by_code = {metric["metric_code"]: metric for metric in metrics}
    index = _build_index(metrics)
    mapping_records: list[dict] = []

    matched = 0
    created = 0
    alias_added = 0
    skipped = 0

    for item in sub_categories:
        sub_code = str(item.get("sub_code") or "").strip()
        sub_name = str(item.get("sub_name") or "").strip()
        sub_name_raw = str(item.get("sub_name_raw") or "").strip()
        cas_name = str(item.get("cas_name") or "").strip()
        if not sub_code or not sub_name:
            continue

        target_metric: dict | None = None
        mapping_method = "unmapped"
        mapping_confidence = 0.0
        override_code = OVERRIDE_BY_SUB_NAME.get(sub_name)
        if override_code:
            target_metric = metric_by_code.get(override_code)
            if target_metric is not None:
                mapping_method = "override"
                mapping_confidence = 1.0
            if target_metric is None and args.create_missing:
                statement_type = _infer_statement_type(cas_name, sub_name)
                target_metric = {
                    "metric_code": override_code,
                    "metric_name_cn": sub_name,
                    "metric_name_en": _metric_name_en_from_code(override_code),
                    "statement_type": statement_type,
                    "value_nature": _infer_value_nature(statement_type, sub_name),
                    "parent_metric_code": None,
                    "patterns_cn": [],
                    "patterns_cn_exact": [],
                    "patterns_en": [],
                    "patterns_en_exact": [],
                }
                metrics.append(target_metric)
                metric_by_code[override_code] = target_metric
                created += 1
                mapping_method = "override_create"
                mapping_confidence = 0.95

        if target_metric is None:
            candidates = index.get(normalize_label(sub_name), [])
            if len(candidates) == 1:
                target_metric = candidates[0]
                matched += 1
                mapping_method = "unique_name"
                mapping_confidence = 0.9
            elif args.create_missing:
                metric_code = f"cas2020_{sub_code}"
                target_metric = metric_by_code.get(metric_code)
                if target_metric is None:
                    statement_type = _infer_statement_type(cas_name, sub_name)
                    target_metric = {
                        "metric_code": metric_code,
                        "metric_name_cn": sub_name,
                        "metric_name_en": _metric_name_en_from_code(metric_code),
                        "statement_type": statement_type,
                        "value_nature": _infer_value_nature(statement_type, sub_name),
                        "parent_metric_code": None,
                        "patterns_cn": [],
                        "patterns_cn_exact": [],
                        "patterns_en": [],
                        "patterns_en_exact": [],
                    }
                    metrics.append(target_metric)
                    metric_by_code[metric_code] = target_metric
                    created += 1
                mapping_method = "auto_create"
                mapping_confidence = 0.5
            else:
                skipped += 1
                mapping_records.append(
                    {
                        "cas_code": item.get("cas_code"),
                        "cas_name": cas_name,
                        "sub_code": sub_code,
                        "sub_name": sub_name,
                        "sub_name_raw": sub_name_raw,
                        "metric_code": None,
                        "mapping_method": mapping_method,
                        "confidence": mapping_confidence,
                    }
                )
                continue

        if target_metric is None:
            skipped += 1
            mapping_records.append(
                {
                    "cas_code": item.get("cas_code"),
                    "cas_name": cas_name,
                    "sub_code": sub_code,
                    "sub_name": sub_name,
                    "sub_name_raw": sub_name_raw,
                    "metric_code": None,
                    "mapping_method": mapping_method,
                    "confidence": mapping_confidence,
                }
            )
            continue

        _ensure_metric_fields(target_metric)

        if sub_name:
            alias_added += _append_cn_pattern(target_metric, sub_name, prefer_exact=True)
        if sub_name_raw and sub_name_raw != sub_name:
            alias_added += _append_cn_pattern(target_metric, sub_name_raw)

        code_alias = f"[{sub_code}] {sub_name_raw or sub_name}"
        alias_added += _append_cn_pattern(target_metric, code_alias)

        mapping_records.append(
            {
                "cas_code": item.get("cas_code"),
                "cas_name": cas_name,
                "sub_code": sub_code,
                "sub_name": sub_name,
                "sub_name_raw": sub_name_raw,
                "metric_code": target_metric["metric_code"],
                "mapping_method": mapping_method,
                "confidence": mapping_confidence,
            }
        )

    output_path.write_text(json.dumps(dictionary, ensure_ascii=False, indent=2), encoding="utf-8")
    mapping_path = Path(args.mapping_output)
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    mapping_path.write_text(
        json.dumps(
            {
                "source_toc": str(toc_path),
                "dictionary": str(output_path),
                "records": mapping_records,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "matched": matched,
                "created": created,
                "alias_added": alias_added,
                "skipped": skipped,
                "output": str(output_path),
                "mapping_output": str(mapping_path),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
