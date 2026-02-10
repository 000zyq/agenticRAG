from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

from app.ingest.metric_defs import normalize_label


STOP_LABELS = {
    "合计",
    "其他",
    "金额",
    "本期",
    "上期",
    "合计,",
}


def _load_dictionary(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    items = data.get("metrics") if isinstance(data, dict) else data
    if not isinstance(items, list):
        raise ValueError("Dictionary file must contain metrics list.")
    return items


def _write_dictionary(path: Path, metrics: list[dict]) -> None:
    payload = {"version": 1, "metrics": metrics}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_labels(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "labels" in data:
        return data["labels"]
    if isinstance(data, list):
        return data
    raise ValueError("Labels file must be a list of label records.")


def _normalize_lang(lang: str | None) -> str:
    if not lang:
        return "unknown"
    lower = lang.lower()
    if lower.startswith("zh"):
        return "cn"
    if lower.startswith("en"):
        return "en"
    return lower


def _infer_statement_type(text: str) -> str | None:
    if re.search(r"(现金|现金流|cash\s*flow)", text, re.IGNORECASE):
        return "cashflow"
    if re.search(r"(收入|成本|费用|利润|收益|税|income|revenue|expense|profit|loss)", text, re.IGNORECASE):
        return "income"
    if re.search(r"(资产|负债|权益|capital|equity|asset|liabil)", text, re.IGNORECASE):
        return "balance"
    return None


def _infer_value_nature(statement_type: str | None, text: str) -> str:
    if re.search(r"(率|%|percentage|ratio)", text, re.IGNORECASE):
        return "ratio"
    if statement_type == "balance":
        return "stock"
    return "flow"


def _concept_code(source: str, concept: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_]+", "_", concept)
    return f"xbrl_{source}_{safe}".lower()


def _build_label_index(metrics: list[dict]) -> tuple[dict[str, str], set[str]]:
    label_index: dict[str, str] = {}
    ambiguous: set[str] = set()

    for metric in metrics:
        code = metric["metric_code"]
        labels = [metric.get("metric_name_cn"), metric.get("metric_name_en")]
        labels += metric.get("patterns_cn", [])
        labels += metric.get("patterns_en", [])
        for label in labels:
            if not label:
                continue
            norm = normalize_label(label)
            if not norm:
                continue
            if norm in label_index and label_index[norm] != code:
                ambiguous.add(norm)
            else:
                label_index[norm] = code
    return label_index, ambiguous


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge taxonomy labels into dictionary.")
    parser.add_argument("--dictionary", default="data/financial_dictionary.json", help="Dictionary JSON path.")
    parser.add_argument("--labels", required=True, help="Labels JSON path produced by import_xbrl_taxonomy.py.")
    parser.add_argument("--output", default="", help="Output dictionary path.")
    parser.add_argument("--create", action="store_true", help="Create new metrics for unmatched concepts.")
    args = parser.parse_args()

    dict_path = Path(args.dictionary)
    labels_path = Path(args.labels)
    output_path = Path(args.output) if args.output else dict_path

    metrics = _load_dictionary(dict_path)
    labels = _load_labels(labels_path)

    label_index, ambiguous = _build_label_index(metrics)
    metric_map = {metric["metric_code"]: metric for metric in metrics}

    concept_labels: dict[str, list[dict]] = defaultdict(list)
    for entry in labels:
        concept = entry.get("concept")
        label = entry.get("label")
        if not concept or not label:
            continue
        concept_labels[concept].append(entry)

    added = 0
    created = 0
    for concept, entries in concept_labels.items():
        mapped_code = None
        for entry in entries:
            label = entry.get("label") or ""
            norm = normalize_label(label)
            if not norm or norm in ambiguous:
                continue
            if norm in label_index:
                mapped_code = label_index[norm]
                break

        if mapped_code:
            metric = metric_map[mapped_code]
        elif args.create:
            sample_text = " ".join(e.get("label", "") for e in entries)
            statement_type = _infer_statement_type(sample_text)
            if not statement_type:
                continue
            metric_code = _concept_code(entries[0].get("source", "xbrl"), concept)
            if metric_code in metric_map:
                metric = metric_map[metric_code]
            else:
                metric = {
                    "metric_code": metric_code,
                    "metric_name_cn": entries[0].get("label"),
                    "metric_name_en": None,
                    "statement_type": statement_type,
                    "value_nature": _infer_value_nature(statement_type, sample_text),
                    "parent_metric_code": None,
                    "patterns_cn": [],
                    "patterns_cn_exact": [],
                    "patterns_en": [],
                    "patterns_en_exact": [],
                }
                metrics.append(metric)
                metric_map[metric_code] = metric
                created += 1
        else:
            continue

        for entry in entries:
            label = entry.get("label") or ""
            norm = normalize_label(label)
            if not norm or norm in {normalize_label(item) for item in STOP_LABELS}:
                continue
            if any(char.isdigit() for char in label):
                continue
            lang = _normalize_lang(entry.get("lang"))
            if lang == "cn":
                bucket = "patterns_cn"
            elif lang == "en":
                bucket = "patterns_en"
            else:
                continue
            if label not in metric[bucket]:
                metric[bucket].append(label)
                added += 1

    _write_dictionary(output_path, metrics)
    print(f"Added {added} labels. Created {created} metrics. Output: {output_path}")


if __name__ == "__main__":
    main()
