from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import tempfile
import zipfile
import xml.etree.ElementTree as ET


XLINK_NS = "http://www.w3.org/1999/xlink"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
XML_NS = "http://www.w3.org/XML/1998/namespace"

XLINK = f"{{{XLINK_NS}}}"
LINK = f"{{{LINK_NS}}}"
XML = f"{{{XML_NS}}}"


def _iter_label_files(root: Path) -> list[Path]:
    label_files: list[Path] = []
    for path in root.rglob("*.xml"):
        name = path.name.lower()
        if "label" in name or re.search(r"(lab|labels?)", name):
            label_files.append(path)
    return label_files


def _concept_from_href(href: str | None) -> str | None:
    if not href:
        return None
    fragment = href.split("#")[-1]
    if ":" in fragment:
        fragment = fragment.split(":")[-1]
    return fragment or None


def _extract_labels(path: Path, source_root: Path, source: str) -> list[dict]:
    try:
        tree = ET.parse(path)
    except ET.ParseError:
        return []
    root = tree.getroot()

    locators: dict[str, str] = {}
    labels: dict[str, list[dict]] = {}
    arcs: list[tuple[str, str]] = []

    for loc in root.findall(f".//{LINK}loc"):
        label = loc.attrib.get(f"{XLINK}label")
        href = loc.attrib.get(f"{XLINK}href")
        concept = _concept_from_href(href)
        if label and concept:
            locators[label] = concept

    for label_node in root.findall(f".//{LINK}label"):
        label_key = label_node.attrib.get(f"{XLINK}label")
        if not label_key:
            continue
        role = label_node.attrib.get(f"{XLINK}role")
        lang = label_node.attrib.get(f"{XML}lang")
        text = "".join(label_node.itertext()).strip()
        if not text:
            continue
        labels.setdefault(label_key, []).append(
            {
                "label": text,
                "lang": lang,
                "role": role,
            }
        )

    for arc in root.findall(f".//{LINK}labelArc"):
        src = arc.attrib.get(f"{XLINK}from")
        dst = arc.attrib.get(f"{XLINK}to")
        if src and dst:
            arcs.append((src, dst))

    results: list[dict] = []
    for src, dst in arcs:
        concept = locators.get(src)
        if not concept:
            continue
        for label_entry in labels.get(dst, []):
            results.append(
                {
                    "concept": concept,
                    "label": label_entry["label"],
                    "lang": label_entry.get("lang"),
                    "role": label_entry.get("role"),
                    "source": source,
                    "file": str(path.relative_to(source_root)),
                }
            )
    return results


def _load_labels(input_path: Path, source: str) -> list[dict]:
    if input_path.is_dir():
        label_files = _iter_label_files(input_path)
        records: list[dict] = []
        for path in label_files:
            records.extend(_extract_labels(path, input_path, source))
        return records

    if input_path.suffix.lower() == ".zip":
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            with zipfile.ZipFile(input_path, "r") as zf:
                zf.extractall(tmp_path)
            label_files = _iter_label_files(tmp_path)
            records: list[dict] = []
            for path in label_files:
                records.extend(_extract_labels(path, tmp_path, source))
            return records

    raise ValueError(f"Unsupported input path: {input_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract labels from XBRL taxonomy label linkbases.")
    parser.add_argument("--source", required=True, help="Source name, e.g., cas or ifrs.")
    parser.add_argument("--input", required=True, help="Path to taxonomy folder or zip archive.")
    parser.add_argument("--output", default="", help="Output JSON file path.")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input not found: {input_path}")

    output_path = Path(args.output) if args.output else Path("data") / "taxonomy" / f"{args.source}_labels.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    records = _load_labels(input_path, args.source)
    if not records:
        raise SystemExit("No labels extracted; check input or label linkbase files.")

    deduped = {
        (rec["concept"], rec["label"], rec.get("lang"), rec.get("role"), rec["source"]): rec
        for rec in records
    }

    output_path.write_text(json.dumps(list(deduped.values()), ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(deduped)} labels to {output_path}")


if __name__ == "__main__":
    main()
