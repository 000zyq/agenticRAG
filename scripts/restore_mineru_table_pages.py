"""Write a separate MinerU content list using physical-page table fragments."""
import argparse
from collections import defaultdict
import json
from pathlib import Path


def table_body(block):
    bodies = [part for part in block["blocks"] if part["type"] == "table_body"]
    spans = [span for body in bodies for line in body["lines"] for span in line["spans"]]
    html = [span["html"] for span in spans if span.get("html")]
    if len(html) > 1:
        raise ValueError("Expected one HTML span per table")
    return (html[0] if html else "", any(body.get("lines_deleted") for body in bodies))


def restore_tables(items, middle):
    tables = defaultdict(list)
    result = [dict(item) for item in items]
    for item in result:
        if item["type"] == "table":
            tables[item["page_idx"]].append(item)
    changed = 0
    seen = set()
    for page in middle["pdf_info"]:
        page_idx = page["page_idx"]
        if page_idx in seen:
            raise ValueError(f"Duplicate middle page index: {page_idx}")
        seen.add(page_idx)
        after = [block for block in page["para_blocks"] if block["type"] == "table"]
        before = {tuple(block["bbox"]): block for block in page["preproc_blocks"] if block["type"] == "table"}
        if len(after) != len(tables[page_idx]):
            raise ValueError(f"Table count mismatch on physical page {page_idx + 1}")
        for item, block in zip(tables[page_idx], after, strict=True):
            final_html, deleted = table_body(block)
            if (item.get("table_body") or "").strip() != final_html.strip():
                raise ValueError(f"Table order or HTML mismatch on physical page {page_idx + 1}")
            original = before.get(tuple(block["bbox"]))
            if original is None:
                continue
            source_html, _ = table_body(original)
            if source_html and source_html != final_html and (final_html or deleted):
                item["table_body"] = source_html
                changed += 1
    if tables.keys() - seen:
        raise ValueError("Content list contains tables outside middle JSON pages")
    return result, changed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("content_list", type=Path)
    parser.add_argument("middle", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    result, changed = restore_tables(json.loads(args.content_list.read_text()),
                                     json.loads(args.middle.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as target:
        json.dump(result, target, ensure_ascii=False)
    print(f"Restored {changed} table fragments")


if __name__ == "__main__":
    main()
