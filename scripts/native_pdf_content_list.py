"""Prepare shared, page-preserving text and ruled tables from a born-digital PDF.

Run with: uv run --no-project --with pdfplumber python scripts/native_pdf_content_list.py ...
This uses the existing content-list interchange format; it does not run MinerU.
"""
import argparse
from html import escape
import json
from pathlib import Path
from time import perf_counter


def html_table(rows):
    return "<table>" + "".join(
        "<tr>" + "".join(f"<td>{escape(''.join((cell or '').splitlines()))}</td>" for cell in row) + "</tr>"
        for row in rows
    ) + "</table>"


def extract(source):
    import pdfplumber

    items = []
    with pdfplumber.open(source) as document:
        for idx, page in enumerate(document.pages):
            previous_bottom = page.bbox[1]

            def emit_text(top, bottom):
                if bottom <= top:
                    return
                region = (page.bbox[0], top, page.bbox[2], bottom)
                text = page.crop(region).extract_text()
                if text and text.strip():
                    items.append({"type": "text", "text": text.strip(), "page_idx": idx})

            tables = sorted(page.find_tables(), key=lambda t: (t.bbox[1], t.bbox[0]))
            for table in tables:
                _, top, _, bottom = table.bbox
                emit_text(previous_bottom, top)
                if top >= previous_bottom:
                    items.append({"type": "table", "table_body": html_table(table.extract()), "page_idx": idx})
                    previous_bottom = bottom
                else:
                    raise ValueError(f"Overlapping table bands on physical page {idx + 1}")
            emit_text(previous_bottom, page.bbox[3])
            page.close()
    return items


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    start = perf_counter()
    items = extract(args.source)
    if not items:
        parser.error("No native PDF text or tables extracted")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(items, stream, ensure_ascii=False)
    print(json.dumps({"engine": "pdfplumber", "items": len(items), "seconds": perf_counter() - start}))


if __name__ == "__main__":
    main()
