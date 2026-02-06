from __future__ import annotations

from pathlib import Path
from pypdf import PdfReader


def parse_pdf(path: str) -> list[dict]:
    reader = PdfReader(path)
    pages = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        text = text.strip()
        if text:
            pages.append({"text": text, "page": i + 1})
    return pages
