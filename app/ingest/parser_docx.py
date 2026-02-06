from __future__ import annotations

from docx import Document


def parse_docx(path: str) -> list[dict]:
    doc = Document(path)
    parts = []
    for i, para in enumerate(doc.paragraphs):
        text = (para.text or "").strip()
        if text:
            parts.append({"text": text, "paragraph": i + 1})
    return parts
