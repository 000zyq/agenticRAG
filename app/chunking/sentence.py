from __future__ import annotations

from app.chunking.utils import split_sentences, count_tokens


def split_sentence(text: str, max_tokens: int, overlap_tokens: int) -> list[str]:
    sentences = split_sentences(text)
    chunks: list[str] = []
    current = ""
    for sent in sentences:
        candidate = f"{current} {sent}".strip() if current else sent
        if count_tokens(candidate) <= max_tokens:
            current = candidate
        else:
            if current:
                chunks.append(current)
            current = sent
    if current:
        chunks.append(current)
    return _apply_overlap(chunks, overlap_tokens)


def _apply_overlap(chunks: list[str], overlap_tokens: int) -> list[str]:
    if overlap_tokens <= 0 or len(chunks) <= 1:
        return chunks
    overlapped: list[str] = []
    for i, chunk in enumerate(chunks):
        if i == 0:
            overlapped.append(chunk)
            continue
        prev = overlapped[-1]
        overlap = prev[-overlap_tokens:] if len(prev) > overlap_tokens else prev
        overlapped.append(f"{overlap}{chunk}")
    return overlapped
