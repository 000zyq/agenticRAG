from __future__ import annotations

from app.chunking.recursive import split_recursive
from app.chunking.sentence import split_sentence
from app.chunking.semantic import split_semantic
from app.config import get_settings


DEFAULT_SEPARATORS = [
    "\n\n",
    "\n",
    "。",
    "！",
    "？",
    ". ",
    "; ",
    ", ",
    " ",
]


def chunk_text(text: str) -> list[str]:
    settings = get_settings()
    strategy = settings.chunk_strategy.lower()
    max_tokens = settings.chunk_size_tokens
    overlap = settings.chunk_overlap_tokens

    if strategy == "recursive":
        return split_recursive(text, max_tokens, overlap, DEFAULT_SEPARATORS)
    if strategy == "sentence":
        return split_sentence(text, max_tokens, overlap)
    if strategy == "semantic":
        return split_semantic(text, max_tokens, overlap)

    return split_recursive(text, max_tokens, overlap, DEFAULT_SEPARATORS)
