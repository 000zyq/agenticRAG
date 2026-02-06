from __future__ import annotations

import math
from typing import List

from app.chunking.utils import split_sentences, count_tokens
from app.ingest.embedding_client import embed_texts


DEFAULT_SIM_THRESHOLD = 0.75


def split_semantic(text: str, max_tokens: int, overlap_tokens: int, sim_threshold: float = DEFAULT_SIM_THRESHOLD) -> list[str]:
    sentences = split_sentences(text)
    if not sentences:
        return []
    vectors = embed_texts(sentences)
    chunks: list[str] = []

    current_sentences = [sentences[0]]
    current_vec = vectors[0]
    current_tokens = count_tokens(sentences[0])

    for i in range(1, len(sentences)):
        sent = sentences[i]
        vec = vectors[i]
        sim = _cosine(current_vec, vec)
        sent_tokens = count_tokens(sent)
        if sim >= sim_threshold and (current_tokens + sent_tokens) <= max_tokens:
            current_sentences.append(sent)
            current_tokens += sent_tokens
            current_vec = _avg_vec(current_vec, vec)
        else:
            chunks.append(" ".join(current_sentences))
            current_sentences = [sent]
            current_vec = vec
            current_tokens = sent_tokens

    if current_sentences:
        chunks.append(" ".join(current_sentences))

    return _apply_overlap(chunks, overlap_tokens)


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _avg_vec(a: List[float], b: List[float]) -> List[float]:
    return [(x + y) / 2.0 for x, y in zip(a, b)]


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
