from __future__ import annotations

from app.chunking.utils import count_tokens


def split_recursive(
    text: str,
    max_tokens: int,
    overlap_tokens: int,
    separators: list[str],
) -> list[str]:
    text = text.strip()
    if not text:
        return []

    if count_tokens(text) <= max_tokens:
        return [text]

    for sep in separators:
        if sep in text:
            parts = [p.strip() for p in text.split(sep) if p.strip()]
            if not parts:
                continue
            chunks: list[str] = []
            current = ""
            for part in parts:
                candidate = f"{current}{sep}{part}" if current else part
                if count_tokens(candidate) <= max_tokens:
                    current = candidate
                else:
                    if current:
                        chunks.append(current)
                    current = part
            if current:
                chunks.append(current)

            # If chunks are still too large, recurse deeper
            final_chunks: list[str] = []
            for chunk in chunks:
                if count_tokens(chunk) > max_tokens:
                    final_chunks.extend(split_recursive(chunk, max_tokens, overlap_tokens, separators[1:]))
                else:
                    final_chunks.append(chunk)
            return _apply_overlap(final_chunks, overlap_tokens)

    # Fallback hard split by token count
    return _hard_split(text, max_tokens, overlap_tokens)


def _hard_split(text: str, max_tokens: int, overlap_tokens: int) -> list[str]:
    # Fallback character split by approximate token ratio
    if max_tokens <= 0:
        return [text]
    tokens = list(text)
    chunks = []
    step = max(1, max_tokens - overlap_tokens)
    for i in range(0, len(tokens), step):
        chunk = "".join(tokens[i : i + max_tokens])
        chunks.append(chunk)
    return chunks


def _apply_overlap(chunks: list[str], overlap_tokens: int) -> list[str]:
    if overlap_tokens <= 0 or len(chunks) <= 1:
        return chunks
    overlapped: list[str] = []
    for i, chunk in enumerate(chunks):
        if i == 0:
            overlapped.append(chunk)
            continue
        prev = overlapped[-1]
        # naive overlap by last N characters of prev
        overlap = prev[-overlap_tokens:] if len(prev) > overlap_tokens else prev
        overlapped.append(f"{overlap}{chunk}")
    return overlapped
