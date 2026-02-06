from __future__ import annotations

import json
from typing import List

from openai import OpenAI

from app.config import get_settings


def rerank(query: str, chunks: list[dict]) -> list[dict]:
    if not chunks:
        return []
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key, base_url=settings.openai_base_url)

    passages = [c["text"] for c in chunks]
    numbered = "\n".join([f"[{i}] {p}" for i, p in enumerate(passages)])
    prompt = (
        "You are a relevance ranker. Given a user query and a list of passages, "
        "return a JSON array of objects with fields index and score (0-10), sorted by score desc. "
        "Only output JSON."
    )
    content = f"Query: {query}\n\nPassages:\n{numbered}"

    resp = client.chat.completions.create(
        model=settings.openai_rerank_model,
        messages=[{"role": "system", "content": prompt}, {"role": "user", "content": content}],
        temperature=0,
    )
    text = resp.choices[0].message.content or ""
    try:
        data = json.loads(text)
        scored = []
        for item in data:
            idx = int(item["index"])
            score = float(item.get("score", 0))
            if 0 <= idx < len(chunks):
                chunk = dict(chunks[idx])
                chunk["rerank_score"] = score
                scored.append(chunk)
        scored.sort(key=lambda c: c.get("rerank_score", 0), reverse=True)
        return scored
    except Exception:
        return chunks
