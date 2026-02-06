from __future__ import annotations

from openai import OpenAI

from app.config import get_settings


def generate_answer(query: str, contexts: list[dict]) -> dict:
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key, base_url=settings.openai_base_url)

    context_blocks = []
    for i, ctx in enumerate(contexts, start=1):
        source = ctx.get("source_path", "unknown")
        page = ctx.get("page", 0)
        text = ctx.get("text", "")
        context_blocks.append(f"[{i}] Source: {source} | Page: {page}\n{text}")

    system_prompt = (
        "You are a helpful assistant. Answer in concise Chinese. "
        "Use the provided sources to answer. If sources are insufficient, say so. "
        "Cite sources using [1], [2], etc." 
    )
    user_prompt = f"Question: {query}\n\nSources:\n" + "\n\n".join(context_blocks)

    resp = client.chat.completions.create(
        model=settings.openai_chat_model,
        messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        temperature=0.2,
    )
    answer = resp.choices[0].message.content or ""

    citations = []
    for i, ctx in enumerate(contexts, start=1):
        citations.append(
            {
                "index": i,
                "source_path": ctx.get("source_path"),
                "page": ctx.get("page"),
                "snippet": ctx.get("text", "")[:200],
            }
        )

    return {"answer": answer, "citations": citations}


def stream_answer(query: str, contexts: list[dict]):
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key, base_url=settings.openai_base_url)

    context_blocks = []
    for i, ctx in enumerate(contexts, start=1):
        source = ctx.get("source_path", "unknown")
        page = ctx.get("page", 0)
        text = ctx.get("text", "")
        context_blocks.append(f"[{i}] Source: {source} | Page: {page}\n{text}")

    system_prompt = (
        "You are a helpful assistant. Answer in concise Chinese. "
        "Use the provided sources to answer. If sources are insufficient, say so. "
        "Cite sources using [1], [2], etc."
    )
    user_prompt = f"Question: {query}\n\nSources:\n" + "\n\n".join(context_blocks)

    stream = client.chat.completions.create(
        model=settings.openai_chat_model,
        messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        temperature=0.2,
        stream=True,
    )

    for chunk in stream:
        delta = chunk.choices[0].delta.content if chunk.choices else None
        if delta:
            yield delta
