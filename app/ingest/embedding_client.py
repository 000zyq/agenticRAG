from __future__ import annotations

from typing import Iterable
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx

from app.config import get_settings


def embed_texts(texts: list[str]) -> list[list[float]]:
    settings = get_settings()
    vectors: list[list[float]] = []
    batch_size = max(1, settings.embedding_batch_size)
    concurrency = max(1, settings.embedding_concurrency)
    batches = [(i, texts[i : i + batch_size]) for i in range(0, len(texts), batch_size)]
    if not batches:
        return []

    logger = logging.getLogger("embedding")

    def _embed_batch(batch_texts: list[str]) -> list[list[float]]:
        payload = {"texts": batch_texts}
        with httpx.Client(timeout=300) as client:
            resp = client.post(settings.embedding_url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return data["vectors"]

    results: list[list[list[float]] | None] = [None] * len(batches)

    if concurrency == 1:
        for idx, batch in enumerate(batches):
            logger.info("Embedding batch %d/%d (size=%d)", idx + 1, len(batches), len(batch[1]))
            results[idx] = _embed_batch(batch[1])
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futures = {}
            for idx, batch in enumerate(batches):
                futures[ex.submit(_embed_batch, batch[1])] = idx
            completed = 0
            for fut in as_completed(futures):
                idx = futures[fut]
                results[idx] = fut.result()
                completed += 1
                logger.info("Embedding batch %d/%d completed", completed, len(batches))

    for item in results:
        if item:
            vectors.extend(item)
    return vectors
