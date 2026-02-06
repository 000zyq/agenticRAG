from __future__ import annotations

import re
from typing import Iterable

import tiktoken


def get_encoder():
    return tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str) -> int:
    enc = get_encoder()
    return len(enc.encode(text))


def split_sentences(text: str) -> list[str]:
    # Simple multilingual sentence splitter (Chinese + English punctuation)
    pattern = r"(?<=[。！？.!?])\s+"
    parts = re.split(pattern, text)
    return [p.strip() for p in parts if p.strip()]
