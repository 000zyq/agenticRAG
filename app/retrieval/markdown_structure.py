"""Literal and structural lookup over page-preserving Markdown."""
from collections import defaultdict
from html import unescape
import re
import unicodedata


HEADING = re.compile(r"^\s*(?:#{1,6}\s+|\([a-z\d]+\)\s+)(.+)$", re.MULTILINE | re.IGNORECASE)
CONTINUED = re.compile(r"\s*\(\s*续\s*\)\s*$")
TABLE = re.compile(r"<table\b[^>]*>(.*?)</table>", re.IGNORECASE | re.DOTALL)
ROW = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL = re.compile(r"</t[dh]>\s*<t[dh]\b[^>]*>", re.IGNORECASE)
PIPE_RULE = re.compile(r"\s*\|?\s*:?-{3,}:?(?:\s*\|\s*:?-{3,}:?)+\s*\|?\s*")


def normalize(text):
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


def table_headers(markdown):
    for table in TABLE.findall(markdown):
        rows = ROW.findall(table)
        if rows:
            header = [rows[0]]
            for row in rows[1:]:
                if not re.search(r"<th\b", row, re.IGNORECASE):
                    break
                header.append(row)
            yield " | ".join(unescape(re.sub(r"<[^>]+>", "", CELL.sub(" | ", row))) for row in header)
    lines = markdown.splitlines()
    for line, rule in zip(lines, lines[1:]):
        if "|" in line and PIPE_RULE.fullmatch(rule):
            yield line.strip()


class MarkdownStructure:
    def __init__(self, pages, passages):
        self.targets = defaultdict(list)
        self.passages = passages
        self.page_passages = defaultdict(list)
        for index, (page, passage) in enumerate(passages):
            self.page_passages[page].append((index, normalize(passage)))
        starts = {}
        for page, text in pages:
            self.targets["text"].append((page, normalize(text), None, None))
            for heading in HEADING.findall(text):
                key = normalize(heading)
                base = CONTINUED.sub("", key)
                start = starts.get(base) if base != key else None
                target = (page, key, heading, start)
                self.targets["heading"].append(target)
                if base == key:
                    starts[base] = target
            for header in table_headers(text):
                self.targets["table_header"].append((page, normalize(header), header, None))

    def locate(self, phrases, scope, match):
        if scope not in {"text", "heading", "table_header"} or match not in {"any", "all"}:
            raise ValueError("Invalid locate scope or match")
        if not isinstance(phrases, list) or not 1 <= len(phrases) <= 12:
            raise ValueError("Supply between 1 and 12 literal phrases")
        if any(not isinstance(p, str) or not p.strip() or len(p) > 120 for p in phrases):
            raise ValueError("Phrases must be nonempty strings up to 120 characters")
        needles = {phrase: normalize(phrase) for phrase in phrases}
        hits = []
        for page, target, label, start in self.targets[scope]:
            matched = [phrase for phrase, needle in needles.items() if needle in target]
            if not matched or (match == "all" and len(matched) != len(needles)):
                continue
            hit = self._reference(page, label, [needles[p] for p in matched])
            hit["matched"] = matched
            if start:
                hit["continuation_of"] = self._reference(start[0], start[2], [start[1]])
            hits.append(hit)
        hits.sort(key=lambda hit: -len(hit["matched"]))
        return {"hits": len(hits), "matches": hits[:15]}

    def _reference(self, page, label, needles):
        candidates = self.page_passages[page]
        if not candidates:
            return {"page": page, "label": label, "passage": None}
        passage, _ = max(candidates, key=lambda item: sum(needle in item[1] for needle in needles))
        return {"page": page, "label": label, "passage": passage}
