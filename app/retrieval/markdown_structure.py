"""Literal and structural lookup over page-preserving Markdown."""
from collections import defaultdict
from html import unescape
import json
import re
import subprocess
import unicodedata


HEADING = re.compile(r"^\s*(?:#{1,6}\s+|\([a-z\d]+\)\s+)(.+)$", re.MULTILINE | re.IGNORECASE)
CONTINUED = re.compile(r"\s*\(\s*续\s*\)\s*$")
TABLE = re.compile(r"<table\b[^>]*>(.*?)</table>", re.IGNORECASE | re.DOTALL)
ROW = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL = re.compile(r"</t[dh]>\s*<t[dh]\b[^>]*>", re.IGNORECASE)
PIPE_RULE = re.compile(r"\s*\|?\s*:?-{3,}:?(?:\s*\|\s*:?-{3,}:?)+\s*\|?\s*")


def normalize(text):
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


def table_row_text(row):
    return unescape(re.sub(r"<[^>]+>", "", CELL.sub(" | ", row)))


def grep_lines(markdown):
    def replace_table(match):
        return "\n" + "\n".join(" ".join(table_row_text(row).split()) for row in ROW.findall(match[1])) + "\n"

    return TABLE.sub(replace_table, markdown).splitlines()


def table_headers(markdown):
    for table in TABLE.findall(markdown):
        rows = ROW.findall(table)
        if rows:
            header = [rows[0]]
            for row in rows[1:]:
                if not re.search(r"<th\b", row, re.IGNORECASE):
                    break
                header.append(row)
            yield " | ".join(table_row_text(row) for row in header)
    lines = markdown.splitlines()
    for line, rule in zip(lines, lines[1:]):
        if "|" in line and PIPE_RULE.fullmatch(rule):
            yield line.strip()


class MarkdownStructure:
    def __init__(self, pages, passages):
        self.targets = defaultdict(list)
        self.passages = passages
        self.page_passages = defaultdict(list)
        self.grep_pages = [(page, grep_lines(text)) for page, text in pages]
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

    def grep(self, patterns, mode, context):
        if mode not in {"literal", "regex"} or type(context) is not int or not 0 <= context <= 2:
            raise ValueError("Use literal or regex mode and 0 to 2 context lines")
        if not isinstance(patterns, list) or not 1 <= len(patterns) <= 12 or any(
            not isinstance(pattern, str) or not pattern.strip() or len(pattern) > 200
            or any(char in pattern for char in ("\n", "\r", "\0")) for pattern in patterns
        ):
            raise ValueError("Supply 1 to 12 nonempty, single-line patterns up to 200 characters")

        lines = [(page, page_lines, i) for page, page_lines in self.grep_pages
                 for i in range(len(page_lines))]
        command = ["rg", "--no-config", "--json", "--ignore-case", "--max-count", "21"]
        if mode == "literal":
            command.append("--fixed-strings")
        for pattern in patterns:
            command.extend(["-e", pattern])
        try:
            result = subprocess.run(
                [*command, "--", "-"], input="\n".join(page_lines[i] for _, page_lines, i in lines),
                capture_output=True, encoding="utf-8", timeout=5, check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError("The grep tool requires ripgrep (rg)") from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("Markdown grep exceeded its time limit") from exc
        if result.returncode not in {0, 1}:
            raise ValueError(result.stderr.strip()[:500] or "Markdown grep failed")

        matches = []
        for event in map(json.loads, result.stdout.splitlines()):
            if event["type"] != "match":
                continue
            data = event["data"]
            page, page_lines, index = lines[data["line_number"] - 1]
            match = data["submatches"][0]
            encoded_line = page_lines[index].encode("utf-8")
            start, end = match["start"], match["end"]
            snippet = encoded_line[max(0, start - 240):max(start + 480, end + 360)].decode("utf-8", errors="ignore")
            reference = self._reference(page, None, [normalize(match["match"]["text"])])
            matches.append({
                "page": page, "passage": reference["passage"], "line": index + 1,
                "text": snippet[:800],
                "before": [s[:240] for s in page_lines[max(0, index - context):index]],
                "after": [s[:240] for s in page_lines[index + 1:index + 1 + context]],
            })
        return {"matches": matches[:20], "truncated": len(matches) > 20}

    def _reference(self, page, label, needles):
        candidates = self.page_passages[page]
        if not candidates:
            return {"page": page, "label": label, "passage": None}
        passage, _ = max(candidates, key=lambda item: sum(needle in item[1] for needle in needles))
        return {"page": page, "label": label, "passage": passage}
