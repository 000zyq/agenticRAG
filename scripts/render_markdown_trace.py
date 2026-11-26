"""Render a Markdown RAG JSONL trace as a self-contained HTML report."""
import argparse
from collections import defaultdict
from html import escape
import json
from pathlib import Path


STYLE = """
:root { color-scheme: light; --ink:#172e32; --paper:#fffef9; --line:#d5dfd9; }
* { box-sizing:border-box; }
body { margin:0; color:var(--ink); background:linear-gradient(145deg,#e3eee8,#f7f4e9);
       font:16px/1.55 Charter,Georgia,"Songti SC",serif; }
main { max-width:940px; margin:auto; padding:clamp(1rem,5vw,3rem); }
header { border-bottom:2px solid var(--ink); padding-bottom:1rem; margin-bottom:1.5rem; }
h1 { font-size:clamp(1.8rem,4vw,2.7rem); margin:0; }
h2 { font-size:1.25rem; margin:.2rem 0 .8rem; }
p { margin:.35rem 0; }
.meta { color:#42605c; }
section { margin:1rem 0; padding:1rem; background:var(--paper);
          border:1px solid var(--line); border-radius:.65rem; }
details { padding:.65rem 0; border-top:1px solid var(--line); }
summary { cursor:pointer; overflow-wrap:anywhere; }
summary:focus-visible { outline:2px solid #087569; }
code,pre { font-family:"SFMono-Regular",Consolas,monospace; }
pre { white-space:pre-wrap; overflow-wrap:anywhere; background:#eef3ef; padding:1rem; }
meter { width:100%; height:.6rem; display:block; margin:.35rem 0; accent-color:#087569; }
"""


def _details(title, value):
    return f"<details><summary>{escape(title)}</summary><pre>{escape(value)}</pre></details>"


def render(events):
    turns = defaultdict(list)
    results = {}
    for event in events:
        if "turn" in event:
            turns[event["turn"]].append(event)
        if event["event"] == "tool_result":
            results[event["id"]] = event
    maximum = max((row["duration_ms"] for row in results.values()), default=0) or 1
    question = next((row["question"] for row in events if row["event"] == "start"), "")
    status = {"answer": "Completed", "answer_error": "Invalid answer", "step_limit": "Round limit reached",
              "model_error": "Model request failed"}.get(events[-1]["event"], "Incomplete")
    parts = [
        '<!doctype html><html lang="en"><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width,initial-scale=1">',
        f"<title>Markdown RAG trace</title><style>{STYLE}</style><main>",
        f"<header><h1>Markdown RAG trace</h1><p>{escape(question)}</p>",
        f'<p class="meta">{status} · {len(turns)} rounds · {len(results)} completed calls</p></header>',
    ]
    for turn, rows in sorted(turns.items()):
        model = next((row for row in rows if row["event"] == "model"), None)
        parts.append(f"<section><h2>Round {turn}</h2>")
        if model:
            parts.append(f'<p class="meta">Model · {model["duration_ms"]:,} ms · {model["tool_calls"]} calls</p>')
        for row in rows:
            kind = row["event"]
            if kind == "tool_call":
                try:
                    args = json.loads(row["arguments"])
                except json.JSONDecodeError:
                    args = row["arguments"]
                label = row["tool"]
                if isinstance(args, dict):
                    if label == "search":
                        label = f'{label} / {args.get("mode")} · {args.get("query")}'
                    elif label == "fuse":
                        label = f'{label} · lists {args.get("lists")}'
                    elif label == "read":
                        label = f'{label} · passage {args.get("passage")}'
                result = results.get(row["id"])
                if result:
                    elapsed = result["duration_ms"]
                    value = result["result"]
                    label += f" · {elapsed:,} ms"
                    for key, name in (("list", "list"), ("hits", "hits"), ("page", "PDF page")):
                        if value.get(key) is not None:
                            label += f" · {name} {value[key]}"
                    if "error" in value:
                        label += " · error"
                    parts.append(f'<meter min="0" max="{maximum}" value="{elapsed}" aria-label="Tool duration"></meter>')
                else:
                    value = None
                    label += " · no recorded result"
                parts.append(_details(label, json.dumps({"arguments": args, "result": value},
                                                        ensure_ascii=False, indent=2)))
            elif kind == "answer":
                parts.append(_details("Final answer", row["answer"] or ""))
            elif kind in {"model_error", "answer_error"}:
                parts.append(_details("Model error", row["error_type"]))
        parts.append("</section>")
    parts.append("</main></html>")
    return "\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    with args.trace.open(encoding="utf-8") as source:
        events = [json.loads(line) for line in source if line.strip()]
    if not events:
        parser.error("trace has no events")
    with args.output.open("x", encoding="utf-8") as output:
        output.write(render(events))


if __name__ == "__main__":
    main()
