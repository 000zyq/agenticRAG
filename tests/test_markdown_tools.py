from io import StringIO
import json
from threading import Barrier
from types import SimpleNamespace

from scripts.chat_markdown import MarkdownTools, ask


def test_markdown_search_and_rrf_preserve_source():
    def embed(texts, mode):
        vectors = [[int("营收" in t), int("城市" in t)] for t in texts]
        sparse = [{"indices": [0, 1], "values": v} for v in vectors]
        return {"dense": vectors, "sparse": sparse} if mode == "dense_sparse" else (
            vectors if mode == "dense" else sparse
        )
    tools = MarkdownTools([(5, "营收"), (8, "城市")], embed)
    handles = [tools.search("营收", mode)["list"] for mode in ("dense", "sparse", "bm25")]
    result = tools.fuse(handles)["preview"][0]
    assert tools.read(result["passage"])["page"] == 5
    assert tools.search("缺失", "bm25")["hits"] == 0


def test_tool_loop_runs_searches_concurrently_and_fuses_afterward():
    query_barrier = Barrier(2, timeout=3)
    document_encodes = []

    def embed(texts, mode):
        if mode == "dense_sparse":
            document_encodes.append(texts)
            return {"dense": [[1]], "sparse": [{"indices": [0], "values": [1]}]}
        query_barrier.wait()
        return [[1]] if mode == "dense" else [{"indices": [0], "values": [1]}]

    def call(id, name, **arguments):
        return SimpleNamespace(id=id, function=SimpleNamespace(name=name, arguments=json.dumps(arguments)))

    calls = [
        call("d", "search", query="profit", mode="dense"),
        call("s", "search", query="profit", mode="sparse"),
        call("f", "fuse", lists=["1", "2"]),
    ]

    def complete(messages, tools):
        if messages[-1]["role"] == "tool":
            assert [msg["tool_call_id"] for msg in messages[-3:]] == ["d", "s", "f"]
            assert all(json.loads(msg["content"])["hits"] == 1 for msg in messages[-3:])
            msg = SimpleNamespace(tool_calls=[], content="answer")
        else:
            msg = SimpleNamespace(tool_calls=calls, model_dump=lambda **_: {"role": "assistant"})
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)])

    trace = StringIO()
    assert ask("profit?", MarkdownTools([(1, "profit")], embed), complete, trace=trace) == "answer"
    assert document_encodes == [["profit"]]
    results = [json.loads(line) for line in trace.getvalue().splitlines()]
    assert {event["id"] for event in results if event["event"] == "tool_result"} == {"d", "s", "f"}
