from io import StringIO
import json
from shutil import which
from threading import Barrier
from types import SimpleNamespace

import pytest

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


def test_locate_links_continuation_and_matches_terms_in_one_header():
    tools = MarkdownTools([
        (1, "## 主要子公司的构成\n| 公司名称 | 注册地 |\n| --- | --- |"),
        (2, "(a) 主要子公司的构成（续）\n<table><tr><td>公司名称</td><td>净利润</td></tr>"
            "<tr><td>样本公司</td><td>10</td></tr></table>"),
        (3, "# 公司名称\n<table><tr><td>指标</td></tr><tr><td>净利润</td></tr></table>"),
    ], None)
    headings = tools.locate(["主要子公司的构成", "未披露"], "heading", "any")["matches"]
    assert [hit["page"] for hit in headings] == [1, 2]
    assert headings[1]["continuation_of"]["page"] == 1
    headers = tools.locate(["公司名称", "净利润"], "table_header", "all")
    assert [tools.read(hit["passage"])["page"] for hit in headers["matches"]] == [2]
    assert tools.locate(["公司名称", "净利润"], "text", "all")["hits"] == 2


def test_third_model_round_can_only_answer():
    offered_tools = []

    def complete(messages, **kwargs):
        offered_tools.append(kwargs.get("tool_choice"))
        if len(offered_tools) == 3:
            assert messages[-1]["role"] == "tool"
            assert json.loads(messages[-1]["content"])["page"] == 5
            msg = SimpleNamespace(tool_calls=[], content="营收见第 5 页。")
        else:
            function = SimpleNamespace(name="read", arguments='{"passage":0}')
            call = SimpleNamespace(id=str(len(offered_tools)), function=function)
            msg = SimpleNamespace(tool_calls=[call], model_dump=lambda **_: {"role": "assistant"})
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)])

    trace = StringIO()
    assert ask("营收在哪一页？", MarkdownTools([(5, "营收")], None), complete, trace=trace) == "营收见第 5 页。"
    assert offered_tools == [None, None, "none"]
    assert json.loads(trace.getvalue().splitlines()[-1])["turn"] == 3


@pytest.mark.skipif(which("rg") is None, reason="ripgrep not installed")
def test_grep_returns_table_value_and_page_local_context():
    tools = MarkdownTools([
        (9, "# 财务指标\n<table><tr><td>指标</td><td>2024</td></tr>"
            "<tr><td>归母净利润</td><td>38,537,237</td></tr></table>"),
        (10, "下一页"),
    ], None)
    result = tools.structure.grep(["归母净利润", "未披露"], "literal", 1)
    hit, = result["matches"]
    assert (hit["page"], hit["text"], hit["before"]) == (9, "归母净利润 | 38,537,237", ["指标 | 2024"])
    assert "下一页" not in hit["after"]
    regex_hit, = tools.structure.grep([r"归母.*38,537,237"], "regex", 0)["matches"]
    assert regex_hit["text"] == hit["text"]

    def complete(messages, tools):
        assert any(tool["function"]["name"] == "grep" for tool in tools)
        if messages[-1]["role"] == "tool":
            assert "38,537,237" in messages[-1]["content"]
            msg = SimpleNamespace(tool_calls=[], content="见第 9 页。")
        else:
            function = SimpleNamespace(name="grep", arguments=json.dumps(
                {"patterns": ["归母净利润"], "mode": "literal", "context": 0}))
            msg = SimpleNamespace(tool_calls=[SimpleNamespace(id="grep", function=function)],
                                  model_dump=lambda **_: {"role": "assistant"})
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)])

    assert ask("归母净利润是多少？", tools, complete) == "见第 9 页。"
