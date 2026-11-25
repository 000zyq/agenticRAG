from scripts.chat_markdown import MarkdownTools


def test_markdown_search_and_rrf_preserve_source():
    def embed(texts, mode):
        vectors = [[int("营收" in t), int("城市" in t)] for t in texts]
        if mode == "dense":
            return vectors
        return [{"indices": [0, 1], "values": v} for v in vectors]
    tools = MarkdownTools([(5, "营收"), (8, "城市")], embed)
    handles = [tools.search("营收", mode)["list"] for mode in ("dense", "sparse", "bm25")]
    result = tools.fuse(handles)["preview"][0]
    assert tools.read(result["passage"])["page"] == 5
    assert tools.search("缺失", "bm25")["hits"] == 0
