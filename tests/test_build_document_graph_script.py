from __future__ import annotations

import runpy
import sys


def test_build_document_graph_script_pending_mode(monkeypatch, capsys) -> None:
    monkeypatch.setattr(sys, "argv", ["build_document_graph.py", "--pending", "--limit", "3"])
    monkeypatch.setattr(
        "app.graph.builder.build_pending_document_graphs",
        lambda limit: {"processed": limit, "failed": 0, "results": []},
    )

    runpy.run_path("scripts/build_document_graph.py", run_name="__main__")

    assert capsys.readouterr().out.strip() == '{"processed": 3, "failed": 0, "results": []}'
