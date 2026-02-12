from __future__ import annotations

from pathlib import Path

from scripts.ingest_financial_report import _mineru_output_summary


def test_mineru_output_summary_non_mineru(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MINERU_OUTPUT_DIR", str(tmp_path))
    summary = _mineru_output_summary("pypdf", Path("tmp/ingest/2024年报.pdf"))
    assert summary == {}


def test_mineru_output_summary_without_env(monkeypatch) -> None:
    monkeypatch.delenv("MINERU_OUTPUT_DIR", raising=False)
    summary = _mineru_output_summary("mineru", Path("tmp/ingest/2024年报.pdf"))
    assert summary == {}


def test_mineru_output_summary_with_content_list(monkeypatch, tmp_path) -> None:
    output_dir = tmp_path / "mineru_output"
    report_dir = output_dir / "2024年报" / "auto"
    report_dir.mkdir(parents=True, exist_ok=True)
    content_list = report_dir / "foo_content_list.json"
    content_list.write_text("[]", encoding="utf-8")
    monkeypatch.setenv("MINERU_OUTPUT_DIR", str(output_dir))

    summary = _mineru_output_summary("mineru", Path("tmp/ingest/2024年报.pdf"))
    assert summary["mineru_output_dir"] == str(output_dir.resolve())
    assert summary["mineru_output_report_dir"] == str((output_dir.resolve() / "2024年报"))
    assert summary["mineru_content_list_path"] == str(content_list.resolve())
