from __future__ import annotations

import json
import subprocess
from pathlib import Path

from app.ingest.financial_report import _mineru_extract


def _write_content_list(path: Path) -> None:
    payload = [
        {
            "page_idx": 0,
            "type": "text",
            "text_level": 1,
            "text": "测试标题",
        },
        {
            "page_idx": 0,
            "type": "text",
            "text": "测试正文",
        },
    ]
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_mineru_extract_uses_artifacts_even_if_cli_returns_nonzero(tmp_path, monkeypatch) -> None:
    output_root = tmp_path / "mineru_output"
    report_stem = "sample_report"
    auto_dir = output_root / report_stem / "auto"
    auto_dir.mkdir(parents=True, exist_ok=True)
    _write_content_list(auto_dir / f"{report_stem}_content_list.json")

    pdf_path = tmp_path / f"{report_stem}.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%mock")

    monkeypatch.setenv("MINERU_CMD", "dummy_cmd -p {input} -o {output}")
    monkeypatch.setenv("MINERU_OUTPUT_DIR", str(output_root))

    def _raise_called_process_error(*args, **kwargs):
        raise subprocess.CalledProcessError(returncode=1, cmd="dummy")

    monkeypatch.setattr(subprocess, "run", _raise_called_process_error)

    pages = _mineru_extract(pdf_path)
    assert pages is not None
    assert len(pages) == 1
    assert pages[0].page == 1
    assert "测试标题" in pages[0].text_md


def test_mineru_extract_returns_none_when_no_artifacts(tmp_path, monkeypatch) -> None:
    output_root = tmp_path / "mineru_empty"
    output_root.mkdir(parents=True, exist_ok=True)
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%mock")

    monkeypatch.setenv("MINERU_CMD", "dummy_cmd -p {input} -o {output}")
    monkeypatch.setenv("MINERU_OUTPUT_DIR", str(output_root))
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: None)

    pages = _mineru_extract(pdf_path)
    assert pages is None
