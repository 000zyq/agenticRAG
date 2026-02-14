from __future__ import annotations

from pathlib import Path

from scripts import ingest_multi_engine as ime


def test_retry_then_success(monkeypatch, tmp_path) -> None:
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")

    calls = {"insert": 0, "resolve": 0}

    def fake_insert_report(*args, **kwargs):
        calls["insert"] += 1
        if calls["insert"] == 1:
            raise RuntimeError("temp failure")
        return 42

    def fake_resolver(report_id: int, min_agree: int, tolerance: str) -> None:
        calls["resolve"] += 1
        assert report_id == 42
        assert min_agree == 2
        assert tolerance == "0.01"

    monkeypatch.setattr(ime, "insert_report", fake_insert_report)
    monkeypatch.setattr(ime, "_run_resolver", fake_resolver)
    monkeypatch.setattr(ime.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ingest_multi_engine.py",
            str(pdf_path),
            "--engines",
            "pypdf",
            "--engine-retries",
            "2",
        ],
    )

    ime.main()
    assert calls["insert"] == 2
    assert calls["resolve"] == 1


def test_all_retries_failed_raises(monkeypatch, tmp_path) -> None:
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")

    def fake_insert_report(*args, **kwargs):
        raise RuntimeError("always fail")

    monkeypatch.setattr(ime, "insert_report", fake_insert_report)
    monkeypatch.setattr(ime.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ingest_multi_engine.py",
            str(pdf_path),
            "--engines",
            "pypdf",
            "--engine-retries",
            "2",
            "--no-resolve",
        ],
    )

    try:
        ime.main()
        assert False, "expected SystemExit"
    except SystemExit as exc:
        assert str(exc) == "No engines succeeded."
