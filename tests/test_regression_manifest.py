from __future__ import annotations

import json
from pathlib import Path
import pytest

from app.ingest.financial_report import extract_financial_report


@pytest.mark.regression
def test_regression_manifest_samples() -> None:
    manifest_path = Path("tests/fixtures/manifest.json")
    if not manifest_path.exists():
        pytest.skip("manifest.json not found")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in manifest.get("reports", []):
        path = Path(item["path"])
        if not path.exists():
            pytest.skip(f"sample missing: {path}")
        pages, meta, tables, parse_method = extract_financial_report(str(path))
        assert pages, "no pages extracted"
        assert parse_method in {"pypdf", "mineru"}
        assert meta.fiscal_year is None or isinstance(meta.fiscal_year, int)
        assert tables is not None
