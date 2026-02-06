# Tests Overview

Test suites in this project focus on financial report parsing, structured extraction, and ingestion safeguards.

Suites:
- Unit: Parsing utilities and metadata extraction.
- Table detection: Minimal table segmentation and row/column alignment.
- Regression: Run extraction on curated sample PDFs from `tests/fixtures/manifest.json`.
- Integration: End-to-end ingest into Postgres (opt-in).

Run:
- `.venv/bin/python -m pytest -m "not integration"`
- `RUN_DB_TESTS=1 .venv/bin/python -m pytest -m integration`
