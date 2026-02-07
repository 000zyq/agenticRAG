#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN=${PYTHON_BIN:-.venv/bin/python}
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN=python
fi

if ! "$PYTHON_BIN" -m ruff --version >/dev/null 2>&1; then
  echo "ruff not installed. Install with: $PYTHON_BIN -m pip install ruff" >&2
  exit 1
fi

"$PYTHON_BIN" -m ruff check .
