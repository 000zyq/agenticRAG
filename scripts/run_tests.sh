#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN=${PYTHON_BIN:-.venv/bin/python}

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python not found at $PYTHON_BIN" >&2
  exit 1
fi

$PYTHON_BIN -m pytest -m "not integration"

if [[ "${RUN_DB_TESTS:-0}" == "1" ]]; then
  $PYTHON_BIN -m pytest -m integration
fi
