#!/usr/bin/env bash
set -euo pipefail

git config core.hooksPath .githooks

echo "Git hooks installed (core.hooksPath=.githooks)."
