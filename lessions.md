## 2026-02-11
- Always create a fresh feature branch from `main` (named by task/feature) to avoid long-lived branch conflicts after merges.
- For GitHub CLI in this environment, unset proxy vars when needed to avoid connectivity errors, and avoid re-requesting approved prefixes.
- If branch protection blocks merge, use `gh pr merge --auto` (squash) instead of retrying a direct merge.
