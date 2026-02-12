.PHONY: eval-fast eval-metric-match eval-regression eval-full

eval-fast:
	.venv/bin/python scripts/eval_pdf2db.py --output tmp/eval_fast.json

eval-metric-match:
	.venv/bin/python scripts/eval_metric_match_cases.py --output tmp/eval_metric_match.json

eval-regression: eval-fast eval-metric-match


eval-full: eval-fast
# TODO: Add full dataset runner; currently uses eval-fast.
