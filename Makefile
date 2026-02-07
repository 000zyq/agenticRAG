.PHONY: eval-fast eval-regression eval-full

eval-fast:
	.venv/bin/python scripts/eval_pdf2db.py --output tmp/eval_fast.json

eval-regression: eval-fast


eval-full: eval-fast
