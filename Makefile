.PHONY: eval-fast eval-regression eval-full

eval-fast:
	.venv/bin/python scripts/eval_pdf2db.py --output tmp/eval_fast.json

eval-regression: eval-fast
# TODO: Add regression dataset runner; currently uses eval-fast.


eval-full: eval-fast
# TODO: Add full dataset runner; currently uses eval-fast.
