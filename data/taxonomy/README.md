Place CAS and IFRS taxonomy packages here (folder or zip). Example:

- `data/taxonomy/cas/` or `data/taxonomy/cas.zip`
- `data/taxonomy/ifrs/` or `data/taxonomy/ifrs.zip`

Workflow:

1. Extract labels
   - `./.venv/bin/python scripts/import_xbrl_taxonomy.py --source cas --input data/taxonomy/cas --output data/taxonomy/cas_labels.json`
   - `./.venv/bin/python scripts/import_xbrl_taxonomy.py --source ifrs --input data/taxonomy/ifrs --output data/taxonomy/ifrs_labels.json`

2. Merge labels into the dictionary
   - `./.venv/bin/python scripts/merge_taxonomy_dictionary.py --labels data/taxonomy/cas_labels.json`
   - `./.venv/bin/python scripts/merge_taxonomy_dictionary.py --labels data/taxonomy/ifrs_labels.json`

3. Sync dictionary to DB
   - `./.venv/bin/python scripts/sync_metric_dictionary.py`
