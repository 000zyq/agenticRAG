import pytest

from scripts.restore_mineru_table_pages import restore_tables


def block(html="", deleted=False):
    return {"type": "table", "bbox": [1, 2, 3, 4], "blocks": [
        {"type": "table_body", "lines_deleted": deleted,
         "lines": [{"spans": [{"html": html}]}] if html else []}]}


def test_merged_table_restores_physical_fragments_without_mutating_original():
    items = [{"type": "table", "page_idx": 0, "table_body": "merged", "table_caption": ["附表"]},
             {"type": "text", "page_idx": 1, "text": "unrelated"},
             {"type": "table", "page_idx": 1}]
    middle = {"pdf_info": [
        {"page_idx": 0, "para_blocks": [block("merged")], "preproc_blocks": [block("first")]},
        {"page_idx": 1, "para_blocks": [block(deleted=True)], "preproc_blocks": [block("second")]},
    ]}
    restored, count = restore_tables(items, middle)
    assert count == 2
    assert restored[0]["table_body"] == "first"
    assert restored[0]["table_caption"] == ["附表"]
    assert restored[1] == items[1]
    assert restored[2]["table_body"] == "second"
    assert items[0]["table_body"] == "merged"
    assert "table_body" not in items[2]
    with pytest.raises(ValueError, match="mismatch"):
        restore_tables([{**items[0], "table_body": "different"}, *items[1:]], middle)
