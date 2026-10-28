"""Tests for org-mode article parsing."""

from crucible.orgparse import parse_org_file


def test_empty_property_does_not_take_the_next_line(tmp_path):
    f = tmp_path / "a.org"
    f.write_text(
        "#+TITLE: A\n\n* A\n:PROPERTIES:\n:ARTICLE_TYPE: summary\n"
        ":SOURCE_KEYS:\n:DERIVED_FROM: ../concepts/b.org\n:STATUS: draft\n:END:\n"
    )
    props = parse_org_file(f).properties
    assert props["SOURCE_KEYS"] == ""
    assert props["DERIVED_FROM"] == "../concepts/b.org"
    assert props["STATUS"] == "draft"
