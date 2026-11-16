"""Tests for org-mode article parsing."""

from crucible.browse import org_to_html
from crucible.orgparse import parse_org, parse_org_file


def test_org_keywords_are_case_insensitive():
    text = "#+title: Lowercase title\n#+filetags: :concept:kinetics:\n"

    meta = parse_org(text)
    title, _, filetags = org_to_html(text)

    assert meta.title == "Lowercase title"
    assert meta.filetags == ["concept", "kinetics"]
    assert title == "Lowercase title"
    assert filetags == ["concept", "kinetics"]


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
