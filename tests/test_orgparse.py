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


def test_empty_keywords_do_not_take_the_next_line():
    text = "#+title:\n#+filetags: :concept:\n* Heading\n"
    meta = parse_org(text)
    title, _, filetags = org_to_html(text)
    assert meta.title == title == ""
    assert meta.filetags == filetags == ["concept"]

    text = "#+title: Article\n#+filetags:\n* Heading\n"
    meta = parse_org(text)
    title, _, filetags = org_to_html(text)
    assert meta.title == title == "Article"
    assert meta.filetags == filetags == []


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


def test_property_drawer_is_case_insensitive():
    text = (
        "#+title: A\n\n* A\n:properties:\n:article_type: summary\n"
        ":Source_Keys: smith2024\n:end:\n"
    )
    meta = parse_org(text)
    assert meta.properties["ARTICLE_TYPE"] == "summary"
    assert meta.properties["SOURCE_KEYS"] == "smith2024"
    _, body, _ = org_to_html(text)
    assert "smith2024" in body
    assert ":properties:" not in body
