"""Surprises found by testing real crucibles after 0.2.0."""

import json

from crucible.database import CrucibleDB
from crucible.orgparse import parse_org, source_keys

from test_sources import ISSUE_BIB, cdir, ingest, invoke, linked_paths, project, write_article  # noqa: F401


def test_source_keys_accepts_commas_and_spaces():
    assert source_keys("a b") == ["a", "b"]
    assert source_keys("a, b") == ["a", "b"]
    assert source_keys("a,b,") == ["a", "b"]
    assert source_keys("") == []


def test_property_keywords_are_read_below_drawers():
    meta = parse_org(
        "#+TITLE: T\n#+PROPERTY: ARTICLE_TYPE method\n#+PROPERTY: STATUS draft\n"
        "#+PROPERTY: header-args:python :results output\n"
        "#+PROPERTY: SOURCE_KEYS a\n#+PROPERTY: SOURCE_KEYS+ b\n\n"
        "* T\n:PROPERTIES:\n:STATUS: reviewed\n:SOURCE_KEYS+: c\n:END:\n"
    )
    assert meta.properties["ARTICLE_TYPE"] == "method"
    assert meta.properties["STATUS"] == "reviewed"  # the drawer wins
    assert meta.properties["SOURCE_KEYS"] == "a b c"
    assert not any(k.startswith("HEADER") for k in meta.properties)


def test_property_keywords_in_a_src_block_are_ignored():
    meta = parse_org("#+BEGIN_SRC org\n#+PROPERTY: ARTICLE_TYPE method\n#+END_SRC\n")
    assert "ARTICLE_TYPE" not in meta.properties


def test_comma_separated_source_keys_link(project, tmp_path):  # noqa: F811
    k1 = ingest(project, tmp_path, "one.md", "First.\n", "--title", "First notes")
    k2 = ingest(project, tmp_path, "two.md", "Second.\n", "--title", "Second notes")
    write_article(project, f"{k1}, {k2}")
    invoke("sync")
    assert len(linked_paths(project)) == 2
    issues = json.loads(invoke("lint", "--json").stdout)
    assert not [i for i in issues if i["type"] == "unresolved_source_key"]


def test_file_level_property_keywords_set_the_type(project):  # noqa: F811
    (cdir(project) / "wiki" / "methods" / "m.org").parent.mkdir(exist_ok=True)
    (cdir(project) / "wiki" / "methods" / "m.org").write_text(
        "#+TITLE: A method\n#+PROPERTY: ARTICLE_TYPE method\n\n* A method\n\nSteps.\n")
    invoke("sync")
    db = CrucibleDB(cdir(project) / "crucible.db")
    try:
        [row] = [a for a in db.list_articles() if a["path"].endswith("methods/m.org")]
    finally:
        db.close()
    assert row["article_type"] == "method"


def test_delete_article_says_sync_brings_it_back(project):  # noqa: F811
    write_article(project, "x")
    invoke("sync")
    result = invoke("delete", ".crucible/wiki/concepts/widgets.org", "--yes")
    assert "next `crucible sync` adds it back" in result.output
    invoke("sync")
    result = invoke("delete", ".crucible/wiki/concepts/widgets.org", "--yes", "--file")
    assert "adds it back" not in result.output


def test_bib_entry_without_a_file_is_info_not_a_warning(project):  # noqa: F811
    # A work cited without its file: older versions made a placeholder
    # source for it, so upgraded crucibles have many.
    (cdir(project) / "references.bib").write_text(ISSUE_BIB)
    write_article(project, "widget2026")
    invoke("sync")
    issues = json.loads(invoke("lint", "--json").stdout)
    [issue] = [i for i in issues if i["type"] == "unresolved_source_key"]
    assert issue["severity"] == "info"
    assert "no file field" in issue["message"]
    write_article(project, "nowhere2026")
    invoke("sync")
    issues = json.loads(invoke("lint", "--json").stdout)
    [issue] = [i for i in issues if i["type"] == "unresolved_source_key"]
    assert issue["severity"] == "warning"
