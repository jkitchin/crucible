"""Regression tests for sync, rebuild, index, manifest and link handling."""

import os
import re
import shutil

import pytest
from click.testing import CliRunner

from crucible.cli import cli
from crucible.database import CrucibleDB
from crucible.orgparse import parse_org

ART = """#+TITLE: {title}
#+FILETAGS: :{tag}:

* {title}
:PROPERTIES:
:ARTICLE_TYPE: {atype}
:SOURCE_KEYS:
:STATUS: draft
:CREATED: 2026-01-01
:END:

{body}
"""


@pytest.fixture
def kb(tmp_path, registry, monkeypatch):
    """An empty crucible; cwd is its root."""
    root = tmp_path / "kb"
    root.mkdir()
    monkeypatch.chdir(root)
    result = CliRunner().invoke(cli, ["init", "--root", str(root)])
    assert result.exit_code == 0, result.output
    return root


def wiki(root):
    return root / ".crucible" / "wiki"


def write(root, rel, title="T", atype="concept", body="", tag="topic"):
    path = wiki(root) / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(ART.format(title=title, atype=atype, body=body, tag=tag))
    return path


def sync():
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    return result


def links(root):
    db = CrucibleDB(root / ".crucible" / "crucible.db")
    try:
        return {(r["from_path"], r["to_path"]) for r in db.all_article_links()}
    finally:
        db.close()


def articles(root):
    db = CrucibleDB(root / ".crucible" / "crucible.db")
    try:
        return {a["path"]: a for a in db.list_articles()}
    finally:
        db.close()


A = ".crucible/wiki/concepts/a.org"
B = ".crucible/wiki/concepts/b.org"


# 1. Symlinks


def test_links_survive_a_symlinked_wiki(kb, tmp_path):
    real = tmp_path / "dropbox-wiki"
    shutil.move(str(wiki(kb)), real)
    os.symlink(real, wiki(kb))
    write(kb, "concepts/a.org", "A", body="See [[file:b.org][B]].")
    write(kb, "concepts/b.org", "B")
    sync()
    assert links(kb) == {(A, B)}


def test_links_to_symlinked_articles_and_dirs(kb, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "s.org").write_text(ART.format(title="S", atype="summary", body="", tag="x"))
    methods = tmp_path / "methods"
    methods.mkdir()
    (methods / "m.org").write_text(ART.format(title="M", atype="method", body="", tag="x"))
    os.symlink(outside / "s.org", wiki(kb) / "summaries" / "s.org")
    shutil.rmtree(wiki(kb) / "methods")
    os.symlink(methods, wiki(kb) / "methods")
    # A symlink loop must not recurse forever.
    os.symlink(wiki(kb), wiki(kb) / "concepts" / "loop")
    write(kb, "concepts/a.org", "A", body=(
        "[[file:../summaries/s.org][S]] and [[file:../methods/m.org][M]]"
    ))
    sync()
    paths = set(articles(kb))
    assert ".crucible/wiki/methods/m.org" in paths
    assert not any("loop" in p for p in paths)
    assert links(kb) == {
        (A, ".crucible/wiki/summaries/s.org"),
        (A, ".crucible/wiki/methods/m.org"),
    }


# 2. Unreadable articles


def test_non_utf8_article_is_skipped_not_fatal(kb):
    write(kb, "concepts/a.org", "A")
    (wiki(kb) / "concepts" / "latin1.org").write_bytes("#+TITLE: Caf\xe9\n".encode("latin-1"))
    result = sync()
    assert "unreadable" in result.output
    assert "latin1.org" in result.stderr
    assert set(articles(kb)) == {A}

    # A missing database is rebuilt despite the bad file.
    (kb / ".crucible" / "crucible.db").unlink()
    result = CliRunner().invoke(cli, ["stats"])
    assert result.exit_code == 0, result.output
    assert (kb / ".crucible" / "crucible.db").exists()
    assert "Articles: 1" in result.stdout


# 3. Properties drawers


def test_drawer_inside_src_block_is_ignored():
    text = (
        "#+TITLE: How to write\n* How to write\n:PROPERTIES:\n:STATUS: draft\n:END:\n"
        "#+begin_src org\n* X\n:PROPERTIES:\n:ARTICLE_TYPE: summary\n"
        ":SOURCE_KEYS: smith2024\n:END:\n#+end_src\n"
    )
    assert parse_org(text).properties == {"STATUS": "draft"}


def test_empty_drawer_does_not_run_on():
    text = "* H\n:PROPERTIES:\n:END:\nProse\n:LOGBOOK:\n- note\n:END:\n"
    assert parse_org(text).properties == {}
    text = "* H\n:PROPERTIES:\n:END:\n* Sub\n:PROPERTIES:\n:ARTICLE_TYPE: summary\n:END:\n"
    assert parse_org(text).properties == {}


def test_only_the_first_drawer_is_read():
    text = (
        "* A\n:PROPERTIES:\n:ARTICLE_TYPE: concept\n:END:\n"
        "** Section\n:PROPERTIES:\n:SOURCE_KEYS: other2020\n:END:\n"
    )
    assert parse_org(text).properties == {"ARTICLE_TYPE": "concept"}


# 4. ARTICLE_TYPE normalisation and index "Other" section


def test_article_type_is_normalised_and_every_article_is_indexed(kb):
    write(kb, "concepts/a.org", "Alpha", atype="Concept")
    write(kb, "concepts/b.org", "Beta", atype="")
    write(kb, "methods/c.org", "Gamma", atype="recipe")
    sync()
    types = {p: a["article_type"] for p, a in articles(kb).items()}
    assert types[A] == "concept"
    assert types[B] == "concept"
    assert types[".crucible/wiki/methods/c.org"] == "recipe"
    index = (wiki(kb) / "index.org").read_text()
    by_type = index.split("* Articles by Concept")[0]
    assert "** Concepts (2)" in by_type
    assert "** Other (1)" in by_type
    assert "Gamma" in by_type
    manifest = (wiki(kb) / "MANIFEST.md").read_text()
    assert "- 3 articles (2 concept, 1 recipe)" in manifest


# 5. DERIVED_FROM relative to the wiki directory


@pytest.mark.parametrize("ref", ["concepts/a.org", "wiki/concepts/a.org", "../concepts/a.org"])
def test_derived_from_resolves(kb, ref):
    write(kb, "concepts/a.org", "Alpha")
    d = write(kb, "comparisons/d.org", "Delta", atype="comparison")
    d.write_text(d.read_text().replace(":STATUS:", f":DERIVED_FROM: {ref}\n:STATUS:"))
    sync()
    db = CrucibleDB(kb / ".crucible" / "crucible.db")
    try:
        rows = db.conn.execute(
            "SELECT a.path FROM article_derivations d JOIN articles a ON a.id = d.source_id"
        ).fetchall()
    finally:
        db.close()
    assert [r["path"] for r in rows] == [A]


# 6. sync refreshes MANIFEST.md and index.org


def test_sync_writes_manifest_and_index(kb):
    write(kb, "concepts/a.org", "Alpha")
    sync()
    assert "- 1 articles" in (wiki(kb) / "MANIFEST.md").read_text()
    assert "Alpha" in (wiki(kb) / "index.org").read_text()
    write(kb, "concepts/b.org", "Beta")
    sync()
    assert "- 2 articles" in (wiki(kb) / "MANIFEST.md").read_text()
    assert "Beta" in (wiki(kb) / "index.org").read_text()


# 7. Case-insensitive filesystems


def test_link_with_wrong_case_resolves_on_case_insensitive_fs(kb):
    write(kb, "concepts/a.org", "Alpha", body="[[file:B.org][B]]")
    write(kb, "concepts/b.org", "Beta")
    if not (wiki(kb) / "concepts" / "B.org").exists():
        pytest.skip("case-sensitive filesystem")
    sync()
    assert links(kb) == {(A, B)}


# 8. Prefix-less file links


def test_prefixless_file_links():
    text = (
        "[[./b.org][B]] [[c.org]] [[../d.org::*H][D]] [[~/e.org]] "
        "[[https://x.org]] [[id:abc.org]] [[cite:k]] [[*Heading]]"
    )
    assert parse_org(text).file_links == ["./b.org", "c.org", "../d.org", "~/e.org"]


def test_prefixless_links_are_synced(kb):
    write(kb, "concepts/a.org", "Alpha", body="[[./b.org][B]]")
    write(kb, "concepts/b.org", "Beta")
    sync()
    assert links(kb) == {(A, B)}


# 9. Index link descriptions


def test_index_escapes_brackets_in_titles(kb):
    write(kb, "concepts/a.org", "Beta [draft]] x")
    sync()
    index = (wiki(kb) / "index.org").read_text()
    assert "[[file:concepts/a.org][Beta (draft)) x]]" in index


# 10. Empty articles are indexed for FTS


def test_empty_article_is_unchanged_on_resync(kb):
    (wiki(kb) / "concepts" / "empty.org").write_text("")
    sync()
    result = sync()
    assert "0 updated, 1 unchanged" in result.output


# 11. Lint broken_link


def test_lint_broken_link_handles_home_and_external_sources(kb, tmp_path):
    home = tmp_path / "home"
    (home / "notes.org").write_text("* Notes\n")
    write(kb, "concepts/a.org", "Alpha", body=(
        "[[file:~/notes.org][notes]] "
        "[[file:../../sources/external/pdfs/paper.pdf][paper]] "
        "[[file:missing.org][missing]]"
    ))
    sync()
    result = CliRunner().invoke(cli, ["lint", "--json"])
    assert result.exit_code == 0, result.output
    import json
    broken = [i["link"] for i in json.loads(result.stdout)
              if i["type"] == "broken_link"]
    assert broken == ["missing.org"]


def test_sync_leaves_generated_pages_alone_when_only_the_date_differs(kb):
    write(kb, "concepts/a.org", title="A")
    sync()
    for name in ("index.org", "MANIFEST.md"):
        page = wiki(kb) / name
        text = page.read_text()
        stale = re.sub(r"^(#\+DATE:|Updated:) .*$", r"\1 [1999-01-01 Fri]",
                       text, flags=re.MULTILINE)
        assert stale != text
        page.write_text(stale)
    sync()
    for name in ("index.org", "MANIFEST.md"):
        assert "1999-01-01" in (wiki(kb) / name).read_text()
    # A real change rewrites the page, date and all.
    write(kb, "concepts/b.org", title="B")
    sync()
    assert "1999-01-01" not in (wiki(kb) / "index.org").read_text()
    assert "concepts/b.org" in (wiki(kb) / "index.org").read_text()
