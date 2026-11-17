"""Regression tests for the third review: sync, lint and the org parser."""

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from crucible.cli import _sync_wiki_to_db, cli
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

A = ".crucible/wiki/concepts/a.org"
B = ".crucible/wiki/concepts/b.org"


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


def write(root, rel, title="T", atype="concept", body="", tag="topic", props=""):
    path = wiki(root) / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    text = ART.format(title=title, atype=atype, body=body, tag=tag)
    if props:
        text = text.replace(":STATUS: draft", f"{props}\n:STATUS: draft")
    path.write_text(text)
    return path


def run(*args, ok=True):
    result = CliRunner().invoke(cli, list(args))
    if ok:
        assert result.exit_code == 0, result.output
    return result


def run_json(*args):
    result = CliRunner().invoke(cli, [*args, "--json"])
    return json.loads(result.output)


def db_of(root):
    return CrucibleDB(root / ".crucible" / "crucible.db")


def query(root, sql, params=()):
    db = db_of(root)
    try:
        return [dict(r) for r in db.conn.execute(sql, params).fetchall()]
    finally:
        db.close()


def lint_types(root, path=None):
    issues = run_json("lint")
    return [i for i in issues if path is None or i.get("path") == path]


def notebook(root, name="lab1.org"):
    path = root / ".crucible" / "sources" / "notebooks" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#+TITLE: Lab one\n\nMeasured things.\n")
    return path


def rebuild(root):
    (root / ".crucible" / "crucible.db").unlink()
    for suffix in ("-wal", "-shm"):
        side = root / ".crucible" / f"crucible.db{suffix}"
        if side.exists():
            side.unlink()
    run("stats")


# 1. The docs describe disk-scan keys as the code assigns them.


def test_docs_describe_the_bare_stem_key(kb):
    notebook(kb)
    run("sync")
    keys = [json.loads(s["metadata"])["cite_key"]
            for s in query(kb, "SELECT metadata FROM sources")]
    assert keys == ["lab1"]
    repo = Path(__file__).resolve().parents[1]
    for doc in ("CLAUDE.md", "skills/crucible/SKILL.md"):
        text = repo.joinpath(doc).read_text()
        assert "is keyed `<stem>-<ext>`" not in text
    help_text = " ".join(run("help", "all").output.split())
    assert "keyed <stem>-<ext>" not in help_text
    assert "keyed <stem>, or <stem>-<ext>" in help_text


# 2. A disk-scanned source can be cited by its key.


def test_lint_accepts_citations_of_disk_scanned_sources(kb):
    notebook(kb)
    write(kb, "concepts/a.org", "A", body="Measured in citep:lab1 and cite:nothere.")
    run("sync")
    missing = {i["key"] for i in lint_types(kb) if i["type"] == "missing_citation"}
    assert missing == {"nothere"}


# 3. derived_at is when a derivation first appeared, not the last sync.


def test_derivation_time_survives_resync(kb):
    write(kb, "concepts/a.org", "A")
    d = write(kb, "comparisons/d.org", "D", atype="comparison",
              props=":DERIVED_FROM: concepts/a.org")
    run("sync")
    db = db_of(kb)
    try:
        db.conn.execute("UPDATE article_derivations SET derived_at = '2000-01-01 00:00:00'")
        db.conn.commit()
    finally:
        db.close()
    d.write_text(d.read_text() + "\nMore.\n")
    run("sync")
    hist = run_json("history", ".crucible/wiki/comparisons/d.org")
    assert [(r["path"], r["derived_at"]) for r in hist["derived_from"]] == [
        (A, "2000-01-01 00:00:00")
    ]
    down = run_json("history", A)["derivatives"]
    assert [r["derived_at"] for r in down] == ["2000-01-01 00:00:00"]
    # A removed derivation goes away.
    d.write_text(d.read_text().replace(":DERIVED_FROM: concepts/a.org\n", ""))
    run("sync")
    assert run_json("history", A) == {"derived_from": [], "derivatives": []}


# 4. A symlink to a directory inside the wiki is an alias, whatever its name.


@pytest.mark.parametrize("alias", ["aa", "zz"])
def test_symlink_inside_the_wiki_does_not_take_over_paths(kb, alias):
    write(kb, "concepts/a.org", "A", body="[[file:b.org][B]]")
    write(kb, "concepts/b.org", "B")
    run("sync")
    os.symlink("concepts", wiki(kb) / alias)
    db = db_of(kb)
    try:
        stats = _sync_wiki_to_db(kb, db)
    finally:
        db.close()
    assert (stats["added"], stats["removed"]) == (0, 0)
    paths = {r["path"] for r in query(kb, "SELECT path FROM articles")}
    assert paths == {A, B}
    rebuild(kb)
    assert {r["path"] for r in query(kb, "SELECT path FROM articles")} == {A, B}


def test_symlink_outside_the_wiki_is_still_followed(kb, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "m.org").write_text(ART.format(title="M", atype="method", body="", tag="x"))
    os.symlink(outside, wiki(kb) / "extra")
    run("sync")
    paths = {r["path"] for r in query(kb, "SELECT path FROM articles")}
    assert ".crucible/wiki/extra/m.org" in paths


# 5. A link to a heading in the same file is not a graph edge.


def test_self_link_is_not_stored(kb):
    write(kb, "summaries/s2.org", "S2", atype="summary",
          body="As noted [[file:s2.org::*S2][above]].")
    run("sync")
    assert query(kb, "SELECT * FROM article_links") == []
    s2 = ".crucible/wiki/summaries/s2.org"
    assert s2 in {a["path"] for a in run_json("orphans")}
    assert run_json("backlinks", s2) == []
    assert run_json("related", s2) == []


# 6. A source whose bib entry and file are both gone is dropped.


def test_vanished_source_is_dropped_like_a_rebuild(kb):
    lab = notebook(kb)
    run("sync")
    assert len(query(kb, "SELECT * FROM sources")) == 1
    lab.unlink()
    run("sync")
    assert query(kb, "SELECT * FROM sources") == []


def test_moved_file_and_bib_only_rows_are_kept(kb):
    lab = notebook(kb)
    bib = kb / ".crucible" / "references.bib"
    bib.write_text(
        "@article{ext2026,\n  title = {External},\n  year = {2026},\n"
        "  file = {.crucible/sources/external/pdfs/ext.pdf},\n}\n"
    )
    write(kb, "concepts/a.org", "A").write_text(
        (wiki(kb) / "concepts" / "a.org").read_text()
        .replace(":SOURCE_KEYS:", ":SOURCE_KEYS: lab1 ext2026"))
    run("sync")
    lab.rename(lab.with_name("lab1.md"))
    run("sync")
    rows = {r["path"] for r in query(kb, "SELECT path FROM sources")}
    assert rows == {
        ".crucible/sources/notebooks/lab1.md",
        ".crucible/sources/external/pdfs/ext.pdf",  # gitignored, still in bib
    }
    linked = run_json("sources", A)
    assert {s["path"] for s in linked} == rows


# 7. ARTICLE_TYPE and STATUS are validated; STATUS is normalised.


def test_status_is_normalised_and_bad_values_are_linted(kb):
    p = write(kb, "concepts/a.org", "A")
    p.write_text(p.read_text().replace(":STATUS: draft", ":STATUS: Reviewed "))
    q = write(kb, "concepts/b.org", "B", atype="Blog")
    q.write_text(q.read_text().replace(":STATUS: draft", ":STATUS: final"))
    run("sync")
    status = {r["path"]: r["status"] for r in query(kb, "SELECT path, status FROM articles")}
    assert status[A] == "reviewed"
    types = {(i["type"], i.get("path")) for i in lint_types(kb)}
    assert ("invalid_status", B) in types
    assert ("invalid_article_type", B) in types
    assert ("invalid_status", A) not in types
    assert ("invalid_article_type", A) not in types


# 8. A DERIVED_FROM entry that names no article is reported.


def test_lint_reports_unresolved_derived_from(kb):
    write(kb, "concepts/a.org", "A")
    write(kb, "comparisons/d.org", "D", atype="comparison",
          props=":DERIVED_FROM: concepts/a.org concepts/typo.org")
    run("sync")
    refs = [i["ref"] for i in lint_types(kb) if i["type"] == "unresolved_derived_from"]
    assert refs == ["concepts/typo.org"]


# 9. The org parser reads only what org treats as live.


def test_links_in_verbatim_and_code_are_ignored():
    m = parse_org("* H\nSee =[[file:x.org]]= and ~[[./y.org]]~ but [[file:z.org]].\n")
    assert m.file_links == ["z.org"]


def test_links_and_cites_in_comments_are_ignored():
    m = parse_org(
        "* H\n#+begin_comment\ncite:a [[file:a.org]]\n#+end_comment\n"
        ": cite:b [[file:b.org]]\n# cite:c [[file:c.org]]\n"
        "* COMMENT old\ncite:d [[file:d.org]]\n** sub\ncite:e\n"
        "* Live\ncite:f [[file:f.org]]\n"
    )
    assert m.cite_keys == ["f"]
    assert m.file_links == ["f.org"]


def test_every_filetags_line_is_read():
    assert parse_org("#+FILETAGS: :a:\n#+filetags: :b:a:\n").filetags == ["a", "b"]


def test_property_names_with_digits_and_plus_accumulation():
    m = parse_org("* H\n:PROPERTIES:\n:SOURCE_KEYS: a\n:SOURCE_KEYS+: b c\n:ID2: x\n:END:\n")
    assert m.properties == {"SOURCE_KEYS": "a b c", "ID2": "x"}


def test_bom_does_not_hide_the_title():
    m = parse_org("\ufeff#+TITLE: T\n#+FILETAGS: :a:\n")
    assert m.title == "T"
    assert m.filetags == ["a"]


# 10. Only links actually inserted are counted.


def test_duplicate_links_are_counted_once(kb):
    write(kb, "concepts/a.org", "A", body="[[file:b.org][B]] and [[file:b.org][again]]")
    write(kb, "concepts/b.org", "B")
    db = db_of(kb)
    try:
        stats = _sync_wiki_to_db(kb, db)
    finally:
        db.close()
    assert stats["article_links"] == 1


# 11. A removed :CREATED: is cleared on incremental sync, as on a rebuild.


def test_removed_created_is_cleared(kb):
    p = write(kb, "concepts/a.org", "A")
    run("sync")
    assert query(kb, "SELECT created_at FROM articles")[0]["created_at"] == "2026-01-01 00:00:00"
    p.write_text(p.read_text().replace(":CREATED: 2026-01-01\n", ""))
    run("sync")
    created = query(kb, "SELECT created_at FROM articles")[0]["created_at"]
    assert created > "2026-01-01 00:00:00"
