"""Regression tests for the third review: ingest, merge, delete and the registry."""

import os
import sqlite3
from contextlib import contextmanager

import pytest

from crucible import ingest as ingest_module
from crucible import registry as registry_module
from crucible.database import CrucibleDB

from test_data_loss_ingest import bib_keys, cite_key, kb, run  # noqa: F401

DOI = "10.1021/acscatal.5b00001"
FETCHED = ("@article{Smith_2015, title={Some Catalysis Paper}, author={Smith, Ann}, "
           "journal={ACS Catal.}, year={2015}, doi={%s}}")

ARTICLE = """#+TITLE: {title}
#+FILETAGS: concept
* {title}
:PROPERTIES:
:ARTICLE_TYPE: concept
:SOURCE_KEYS: {keys}
:STATUS: draft
:CREATED: 2024-01-01
:END:
{body}
"""


@contextmanager
def inside(path):
    old = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


@pytest.fixture
def other(kb, tmp_path):  # noqa: F811
    """A second initialized crucible to merge from; cwd stays at kb."""
    root = tmp_path / "other"
    root.mkdir()
    run("init", "--root", str(root))
    return root


def sync_in(root):
    with inside(root):
        run("sync")


def write_article(root, rel, title="Thing", keys="", body="Text."):
    path = root / ".crucible/wiki" / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(ARTICLE.format(title=title, keys=keys, body=body))
    return path


def rows(root):
    db = CrucibleDB(root / ".crucible/crucible.db")
    try:
        return {s["path"]: s for s in db.list_sources()}
    finally:
        db.close()


# 1. Only papers and web pages carry their own DOI ---------------------------------

@pytest.fixture
def doi_lookup(monkeypatch):
    calls = []

    def fetch(doi, timeout=10.0):
        calls.append(doi)
        return FETCHED % doi

    monkeypatch.setattr(ingest_module, "fetch_bibtex_from_doi", fetch)
    return calls


@pytest.mark.parametrize("name, content", [
    ("notes.org", f"#+TITLE: My lab notes\nTried https://doi.org/{DOI} today.\n"),
    ("run.csv", f"ref,{DOI}\n1,2\n"),
])
def test_notebook_or_data_citing_a_doi_does_not_become_that_paper(
        kb, tmp_path, doi_lookup, name, content):  # noqa: F811
    (tmp_path / name).write_text(content)
    result = run("ingest", str(tmp_path / name))
    assert doi_lookup == []
    assert "Some Catalysis Paper" not in result.output
    bib = (kb / ".crucible/references.bib").read_text()
    assert "doi" not in bib and "Some Catalysis Paper" not in bib
    (row,) = rows(kb).values()
    assert "doi" not in row["metadata"]
    if name == "notes.org":
        assert row["title"] == "My lab notes"


def test_explicit_doi_still_applies_to_a_notebook(kb, tmp_path, doi_lookup):  # noqa: F811
    (tmp_path / "notes.org").write_text("#+TITLE: My lab notes\nbody\n")
    run("ingest", str(tmp_path / "notes.org"), "--doi", DOI)
    assert doi_lookup == [DOI]
    assert DOI in (kb / ".crucible/references.bib").read_text()


def test_guessed_doi_of_another_paper_is_dropped_entirely(kb, tmp_path, doi_lookup):  # noqa: F811
    (tmp_path / "page.md").write_text(f"# My summary of it\n\ndoi: {DOI}\n")
    result = run("ingest", str(tmp_path / "page.md"), "--date", "2024-05-01")
    assert doi_lookup == [DOI]
    assert "Ingested: My summary of it" in result.output
    assert "Ignored DOI" in result.output
    bib = (kb / ".crucible/references.bib").read_text()
    assert "My summary of it" in bib
    for leaked in (DOI, "Some Catalysis Paper", "Smith", "2015", "ACS Catal"):
        assert leaked not in bib
    assert "2024" in bib


def test_guessed_doi_whose_title_matches_is_kept(kb, tmp_path, doi_lookup):  # noqa: F811
    (tmp_path / "page.md").write_text(f"# Some catalysis paper.\n\ndoi: {DOI}\n")
    result = run("ingest", str(tmp_path / "page.md"))
    assert "Ignored DOI" not in result.output
    bib = (kb / ".crucible/references.bib").read_text()
    assert DOI in bib and "Smith" in bib


def test_web_page_without_a_title_takes_the_fetched_one(kb, tmp_path, doi_lookup):  # noqa: F811
    # No declared title, but the page prints the fetched title by its DOI.
    # (A bare DOI is not enough: a note citing a paper looks the same.)
    (tmp_path / "page.md").write_text(f"Some catalysis paper\n\ndoi: {DOI}\n")
    result = run("ingest", str(tmp_path / "page.md"))
    assert "Ingested: Some Catalysis Paper" in result.output


# 2. Article links follow sources the merge renames --------------------------------

def test_merge_rewrites_links_to_renamed_sources(kb, other, tmp_path):  # noqa: F811
    for root, sub, text, author in ((kb, "x", "t\n1\n", "Lee"), (other, "y", "t\n2\n", "Kim")):
        (tmp_path / sub).mkdir()
        (tmp_path / sub / "data.csv").write_text(text)
        with inside(root):
            run("ingest", str(tmp_path / sub / "data.csv"), "-a", author, "--date", "2022")
    write_article(other, "concepts/thing.org", keys="kim2022",
                  body="Data in [[file:../../sources/data/data.csv][the data]] and "
                       "[[file:../../sources/data/data.txt][its text]].")
    sync_in(other)
    run("merge", str(other), "-y")
    text = (kb / ".crucible/wiki/concepts/thing.org").read_text()
    assert "[[file:../../sources/data/data_1.csv][the data]]" in text
    assert "[[file:../../sources/data/data_1.txt][its text]]" in text
    assert (kb / ".crucible/sources/data/data_1.csv").read_text() == "t\n2\n"


# 3. A disk-scanned source keeps its key when the merge renames it ------------------

def test_renamed_disk_scanned_source_keeps_its_key(kb, other, tmp_path):  # noqa: F811
    (tmp_path / "foo.csv").write_text("x,y\n1,2\n")
    run("ingest", str(tmp_path / "foo.csv"), "-a", "Smith, J", "--date", "2024")
    data = other / ".crucible/sources/data"
    data.mkdir(parents=True, exist_ok=True)
    (data / "foo.csv").write_text("a,b\n3,4\n")
    write_article(other, "concepts/thing.org", keys="foo", body="See cite:foo.")
    sync_in(other)
    run("merge", str(other), "-y")

    def key_of_foo_1():
        md = rows(kb)[".crucible/sources/data/foo_1.csv"]["metadata"]
        return ingest_module._source_metadata({"metadata": md})["cite_key"]

    assert key_of_foo_1() == "foo"
    assert "foo" in bib_keys(kb)
    # It survives a rebuild of the database from the files.
    (kb / ".crucible/crucible.db").unlink()
    run("sync")
    assert key_of_foo_1() == "foo"
    con = sqlite3.connect(kb / ".crucible/crucible.db")
    linked = con.execute(
        "select s.path from article_sources x join sources s on s.id = x.source_id "
        "join articles a on a.id = x.article_id where a.path like '%thing.org'").fetchall()
    assert linked == [(".crucible/sources/data/foo_1.csv",)]


# 4. Bibliography-only entries with a key the target uses ----------------------------

def _bib_only(root, entry):
    bib = root / ".crucible/references.bib"
    bib.write_text((bib.read_text() if bib.exists() else "") + "\n" + entry + "\n")


def test_bib_only_entry_for_a_different_paper_is_a_conflict(kb, other):  # noqa: F811
    _bib_only(kb, "@article{wang2020, title={Paper A}, doi={10.1/a}}")
    _bib_only(other, "@article{wang2020, title={Paper B}, doi={10.1/b}}")
    write_article(other, "concepts/thing.org")
    sync_in(other)
    run("sync")
    result = run("merge", str(other), "-y", ok=False)
    assert result.exit_code != 0
    assert "wang2020" in result.output and "different DOIs" in result.output
    assert not (kb / ".crucible/wiki/concepts/thing.org").exists()


def test_bib_only_entry_for_the_same_paper_merges(kb, other):  # noqa: F811
    _bib_only(kb, "@article{wang2020, title={Paper A}, doi={10.1/a}}")
    _bib_only(other, "@article{wang2020, title={Paper {A}}, doi={https://doi.org/10.1/A}}")
    sync_in(other)
    run("merge", str(other), "-y")
    assert bib_keys(kb).count("wang2020") == 1


# 5. Merging twice adds nothing; other wiki files are copied -------------------------

def test_merging_twice_does_not_duplicate_and_copies_figures(kb, other):  # noqa: F811
    write_article(other, "concepts/thing.org", body="[[file:fig.png]]")
    (other / ".crucible/wiki/concepts/fig.png").write_bytes(b"\x89PNG one")
    sync_in(other)
    run("merge", str(other), "-y")
    result = run("merge", str(other), "-y")
    assert "Skipped 2 wiki file(s)" in result.output
    assert sorted(p.name for p in (kb / ".crucible/wiki/concepts").iterdir()) == [
        "fig.png", "thing.org"]
    assert (kb / ".crucible/wiki/concepts/fig.png").read_bytes() == b"\x89PNG one"


def test_colliding_figure_is_renamed_and_linked(kb, other):  # noqa: F811
    (kb / ".crucible/wiki/concepts/fig.png").write_bytes(b"target")
    write_article(other, "concepts/thing.org", body="[[file:fig.png]]")
    (other / ".crucible/wiki/concepts/fig.png").write_bytes(b"incoming")
    (other / ".crucible/wiki/MANIFEST.md").write_text("generated")
    sync_in(other)
    run("merge", str(other), "-y")
    concepts = kb / ".crucible/wiki/concepts"
    assert (concepts / "fig.png").read_bytes() == b"target"
    assert (concepts / "fig_1.png").read_bytes() == b"incoming"
    assert "[[file:fig_1.png]]" in (concepts / "thing.org").read_text()


# 6. --bibtex with text before the entry ------------------------------------------

def test_bibtex_export_with_leading_comment_is_used(kb, tmp_path):  # noqa: F811
    (tmp_path / "paper.txt").write_text("hello world paper\n")
    (tmp_path / "ref.bib").write_text(
        "% Encoding: UTF-8\n@article{mykey2020,\n  title={Real Title},\n"
        "  author={Doe, Jane},\n  year={2020}\n}\n")
    result = run("ingest", str(tmp_path / "paper.txt"), "--bibtex", "@" + str(tmp_path / "ref.bib"))
    assert cite_key(result) == "doe2020"
    bib = (kb / ".crucible/references.bib").read_text()
    assert "Real Title" in bib and "Encoding" not in bib


def test_bibtex_without_an_entry_is_an_error(kb, tmp_path):  # noqa: F811
    (tmp_path / "paper.txt").write_text("hello\n")
    result = run("ingest", str(tmp_path / "paper.txt"), "--bibtex", "% just a comment", ok=False)
    assert result.exit_code != 0 and "not a complete BibTeX entry" in result.output
    assert not (kb / ".crucible/sources/external/web/paper.txt").exists()


# 7. A --date the entry contradicts --------------------------------------------------

def test_key_uses_the_entry_year_and_warns_about_the_date(kb, tmp_path):  # noqa: F811
    (tmp_path / "p1.txt").write_text("paper one\n")
    result = run("ingest", str(tmp_path / "p1.txt"), "--date", "2020-01-05", "--bibtex",
                 "@article{x, title={T}, author={Doe, Jane}, year={2019}}")
    assert cite_key(result) == "doe2019"
    assert "Warning" in result.output and "2020-01-05" in result.output


def test_agreeing_date_gives_no_warning(kb, tmp_path):  # noqa: F811
    (tmp_path / "p1.txt").write_text("paper one\n")
    result = run("ingest", str(tmp_path / "p1.txt"), "--date", "2019-03-01", "--bibtex",
                 "@article{x, title={T}, author={Doe, Jane}, year={2019}}")
    assert cite_key(result) == "doe2019" and "Warning" not in result.output


# 8. An unreadable registry ---------------------------------------------------------

@pytest.mark.parametrize("kind", ["directory", "unreadable"])
def test_unreadable_registry_does_not_crash(kb, kind):  # noqa: F811
    path = registry_module.REGISTRY_PATH
    path.unlink()
    if kind == "directory":
        path.mkdir()
    else:
        path.write_text("{}")
        os.chmod(path, 0)
    try:
        if kind == "unreadable" and os.access(path, os.R_OK):
            pytest.skip("running with privileges that ignore file modes")
        result = run("sync")
        assert "not registered globally" in result.output
        listed = run("registry", "list", ok=False)
        assert listed.exit_code != 0
        assert "cannot be read" in listed.output
        assert "No crucible instances registered" not in listed.output
    finally:
        if kind == "unreadable":
            os.chmod(path, 0o600)


# 9. Per-source options with a glob -------------------------------------------------

@pytest.mark.parametrize("option", [
    ["--title", "T"], ["--url", "https://x.org"], ["--doi", DOI],
    ["--bibtex", "@article{k, title={T}}"],
])
def test_glob_rejects_per_source_options(kb, tmp_path, option):  # noqa: F811
    for name in ("a.txt", "b.txt"):
        (tmp_path / name).write_text(name)
    result = run("ingest", str(tmp_path / "*.txt"), *option, ok=False)
    assert result.exit_code != 0
    assert option[0] in result.output and "glob" in result.output
    assert rows(kb) == {}


# 10. delete prefers exact matches ---------------------------------------------------

def test_delete_by_cite_key_deletes_the_source_not_its_summary(kb, tmp_path):  # noqa: F811
    (tmp_path / "paper.txt").write_text("text\n")
    run("ingest", str(tmp_path / "paper.txt"), "-a", "Smith", "--date", "2024")
    write_article(kb, "summaries/smith2024.org", title="Summary", keys="smith2024")
    run("sync")
    result = run("delete", "-y", "smith2024")
    assert "Source:" in result.output
    assert rows(kb) == {}
    db = CrucibleDB(kb / ".crucible/crucible.db")
    try:
        assert db.get_article_by_path(".crucible/wiki/summaries/smith2024.org")
    finally:
        db.close()


def test_ambiguous_fragment_deletes_nothing(kb):  # noqa: F811
    write_article(kb, "concepts/alpha-one.org", title="Alpha one")
    write_article(kb, "concepts/alpha-two.org", title="Alpha two")
    run("sync")
    result = run("delete", "-y", "alpha", ok=False)
    assert result.exit_code != 0
    assert "alpha-one.org" in result.output and "alpha-two.org" in result.output
    db = CrucibleDB(kb / ".crucible/crucible.db")
    try:
        assert len(db.list_articles()) == 2
    finally:
        db.close()


def test_unique_fragment_still_deletes(kb):  # noqa: F811
    write_article(kb, "concepts/alpha-one.org", title="Alpha one")
    run("sync")
    run("delete", "-y", "alpha")
    db = CrucibleDB(kb / ".crucible/crucible.db")
    try:
        assert db.list_articles() == []
    finally:
        db.close()


# 11. Duplicate detection does not hash every stored file -------------------------------

def test_duplicate_check_hashes_only_same_size_files(kb, tmp_path, monkeypatch):  # noqa: F811
    data = kb / ".crucible/sources/data"
    data.mkdir(parents=True, exist_ok=True)
    for i in range(5):
        (data / f"scan{i}.csv").write_text("x" * (10 + i))   # disk-scanned, no hash
    run("sync")
    hashed = []
    real = ingest_module.file_sha256
    monkeypatch.setattr(ingest_module, "file_sha256",
                        lambda p: hashed.append(os.path.basename(p)) or real(p))
    (tmp_path / "new.csv").write_text("y" * 12)
    run("ingest", str(tmp_path / "new.csv"))
    # The new file itself, its sidecar, and the one stored file of its size.
    assert [h for h in hashed if h.startswith("scan")] == ["scan2.csv"]

    (tmp_path / "copy.csv").write_text("x" * 13)
    result = run("ingest", str(tmp_path / "copy.csv"))
    assert "Already ingested" in result.output
