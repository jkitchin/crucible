"""Regression tests for the fourth review: ingest paths, re-ingest and the bib file field."""

import json
import shutil

import pytest
from test_data_loss_ingest import kb, run  # noqa: F401

from crucible import bibtex
from crucible import ingest as ingest_module
from crucible.database import CrucibleDB

BIB = ".crucible/references.bib"


def db_of(root):
    return CrucibleDB(root / ".crucible" / "crucible.db")


def ingest(root, path, **kwargs):
    kwargs.setdefault("date", "2024-01-02")
    db = db_of(root)
    try:
        return ingest_module.ingest_source(root, db, path, **kwargs)
    finally:
        db.close()


def rows(root):
    """(path, cite key, shareable, source_type) of every source row."""
    db = db_of(root)
    try:
        return sorted(
            (s["path"], json.loads(s["metadata"] or "{}").get("cite_key"),
             bool(s["shareable"]), s["source_type"])
            for s in db.list_sources()
        )
    finally:
        db.close()


def rebuild(root):
    for f in (root / ".crucible").glob("crucible.db*"):
        f.unlink()
    run("sync")


# 1. A symlinked sources directory keeps project-relative paths.


@pytest.mark.parametrize("area,name,body", [
    ("notebooks", "entry.org", "#+TITLE: My entry\nhello\n"),
    ("external/pdfs", "paper.pdf", "%PDF-1.4 not really a pdf\n"),
])
def test_symlinked_sources_dir_keeps_relative_paths(kb, tmp_path, area, name, body):  # noqa: F811
    linked = kb / ".crucible/sources" / area
    shutil.rmtree(linked, ignore_errors=True)
    real = tmp_path / "elsewhere"
    real.mkdir()
    linked.symlink_to(real)
    src = tmp_path / name
    src.write_text(body)
    result = ingest(kb, src)
    assert result["relative_path"] == f".crucible/sources/{area}/{name}"

    run("sync")
    bib = (kb / BIB).read_text()
    before = rows(kb)
    assert [r[0] for r in before] == [f".crucible/sources/{area}/{name}"]
    assert f"file = {{.crucible/sources/{area}/{name}}}" in bib
    run("sync")
    assert (kb / BIB).read_text() == bib  # sync does not rewrite the bib

    rebuild(kb)
    assert rows(kb) == before
    assert (kb / BIB).read_text() == bib
    if area.startswith("external"):
        assert before[0][2] is False


def test_bib_file_path_is_lexical_inside_project(kb, tmp_path):  # noqa: F811
    linked = kb / ".crucible/sources/notebooks"
    shutil.rmtree(linked)
    real = tmp_path / "real_nb"
    real.mkdir()
    linked.symlink_to(real)
    (real / "a.org").write_text("x")
    assert (ingest_module._bib_file_path(kb, ".crucible/sources/notebooks/a.org")
            == ".crucible/sources/notebooks/a.org")
    assert (ingest_module._bib_file_path(kb, str(kb / ".crucible/sources/notebooks/a.org"))
            == ".crucible/sources/notebooks/a.org")
    assert (ingest_module._bib_file_path(kb, ".crucible/sources/notebooks/../data/b.csv")
            == ".crucible/sources/data/b.csv")


# 2. A sidecar beside a file with glob characters in its name is not a source.


def test_sidecar_of_bracketed_name_is_not_indexed(kb, tmp_path):  # noqa: F811
    src = tmp_path / "run[1].csv"
    src.write_text("a,b\n1,2\n")
    ingest(kb, src)
    assert (kb / ".crucible/sources/data/run[1].txt").exists()
    run("sync")
    assert [r[0] for r in rows(kb)] == [".crucible/sources/data/run[1].csv"]
    rebuild(kb)
    assert [r[0] for r in rows(kb)] == [".crucible/sources/data/run[1].csv"]


# 3. Re-ingesting on a fresh clone restores the missing external file.


def test_reingest_on_fresh_clone_restores_file(kb, tmp_path):  # noqa: F811
    src = tmp_path / "paper.md"
    src.write_text("# A Paper\nbody\n")
    first = ingest(kb, src)
    stored = kb / first["relative_path"]
    assert first["relative_path"].startswith(".crucible/sources/external/")
    stored.unlink()
    stored.with_suffix(".txt").unlink()  # gitignored: absent on a clone
    rebuild(kb)
    bib = (kb / BIB).read_text()
    before = rows(kb)
    db = db_of(kb)
    try:
        (row_id,) = [s["id"] for s in db.list_sources()]
    finally:
        db.close()

    again = ingest(kb, src)
    assert again["cite_key"] == first["cite_key"]
    assert again["relative_path"] == first["relative_path"]
    assert again["source_id"] == row_id
    assert not again["duplicate"]
    assert stored.read_bytes() == src.read_bytes()
    assert stored.with_suffix(".txt").exists()
    assert (kb / BIB).read_text() == bib  # no new entry
    assert rows(kb) == before
    db = db_of(kb)
    try:
        md = json.loads(db.get_source(again["source_id"])["metadata"])
    finally:
        db.close()
    assert md["content_hash"] == ingest_module.file_sha256(src)
    run("sync")
    assert rows(kb) == before
    assert (kb / BIB).read_text() == bib


# 4. A hash match whose file is gone restores the file.


def test_reingest_after_file_deleted_restores_it(kb, tmp_path):  # noqa: F811
    src = tmp_path / "paper.md"
    src.write_text("# A Paper\nbody\n")
    first = ingest(kb, src)
    stored = kb / first["relative_path"]
    stored.unlink()
    again = ingest(kb, src)
    assert not again["duplicate"]
    assert again["source_id"] == first["source_id"]
    assert again["cite_key"] == first["cite_key"]
    assert stored.read_bytes() == src.read_bytes()
    assert bibtex.keys((kb / BIB).read_text()) == [first["cite_key"]]
    # Now present: a third ingest is a plain duplicate.
    assert ingest(kb, src)["duplicate"]


# 5. Sync drops the claim of an ingest row whose bib entry was removed, as a
# rebuild does.


def test_sync_matches_rebuild_when_entries_are_removed(kb, tmp_path):  # noqa: F811
    for name, body in (("lab.org", "#+TITLE: Lab day\nx\n"),
                       ("paper.md", "# A Paper\nbody\n"),
                       ("run.csv", "a\n1\n")):
        src = tmp_path / name
        src.write_text(body)
        ingest(kb, src)
    run("sync")
    (kb / BIB).write_text("")
    run("sync")
    synced = rows(kb)
    rebuild(kb)
    assert rows(kb) == synced
    assert {r[0] for r in synced} == {
        ".crucible/sources/notebooks/lab.org",
        ".crucible/sources/data/run.csv",
    }
    assert {r[1] for r in synced} == {"lab", "run"}


def test_removed_entry_file_claimed_by_another_entry(kb, tmp_path):  # noqa: F811
    src = tmp_path / "paper.md"
    src.write_text("# A Paper\nbody\n")
    first = ingest(kb, src)
    path = first["relative_path"]
    (kb / BIB).write_text(
        f"@online{{renamed2024,\n  title = {{Renamed}},\n  year = {{2024}},\n"
        f"  file = {{{path}}},\n}}\n"
    )
    run("sync")
    synced = rows(kb)
    assert synced == [(path, "renamed2024", False, "web")]
    rebuild(kb)
    assert rows(kb) == synced


# 6. Balanced braces in a file name survive the bib file field.


def test_braces_in_file_name_round_trip(kb, tmp_path):  # noqa: F811
    src = tmp_path / "run{2}.csv"
    src.write_text("a\n1\n")
    result = ingest(kb, src)
    assert result["relative_path"] == ".crucible/sources/data/run{2}.csv"
    bib = (kb / BIB).read_text()
    assert "file = {.crucible/sources/data/run{2}.csv}" in bib
    run("sync")
    synced = rows(kb)
    assert [r[0] for r in synced] == [".crucible/sources/data/run{2}.csv"]
    assert (kb / BIB).read_text() == bib
    rebuild(kb)
    assert rows(kb) == synced
    assert (kb / BIB).read_text() == bib


def test_failed_ingest_of_braced_name_leaves_no_entry(kb, tmp_path, monkeypatch):  # noqa: F811
    src = tmp_path / "run{2}.csv"
    src.write_text("a\n1\n")
    before = (kb / BIB).read_text() if (kb / BIB).exists() else ""

    def boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(CrucibleDB, "add_source", boom)
    with pytest.raises(RuntimeError):
        ingest(kb, src)
    after = (kb / BIB).read_text() if (kb / BIB).exists() else ""
    assert bibtex.keys(after) == bibtex.keys(before)
    assert list((kb / ".crucible/sources/data").glob("run*")) == []


def test_unbalanced_brace_name_is_stored_without_braces(kb, tmp_path):  # noqa: F811
    src = tmp_path / "run{2.csv"
    src.write_text("a\n1\n")
    result = ingest(kb, src)
    assert result["relative_path"] == ".crucible/sources/data/run2.csv"
    run("sync")
    synced = rows(kb)
    rebuild(kb)
    assert rows(kb) == synced == [(".crucible/sources/data/run2.csv", result["cite_key"],
                                   True, "data")]


def test_set_bib_field_keeps_balanced_braces():
    entry = "@misc{k,\n  title = {T},\n}"
    out = ingest_module.set_bib_field(entry, "file", "a/b{c}d.csv")
    assert bibtex.first_entry(out).get("file") == "a/b{c}d.csv"
    out = ingest_module.generate_bib_entry("k", "T", None, None, None, "data",
                                           file="a/{x}.csv")
    assert bibtex.first_entry(out).get("file") == "a/{x}.csv"


# 7. DOIs with balanced brackets or angle brackets are kept whole.


@pytest.mark.parametrize("text,doi", [
    ("doi: 10.1061/(ASCE)0733-9399(2008)134:1(1)", "10.1061/(ASCE)0733-9399(2008)134:1(1)"),
    ("(doi 10.1061/(ASCE)0733-9399(2008)134:1(1))", "10.1061/(ASCE)0733-9399(2008)134:1(1)"),
    ("DOI 10.1002/(SICI)1097-4636(199706)35:4<513::AID-JBM12>3.0.CO;2-R.",
     "10.1002/(SICI)1097-4636(199706)35:4<513::AID-JBM12>3.0.CO;2-R"),
    ("<10.1002/(SICI)1097-4636(199706)35:4<513::AID-JBM12>3.0.CO;2-R>",
     "10.1002/(SICI)1097-4636(199706)35:4<513::AID-JBM12>3.0.CO;2-R"),
    ("see (10.1021/acscatal.5b00001).", "10.1021/acscatal.5b00001"),
    ("[10.1021/acscatal.5b00001](https://doi.org/x)", "10.1021/acscatal.5b00001"),
    ("doi:10.1021/acscatal.5b00001, and more", "10.1021/acscatal.5b00001"),
    ("<doi>10.1000/abc</doi>", "10.1000/abc"),
])
def test_doi_extraction_keeps_balanced_brackets(text, doi):
    assert ingest_module.extract_doi_from_text(text) == doi
