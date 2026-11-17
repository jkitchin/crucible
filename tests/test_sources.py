"""Reconciling the sources table with references.bib and committed files (#6)."""

import json

import pytest
from click.testing import CliRunner

from crucible.cli import cli
from crucible.database import CrucibleDB
from crucible.ingest import (
    parse_bib_file,
    set_bib_field,
    source_type_for_path,
)

ARTICLE = """#+TITLE: Widgets
#+FILETAGS: :concept:widgets:

* Widgets
:PROPERTIES:
:ARTICLE_TYPE: concept
:SOURCE_KEYS: {keys}
:STATUS: draft
:END:

Widgets.
"""


def invoke(*args):
    result = CliRunner().invoke(cli, list(args))
    assert result.exit_code == 0, result.output
    return result


@pytest.fixture
def project(tmp_path, registry, monkeypatch):
    root = tmp_path / "kb"
    root.mkdir()
    monkeypatch.chdir(root)
    invoke("init", "--root", str(root))
    return root


def cdir(root):
    return root / ".crucible"


def sources(root):
    db = CrucibleDB(cdir(root) / "crucible.db")
    try:
        rows = db.list_sources()
    finally:
        db.close()
    for row in rows:
        row["metadata"] = json.loads(row["metadata"] or "{}")
    return sorted(rows, key=lambda r: r["path"])


def linked_paths(root, article="concepts/widgets.org"):
    rows = json.loads(invoke("sources", f".crucible/wiki/{article}", "--json").stdout)
    return {r["path"] for r in rows}


def write_article(root, keys):
    (cdir(root) / "wiki" / "concepts" / "widgets.org").write_text(ARTICLE.format(keys=keys))


def delete_db(root):
    for path in cdir(root).glob("crucible.db*"):
        path.unlink()


def ingest(root, tmp_path, name, contents, *options):
    src = tmp_path / "staging" / name
    src.parent.mkdir(exist_ok=True)
    src.write_text(contents)
    invoke("ingest", str(src), *options)
    return parse_bib_file(cdir(root) / "references.bib")[-1]["cite_key"]


# -- The repro from #6 -------------------------------------------------------

ISSUE_BIB = "@misc{widget2026,\n  title = {Widget Notes},\n  year = {2026},\n}\n"


def test_entry_without_file_gets_no_phantom_row(project):
    """#6: the rebuild made a .crucible/bib/ row that captured the article's links."""
    notes = cdir(project) / "sources" / "data" / "widget-notes.md"
    notes.write_text("Primary source text about widgets.\n")
    (cdir(project) / "references.bib").write_text(ISSUE_BIB)
    write_article(project, "widget2026")
    delete_db(project)
    invoke("sync")

    rows = sources(project)
    assert [r["path"] for r in rows] == [".crucible/sources/data/widget-notes.md"]
    assert rows[0]["source_type"] == "data"  # not "web", from the .md extension
    assert not (cdir(project) / "bib").exists()

    # The key is unresolved, and lint says how to fix it.
    issues = json.loads(invoke("lint", "--json").stdout)
    [issue] = [i for i in issues if i["type"] == "unresolved_source_key"]
    assert issue["key"] == "widget2026"
    assert "no file field" in issue["message"]


def test_entry_with_file_takes_over_the_disk_row(project):
    notes = cdir(project) / "sources" / "data" / "widget-notes.md"
    notes.write_text("Primary source text about widgets.\n")
    (cdir(project) / "references.bib").write_text(
        set_bib_field(ISSUE_BIB, "file", ".crucible/sources/data/widget-notes.md")
    )
    write_article(project, "widget2026")
    delete_db(project)
    invoke("sync")

    [row] = sources(project)
    assert row["path"] == ".crucible/sources/data/widget-notes.md"
    assert row["metadata"]["cite_key"] == "widget2026"
    assert row["title"] == "Widget Notes"
    assert row["source_type"] == "data"
    assert linked_paths(project) == {row["path"]}
    assert not json.loads(invoke("undigested", "--json").stdout)


# -- Ingest records the stored file ------------------------------------------

def test_ingest_writes_the_file_field(project, tmp_path):
    key = ingest(project, tmp_path, "run.csv", "a,b\n1,2\n",
                 "--shareable", "--type", "data", "--title", "Run data")
    [entry] = parse_bib_file(cdir(project) / "references.bib")
    assert entry["cite_key"] == key
    assert entry["file"] == ".crucible/sources/data/run.csv"


def test_ingest_replaces_a_file_field_in_supplied_bibtex(project, tmp_path):
    bibtex = (
        "@article{zotero,\n  title = {Supplied},\n"
        "  file = {Full Text:/home/someone/Zotero/x.pdf:application/pdf},\n"
        "  year = {2020},\n}"
    )
    ingest(project, tmp_path, "paper.pdf", "%PDF-1.4 x\n", "--bibtex", bibtex)
    [entry] = parse_bib_file(cdir(project) / "references.bib")
    assert entry["file"] == ".crucible/sources/external/pdfs/paper.pdf"
    assert entry["title"] == "Supplied" and entry["year"] == "2020"


def test_set_bib_field_replaces_existing_values():
    entry = '@misc{k,\n  file = "old.pdf",\n  title = {T}\n}'
    updated = set_bib_field(entry, "file", "new.pdf")
    assert updated.count("file") == 1
    assert "file = {new.pdf}" in updated and "title = {T}" in updated


# -- Projects ingested before ingest recorded the file -----------------------

def test_sync_backfills_file_fields_from_the_database(project, tmp_path):
    key = ingest(project, tmp_path, "notes.md", "# Widget Kinetics\n",
                 "--shareable", "--type", "notebook")
    bib = cdir(project) / "references.bib"
    bib.write_text(bib.read_text().replace(
        "  file = {.crucible/sources/notebooks/notes.md},\n", ""))
    assert "file" not in parse_bib_file(bib)[0]

    invoke("sync")  # the database still knows where the file is
    assert parse_bib_file(bib)[0]["file"] == ".crucible/sources/notebooks/notes.md"

    write_article(project, key)
    invoke("sync")
    before = sources(project)
    delete_db(project)
    invoke("sync")
    after = sources(project)
    assert [(r["path"], r["title"], r["source_type"], r["metadata"]["cite_key"])
            for r in after] == [
        (r["path"], r["title"], r["source_type"], r["metadata"]["cite_key"])
        for r in before
    ]
    assert linked_paths(project) == {".crucible/sources/notebooks/notes.md"}


def test_sync_repairs_a_database_rebuilt_by_earlier_versions(project, tmp_path):
    """An earlier rebuild left a placeholder row holding the links and a disk row."""
    notes = cdir(project) / "sources" / "data" / "widget-notes.md"
    notes.write_text("Primary source text about widgets.\n")
    write_article(project, "widget2026")
    invoke("sync")  # the article, and a disk row for the file
    db = CrucibleDB(cdir(project) / "crucible.db")
    try:
        placeholder = db.add_source(
            path=".crucible/bib/widget2026", title="Widget Notes",
            source_type="other", metadata={"cite_key": "widget2026"},
        )
        article = db.get_article_by_path(".crucible/wiki/concepts/widgets.org")
        db.link_article_source(article["id"], placeholder)
    finally:
        db.close()
    assert linked_paths(project) == {".crucible/bib/widget2026"}

    (cdir(project) / "references.bib").write_text(
        set_bib_field(ISSUE_BIB, "file", ".crucible/sources/data/widget-notes.md")
    )
    invoke("sync")
    [row] = sources(project)
    assert row["path"] == ".crucible/sources/data/widget-notes.md"
    assert row["metadata"]["cite_key"] == "widget2026"
    assert row["source_type"] == "data"
    assert linked_paths(project) == {row["path"]}


def test_placeholder_rows_are_removed(project):
    (cdir(project) / "references.bib").write_text(ISSUE_BIB)
    db = CrucibleDB(cdir(project) / "crucible.db")
    try:
        db.add_source(path=".crucible/bib/widget2026", title="Widget Notes",
                      source_type="other", metadata={"cite_key": "widget2026"})
    finally:
        db.close()
    invoke("sync")
    assert sources(project) == []


def test_a_file_named_like_a_key_is_not_taken_over(project):
    data = cdir(project) / "sources" / "data"
    (data / "smith2024.csv").write_text("unrelated\n")
    (data / "other.csv").write_text("the real source\n")
    invoke("sync")
    (cdir(project) / "references.bib").write_text(
        "@misc{smith2024,\n  title = {Smith data},\n"
        "  file = {.crucible/sources/data/other.csv},\n}\n"
    )
    invoke("sync")
    by_path = {r["path"]: r for r in sources(project)}
    assert by_path[".crucible/sources/data/other.csv"]["title"] == "Smith data"
    assert by_path[".crucible/sources/data/smith2024.csv"]["title"] == "Smith2024"


def test_a_file_belongs_to_the_first_entry_naming_it(project, tmp_path):
    """Two entries naming one file: the second does not take the first one's row."""
    key = ingest(project, tmp_path, "run.csv", "a,b\n1,2\n",
                 "--shareable", "--type", "data", "--title", "Ours")
    bib = cdir(project) / "references.bib"
    bib.write_text(bib.read_text() + (
        "\n@misc{theirs2020,\n  title = {Theirs},\n"
        "  file = {.crucible/sources/data/run.csv},\n}\n"
    ))
    for rebuild in (False, True):
        if rebuild:
            delete_db(project)
        invoke("sync")
        [row] = sources(project)
        assert (row["title"], row["metadata"]["cite_key"]) == ("Ours", key)


@pytest.mark.parametrize(
    "path, source_type",
    [
        (".crucible/sources/external/pdfs/a.pdf", "pdf"),
        (".crucible/sources/external/web/a.html", "web"),
        (".crucible/sources/notebooks/a.md", "notebook"),
        (".crucible/sources/data/a.md", "data"),
        (".crucible/sources/data/a.pdf", "pdf"),
    ],
)
def test_source_type_comes_from_the_storage_directory(path, source_type):
    assert source_type_for_path(path) == source_type


def test_sync_upgrades_sources_ingested_before_content_hashes(project, tmp_path):
    """Review of #25: rows from before 2026-06 have no content_hash."""
    key = ingest(project, tmp_path, "note.md", "# Note\n",
                 "--shareable", "--type", "notebook", "--date", "2026-01-01")
    db = CrucibleDB(cdir(project) / "crucible.db")
    try:
        [row] = db.list_sources()
        metadata = json.loads(row["metadata"])
        del metadata["content_hash"]
        db.update_source(row["id"], metadata=metadata)
    finally:
        db.close()
    bib = cdir(project) / "references.bib"
    bib.write_text(bib.read_text().replace(
        "  file = {.crucible/sources/notebooks/note.md},\n", ""))
    write_article(project, key)

    invoke("sync")
    assert parse_bib_file(bib)[0]["file"] == ".crucible/sources/notebooks/note.md"

    delete_db(project)
    invoke("sync")
    [row] = sources(project)
    assert (row["path"], row["metadata"]["cite_key"]) == (
        ".crucible/sources/notebooks/note.md", key)
    assert linked_paths(project) == {row["path"]}


def test_sync_keeps_a_users_edit_to_a_file_field(project):
    data = cdir(project) / "sources" / "data"
    (data / "old.csv").write_text("old\n")
    (data / "new.csv").write_text("new\n")
    bib = cdir(project) / "references.bib"
    bib.write_text("@misc{smith2024,\n  title = {Smith data},\n"
                   "  file = {.crucible/sources/data/old.csv},\n}\n")
    invoke("sync")
    bib.write_text(bib.read_text().replace("old.csv", "new.csv"))
    invoke("sync")
    assert parse_bib_file(bib)[0]["file"] == ".crucible/sources/data/new.csv"
    by_path = {r["path"]: r for r in sources(project)}
    assert by_path[".crucible/sources/data/new.csv"]["metadata"]["cite_key"] == "smith2024"


def test_ingest_records_the_file_path_verbatim(project, tmp_path):
    ingest(project, tmp_path, "long name  notes.csv", "a\n",
           "--shareable", "--type", "data", "--title", "Spaced")
    [entry] = parse_bib_file(cdir(project) / "references.bib")
    assert entry["file"] == ".crucible/sources/data/long name  notes.csv"


def test_web_sources_are_online_entries(project, tmp_path):
    ingest(project, tmp_path, "page.html", "<p>x</p>\n",
           "--shareable", "--type", "web", "--title", "Page")
    [entry] = parse_bib_file(cdir(project) / "references.bib")
    assert entry["entry_type"] == "online"
    assert source_type_for_path(entry["file"], entry["entry_type"]) == "web"


def test_duplicate_cite_keys_are_linked_and_flagged(project):
    """Two sources sharing a cite key are not silently collapsed to one (#11)."""
    for subdir in ("data", "notebooks"):
        folder = cdir(project) / "sources" / subdir
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "shared2026.md").write_text(f"Synthetic {subdir} source.")
    write_article(project, "shared2026")

    result = invoke("sync")
    assert "more than one source" in result.output

    expected = {
        ".crucible/sources/data/shared2026.md",
        ".crucible/sources/notebooks/shared2026.md",
    }
    assert [s["metadata"]["cite_key"] for s in sources(project)] == ["shared2026"] * 2
    assert linked_paths(project) == expected

    issues = json.loads(invoke("lint", "--json").stdout)
    dup = [i for i in issues if i["type"] == "duplicate_cite_key"]
    assert len(dup) == 1 and dup[0]["key"] == "shared2026"
    assert set(dup[0]["sources"]) == expected
    ambiguous = [i for i in issues if i["type"] == "ambiguous_source_key"]
    assert len(ambiguous) == 1
    assert ambiguous[0]["path"] == ".crucible/wiki/concepts/widgets.org"
    assert set(ambiguous[0]["sources"]) == expected


def test_unique_cite_key_is_not_flagged_ambiguous(project):
    folder = cdir(project) / "sources" / "data"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "solo2026.md").write_text("Synthetic source.")
    write_article(project, "solo2026")

    assert "more than one source" not in invoke("sync").output
    issues = json.loads(invoke("lint", "--json").stdout)
    assert not [
        i for i in issues
        if i["type"] in ("duplicate_cite_key", "ambiguous_source_key")
    ]
