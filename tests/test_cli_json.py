"""Tests for --json output and project discovery in the CLI."""

import errno
import json
import os
from contextlib import contextmanager
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from crucible.cli import _sync_wiki_to_db, cli
from crucible.database import CrucibleDB

CONCEPT = """#+TITLE: Activation energy
#+FILETAGS: :concept:kinetics:

* Activation energy
:PROPERTIES:
:ARTICLE_TYPE: concept
:SOURCE_KEYS:
:STATUS: draft
:CREATED: 2026-01-01
:END:

The energy barrier for a reaction. See [[file:../summaries/smith.org][Smith]].
"""

SUMMARY = """#+TITLE: Smith 2024 results
#+FILETAGS: :summary:kinetics:

* Smith 2024 results
:PROPERTIES:
:ARTICLE_TYPE: summary
:SOURCE_KEYS:
:DERIVED_FROM: ../concepts/activation-energy.org
:STATUS: draft
:CREATED: 2026-01-02
:END:

Measured activation energies on platinum. See
[[file:../concepts/activation-energy.org][activation energy]] and [[file:missing.org][a missing article]].
"""


@pytest.fixture
def project(tmp_path, registry, monkeypatch):
    """A synced crucible with two linked articles; cwd is its root."""
    root = tmp_path / "kb"
    root.mkdir()
    monkeypatch.chdir(root)
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(root)])
    assert result.exit_code == 0, result.output
    # Only an explicit `crucible install` writes the global directive.
    assert not (registry.parent / ".claude" / "CLAUDE.md").exists()
    wiki = root / ".crucible" / "wiki"
    (wiki / "concepts" / "activation-energy.org").write_text(CONCEPT)
    (wiki / "summaries" / "smith.org").write_text(SUMMARY)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    return root


def run_json(*args):
    result = CliRunner().invoke(cli, [*args, "--json"])
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)


CONCEPT_PATH = ".crucible/wiki/concepts/activation-energy.org"
SUMMARY_PATH = ".crucible/wiki/summaries/smith.org"


def test_about_json_gives_project_paths(project):
    data = run_json("about")
    assert data["project"]["root"] == str(project.resolve())
    assert data["project"]["wiki"].endswith(".crucible/wiki")
    assert data["project"]["stats"]["articles"] == 2
    assert any(i["path"] == str(project.resolve()) for i in data["registry"])


def test_stats_json(project):
    assert run_json("stats")["articles"] == 2


def test_search_json(project):
    results = run_json("search", "platinum", "--mode", "fts")
    assert [r["path"] for r in results] == [SUMMARY_PATH]
    assert run_json("search", "nothingmatchesthis", "--mode", "fts") == []


def test_concepts_json(project):
    names = {c["name"]: c["article_count"] for c in run_json("concepts")}
    assert names["kinetics"] == 2
    assert {r["path"] for r in run_json("concept", "kinetics")} == {CONCEPT_PATH, SUMMARY_PATH}


def test_link_queries_json(project):
    assert [r["path"] for r in run_json("backlinks", CONCEPT_PATH)] == [SUMMARY_PATH]
    assert SUMMARY_PATH in [r["path"] for r in run_json("related", CONCEPT_PATH)]
    history = run_json("history", SUMMARY_PATH)
    assert [r["path"] for r in history["derived_from"]] == [CONCEPT_PATH]
    assert history["derivatives"] == []
    assert run_json("sources", CONCEPT_PATH) == []
    assert isinstance(run_json("orphans"), list)
    assert run_json("undigested") == []


def test_lint_json_issues_name_their_file(project):
    issues = run_json("lint")
    broken = [i for i in issues if i["type"] == "broken_link"]
    assert broken and broken[0]["path"] == SUMMARY_PATH
    assert broken[0]["link"].endswith("missing.org")
    # The old flag spelling still works.
    result = CliRunner().invoke(cli, ["lint", "-j"])
    assert json.loads(result.stdout) == issues


@pytest.mark.parametrize("rebuild", [False, True])
def test_sync_does_not_invent_a_source_for_an_unmapped_bib_entry(project, rebuild):
    cdir = project / ".crucible"
    bib = cdir / "references.bib"
    original_bib = "@misc{widget2026,\n  title = {Widget Notes},\n}\n"
    bib.write_text(original_bib)
    (cdir / "sources/data/unrelated.md").write_text("Unrelated source.\n")
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: widget2026")
    )
    if rebuild:
        (cdir / "crucible.db").unlink()

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    db = CrucibleDB(cdir / "crucible.db")
    try:
        assert [row["path"] for row in db.list_sources()] == [
            ".crucible/sources/data/unrelated.md"
        ]
    finally:
        db.close()
    assert run_json("sources", CONCEPT_PATH) == []
    assert bib.read_text() == original_bib
    assert "widget2026" in result.stderr
    assert "no file field" in result.stderr


def test_sync_registers_an_explicit_bib_file(project):
    cdir = project / ".crucible"
    (cdir / "sources/data/evidence.md").write_text("Mapped source.\n")
    (cdir / "references.bib").write_text(
        "@misc{widget2026,\n"
        "  title = {Widget Notes},\n"
        "  file = {.crucible/sources/data/evidence.md},\n"
        "}\n"
    )
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: widget2026")
    )

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert [row["path"] for row in run_json("sources", CONCEPT_PATH)] == [
        ".crucible/sources/data/evidence.md"
    ]
    assert "widget2026" not in result.stderr


@pytest.mark.parametrize("with_bib", [False, True])
def test_sync_warns_about_an_existing_phantom_source_without_deleting_it(
    project, with_bib
):
    cdir = project / ".crucible"
    if with_bib:
        (cdir / "references.bib").write_text("@misc{widget2026,\n  title = {Widget},\n}\n")
    db = CrucibleDB(cdir / "crucible.db")
    try:
        db.add_source(
            path=".crucible/bib/widget2026",
            title="Widget",
            source_type="other",
            metadata={"cite_key": "widget2026"},
        )
    finally:
        db.close()

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    db = CrucibleDB(cdir / "crucible.db")
    try:
        assert any(row["path"] == ".crucible/bib/widget2026" for row in db.list_sources())
    finally:
        db.close()
    assert "widget2026" in result.stderr
    assert "synthetic" in result.stderr


@pytest.mark.parametrize("metadata", ["[]", '{"cite_key": ["invalid"]}'])
def test_sync_handles_invalid_source_metadata_with_no_bibliography(project, metadata):
    cdir = project / ".crucible"
    db = CrucibleDB(cdir / "crucible.db")
    try:
        source_id = db.add_source(
            path=".crucible/sources/data/legacy.md", title="Legacy", source_type="data"
        )
        db.conn.execute("UPDATE sources SET metadata = ? WHERE id = ?", (metadata, source_id))
        db.conn.commit()
    finally:
        db.close()

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert "invalid metadata" in result.stderr
    assert "legacy.md" in result.stderr


@pytest.mark.parametrize("metadata", ["[]", "not json", '{"cite_key": ["invalid"]}'])
def test_ingest_and_lint_tolerate_invalid_source_metadata(project, tmp_path, metadata):
    cdir = project / ".crucible"
    db = CrucibleDB(cdir / "crucible.db")
    try:
        source_id = db.add_source(
            path=".crucible/sources/data/legacy.md", title="Legacy", source_type="data"
        )
        db.conn.execute("UPDATE sources SET metadata = ? WHERE id = ?", (metadata, source_id))
        db.conn.commit()
    finally:
        db.close()

    original = tmp_path / "new-notes.md"
    original.write_text("New notes.\n")
    result = CliRunner().invoke(cli, ["ingest", str(original), "--title", "New Notes"])
    assert result.exit_code == 0, result.output
    assert isinstance(run_json("lint"), list)


def test_source_cite_key_accepts_only_nonempty_strings():
    from crucible.ingest import _source_cite_key, _source_metadata

    assert _source_cite_key({"metadata": '{"cite_key": "k"}'}) == "k"
    for raw in ["[]", "not json", '{"cite_key": ""}', '{"cite_key": ["k"]}', None]:
        assert _source_cite_key({"metadata": raw}) is None
    assert _source_metadata({"metadata": "[]"}) == {}


def test_committed_sources_take_their_type_from_their_directory(project):
    cdir = project / ".crucible"
    (cdir / "sources/data/readings.md").write_text("Data notes.\n")
    (cdir / "sources/data/paper.pdf").write_text("%PDF-1.4\n")
    (cdir / "sources/notebooks/day1.md").write_text("Lab notes.\n")
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    db = CrucibleDB(cdir / "crucible.db")
    try:
        types = {row["path"].split("/", 2)[-1]: row["source_type"] for row in db.list_sources()}
    finally:
        db.close()
    assert types == {
        "data/readings.md": "data",
        "data/paper.pdf": "pdf",
        "notebooks/day1.md": "notebook",
    }


def _bib_key_for(bib_text, title):
    from crucible.ingest import _BIB_ENTRY_RE

    return next(m.group(2) for m in _BIB_ENTRY_RE.finditer(bib_text) if title in m.group(3))


@pytest.mark.parametrize("source_type", ["web", "notebook"])
def test_ingested_source_keys_resolve_after_a_rebuild(project, tmp_path, source_type):
    cdir = project / ".crucible"
    original = tmp_path / "widget-notes.md"
    original.write_text("Primary notes about widgets.\n")
    result = CliRunner().invoke(
        cli, ["ingest", str(original), "--title", "Widget Notes", "--type", source_type]
    )
    assert result.exit_code == 0, result.output
    bib = (cdir / "references.bib").read_text()
    key = _bib_key_for(bib, "Widget Notes")
    assert "file = {.crucible/sources/" in bib
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", f":SOURCE_KEYS: {key}")
    )

    (cdir / "crucible.db").unlink()
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert "Warning" not in result.stderr
    [linked] = run_json("sources", CONCEPT_PATH)
    assert linked["path"].endswith("widget-notes.md")
    assert not [i for i in run_json("lint") if i["type"] == "unresolved_source_key"]


def test_sync_records_known_paths_in_the_bibliography(project):
    cdir = project / ".crucible"
    (cdir / "sources/data/evidence.md").write_text("Mapped source.\n")
    db = CrucibleDB(cdir / "crucible.db")
    try:
        db.add_source(
            path=".crucible/sources/data/evidence.md",
            title="Widget Notes",
            source_type="data",
            metadata={"cite_key": "widget2026"},
        )
    finally:
        db.close()
    bib = cdir / "references.bib"
    bib.write_text(
        "@misc{other2025,\n  title = {Other},\n}\n\n"
        "@misc{widget2026,\n  title = {Widget Notes}\n}\n"
    )
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: widget2026")
    )

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert "Recorded source file paths or types for 1 bibliography entries" in result.stdout
    assert "file = {.crucible/sources/data/evidence.md}" in bib.read_text()
    # A .md file defaults to web, so the data type is recorded too.
    assert "crucible_type = {data}" in bib.read_text()
    assert "@misc{other2025,\n  title = {Other},\n}" in bib.read_text()

    (cdir / "crucible.db").unlink()
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert [row["path"] for row in run_json("sources", CONCEPT_PATH)] == [
        ".crucible/sources/data/evidence.md"
    ]


def test_sync_moves_a_synthetic_source_once_its_entry_names_a_file(project):
    cdir = project / ".crucible"
    (cdir / "sources/data/evidence.md").write_text("Mapped source.\n")
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: widget2026")
    )
    db = CrucibleDB(cdir / "crucible.db")
    try:
        db.add_source(
            path=".crucible/bib/widget2026",
            title="Widget",
            source_type="other",
            metadata={"cite_key": "widget2026"},
        )
    finally:
        db.close()
    (cdir / "references.bib").write_text(
        "@misc{widget2026,\n  title = {Widget},\n"
        "  file = {.crucible/sources/data/evidence.md},\n}\n"
    )

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert "synthetic" not in result.stderr
    paths = [row["path"] for row in run_json("sources", CONCEPT_PATH)]
    assert paths == [".crucible/sources/data/evidence.md"]
    db = CrucibleDB(cdir / "crucible.db")
    try:
        assert [row["path"] for row in db.list_sources()] == paths
    finally:
        db.close()


def test_unmapped_bib_entries_share_one_warning(project):
    cdir = project / ".crucible"
    (cdir / "references.bib").write_text(
        "".join(f"@misc{{key{i},\n  title = {{T{i}}},\n}}\n\n" for i in range(15))
    )
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert result.stderr.count("Warning:") == 1
    assert "15 bibliography entries have no file field" in result.stderr
    assert "(5 more)" in result.stderr


def test_add_bib_file_field_handles_single_line_entries(tmp_path):
    from crucible.ingest import add_bib_file_field, parse_bib_file

    entry = add_bib_file_field(
        "@article{k, title={A {B} c}, year={2024}}", ".crucible/sources/external/pdfs/a.pdf"
    )
    bib = tmp_path / "references.bib"
    bib.write_text(entry + "\n")
    [parsed] = parse_bib_file(bib)
    assert parsed["cite_key"] == "k"
    assert parsed["file"] == ".crucible/sources/external/pdfs/a.pdf"
    assert add_bib_file_field(entry, "elsewhere.pdf") == entry
    # Paths are written verbatim, and skipped when BibTeX cannot hold them.
    spaced = add_bib_file_field("@misc{s,\n  title = {S},\n}", "data/a  b.md")
    assert "file = {data/a  b.md}" in spaced
    for unsafe in ["data/a{b}.md", "data/a\nb.md"]:
        assert add_bib_file_field("@misc{s,\n}", unsafe) == "@misc{s,\n}"


def test_lint_resolves_source_keys_like_sync(project):
    cdir = project / ".crucible"
    external = cdir / "sources" / "external" / "pdfs"
    external.mkdir(parents=True, exist_ok=True)
    records = [
        ("paper.pdf", "Canonical source", {"cite_key": "known2026"}),
        ("legacy-path-file.pdf", "Legacy source", {}),
        ("other.pdf", "Vintage source", {}),
        ("badmetadata-file.pdf", "Old source", {}),
        ("nonobject-list.pdf", "List legacy source", {}),
        ("nonobject-null.pdf", "Null legacy source", {}),
        ("invalid-key.pdf", "Invalid key source", {}),
        ("known2026-copy.pdf", "Lower priority", {}),
        ("falsepositive2026.pdf", "Different source", {"cite_key": "different2026"}),
    ]
    malformed_metadata = {
        "badmetadata-file.pdf": "{invalid",
        "nonobject-list.pdf": "[]",
        "nonobject-null.pdf": "null",
        "invalid-key.pdf": '{"cite_key": ["not-a-key"]}',
    }
    db = CrucibleDB(cdir / "crucible.db")
    try:
        for filename, title, metadata in records:
            path = external / filename
            path.write_text("Synthetic source.")
            source_id = db.add_source(
                path=str(path.relative_to(project)),
                title=title,
                source_type="pdf",
                metadata=metadata,
            )
            if filename in malformed_metadata:
                db.conn.execute(
                    "UPDATE sources SET metadata = ? WHERE id = ?",
                    (malformed_metadata[filename], source_id),
                )
                db.conn.commit()
    finally:
        db.close()

    keys = (
        "known2026 legacy-path Vintage badmetadata nonobject-list nonobject-null invalid-key "
        "missing2026 falsepositive2026"
    )
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", f":SOURCE_KEYS: {keys}")
    )
    issues_before_sync = run_json("lint")
    assert any(
        issue["type"] == "unresolved_source_key" and issue["key"] == "missing2026"
        for issue in issues_before_sync
    )
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    sources = run_json("sources", CONCEPT_PATH)
    assert {source["path"] for source in sources} == {
        ".crucible/sources/external/pdfs/paper.pdf",
        ".crucible/sources/external/pdfs/legacy-path-file.pdf",
        ".crucible/sources/external/pdfs/other.pdf",
        ".crucible/sources/external/pdfs/badmetadata-file.pdf",
        ".crucible/sources/external/pdfs/nonobject-list.pdf",
        ".crucible/sources/external/pdfs/nonobject-null.pdf",
        ".crucible/sources/external/pdfs/invalid-key.pdf",
    }

    issues = run_json("lint")
    unresolved = {
        issue["key"]: issue
        for issue in issues
        if issue["type"] == "unresolved_source_key" and issue["path"] == CONCEPT_PATH
    }
    assert set(unresolved) == {"missing2026", "falsepositive2026"}
    for key, issue in unresolved.items():
        assert issue["severity"] == "warning"
        assert key in issue["message"]
    result = CliRunner().invoke(cli, ["lint", "-j"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == issues


def test_rebuild_does_not_assign_title_only_bib_to_disk_source(project):
    cdir = project / ".crucible"
    csv = cdir / "sources" / "data" / "experiment.csv"
    csv.parent.mkdir(parents=True, exist_ok=True)
    csv.write_text("Synthetic unrelated data.")
    (cdir / "references.bib").write_text(
        "@misc{smith2024,\n  title = {Experiment}\n}\n"
    )
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: smith2024")
    )
    for db_file in cdir.glob("crucible.db*"):
        db_file.unlink()

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    db = CrucibleDB(cdir / "crucible.db")
    try:
        rows = db.list_sources()
    finally:
        db.close()
    assert str(csv.relative_to(project)) in {source["path"] for source in rows}
    sources = run_json("sources", CONCEPT_PATH)
    assert str(csv.relative_to(project)) not in {source["path"] for source in sources}


def test_registry_list_json(project):
    entries = run_json("registry", "list")
    entry = next(e for e in entries if e["path"] == str(project.resolve()))
    assert entry["db"].endswith("crucible.db")


def test_registry_dir_is_not_a_project(tmp_path, registry, monkeypatch):
    # A wiki/ inside the registry dir (e.g. from `crucible init` in ~)
    # must not make every directory under home a crucible.
    (registry / "wiki").mkdir(parents=True)
    work = registry.parent / "projects" / "thing"
    work.mkdir(parents=True)
    monkeypatch.chdir(work)
    result = CliRunner().invoke(cli, ["stats"])
    assert result.exit_code != 0
    assert "Not inside a crucible project" in result.output


def test_init_refuses_the_home_directory(registry):
    result = CliRunner().invoke(cli, ["init", "--root", str(registry.parent)])
    assert result.exit_code != 0
    assert "global crucible registry" in result.output
    assert not (registry / "wiki").exists()


def test_sync_stores_and_updates_article_metadata(project):
    results = run_json("search", "platinum", "--mode", "fts")
    assert results[0]["status"] == "draft"
    summary = project / SUMMARY_PATH
    summary.write_text(SUMMARY.replace(":STATUS: draft", ":STATUS: verified")
                       .replace("#+TITLE: Smith 2024 results", "#+TITLE: Smith 2024 measurements"))
    assert CliRunner().invoke(cli, ["sync"]).exit_code == 0
    results = run_json("search", "platinum", "--mode", "fts")
    assert results[0]["status"] == "verified"
    assert results[0]["title"] == "Smith 2024 measurements"


def test_sync_preserves_lowercase_org_keywords(project):
    concept = project / CONCEPT_PATH
    concept.write_text(
        CONCEPT.replace("#+TITLE: Activation energy", "#+title: Lowercase title")
        .replace("#+FILETAGS: :concept:kinetics:", "#+filetags: :concept:lowercase-tag:")
    )

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    results = run_json("search", "barrier", "--mode", "fts")
    assert results[0]["title"] == "Lowercase title"
    concepts = {c["name"] for c in run_json("concepts")}
    assert "lowercase-tag" in concepts


def test_sync_ignores_generated_index(project):
    result = CliRunner().invoke(cli, ["index"])
    assert result.exit_code == 0, result.output
    assert (project / ".crucible" / "wiki" / "index.org").exists()

    nested_index = ".crucible/wiki/concepts/index.org"
    (project / nested_index).write_text(CONCEPT)
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    assert run_json("stats")["articles"] == 3
    issues = run_json("lint")
    assert not any(
        issue.get("path") == ".crucible/wiki/index.org"
        and issue["type"] == "missing_property"
        for issue in issues
    )
    assert any(
        issue.get("path") == nested_index and issue["type"] == "missing_property"
        for issue in issues
    )


def test_sync_prunes_last_article_after_index_generation(project):
    (project / SUMMARY_PATH).unlink()
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    cdir = project / ".crucible"
    db = CrucibleDB(cdir / "crucible.db")
    try:
        article_id = db.get_article_by_path(CONCEPT_PATH)["id"]
        source_id = db.add_source(
            path=".crucible/sources/external/pdfs/registered.pdf",
            title="Registered source", source_type="pdf",
            metadata={"cite_key": "registered2026"},
        )
        db.link_article_source(article_id, source_id)
        assert db.conn.execute(
            "SELECT rowid FROM article_fts WHERE rowid = ?", (article_id,)
        ).fetchone()
    finally:
        db.close()

    result = CliRunner().invoke(cli, ["index"])
    assert result.exit_code == 0, result.output
    assert (cdir / "wiki" / "index.org").exists()
    (project / CONCEPT_PATH).unlink()

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert "1 removed" in result.output
    db = CrucibleDB(cdir / "crucible.db")
    try:
        assert db.get_article_by_path(CONCEPT_PATH) is None
        assert not db.conn.execute(
            "SELECT rowid FROM article_fts WHERE rowid = ?", (article_id,)
        ).fetchone()
        assert not db.conn.execute(
            "SELECT article_id FROM article_sources WHERE article_id = ?", (article_id,)
        ).fetchone()
        assert db.get_source(source_id) is not None
    finally:
        db.close()


@pytest.mark.parametrize("blocked_subdir", ["wiki", "concepts", "entry"])
def test_sync_preserves_articles_when_wiki_traversal_fails(project, monkeypatch, blocked_subdir):
    cdir = project / ".crucible"
    wiki = cdir / "wiki"
    blocked = wiki if blocked_subdir == "wiki" else wiki / "concepts"
    db = CrucibleDB(cdir / "crucible.db")
    try:
        article_ids = {row["id"] for row in db.list_articles()}
        article_id = db.get_article_by_path(CONCEPT_PATH)["id"]
        source_id = db.add_source(
            path=".crucible/sources/external/pdfs/registered.pdf",
            title="Registered source", source_type="pdf",
            metadata={"cite_key": "registered2026"},
        )
        db.link_article_source(article_id, source_id)
        article_links = {
            (row["from_id"], row["to_id"])
            for row in db.conn.execute("SELECT from_id, to_id FROM article_links")
        }
        assert article_links
        original_scandir = os.scandir

        @contextmanager
        def scandir_with_blocked_entry(path):
            with original_scandir(path) as entries:
                wrapped = []
                for entry in entries:
                    if os.path.normpath(entry.path) == str(blocked):
                        inaccessible = Mock()
                        inaccessible.name = entry.name
                        inaccessible.path = entry.path
                        inaccessible.stat.side_effect = PermissionError(
                            errno.EACCES, "Synthetic traversal failure", str(blocked)
                        )
                        wrapped.append(inaccessible)
                    else:
                        wrapped.append(entry)
                yield iter(wrapped)

        def fail_scandir(path):
            if not isinstance(path, (str, bytes, os.PathLike)):
                return original_scandir(path)
            scanned = os.path.normpath(os.fsdecode(path))
            if blocked_subdir == "entry" and scanned == str(wiki):
                return scandir_with_blocked_entry(path)
            if blocked_subdir != "entry" and scanned == str(blocked):
                raise PermissionError(errno.EACCES, "Synthetic traversal failure", str(blocked))
            return original_scandir(path)

        monkeypatch.setattr(os, "scandir", fail_scandir)
        with pytest.raises(PermissionError) as error:
            _sync_wiki_to_db(project, db)

        assert {row["id"] for row in db.list_articles()} == article_ids
        assert {
            (row["from_id"], row["to_id"])
            for row in db.conn.execute("SELECT from_id, to_id FROM article_links")
        } == article_links
        assert db.conn.execute(
            "SELECT article_id FROM article_sources WHERE article_id = ? AND source_id = ?",
            (article_id, source_id),
        ).fetchone()
        for article_id in article_ids:
            assert db.conn.execute(
                "SELECT rowid FROM article_fts WHERE rowid = ?", (article_id,)
            ).fetchone()
        assert error.value.filename == str(blocked)
    finally:
        db.close()


def test_sync_closes_database_when_wiki_traversal_fails(project, monkeypatch):
    db = CrucibleDB(project / ".crucible" / "crucible.db")
    close = Mock(wraps=db.close)
    monkeypatch.setattr(db, "close", close)
    monkeypatch.setattr("crucible.cli.get_db", lambda _root: db)
    scandir = os.scandir
    wiki = project / ".crucible" / "wiki"

    def fail_scandir(path):
        if os.fsdecode(path) == str(wiki):
            raise PermissionError(errno.EACCES, "Synthetic traversal failure", str(wiki))
        return scandir(path)

    monkeypatch.setattr(os, "scandir", fail_scandir)
    result = CliRunner().invoke(cli, ["sync"])
    assert isinstance(result.exception, PermissionError)
    close.assert_called_once_with()


def test_sync_does_not_prune_when_wiki_directory_is_missing(project):
    cdir = project / ".crucible"
    wiki = cdir / "wiki"
    moved_wiki = cdir / "wiki-unavailable"
    db = CrucibleDB(cdir / "crucible.db")
    try:
        article_ids = {row["id"] for row in db.list_articles()}
        assert article_ids
        article_id = min(article_ids)
        source_id = db.add_source(
            path=".crucible/sources/external/pdfs/registered.pdf",
            title="Registered source", source_type="pdf",
            metadata={"cite_key": "registered2026"},
        )
        db.link_article_source(article_id, source_id)
        wiki.rename(moved_wiki)
        try:
            stats = _sync_wiki_to_db(project, db)
            assert stats["removed"] == 0
            assert {row["id"] for row in db.list_articles()} == article_ids
            assert db.conn.execute(
                "SELECT article_id FROM article_sources WHERE article_id = ? AND source_id = ?",
                (article_id, source_id),
            ).fetchone()
            for article_id in article_ids:
                assert db.conn.execute(
                    "SELECT rowid FROM article_fts WHERE rowid = ?", (article_id,)
                ).fetchone()
        finally:
            moved_wiki.rename(wiki)
    finally:
        db.close()


def test_install_points_the_global_directive_at_this_crucible(project, registry):
    result = CliRunner().invoke(cli, ["install"])
    assert result.exit_code == 0, result.output
    assert str(project.resolve()) in (registry.parent / ".claude" / "CLAUDE.md").read_text()


def test_init_acts_on_its_root_not_the_current_directory(project, tmp_path, monkeypatch):
    # Run from inside one crucible, init another: the new one gets the
    # manifest and the registry entry.
    other = tmp_path / "other"
    other.mkdir()
    result = CliRunner().invoke(cli, ["init", "--root", str(other)])
    assert result.exit_code == 0, result.output
    assert (other / ".crucible" / "wiki" / "MANIFEST.md").exists()
    paths = [e["path"] for e in run_json("registry", "list")]
    assert str(other.resolve()) in paths


def test_sync_skips_emacs_lock_files(project):
    # Emacs marks a buffer with unsaved changes with a dangling symlink.
    lock = project / ".crucible" / "wiki" / "concepts" / ".#activation-energy.org"
    lock.symlink_to("user@host.1234:1700000000")

    for command in (["sync"], ["lint", "--json"]):
        result = CliRunner().invoke(cli, command)
        assert result.exit_code == 0, result.output
    assert run_json("stats")["articles"] == 2


def test_legacy_source_keys_match_whole_tokens(project):
    cdir = project / ".crucible"
    db = CrucibleDB(cdir / "crucible.db")
    try:
        db.add_source(
            path=".crucible/sources/external/web/page.html",
            title="Some web page",
            source_type="web",
            metadata={},
        )
    finally:
        db.close()
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: ml")
    )
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    assert run_json("sources", CONCEPT_PATH) == []
    assert any(
        issue["type"] == "unresolved_source_key" and issue["key"] == "ml"
        for issue in run_json("lint")
    )


INDEX_PATH = ".crucible/wiki/index.org"


def test_sync_ignores_generated_index_in_stats_concepts_and_lint(project):
    runner = CliRunner()
    assert runner.invoke(cli, ["index"]).exit_code == 0
    assert (project / INDEX_PATH).exists()
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert run_json("stats")["articles"] == 2
    paths = {r["path"] for r in run_json("concept", "kinetics")}
    assert INDEX_PATH not in paths
    assert not [i for i in run_json("lint") if i.get("path") == INDEX_PATH]


def test_sync_quietly_prunes_index_registered_by_older_versions(project):
    from crucible.cli import get_db
    runner = CliRunner()
    assert runner.invoke(cli, ["index"]).exit_code == 0
    db = get_db(project)
    db.add_article(path=INDEX_PATH, title="Crucible Wiki Index", article_type="concept")
    db.close()
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert "0 removed" in result.output
    assert run_json("stats")["articles"] == 2


def test_nested_index_org_is_an_ordinary_article(project):
    nested = ".crucible/wiki/concepts/index.org"
    (project / nested).write_text(CONCEPT.replace("Activation energy", "Miller index"))
    issues = run_json("lint")
    assert any(i["type"] == "untracked" and i["path"] == nested for i in issues)
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert run_json("stats")["articles"] == 3
    assert nested in {r["path"] for r in run_json("concept", "kinetics")}


def _sync():
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output


def test_sync_drops_tags_removed_from_a_file(project):
    path = project / CONCEPT_PATH
    path.write_text(path.read_text().replace(":kinetics:", ":thermo:"))
    _sync()
    assert [r["path"] for r in run_json("concept", "kinetics")] == [SUMMARY_PATH]
    assert [r["path"] for r in run_json("concept", "thermo")] == [CONCEPT_PATH]


def test_sync_drops_links_and_derivations_removed_from_a_file(project):
    path = project / SUMMARY_PATH
    text = path.read_text()
    text = text.replace(":DERIVED_FROM: ../concepts/activation-energy.org\n", "")
    text = text.replace("[[file:../concepts/activation-energy.org][activation energy]]",
                        "activation energy")
    path.write_text(text)
    _sync()
    assert run_json("backlinks", CONCEPT_PATH) == []
    assert run_json("history", SUMMARY_PATH)["derived_from"] == []


def test_sync_drops_source_keys_removed_from_a_file(project):
    db = CrucibleDB(project / ".crucible" / "crucible.db")
    try:
        db.add_source(
            path=".crucible/sources/external/pdfs/paper.pdf",
            title="A paper", source_type="pdf",
            metadata={"cite_key": "paper2026"},
        )
    finally:
        db.close()
    path = project / CONCEPT_PATH
    original = path.read_text()
    path.write_text(original.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: paper2026"))
    _sync()
    assert [s["title"] for s in run_json("sources", CONCEPT_PATH)] == ["A paper"]
    path.write_text(original)
    _sync()
    assert run_json("sources", CONCEPT_PATH) == []


def test_sync_prunes_concepts_no_article_uses(project):
    path = project / CONCEPT_PATH
    path.write_text(path.read_text().replace(":kinetics:", ":kinetics:solo:"))
    _sync()
    assert "solo" in {c["name"] for c in run_json("concepts")}
    path.unlink()
    _sync()
    names = {c["name"]: c["article_count"] for c in run_json("concepts")}
    assert names == {"kinetics": 1}


def test_incremental_sync_matches_a_fresh_rebuild(project, tmp_path):
    """The DB must stay regenerable from the wiki: after edits, an
    incrementally synced DB holds the same relations as a fresh one."""

    concept = project / CONCEPT_PATH
    concept.write_text(concept.read_text().replace(":kinetics:", ":catalysis:")
                       .replace("See [[file:../summaries/smith.org][Smith]].", ""))
    summary = project / SUMMARY_PATH
    summary.write_text(summary.read_text().replace(":kinetics:", ":kinetics:platinum:"))
    _sync()

    def relations(db):
        q = lambda sql: sorted(tuple(r.values()) for r in db.conn.execute(sql))  # noqa: E731
        return {
            "tags": q("SELECT a.path, c.name FROM article_concepts ac "
                      "JOIN articles a ON a.id = ac.article_id "
                      "JOIN concepts c ON c.id = ac.concept_id"),
            "concepts": q("SELECT name FROM concepts"),
            "links": q("SELECT f.path, t.path FROM article_links l "
                       "JOIN articles f ON f.id = l.from_id "
                       "JOIN articles t ON t.id = l.to_id"),
            "derivations": q("SELECT d.path, s.path FROM article_derivations x "
                             "JOIN articles d ON d.id = x.derived_id "
                             "JOIN articles s ON s.id = x.source_id"),
        }

    synced = CrucibleDB(project / ".crucible" / "crucible.db")
    fresh = CrucibleDB(tmp_path / "fresh.db")
    fresh.initialize()
    try:
        _sync_wiki_to_db(project, fresh)
        assert relations(synced) == relations(fresh)
    finally:
        synced.close()
        fresh.close()


def test_concept_prune_survives_a_null_concept_id(project):
    """A NULL concept_id must not stop unused concepts being pruned
    (`id NOT IN (... NULL ...)` matches no rows)."""
    db = CrucibleDB(project / ".crucible" / "crucible.db")
    try:
        article_id = db.get_article_by_path(CONCEPT_PATH)["id"]
        db.conn.execute(
            "INSERT INTO article_concepts (article_id, concept_id) VALUES (?, NULL)",
            (article_id,),
        )
        db.add_concept("unused")
        db.prune_unused_concepts()
        names = {c["name"] for c in db.list_concepts()}
    finally:
        db.close()
    assert "unused" not in names
    assert "kinetics" in names
