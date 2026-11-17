"""Tests for --json output and project discovery in the CLI."""

import errno
import json
import os
import shutil
from contextlib import contextmanager
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from crucible import registry as registry_module
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
def registry(tmp_path, monkeypatch):
    """A temporary home directory and global registry.

    `crucible init` installs the skill into ~/.claude/, so HOME must be
    temporary too.
    """
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    reg = home / ".crucible"
    monkeypatch.setattr(registry_module, "REGISTRY_DIR", reg)
    monkeypatch.setattr(registry_module, "REGISTRY_PATH", reg / "registry.json")
    return reg


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


@pytest.mark.parametrize(
    ("absolute_file", "collision", "alias", "double_space", "bib_alias", "registered"),
    [(False, False, False, False, False, False), (True, False, False, False, False, False),
     (False, True, False, False, False, False), (True, True, False, False, False, False),
     (False, False, True, False, False, False), (False, True, False, True, False, False),
     (False, True, False, False, True, False), (False, True, True, False, True, False),
     (False, True, False, False, False, True)],
)
def test_merge_preserves_imported_source_after_database_rebuild(
    project, tmp_path, monkeypatch, absolute_file, collision, alias, double_space, bib_alias,
    registered
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output

    filename = "evidence  notes.md" if double_space else "evidence notes.md"
    original = tmp_path / filename
    original.write_text("Imported source content.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(
        cli,
        ["ingest", str(original), "--type", "data",
         "--title", "Synthetic Evidence", "--date", "2026-01-01"],
    )
    assert result.exit_code == 0, result.output
    source_path = f".crucible/sources/data/{filename}"
    if bib_alias:
        try:
            (source / ".crucible/alias.md").symlink_to(f"sources/data/{filename}")
        except (OSError, NotImplementedError) as exc:
            pytest.skip(f"Symlinks unavailable: {exc}")
        file_value = ".crucible/alias.md"
    else:
        file_value = str(source / source_path) if absolute_file else source_path
    file_field = f'"{file_value}"' if absolute_file else f"{{{file_value}}}"
    bib = source / ".crucible" / "references.bib"
    bib.write_text(bib.read_text().replace("\n}", f"\n  file = {file_field},\n}}", 1))
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic2026")
    )
    if alias:
        try:
            (source / ".crucible/sources/data/alias.md").symlink_to(filename)
        except (OSError, NotImplementedError) as exc:
            pytest.skip(f"Symlinks unavailable: {exc}")
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    unrelated = f"@misc{{other2026,\n  file = {{{tmp_path / 'other.pdf'}}},\n}}"
    bib.write_text(bib.read_text() + "\n" + unrelated + "\n")

    monkeypatch.chdir(project)
    if collision:
        (project / source_path).write_text("Unrelated target content.\n")
    if registered:
        result = runner.invoke(cli, ["sync"])
        assert result.exit_code == 0, result.output
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert unrelated in (project / ".crucible" / "references.bib").read_text()
    renamed_path = f".crucible/sources/data/{original.stem}_1.md" if collision else source_path
    assert [row["path"] for row in run_json("sources", article)] == [renamed_path]

    rebuilt = tmp_path / "rebuilt"
    shutil.copytree(
        project / ".crucible",
        rebuilt / ".crucible",
        ignore=shutil.ignore_patterns("crucible.db", "crucible.db-wal", "crucible.db-shm"),
    )
    monkeypatch.chdir(rebuilt)
    assert [row["path"] for row in run_json("sources", article)] == [renamed_path]
    assert (rebuilt / renamed_path).read_text() == "Imported source content.\n"


@pytest.mark.parametrize(
    ("filename", "bib_filename"),
    [
        ("long name  notes.md", "long\n    name  notes.md"),
        ("literal\nname.md", "literal\nname.md"),
    ],
)
def test_merge_preserves_wrapped_and_literal_newline_bib_paths(
    project, tmp_path, monkeypatch, filename, bib_filename
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    original = tmp_path / filename
    original.write_text("Imported source content.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(
        cli,
        ["ingest", str(original), "--type", "data",
         "--title", "Synthetic Evidence", "--date", "2026-01-01"],
    )
    assert result.exit_code == 0, result.output
    source_path = f".crucible/sources/data/{filename}"
    bib = source / ".crucible/references.bib"
    bib.write_text(bib.read_text().replace(
        "\n}", f"\n  file = {{.crucible/sources/data/{bib_filename}}},\n}}", 1
    ))
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic2026"))
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    rebuilt_source = tmp_path / "rebuilt-source"
    shutil.copytree(
        source / ".crucible",
        rebuilt_source / ".crucible",
        ignore=shutil.ignore_patterns("crucible.db", "crucible.db-wal", "crucible.db-shm"),
    )
    monkeypatch.chdir(rebuilt_source)
    assert [row["path"] for row in run_json("sources", article)] == [source_path]

    monkeypatch.chdir(project)
    (project / source_path).write_text("Existing target content.\n")
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    renamed_path = f".crucible/sources/data/{original.stem}_1.md"
    rebuilt_target = tmp_path / "rebuilt-target"
    shutil.copytree(
        project / ".crucible",
        rebuilt_target / ".crucible",
        ignore=shutil.ignore_patterns("crucible.db", "crucible.db-wal", "crucible.db-shm"),
    )
    monkeypatch.chdir(rebuilt_target)
    assert [row["path"] for row in run_json("sources", article)] == [renamed_path]


def test_merge_preserves_punctuated_bib_keys_after_rebuild(project, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    original = tmp_path / "evidence.md"
    original.write_text("Imported source content.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(
        cli,
        ["ingest", str(original), "--type", "data",
         "--title", "Synthetic Evidence", "--date", "2026-01-01"],
    )
    assert result.exit_code == 0, result.output
    source_path = ".crucible/sources/data/evidence.md"
    bib = source / ".crucible/references.bib"
    bib.write_text(
        bib.read_text()
        .replace("{synthetic2026,", "{synthetic-2026,")
        .replace("\n}", f"\n  file = {{{source_path}}},\n}}", 1)
    )
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.conn.execute(
            "UPDATE sources SET metadata = ?",
            (json.dumps({"cite_key": "synthetic-2026"}),),
        )
        db.conn.commit()
    finally:
        db.close()
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic-2026"))
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    (project / source_path).write_text("Existing target content.\n")
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert "synthetic-2026" in (project / ".crucible/references.bib").read_text()
    rebuilt = tmp_path / "rebuilt"
    shutil.copytree(
        project / ".crucible",
        rebuilt / ".crucible",
        ignore=shutil.ignore_patterns("crucible.db", "crucible.db-wal", "crucible.db-shm"),
    )
    monkeypatch.chdir(rebuilt)
    assert [row["path"] for row in run_json("sources", article)] == [
        ".crucible/sources/data/evidence_1.md"
    ]


def test_merge_prefers_matching_citation_key_over_direct_alias(project, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    original = tmp_path / "paper.md"
    original.write_text("Imported source content.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(
        cli,
        ["ingest", str(original), "--type", "data",
         "--title", "Synthetic Evidence", "--date", "2026-01-01"],
    )
    assert result.exit_code == 0, result.output
    try:
        (source / ".crucible/sources/data/alias.md").symlink_to("paper.md")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    bib = source / ".crucible/references.bib"
    bib.write_text(bib.read_text().replace(
        "\n}", "\n  file = {.crucible/sources/data/alias.md},\n}", 1
    ))
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic2026"))
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    (project / ".crucible/sources/data/paper.md").write_text("Existing target content.\n")
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert "file = {.crucible/sources/data/paper_1.md}" in (
        project / ".crucible/references.bib"
    ).read_text()
    rebuilt = tmp_path / "rebuilt"
    shutil.copytree(
        project / ".crucible",
        rebuilt / ".crucible",
        ignore=shutil.ignore_patterns("crucible.db", "crucible.db-wal", "crucible.db-shm"),
    )
    monkeypatch.chdir(rebuilt)
    assert [row["path"] for row in run_json("sources", article)] == [
        ".crucible/sources/data/paper_1.md"
    ]


@pytest.mark.parametrize("source_exists", [False, True])
def test_merge_rejects_unindexed_bibliography_source_before_copy(
    project, tmp_path, monkeypatch, source_exists
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    rel_path = ".crucible/sources/data/paper.md"
    if source_exists:
        (source / rel_path).write_text("Incoming content.\n")
    (source / ".crucible/references.bib").write_text(
        f"@misc{{paper2026,\n  file = {{{rel_path}}},\n}}\n"
    )
    (project / rel_path).write_text("Unrelated target content.\n")
    target_bib = project / ".crucible/references.bib"
    before = target_bib.read_text() if target_bib.exists() else None
    monkeypatch.chdir(project)
    for option in ("--dry-run", "--yes"):
        result = runner.invoke(cli, ["merge", str(source), option])
        assert result.exit_code != 0
        assert "bibliography" in result.output.lower()
        assert (project / rel_path).read_text() == "Unrelated target content.\n"
        assert (target_bib.read_text() if target_bib.exists() else None) == before


def test_merge_rejects_duplicate_incoming_bib_keys_before_copying(project, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    source_dir = source / ".crucible/sources/data"
    (source_dir / "first.md").write_text("First source.\n")
    (source_dir / "second.md").write_text("Second source.\n")
    (source / ".crucible/references.bib").write_text(
        "@misc{duplicate2026,\n  file = {.crucible/sources/data/first.md},\n}\n"
        "@misc{duplicate2026,\n  file = {.crucible/sources/data/second.md},\n}\n"
    )
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    target_bib = project / ".crucible/references.bib"
    before = target_bib.read_text() if target_bib.exists() else None
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "duplicate" in result.output.lower()
        assert not (project / ".crucible/sources/data/first.md").exists()
        assert not (project / ".crucible/sources/data/second.md").exists()
        assert (target_bib.read_text() if target_bib.exists() else None) == before


def test_merge_reserves_filenames_for_multiple_imported_sources(project, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    source_dir = source / ".crucible/sources/data"
    (source_dir / "evidence.md").write_text("First imported source.\n")
    (source_dir / "evidence_1.md").write_text("Second imported source.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    target_dir = project / ".crucible/sources/data"
    (target_dir / "evidence.md").write_text("Existing target source.\n")
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    assert preview.exit_code == 0, preview.output
    planned = {
        line.split(" -> ", 1)[1]
        for line in preview.output.splitlines()
        if line.startswith("  source: ")
    }
    assert len(planned) == 2
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (target_dir / "evidence.md").read_text() == "Existing target source.\n"
    assert {str(path.relative_to(project)) for path in target_dir.iterdir()
            if path.name != "evidence.md"} == planned
    assert {(project / path).read_text() for path in planned} == {
        "First imported source.\n", "Second imported source.\n"
    }


def test_merge_reserves_filenames_across_symlinked_target_directories(
    project, tmp_path, monkeypatch
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    (source / ".crucible/sources/data/paper.md").write_text("Imported data.\n")
    (source / ".crucible/sources/notebooks/paper_1.md").write_text("Imported notebook.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    target_data = project / ".crucible/sources/data"
    (target_data / "paper.md").write_text("Existing target.\n")
    target_notebooks = project / ".crucible/sources/notebooks"
    target_notebooks.rmdir()
    try:
        target_notebooks.symlink_to("data", target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    assert preview.exit_code == 0, preview.output
    planned = [
        project / line.split(" -> ", 1)[1]
        for line in preview.output.splitlines()
        if line.startswith("  source: ")
    ]
    assert len(planned) == len({path.resolve() for path in planned}) == 2
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (target_data / "paper.md").read_text() == "Existing target.\n"
    assert {path.read_text() for path in planned} == {"Imported data.\n", "Imported notebook.\n"}


@pytest.mark.parametrize("existing_sidecar", [False, True])
def test_merge_does_not_overwrite_existing_or_planned_text_sidecars(
    project, tmp_path, monkeypatch, existing_sidecar
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    source_dir = source / ".crucible/sources/data"
    (source_dir / "paper.pdf").write_bytes(b"Imported PDF")
    (source_dir / "paper.txt").write_text("Imported PDF text.\n")
    (source_dir / "paper_1.txt").write_text("Independent imported text.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    target_dir = project / ".crucible/sources/data"
    (target_dir / "paper.pdf").write_bytes(b"Existing PDF")
    if existing_sidecar:
        (target_dir / "paper_1.txt").write_text("Existing target text.\n")
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    assert preview.exit_code == 0, preview.output
    planned = {
        line.split(" -> ", 1)[1]
        for line in preview.output.splitlines()
        if line.startswith("  source: ")
    }
    assert len(planned) == 2
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (target_dir / "paper.pdf").read_bytes() == b"Existing PDF"
    if existing_sidecar:
        assert (target_dir / "paper_1.txt").read_text() == "Existing target text.\n"
    imported_pdf = next(project / path for path in planned if path.endswith(".pdf"))
    imported_text = next(project / path for path in planned if path.endswith(".txt"))
    assert imported_pdf.read_bytes() == b"Imported PDF"
    assert imported_pdf.with_suffix(".txt").read_text() == "Imported PDF text.\n"
    assert imported_text.read_text() == "Independent imported text.\n"


def test_merge_does_not_write_through_dangling_target_symlinks(project, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    source_dir = source / ".crucible/sources/data"
    (source_dir / "paper.pdf").write_bytes(b"Imported PDF")
    (source_dir / "paper.txt").write_text("Imported text.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    target_dir = project / ".crucible/sources/data"
    try:
        (target_dir / "paper.pdf").symlink_to("missing-pdf")
        (target_dir / "paper_1.txt").symlink_to("missing-text")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    assert preview.exit_code == 0, preview.output
    assert "-> .crucible/sources/data/paper_2.pdf" in preview.output
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (target_dir / "paper.pdf").is_symlink()
    assert (target_dir / "paper_1.txt").is_symlink()
    assert not (target_dir / "missing-pdf").exists()
    assert not (target_dir / "missing-text").exists()
    assert (target_dir / "paper_2.pdf").read_bytes() == b"Imported PDF"
    assert (target_dir / "paper_2.txt").read_text() == "Imported text.\n"


@pytest.mark.parametrize("private_part", ["source", "sidecar", "outside-crucible"])
def test_merge_refuses_private_files_linked_into_shareable_sources(
    project, tmp_path, monkeypatch, private_part
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    data = source / ".crucible/sources/data"
    private = source / ".crucible/sources/external/pdfs"
    (private / "private.pdf").write_bytes(b"Private PDF")
    (private / "private.txt").write_text("Private extracted text.\n")
    try:
        if private_part == "source":
            (data / "paper.pdf").symlink_to("../external/pdfs/private.pdf")
        elif private_part == "outside-crucible":
            (source / "private.pdf").write_bytes(b"Private project file")
            (data / "paper.pdf").symlink_to("../../../private.pdf")
        else:
            (data / "paper.pdf").write_bytes(b"Public PDF")
            (data / "paper.txt").symlink_to("../external/pdfs/private.txt")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "private" in result.output.lower() or "outside" in result.output.lower()
        assert not (project / ".crucible/sources/data/paper.pdf").exists()
        assert not (project / ".crucible/sources/data/paper.txt").exists()


@pytest.mark.parametrize("reuse", [False, True])
def test_merge_refuses_private_metadata_in_shareable_source_paths(
    project, tmp_path, monkeypatch, reuse
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    data = ".crucible/sources/data"
    (source / data / "paper.pdf").write_bytes(b"Private source.")
    (source / data / "paper.txt").write_text("Private text.\n")
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.add_source(path=f"{data}/paper.pdf", title="Paper", source_type="pdf",
                      shareable=False, metadata={"cite_key": "paper2026"})
    finally:
        db.close()
    if reuse:
        (project / data / "paper.pdf").write_bytes(b"Private source.")
        db = CrucibleDB(project / ".crucible/crucible.db")
        try:
            db.add_source(path=f"{data}/paper.pdf", title="Paper", source_type="pdf",
                          shareable=True, metadata={"cite_key": "paper2026"})
        finally:
            db.close()

    monkeypatch.chdir(project)
    for option in ("--dry-run", "--yes"):
        result = runner.invoke(cli, ["merge", str(source), option])
        assert result.exit_code != 0
        assert "private" in result.output.lower()
        assert not (project / data / "paper.txt").exists()
        if not reuse:
            assert not (project / data / "paper.pdf").exists()


@pytest.mark.parametrize("outside_crucible", [False, True])
def test_merge_refuses_target_directory_symlinks_outside_the_crucible(
    project, tmp_path, monkeypatch, outside_crucible
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    (source / ".crucible/sources/data/paper.md").write_text("Imported source.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    outside = tmp_path / "outside"
    outside.mkdir()
    if outside_crucible:
        outside = project
    target_data = project / ".crucible/sources/data"
    target_data.rmdir()
    try:
        target_data.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "outside" in result.output.lower()
        assert not (outside / "paper.md").exists()


@pytest.mark.parametrize(
    "target_state", ["same", "different", "missing", "other-path", "same-other-path"]
)
def test_merge_registered_source_with_same_citation_key(
    project, tmp_path, monkeypatch, target_state
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    inputs = []
    for name, content in (
        ("incoming", "Imported content.\n"),
        ("existing", "Imported content.\n" if target_state in {"same", "same-other-path"}
         else "Other content.\n"),
    ):
        folder = tmp_path / name
        folder.mkdir()
        filename = (
            "other.md" if name == "existing" and target_state.endswith("other-path")
            else "evidence.md"
        )
        path = folder / filename
        path.write_text(content)
        inputs.append(path)
    ingest_args = ["--type", "data", "--title", "Synthetic Evidence", "--date", "2026-01-01"]
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["ingest", str(inputs[0]), *ingest_args])
    assert result.exit_code == 0, result.output
    source_path = ".crucible/sources/data/evidence.md"
    bib = source / ".crucible/references.bib"
    bib.write_text(bib.read_text().replace(
        "\n}", f'\n  file = "{source / source_path}",\n}}', 1
    ))
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic2026"))
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    result = runner.invoke(cli, ["ingest", str(inputs[1]), *ingest_args])
    assert result.exit_code == 0, result.output
    target_path = project / source_path
    if target_state == "missing":
        target_path.unlink()
    if target_state.endswith("other-path"):
        target_path.write_text("Different registered source.\n")
        result = runner.invoke(cli, ["sync"])
        assert result.exit_code == 0, result.output
    target_bib = project / ".crucible/references.bib"
    target_bib.write_text("")
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    if target_state not in {"same", "same-other-path"}:
        for outcome in (preview, result):
            assert outcome.exit_code != 0
            assert "Cannot safely merge source with citation key 'synthetic2026'" in outcome.output
        assert not (project / article).exists()
        assert target_bib.read_text() == ""
        if target_state == "different":
            assert target_path.read_text() == "Other content.\n"
        elif target_state == "other-path":
            assert target_path.read_text() == "Different registered source.\n"
        else:
            assert not target_path.exists()
        return

    assert preview.exit_code == 0, preview.output
    assert result.exit_code == 0, result.output
    assert "Skipped 1 existing sources" in result.output
    expected_path = (
        ".crucible/sources/data/other.md" if target_state == "same-other-path" else source_path
    )
    assert [row["path"] for row in run_json("sources", article)] == [expected_path]
    rebuilt = tmp_path / "rebuilt"
    shutil.copytree(
        project / ".crucible",
        rebuilt / ".crucible",
        ignore=shutil.ignore_patterns("crucible.db", "crucible.db-wal", "crucible.db-shm"),
    )
    monkeypatch.chdir(rebuilt)
    assert [row["path"] for row in run_json("sources", article)] == [expected_path]
    assert (rebuilt / expected_path).read_text() == "Imported content.\n"


@pytest.mark.parametrize("target_sidecar", ["missing", "same", "different", "private"])
def test_merge_keeps_text_sidecars_when_reusing_identical_sources(
    project, tmp_path, monkeypatch, target_sidecar
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    rel_path = ".crucible/sources/data/paper.pdf"
    for root in (source, project):
        (root / rel_path).write_bytes(b"Identical source.")
        db = CrucibleDB(root / ".crucible/crucible.db")
        try:
            db.add_source(path=rel_path, title="Paper", source_type="pdf",
                          shareable=True, metadata={"cite_key": "paper2026"})
        finally:
            db.close()
    source_text = source / ".crucible/sources/data/paper.txt"
    if target_sidecar == "private":
        private_text = source / ".crucible/sources/external/paper.txt"
        private_text.parent.mkdir(parents=True, exist_ok=True)
        private_text.write_text("Private text.\n")
        try:
            source_text.symlink_to(private_text)
        except (OSError, NotImplementedError) as exc:
            pytest.skip(f"Symlinks unavailable: {exc}")
    else:
        source_text.write_text("Incoming text.\n")
    target_text = project / ".crucible/sources/data/paper.txt"
    if target_sidecar in {"same", "different"}:
        target_text.write_text(
            "Incoming text.\n" if target_sidecar == "same" else "Existing text.\n"
        )

    monkeypatch.chdir(project)
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    if target_sidecar in {"different", "private"}:
        assert preview.exit_code != 0
        assert ("sidecar" if target_sidecar == "different" else "private") in preview.output.lower()
        result = runner.invoke(cli, ["merge", str(source), "--yes"])
        assert result.exit_code != 0
        if target_sidecar == "different":
            assert target_text.read_text() == "Existing text.\n"
        else:
            assert not target_text.exists()
    else:
        assert preview.exit_code == 0, preview.output
        if target_sidecar == "missing":
            assert "sidecar" in preview.output.lower()
            assert not target_text.exists()
        result = runner.invoke(cli, ["merge", str(source), "--yes"])
        assert result.exit_code == 0, result.output
        assert target_text.read_text() == "Incoming text.\n"


@pytest.mark.parametrize("target_state", ["source-collision", "sidecar-collision", "reuse"])
def test_merge_rewrites_bibliography_paths_for_text_sidecars(
    project, tmp_path, monkeypatch, target_state
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    data = ".crucible/sources/data"
    (source / data / "paper.pdf").write_bytes(b"Incoming source.")
    (source / data / "paper.txt").write_text("Incoming text.\n")
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.add_source(path=f"{data}/paper.pdf", title="Paper", source_type="pdf",
                      shareable=True, metadata={"cite_key": "paper2026"})
    finally:
        db.close()
    (source / ".crucible/references.bib").write_text(
        f"@misc{{paper2026,\n  file = {{{data}/paper.txt}},\n}}\n"
    )
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: paper2026"))

    if target_state == "reuse":
        (project / data / "different.pdf").write_bytes(b"Incoming source.")
        db = CrucibleDB(project / ".crucible/crucible.db")
        try:
            db.add_source(path=f"{data}/different.pdf", title="Paper", source_type="pdf",
                          shareable=True, metadata={"cite_key": "paper2026"})
        finally:
            db.close()
        expected = f"{data}/different.txt"
    else:
        collision = "paper.pdf" if target_state == "source-collision" else "paper.txt"
        (project / data / collision).write_text("Existing target content.\n")
        expected = f"{data}/paper_1.txt"

    monkeypatch.chdir(project)
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (project / expected).read_text() == "Incoming text.\n"
    assert f"file = {{{expected}}}" in (project / ".crucible/references.bib").read_text()

    rebuilt = tmp_path / "rebuilt"
    shutil.copytree(
        project / ".crucible",
        rebuilt / ".crucible",
        ignore=shutil.ignore_patterns("crucible.db", "crucible.db-wal", "crucible.db-shm"),
    )
    monkeypatch.chdir(rebuilt)
    assert [row["path"] for row in run_json("sources", article)] == [expected]


@pytest.mark.parametrize("option", ["--dry-run", "--yes"])
def test_merge_rejects_nonfile_text_sidecar_before_copy(project, tmp_path, monkeypatch, option):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    data = ".crucible/sources/data"
    (source / data / "paper.pdf").write_bytes(b"Incoming source.")
    (source / data / "paper.txt").mkdir()
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.add_source(path=f"{data}/paper.pdf", title="Paper", source_type="pdf",
                      shareable=True, metadata={"cite_key": "paper2026"})
    finally:
        db.close()
    monkeypatch.chdir(project)
    result = runner.invoke(cli, ["merge", str(source), option])
    assert result.exit_code != 0
    assert "not a regular file" in result.output.lower()
    assert not (project / data / "paper.pdf").exists()


def test_merge_refuses_duplicate_incoming_citation_keys(project, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    for name in ("first.md", "second.md"):
        (source / ".crucible/sources/data" / name).write_text(name)
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    db = CrucibleDB(source / ".crucible/crucible.db")
    for row in db.list_sources():
        db.conn.execute(
            "UPDATE sources SET metadata = ? WHERE id = ?",
            (json.dumps({"cite_key": "duplicate2026"}), row["id"]),
        )
    db.conn.commit()
    db.close()

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "Cannot safely merge source with citation key 'duplicate2026'" in result.output
        assert not (project / ".crucible/sources/data/first.md").exists()
        assert not (project / ".crucible/sources/data/second.md").exists()


@pytest.mark.parametrize("source_indexed", [False, True])
def test_merge_keeps_bibliography_only_entries_without_copying_phantom_files(
    project, tmp_path, monkeypatch, source_indexed
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    (source / ".crucible/references.bib").write_text(
        "@misc{legacy2026,\n  title = {Legacy entry},\n}\n"
    )
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: legacy2026"))
    monkeypatch.chdir(source)
    if source_indexed:
        result = runner.invoke(cli, ["sync"])
        assert result.exit_code == 0, result.output
    assert not (source / ".crucible/bib/legacy2026").exists()

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code == 0, result.output
        assert "bibliography-only" in result.output.lower()
    assert "legacy2026" in (project / ".crucible/references.bib").read_text()
    assert (project / article).exists()
    assert not (project / ".crucible/bib/legacy2026").exists()


def test_merge_warns_when_an_indexed_source_lacks_a_durable_bib_mapping(
    project, tmp_path, monkeypatch
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    rel_path = ".crucible/sources/data/paper.md"
    (source / rel_path).write_text("Incoming content.\n")
    (source / ".crucible/references.bib").write_text(
        "@misc{paper2026,\n  title = {Paper},\n}\n"
    )
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.add_source(path=rel_path, title="Paper", source_type="data",
                      shareable=True, metadata={"cite_key": "paper2026"})
    finally:
        db.close()
    monkeypatch.chdir(project)
    for option in ("--dry-run", "--yes"):
        result = runner.invoke(cli, ["merge", str(source), option])
        assert result.exit_code == 0, result.output
        assert "no durable file mapping" in result.output.lower()
    assert (project / rel_path).read_text() == "Incoming content.\n"


def test_merge_rejects_duplicate_keys_even_when_one_source_is_outside_crucible(
    project, tmp_path, monkeypatch
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    local = source / ".crucible/sources/data/local.md"
    local.write_text("Local source.\n")
    outside = tmp_path / "outside.md"
    outside.write_text("External source.\n")
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        for path in (".crucible/sources/data/local.md", str(outside)):
            db.add_source(path=path, title="Source", source_type="data",
                          metadata={"cite_key": "duplicate2026"})
    finally:
        db.close()

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "duplicate2026" in result.output
        assert not (project / ".crucible/sources/data/local.md").exists()


@pytest.mark.parametrize("invalid_in", ["source", "target"])
def test_merge_reports_invalid_source_metadata_before_copying(
    project, tmp_path, monkeypatch, invalid_in
):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    (source / ".crucible/sources/data/incoming.md").write_text("Incoming source.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    db_root = source if invalid_in == "source" else project
    db = CrucibleDB(db_root / ".crucible/crucible.db")
    try:
        if invalid_in == "target":
            db.add_source(path=".crucible/sources/data/legacy.md", title="Legacy",
                          source_type="data")
        db.conn.execute("UPDATE sources SET metadata = ?", ("{invalid",))
        db.conn.commit()
    finally:
        db.close()

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "invalid metadata" in result.output.lower()
        assert not (project / ".crucible/sources/data/incoming.md").exists()


def test_merge_refuses_missing_incoming_files(project, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    missing = source / ".crucible/sources/data/evidence.md"
    missing.write_text("Imported source.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    missing.unlink()

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "source file is missing" in result.output
        assert not (project / ".crucible/sources/data/evidence.md").exists()


def test_merge_reports_ambiguous_bibliography_source(project, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    original = tmp_path / "evidence.md"
    original.write_text("Imported source content.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["ingest", str(original), "--type", "data"])
    assert result.exit_code == 0, result.output
    try:
        (source / ".crucible/sources/data/alias.md").symlink_to("evidence.md")
        (source / ".crucible/link.md").symlink_to("sources/data/evidence.md")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    bib = source / ".crucible/references.bib"
    bib.write_text(
        bib.read_text()
        + "\n@misc{unmatched2026,\n  file = {.crucible/link.md},\n}\n"
    )
    (source / ".crucible/wiki/concepts/probe.org").write_text(CONCEPT)

    monkeypatch.chdir(project)
    target_bib = project / ".crucible/references.bib"
    original_bib = "@misc{existing2026,\n  title = {Existing entry},\n}\n"
    target_bib.write_text(original_bib)
    result = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    assert result.exit_code != 0
    assert "Cannot safely map bibliography entry 'unmatched2026'" in result.output
    assert "may already have copied files" not in result.output
    assert not (project / ".crucible/sources/data/evidence.md").exists()
    assert not (project / ".crucible/wiki/concepts/probe.org").exists()
    assert target_bib.read_text() == original_bib

    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code != 0
    assert "Cannot safely map bibliography entry 'unmatched2026'" in result.output
    assert "may already have copied files" not in result.output
    assert not (project / ".crucible/sources/data/evidence.md").exists()
    assert not (project / ".crucible/wiki/concepts/probe.org").exists()
    assert target_bib.read_text() == original_bib


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


def _ingest_into_new_crucible(tmp_path, monkeypatch, name, content):
    """Create a crucible holding one ingested data source keyed synthetic2026."""
    root = tmp_path / name
    root.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(root)])
    assert result.exit_code == 0, result.output
    original = tmp_path / f"{name}-evidence.md"
    original.write_text(content)
    monkeypatch.chdir(root)
    result = runner.invoke(
        cli,
        ["ingest", str(original), "--type", "data",
         "--title", "Synthetic Evidence", "--date", "2026-01-01"],
    )
    assert result.exit_code == 0, result.output
    assert "{synthetic2026," in (root / ".crucible/references.bib").read_text()
    return root


def test_merge_lists_every_problem_at_once(project, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    first = source / ".crucible/sources/data/first.md"
    second = source / ".crucible/sources/data/second.md"
    first.write_text("First.\n")
    second.write_text("Second.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    first.unlink()
    second.unlink()

    monkeypatch.chdir(project)
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code != 0
    assert "2 problems" in result.output
    assert "first.md" in result.output and "second.md" in result.output


def test_merge_refuses_before_asking_for_confirmation(project, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--root", str(source)])
    assert result.exit_code == 0, result.output
    missing = source / ".crucible/sources/data/evidence.md"
    missing.write_text("Imported source.\n")
    monkeypatch.chdir(source)
    assert runner.invoke(cli, ["sync"]).exit_code == 0
    missing.unlink()

    monkeypatch.chdir(project)
    result = runner.invoke(cli, ["merge", str(source)], input="y\n")
    assert result.exit_code != 0
    assert "source file is missing" in result.output
    assert "Proceed with merge?" not in result.output


def test_merge_refuses_key_already_in_unsynced_target_bibliography(
    project, tmp_path, monkeypatch
):
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Imported.\n")
    # The target's bibliography has the key, but its database has no source
    # for it, so merge cannot detect the clash from the database alone.
    target_bib = project / ".crucible/references.bib"
    with target_bib.open("a") as f:
        f.write("\n@misc{synthetic2026,\n  title = {Something else},\n}\n")
    before = target_bib.read_text()

    monkeypatch.chdir(project)
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code != 0
    assert "target bibliography already has an entry" in result.output
    assert not list((project / ".crucible/sources/data").glob("*evidence*"))
    assert target_bib.read_text() == before


def test_merge_reuses_identical_source_with_same_key(project, tmp_path, monkeypatch):
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Shared bytes.\n")
    target = _ingest_into_new_crucible(tmp_path, monkeypatch, "target", "Shared bytes.\n")
    before = sorted(p.name for p in (target / ".crucible/sources/data").iterdir())

    monkeypatch.chdir(target)
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert "Skipped 1 existing sources" in result.output
    assert sorted(p.name for p in (target / ".crucible/sources/data").iterdir()) == before
