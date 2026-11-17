"""Tests for --json output and project discovery in the CLI."""

import json
import shutil

import pytest
from click.testing import CliRunner

from crucible import registry as registry_module
from crucible.cli import cli
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
