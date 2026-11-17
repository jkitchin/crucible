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
