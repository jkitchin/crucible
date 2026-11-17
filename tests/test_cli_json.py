"""Tests for --json output and project discovery in the CLI."""

import json

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


@pytest.mark.parametrize("rebuild", [False, True])
def test_sync_does_not_invent_a_source_for_an_unmapped_bib_entry(project, caplog, rebuild):
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
    assert "widget2026" in caplog.text
    assert "no file" in caplog.text.lower()


def test_sync_registers_an_explicit_bib_file(project, caplog):
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
    assert "widget2026" not in caplog.text


@pytest.mark.parametrize("with_bib", [False, True])
def test_sync_warns_about_an_existing_phantom_source_without_deleting_it(
    project, caplog, with_bib
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
    assert "widget2026" in caplog.text
    assert "synthetic" in caplog.text.lower()


@pytest.mark.parametrize("metadata", ["[]", '{"cite_key": ["invalid"]}'])
def test_sync_handles_invalid_source_metadata_with_no_bibliography(project, caplog, metadata):
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
    assert "invalid" in caplog.text.lower()


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
