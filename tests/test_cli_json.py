"""Tests for --json output and project discovery in the CLI."""

import json

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
        ("known2026-copy.pdf", "Lower priority", {}),
        ("falsepositive2026.pdf", "Different source", {"cite_key": "different2026"}),
    ]
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
            if filename == "badmetadata-file.pdf":
                db.conn.execute(
                    "UPDATE sources SET metadata = ? WHERE id = ?",
                    ("{invalid", source_id),
                )
                db.conn.commit()
    finally:
        db.close()

    keys = "known2026 legacy-path Vintage badmetadata missing2026 falsepositive2026"
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", f":SOURCE_KEYS: {keys}")
    )
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    sources = run_json("sources", CONCEPT_PATH)
    assert {source["path"] for source in sources} == {
        ".crucible/sources/external/pdfs/paper.pdf",
        ".crucible/sources/external/pdfs/legacy-path-file.pdf",
        ".crucible/sources/external/pdfs/other.pdf",
        ".crucible/sources/external/pdfs/badmetadata-file.pdf",
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
    assert [source["path"] for source in sources] == [".crucible/bib/smith2024"]
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
