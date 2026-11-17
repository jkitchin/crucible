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
    # A row with neither a file nor a bib entry is dropped by sync.
    _touch(project / ".crucible/sources/external/pdfs/registered.pdf")
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


def _touch(path):
    """Create a source file, so sync keeps a row that has no bib entry."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("Synthetic source.")


def test_sync_drops_source_keys_removed_from_a_file(project):
    _touch(project / ".crucible/sources/external/pdfs/paper.pdf")
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


def test_sync_replaces_source_keys_changed_in_a_file(project):
    """Issue #10: swapping one SOURCE_KEYS entry for another must leave
    only the current source linked, not both."""
    db = CrucibleDB(project / ".crucible" / "crucible.db")
    try:
        for key, title in (("sourcea2026", "Source A"), ("sourceb2026", "Source B")):
            _touch(project / f".crucible/sources/external/pdfs/{key}.pdf")
            db.add_source(
                path=f".crucible/sources/external/pdfs/{key}.pdf",
                title=title, source_type="pdf",
                metadata={"cite_key": key},
            )
    finally:
        db.close()
    path = project / CONCEPT_PATH
    original = path.read_text()
    path.write_text(original.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: sourcea2026"))
    _sync()
    assert [s["title"] for s in run_json("sources", CONCEPT_PATH)] == ["Source A"]
    path.write_text(original.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: sourceb2026"))
    _sync()
    assert [s["title"] for s in run_json("sources", CONCEPT_PATH)] == ["Source B"]


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


def _sync_output():
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    return result.output


def _with_embeddings(project, fn):
    """Run fn(db, ids) with an embeddings table holding a row per article."""
    from crucible.embeddings import EmbeddingIndex
    db = CrucibleDB(project / ".crucible" / "crucible.db")
    try:
        EmbeddingIndex(db.conn).initialize()
        ids = {p: db.get_article_by_path(p)["id"] for p in (CONCEPT_PATH, SUMMARY_PATH)}
        return fn(db, ids)
    finally:
        db.close()


def _embedded_paths(project):
    def read(db, ids):
        rows = db.conn.execute("SELECT article_id FROM embeddings").fetchall()
        present = {r["article_id"] for r in rows}
        return {p for p, i in ids.items() if i in present}
    return _with_embeddings(project, read)


def _seed(project):
    """Give every article an embedding, as a marker that sync kept it."""
    def seed(db, ids):
        for i in ids.values():
            db.conn.execute(
                "INSERT INTO embeddings (article_id, model, dimensions) VALUES (?, 'm', 768)",
                (i,),
            )
        db.conn.commit()
    _with_embeddings(project, seed)


def _updated_at(project, path):
    db = CrucibleDB(project / ".crucible" / "crucible.db")
    try:
        return db.get_article_by_path(path)["updated_at"]
    finally:
        db.close()


def test_sync_leaves_unchanged_articles_alone(project):
    _seed(project)
    assert "0 added, 0 updated, 2 unchanged" in _sync_output()
    assert _updated_at(project, CONCEPT_PATH) == "2026-01-01 00:00:00"
    assert _embedded_paths(project) == {CONCEPT_PATH, SUMMARY_PATH}


def test_sync_rewrites_only_changed_articles(project):
    _seed(project)
    path = project / CONCEPT_PATH
    path.write_text(path.read_text().replace("energy barrier", "zeolite barrier"))
    assert "0 added, 1 updated, 1 unchanged" in _sync_output()
    # The edited article's embedding is stale, so it is dropped for re-embedding.
    assert _embedded_paths(project) == {SUMMARY_PATH}
    assert [r["path"] for r in run_json("search", "zeolite", "--mode", "fts")] == [CONCEPT_PATH]


def test_sync_counts_a_metadata_only_edit_as_a_change(project):
    path = project / CONCEPT_PATH
    path.write_text(path.read_text().replace(":STATUS: draft", ":STATUS: reviewed"))
    assert "1 updated, 1 unchanged" in _sync_output()


def test_unchanged_article_links_resolve_when_their_target_appears(project):
    """Relations are rebuilt even for unchanged files: smith.org links to
    missing.org, which only exists after the first sync."""
    missing = ".crucible/wiki/summaries/missing.org"
    (project / missing).write_text(SUMMARY.replace("Smith 2024 results", "Missing"))
    assert "1 added, 0 updated, 2 unchanged" in _sync_output()
    assert SUMMARY_PATH in [r["path"] for r in run_json("backlinks", missing)]


def _dates(db_path, path):
    db = CrucibleDB(db_path)
    try:
        row = db.get_article_by_path(path)
        return row["created_at"], row["updated_at"]
    finally:
        db.close()


def _db(project):
    return project / ".crucible" / "crucible.db"


def _set_modified(project, path, value):
    p = project / path
    p.write_text(p.read_text().replace(":STATUS:", f":MODIFIED: {value}\n:STATUS:"))


def test_article_dates_come_from_created_and_modified(project):
    # CREATED alone sets both dates.
    assert _dates(_db(project), CONCEPT_PATH) == ("2026-01-01 00:00:00",) * 2
    _set_modified(project, CONCEPT_PATH, "[2026-03-05 Thu 14:30]")
    _sync()
    assert _dates(_db(project), CONCEPT_PATH) == ("2026-01-01 00:00:00", "2026-03-05 14:30:00")
    # list_articles orders by updated_at, newest first.
    db = CrucibleDB(_db(project))
    try:
        assert db.list_articles()[0]["path"] == CONCEPT_PATH
    finally:
        db.close()


def test_article_dates_survive_a_rebuild(project, tmp_path):
    _set_modified(project, SUMMARY_PATH, "2026-02-03")
    _sync()
    fresh_path = tmp_path / "fresh.db"
    fresh = CrucibleDB(fresh_path)
    fresh.initialize()
    try:
        _sync_wiki_to_db(project, fresh)
    finally:
        fresh.close()
    for path in (CONCEPT_PATH, SUMMARY_PATH):
        assert _dates(fresh_path, path) == _dates(_db(project), path)
    assert _dates(fresh_path, SUMMARY_PATH) == ("2026-01-02 00:00:00", "2026-02-03 00:00:00")


def test_modified_is_used_as_is_even_when_the_file_changed(project):
    _set_modified(project, CONCEPT_PATH, "2026-03-05")
    _sync()
    path = project / CONCEPT_PATH
    path.write_text(path.read_text().replace("energy barrier", "zeolite barrier"))
    assert "1 updated" in _sync_output()
    assert _dates(_db(project), CONCEPT_PATH)[1] == "2026-03-05 00:00:00"


def test_without_modified_an_edit_keeps_the_created_date(project):
    """With :CREATED: only, the file still decides: an edit that doesn't
    add :MODIFIED: leaves updated_at at the :CREATED: date."""
    path = project / CONCEPT_PATH
    path.write_text(path.read_text().replace("energy barrier", "zeolite barrier"))
    assert "1 updated" in _sync_output()
    assert _dates(_db(project), CONCEPT_PATH) == ("2026-01-01 00:00:00",) * 2


def test_without_any_date_property_an_edit_uses_sync_time(project):
    path = project / CONCEPT_PATH
    path.write_text(path.read_text().replace(":CREATED: 2026-01-01\n", ""))
    _sync()
    path.write_text(path.read_text().replace("energy barrier", "zeolite barrier"))
    _sync()
    assert _dates(_db(project), CONCEPT_PATH)[1] > "2026-01-01 00:00:00"


def test_upgraded_created_only_db_matches_a_fresh_rebuild(project, tmp_path):
    """A wiki written before :MODIFIED: existed (every article has :CREATED:
    only) and a DB whose dates are sync times, as older versions left it:
    after one sync it must hold the same dates as a fresh rebuild."""
    db = CrucibleDB(_db(project))
    try:
        db.conn.execute("UPDATE articles SET created_at = '2030-05-06 10:53:19', "
                        "updated_at = '2030-05-06 10:53:19'")
        db.conn.commit()
    finally:
        db.close()
    _sync()
    fresh_path = tmp_path / "fresh.db"
    fresh = CrucibleDB(fresh_path)
    fresh.initialize()
    try:
        _sync_wiki_to_db(project, fresh)
    finally:
        fresh.close()
    for path in (CONCEPT_PATH, SUMMARY_PATH):
        assert _dates(_db(project), path) == _dates(fresh_path, path)
    assert _dates(fresh_path, SUMMARY_PATH) == ("2026-01-02 00:00:00",) * 2


def test_dates_from_an_older_db_are_corrected_without_dropping_embeddings(project):
    """Upgrading: rows written before dates came from properties are fixed
    on the next sync, but the content is unchanged so embeddings are kept."""
    _seed(project)  # embeddings for both articles
    db = CrucibleDB(_db(project))
    try:
        db.conn.execute("UPDATE articles SET created_at = '2030-01-01 00:00:00'")
        db.conn.commit()
    finally:
        db.close()
    assert "0 added, 2 updated, 0 unchanged" in _sync_output()
    assert _dates(_db(project), CONCEPT_PATH)[0] == "2026-01-01 00:00:00"
    assert _embedded_paths(project) == {CONCEPT_PATH, SUMMARY_PATH}
    assert "0 updated, 2 unchanged" in _sync_output()


def test_unparseable_dates_fall_back_to_sync_time(project):
    path = project / CONCEPT_PATH
    path.write_text(path.read_text().replace(":CREATED: 2026-01-01", ":CREATED: someday"))
    _sync()
    created, _ = _dates(_db(project), CONCEPT_PATH)
    assert created == "2026-01-01 00:00:00"  # left as it was
    (project / ".crucible" / "wiki" / "concepts" / "new.org").write_text(
        CONCEPT.replace("Activation energy", "New").replace("2026-01-01", "2026-02-30"))
    _sync()
    assert _dates(_db(project), ".crucible/wiki/concepts/new.org")[0] > "2026-01-01"
