"""A merge is all or nothing: a failure partway leaves the target unchanged (#27)."""

import pytest
from click.testing import CliRunner

import crucible.cli as cli_module
from crucible.cli import cli
from crucible.database import CrucibleDB

from test_merge import (  # noqa: F401
    CONCEPT, _ingest_into_new_crucible, make_crucible, project,
)

LINKING = """#+TITLE: Linking
#+FILETAGS: :concept:

* Linking
:PROPERTIES:
:ARTICLE_TYPE: concept
:SOURCE_KEYS: synthetic2026
:STATUS: draft
:CREATED: 2026-01-01
:END:

See [[file:activation-energy.org][one]] and [[file:activation-energy_1.org][two]].
"""


def snapshot(root):
    """Every file in the target's .crucible/ (but the database) and its source rows."""
    cdir = root / ".crucible"
    files = {
        str(p.relative_to(cdir)): p.read_bytes()
        for p in cdir.rglob("*")
        if p.is_file() and not p.name.startswith("crucible.db") and p.suffix != ".lock"
    }
    dirs = {str(p.relative_to(cdir)) for p in cdir.rglob("*") if p.is_dir()}
    db = CrucibleDB(cdir / "crucible.db")
    try:
        rows = sorted(s["path"] for s in db.list_sources())
    finally:
        db.close()
    return files, dirs, rows


@pytest.fixture
def incoming(project, tmp_path, monkeypatch):  # noqa: F811
    """A crucible with a source, a bib entry and two articles, one colliding."""
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Imported.\n")
    wiki = source / ".crucible/wiki/concepts"
    # Differs from the target's copy, so it is renamed rather than skipped.
    (wiki / "activation-energy.org").write_text(CONCEPT + "\nIncoming version.\n")
    (wiki / "activation-energy_1.org").write_text(CONCEPT.replace("Activation", "Other"))
    (wiki / "linking.org").write_text(LINKING)
    (source / ".crucible/sources/data/new-dir").mkdir()
    (source / ".crucible/sources/data/new-dir/extra.md").write_text("Extra.\n")
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    monkeypatch.chdir(project)
    return source


def fail_on_call(monkeypatch, target, name, n, exc=OSError("disk full")):
    """Make target.name raise exc on its nth call."""
    real = getattr(target, name)
    calls = {"n": 0}

    def wrapper(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == n:
            raise exc
        return real(*args, **kwargs)

    monkeypatch.setattr(target, name, wrapper)


def assert_unchanged(project, before, result):
    assert result.exit_code != 0
    assert snapshot(project) == before
    assert not list((project / ".crucible").glob(".merge-staging-*"))


def test_failure_while_copying_leaves_target_unchanged(project, incoming, monkeypatch):
    before = snapshot(project)
    fail_on_call(monkeypatch, cli_module.shutil, "copy2", 3)
    result = CliRunner().invoke(cli, ["merge", str(incoming), "--yes"])
    assert_unchanged(project, before, result)
    assert "Nothing was merged; the target is unchanged." in result.output


def test_failure_while_moving_into_place_leaves_target_unchanged(
    project, incoming, monkeypatch
):
    before = snapshot(project)
    fail_on_call(monkeypatch, cli_module.os, "rename", 4)
    result = CliRunner().invoke(cli, ["merge", str(incoming), "--yes"])
    assert_unchanged(project, before, result)


def test_failure_in_database_undoes_files_and_bibliography(project, incoming, monkeypatch):
    before = snapshot(project)
    fail_on_call(monkeypatch, CrucibleDB, "add_source", 2, RuntimeError("db broke"))
    result = CliRunner().invoke(cli, ["merge", str(incoming), "--yes"])
    assert_unchanged(project, before, result)
    assert "db broke" in result.output


def test_interrupt_leaves_target_unchanged(project, incoming, monkeypatch):
    before = snapshot(project)
    fail_on_call(monkeypatch, CrucibleDB, "add_source", 1, KeyboardInterrupt())
    result = CliRunner().invoke(cli, ["merge", str(incoming), "--yes"])
    assert_unchanged(project, before, result)


def test_merge_succeeds_after_a_failed_attempt(project, incoming, monkeypatch):
    with monkeypatch.context() as m:
        fail_on_call(m, CrucibleDB, "add_source", 1, RuntimeError("db broke"))
        assert CliRunner().invoke(cli, ["merge", str(incoming), "--yes"]).exit_code != 0
    result = CliRunner().invoke(cli, ["merge", str(incoming), "--yes"])
    assert result.exit_code == 0, result.output
    cdir = project / ".crucible"
    assert (cdir / "sources/data/new-dir/extra.md").read_text() == "Extra.\n"
    assert "{synthetic2026," in (cdir / "references.bib").read_text()
    db = CrucibleDB(cdir / "crucible.db")
    try:
        paths = {s["path"] for s in db.list_sources()}
    finally:
        db.close()
    assert ".crucible/sources/data/new-dir/extra.md" in paths


def test_renamed_articles_keep_their_links_apart(project, incoming):
    # Target has activation-energy.org; incoming activation-energy.org becomes
    # _1, and incoming _1 becomes _1_1. Each link must follow its own article.
    result = CliRunner().invoke(cli, ["merge", str(incoming), "--yes"])
    assert result.exit_code == 0, result.output
    wiki = project / ".crucible/wiki/concepts"
    assert "Other energy" in (wiki / "activation-energy_1_1.org").read_text()
    text = (wiki / "linking.org").read_text()
    assert "[[file:activation-energy_1.org][one]]" in text
    assert "[[file:activation-energy_1_1.org][two]]" in text


def test_sync_failure_after_merge_says_files_are_in_place(project, incoming, monkeypatch):
    def broken_sync(*args, **kwargs):
        raise RuntimeError("sync broke")

    monkeypatch.setattr(cli_module, "_sync_wiki_to_db", broken_sync)
    result = CliRunner().invoke(cli, ["merge", str(incoming), "--yes"])
    assert result.exit_code != 0
    assert "Merged all files" in result.output
    assert "crucible sync" in result.output
    assert (project / ".crucible/sources/data/new-dir/extra.md").exists()


def test_merge_warns_about_leftover_staging_directory(project, incoming):
    (project / ".crucible/.merge-staging-old").mkdir()
    result = CliRunner().invoke(cli, ["merge", str(incoming), "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "interrupted merge" in result.output


def test_init_gitignores_merge_staging(project):
    assert ".crucible/.merge-staging-*/" in (project / ".gitignore").read_text()

