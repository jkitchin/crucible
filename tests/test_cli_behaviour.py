"""CLI behaviour: in-process helpers, exit codes, path arguments, install."""

import json
import re
from pathlib import Path

from click.testing import CliRunner

from crucible import package_version
from crucible.cli import _skill_source, cli

from test_cli_json import (  # noqa: F401
    CONCEPT_PATH,
    SUMMARY_PATH,
    project,
    run_json,
)


def invoke(*args):
    return CliRunner().invoke(cli, list(args))


def no_crucible_on_path(monkeypatch, tmp_path):
    """Leave PATH with no `crucible` (or anything else) on it."""
    empty = tmp_path / "empty-bin"
    empty.mkdir(exist_ok=True)
    monkeypatch.setenv("PATH", str(empty))


# -- 1. init/install/vscode/publish work without `crucible` on PATH --------


def test_init_writes_manifest_without_crucible_on_path(tmp_path, registry, monkeypatch):
    no_crucible_on_path(monkeypatch, tmp_path)
    root = tmp_path / "kb"
    root.mkdir()
    monkeypatch.chdir(root)
    result = invoke("init", "--root", str(root))
    assert result.exit_code == 0, result.output
    assert (root / ".crucible/wiki/MANIFEST.md").is_file()


def test_install_regenerates_manifest_in_process(project, tmp_path, monkeypatch):  # noqa: F811
    no_crucible_on_path(monkeypatch, tmp_path)
    manifest = project / ".crucible/wiki/MANIFEST.md"
    manifest.unlink()
    result = invoke("install")
    assert result.exit_code == 0, result.output
    assert manifest.is_file()


def test_vscode_writes_viz_in_process(project, tmp_path, monkeypatch):  # noqa: F811
    no_crucible_on_path(monkeypatch, tmp_path)
    viz = project / ".crucible/wiki/viz.html"
    assert not viz.exists()
    result = invoke("vscode")
    # `code` is not on PATH either, but the graph is written first.
    assert result.exit_code == 1
    assert "'code' command not found" in result.output
    assert "Activation energy" in viz.read_text()


def test_publish_writes_index_in_process(project, tmp_path, monkeypatch):  # noqa: F811
    no_crucible_on_path(monkeypatch, tmp_path)
    index = project / ".crucible/wiki/index.org"
    assert not index.exists()
    result = invoke("publish", "--init-only")
    assert result.exit_code == 0, result.output
    assert "activation-energy.org" in index.read_text()
    result = invoke("publish")
    assert result.exit_code == 1
    assert "'scimax' command not found" in result.output


# -- 2. a backslash in the project path ------------------------------------


def test_reinstall_with_backslash_in_project_path(tmp_path, registry, monkeypatch):
    root = tmp_path / "we\\Uird"
    root.mkdir()
    monkeypatch.chdir(root)
    assert invoke("init", "--root", str(root)).exit_code == 0
    for _ in range(2):
        result = invoke("install")
        assert result.exit_code == 0, result.output
    claude_md = (registry.parent / ".claude/CLAUDE.md").read_text()
    assert f"`{root.resolve()}`" in claude_md
    assert claude_md.count("<!-- crucible-knowledge-base -->") == 2


# -- 3. path arguments -----------------------------------------------------


def test_path_arguments_resolve_from_cwd(project, monkeypatch):  # noqa: F811
    expected = [SUMMARY_PATH]
    for arg in (CONCEPT_PATH, f"./{CONCEPT_PATH}",
                str(project / CONCEPT_PATH), "concepts/activation-energy.org"):
        assert [r["path"] for r in run_json("backlinks", arg)] == expected, arg
    monkeypatch.chdir(project / ".crucible/wiki/concepts")
    assert [r["path"] for r in run_json("backlinks", "activation-energy.org")] == expected
    assert [r["path"] for r in run_json("related", "./activation-energy.org")] == expected
    history = run_json("history", "../summaries/smith.org")
    assert [r["path"] for r in history["derived_from"]] == [CONCEPT_PATH]
    assert run_json("sources", "activation-energy.org") == []


def test_unknown_article_path_is_an_error(project):  # noqa: F811
    for command in ("backlinks", "sources", "related", "history"):
        result = invoke(command, "nosuch.org", "--json")
        assert result.exit_code == 1, command
        assert "No article at nosuch.org" in result.output
        assert result.stdout == ""


# -- 4. concept names ------------------------------------------------------


def test_concept_is_case_insensitive(project):  # noqa: F811
    assert {r["path"] for r in run_json("concept", "KINETICS")} == {CONCEPT_PATH, SUMMARY_PATH}


# -- 5. version --------------------------------------------------------------


def test_version_comes_from_the_distribution():
    from importlib.metadata import version
    assert package_version() == version("crucible-llm")
    result = invoke("--version")
    assert result.exit_code == 0
    assert package_version() in result.output


def test_about_reports_the_package_version(project):  # noqa: F811
    assert run_json("about")["version"] == package_version()


# -- 6. exit codes -----------------------------------------------------------


def test_help_unknown_topic_fails():
    result = invoke("help", "nosuch")
    assert result.exit_code == 1
    assert "Unknown topic: nosuch" in result.output
    assert invoke("help", "sync").exit_code == 0


def test_source_not_found_fails(project):  # noqa: F811
    result = invoke("source", "nosuch", "--json")
    assert result.exit_code == 1
    assert result.stdout == ""
    assert "Source not found: nosuch" in result.output


def test_lint_exit_codes(project):  # noqa: F811
    # The fixture has warnings (a broken link) but no errors.
    assert invoke("lint").exit_code == 0
    assert invoke("lint", "--strict").exit_code == 1
    (project / CONCEPT_PATH).unlink()  # missing_file is an error
    result = invoke("lint", "--json")
    assert result.exit_code == 1
    assert any(i["severity"] == "error" for i in json.loads(result.stdout))
    assert invoke("lint").exit_code == 1


# -- 7. install keeps local skill edits; uninstall removes only its files ----


def test_install_backs_up_edited_skill_and_uninstall_spares_user_files(
        tmp_path, registry, monkeypatch):
    monkeypatch.chdir(tmp_path)
    skill_dir = registry.parent / ".claude/skills/crucible"
    shipped = (_skill_source() / "SKILL.md").read_text()

    assert invoke("install").exit_code == 0
    assert (skill_dir / "SKILL.md").read_text() == shipped
    # Reinstalling an unedited copy makes no backup.
    assert invoke("install").exit_code == 0
    assert not (skill_dir / "SKILL.md.bak").exists()

    (skill_dir / "SKILL.md").write_text("my notes")
    (skill_dir / "extra.md").write_text("mine")
    result = invoke("install")
    assert result.exit_code == 0, result.output
    assert "local changes" in result.output
    assert (skill_dir / "SKILL.md.bak").read_text() == "my notes"
    assert (skill_dir / "SKILL.md").read_text() == shipped

    result = invoke("uninstall")
    assert result.exit_code == 0, result.output
    assert not (skill_dir / "SKILL.md").exists()
    assert sorted(p.name for p in skill_dir.iterdir()) == ["SKILL.md.bak", "extra.md"]


def test_uninstall_keeps_an_edited_skill_and_removes_an_empty_dir(
        tmp_path, registry, monkeypatch):
    monkeypatch.chdir(tmp_path)
    skill_dir = registry.parent / ".claude/skills/crucible"
    assert invoke("install").exit_code == 0
    (skill_dir / "SKILL.md").write_text("edited after install")
    assert invoke("uninstall").exit_code == 0
    assert (skill_dir / "SKILL.md").read_text() == "edited after install"

    assert invoke("install").exit_code == 0  # backs the edit up, then installs
    (skill_dir / "SKILL.md.bak").unlink()
    assert invoke("uninstall").exit_code == 0
    assert not skill_dir.exists()


# -- 8. missing concept articles -------------------------------------------


MULTI = """#+TITLE: Catalysis
#+FILETAGS: :concept:catalysis:kinetics:adsorption:

* Catalysis
:PROPERTIES:
:ARTICLE_TYPE: concept
:SOURCE_KEYS:
:STATUS: draft
:CREATED: 2026-01-03
:END:

Catalysis, see [[file:activation-energy.org][activation energy]].
"""


def missing_concepts():
    return {re.search(r"'(.*)'", i["message"]).group(1) for i in run_json("lint")
            if i["type"] == "missing_concept_article"}


def test_missing_concept_article_needs_a_matching_slug_or_title(project):  # noqa: F811
    # One concept article tagged with three concepts covers only its own.
    (project / ".crucible/wiki/concepts/catalysis.org").write_text(MULTI)
    assert invoke("sync").exit_code == 0
    missing = missing_concepts()
    assert {"kinetics", "adsorption"} <= missing
    assert "catalysis" not in missing


def test_concept_slug_matches_underscored_tag(project):  # noqa: F811
    path = Path(".crucible/wiki/concepts/activation-energy.org")
    text = (project / path).read_text().replace(
        ":concept:kinetics:", ":concept:activation_energy:")
    (project / path).write_text(text)
    assert invoke("sync").exit_code == 0
    assert "activation_energy" not in missing_concepts()
