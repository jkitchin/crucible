"""Round-3 review fixes: init/update/install/uninstall, path arguments, help."""

import subprocess
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from crucible import cli as cli_module
from crucible.cli import _skill_source, cli

from test_cli_json import (  # noqa: F401
    CONCEPT_PATH,
    SUMMARY_PATH,
    project,
    run_json,
)


def invoke(*args, input=None):
    return CliRunner().invoke(cli, list(args), input=input)


def make_root(tmp_path, monkeypatch, name="kb"):
    root = tmp_path / name
    root.mkdir()
    monkeypatch.chdir(root)
    return root


# -- 1. an existing .gitignore gets crucible's entries ---------------------


def test_init_appends_missing_entries_to_an_existing_gitignore(tmp_path, registry, monkeypatch):
    root = make_root(tmp_path, monkeypatch)
    (root / ".gitignore").write_text("node_modules/\n.crucible/crucible.db")
    result = invoke("init", "--root", str(root))
    assert result.exit_code == 0, result.output
    text = (root / ".gitignore").read_text()
    lines = text.splitlines()
    assert lines[0] == "node_modules/"
    for pattern in (".crucible/sources/external/", ".crucible/crucible.db-wal",
                    ".crucible/references.bib.lock", ".crucible/.merge-staging-*/"):
        assert pattern in lines
    assert lines.count(".crucible/crucible.db") == 1  # already there: not repeated
    assert cli_module._GITIGNORE_MARKER in lines

    # Idempotent, in init and in update.
    assert invoke("init", "--root", str(root)).exit_code == 0
    result = invoke("update")
    assert result.exit_code == 0, result.output
    assert "Gitignore: up to date" in result.output
    assert (root / ".gitignore").read_text() == text


def test_update_adds_entries_missing_from_gitignore(tmp_path, registry, monkeypatch):
    root = make_root(tmp_path, monkeypatch)
    assert invoke("init", "--root", str(root)).exit_code == 0
    (root / ".gitignore").write_text("# mine\n")
    result = invoke("update")
    assert result.exit_code == 0, result.output
    assert ".crucible/sources/external/" in (root / ".gitignore").read_text().splitlines()


def test_gitignore_actually_ignores_external_sources(tmp_path, registry, monkeypatch):
    if subprocess.run(["git", "--version"], capture_output=True).returncode != 0:
        pytest.skip("git not available")
    root = make_root(tmp_path, monkeypatch)
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    (root / ".gitignore").write_text("*.pyc\n")
    assert invoke("init", "--root", str(root)).exit_code == 0
    paper = root / ".crucible/sources/external/pdfs/paper.pdf"
    paper.write_bytes(b"%PDF")
    check = subprocess.run(["git", "check-ignore", "-q", str(paper)], cwd=root)
    assert check.returncode == 0


# -- 2. uninstall without a record checks hashes ---------------------------


def test_uninstall_without_record_keeps_an_edited_skill(tmp_path, registry, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert invoke("install").exit_code == 0
    dest = Path.home() / ".claude/skills/crucible"
    (dest / ".crucible-installed.json").unlink()
    (dest / "SKILL.md").write_text("my own notes\n")
    result = invoke("uninstall")
    assert result.exit_code == 0, result.output
    assert (dest / "SKILL.md").read_text() == "my own notes\n"
    assert "Kept" in result.output


def test_uninstall_without_record_removes_an_unedited_copy(tmp_path, registry, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert invoke("install").exit_code == 0
    dest = Path.home() / ".claude/skills/crucible"
    (dest / ".crucible-installed.json").unlink()
    result = invoke("uninstall")
    assert result.exit_code == 0, result.output
    assert not dest.exists()


# -- 3. symlinks in the skill directory ------------------------------------


def test_init_with_skill_dir_symlinked_to_the_source(tmp_path, registry, monkeypatch):
    skills = Path.home() / ".claude/skills"
    skills.mkdir(parents=True)
    (skills / "crucible").symlink_to(_skill_source())
    before = {p.name for p in _skill_source().iterdir()}
    root = make_root(tmp_path, monkeypatch)
    result = invoke("init", "--root", str(root))
    assert result.exit_code == 0, result.output
    assert "already links to" in result.output
    # Nothing written into the skill source through the link.
    assert {p.name for p in _skill_source().iterdir()} == before


def test_install_does_not_write_through_a_symlinked_skill_file(tmp_path, registry, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dest = Path.home() / ".claude/skills/crucible"
    dest.mkdir(parents=True)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    target = elsewhere / "notes.md"
    target.write_text("precious\n")
    (dest / "SKILL.md").symlink_to(target)
    result = invoke("install")
    assert result.exit_code == 0, result.output
    assert target.read_text() == "precious\n"
    assert (dest / "SKILL.md").is_symlink()
    assert "symlink" in result.output
    assert not (elsewhere / ".crucible-installed.json").exists()
    # Uninstall leaves the link alone too.
    assert invoke("uninstall").exit_code == 0
    assert target.read_text() == "precious\n"


def test_install_with_skill_file_symlinked_to_the_source(tmp_path, registry, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dest = Path.home() / ".claude/skills/crucible"
    dest.mkdir(parents=True)
    (dest / "SKILL.md").symlink_to(_skill_source() / "SKILL.md")
    result = invoke("install")
    assert result.exit_code == 0, result.output
    assert "already links to" in result.output


def test_install_leaves_a_skill_dir_linked_elsewhere(tmp_path, registry, monkeypatch):
    monkeypatch.chdir(tmp_path)
    other = tmp_path / "other-skill"
    other.mkdir()
    skills = Path.home() / ".claude/skills"
    skills.mkdir(parents=True)
    (skills / "crucible").symlink_to(other)
    result = invoke("install")
    assert result.exit_code == 0, result.output
    assert list(other.iterdir()) == []
    assert invoke("uninstall").exit_code == 0
    assert (skills / "crucible").is_symlink()


# -- 4. unpaired directive markers -----------------------------------------


MARKER = "<!-- crucible-knowledge-base -->"


def test_install_and_uninstall_warn_on_an_unpaired_marker(project, registry):  # noqa: F811
    claude_md = Path.home() / ".claude/CLAUDE.md"
    claude_md.parent.mkdir(parents=True, exist_ok=True)
    original = f"# mine\n{MARKER}\nhalf a block\n\nkeep this\n"
    claude_md.write_text(original)
    result = invoke("install")
    assert result.exit_code == 0, result.output
    assert "unpaired" in result.output
    assert "updated existing directive" not in result.output
    assert claude_md.read_text() == original
    result = invoke("uninstall")
    assert "unpaired" in result.output
    assert "Removed directive" not in result.output
    assert claude_md.read_text() == original


# -- 5. path arguments -----------------------------------------------------


def test_wiki_prefixed_paths_resolve(project):  # noqa: F811
    # The form the tutorial uses: relative to .crucible/.
    assert run_json("backlinks", "wiki/concepts/activation-energy.org")
    assert run_json("sources", "wiki/summaries/smith.org") is not None
    result = invoke("history", "wiki/summaries/smith.org")
    assert result.exit_code == 0, result.output


def _add_source(root):
    data = root / ".crucible/sources/data/results.csv"
    data.write_text("a,b\n1,2\n")
    assert invoke("sync").exit_code == 0
    return data


def test_source_accepts_path_forms(project, monkeypatch):  # noqa: F811
    data = _add_source(project)
    rel = ".crucible/sources/data/results.csv"
    for arg in (rel, "./" + rel, str(data), "sources/data/results.csv"):
        assert run_json("source", arg)["path"] == rel, arg
    monkeypatch.chdir(data.parent)
    assert run_json("source", "results.csv")["path"] == rel
    assert run_json("source", "./results.csv")["path"] == rel


def test_source_rejects_an_empty_path(project):  # noqa: F811
    _add_source(project)
    for arg in ("", "  "):
        result = invoke("source", arg)
        assert result.exit_code == 1
        assert "empty" in result.output


def test_backlinks_rejects_an_empty_path(project):  # noqa: F811
    result = invoke("backlinks", "")
    assert result.exit_code == 1


def test_delete_accepts_a_cwd_relative_path(project, monkeypatch):  # noqa: F811
    monkeypatch.chdir(project / ".crucible/wiki/concepts")
    result = invoke("delete", "./activation-energy.org", "-y")
    assert result.exit_code == 0, result.output
    assert "Activation energy" in result.output
    assert run_json("stats")["articles"] == 1


def test_delete_rejects_an_empty_path(project):  # noqa: F811
    result = invoke("delete", "", "-y")
    assert result.exit_code == 1
    assert run_json("stats")["articles"] == 2  # nothing deleted


# -- 6. concept matches like _concept_key ----------------------------------


def test_concept_ignores_case_and_punctuation(project):  # noqa: F811
    wiki = project / ".crucible/wiki/concepts"
    (wiki / "ea.org").write_text(
        "#+TITLE: Ea\n#+FILETAGS: :concept:activation_energy:\n\n"
        "* Ea\n:PROPERTIES:\n:ARTICLE_TYPE: concept\n:STATUS: draft\n"
        ":CREATED: 2026-01-01\n:MODIFIED: 2026-01-01\n:END:\nText.\n"
    )
    assert invoke("sync").exit_code == 0
    for name in ("activation energy", "Activation-Energy", "activation_energy"):
        paths = [r["path"] for r in run_json("concept", name)]
        assert ".crucible/wiki/concepts/ea.org" in paths, name


# -- 7. help covers every command ------------------------------------------


def _all_commands(group, prefix=""):
    for name, cmd in group.commands.items():
        yield prefix + name, cmd
        if isinstance(cmd, click.Group):
            yield from _all_commands(cmd, prefix + name + " ")


def test_help_all_mentions_every_command_and_option():
    result = invoke("help", "all")
    assert result.exit_code == 0
    text = result.output
    for name, cmd in _all_commands(cli):
        assert f"crucible {name} " in text or f"crucible {name}\n" in text, name
        for param in cmd.params:
            if isinstance(param, click.Option):
                for opt in param.opts + param.secondary_opts:
                    assert opt in text, (name, opt)
    assert "--version" in text
    assert ":DERIVED_FROM:" in invoke("help", "org-format").output


def test_skill_md_mentions_history_install_update_viz_browse():
    text = (_skill_source() / "SKILL.md").read_text()
    for command in ("history", "install", "update", "viz", "browse"):
        assert f"crucible {command}" in text, command


# -- 8. init creates an empty references.bib -------------------------------


def test_init_creates_an_empty_bib_that_works(tmp_path, registry, monkeypatch):
    root = make_root(tmp_path, monkeypatch)
    assert invoke("init", "--root", str(root)).exit_code == 0
    bib = root / ".crucible/references.bib"
    assert bib.is_file() and bib.read_text() == ""
    assert invoke("sync").exit_code == 0
    assert invoke("lint").exit_code == 0
    paper = tmp_path / "notes.org"
    paper.write_text("#+TITLE: Notes\nSome notes.\n")
    result = invoke("ingest", str(paper))
    assert result.exit_code == 0, result.output
    assert "@" in bib.read_text()


# -- 9. --version without distribution metadata ----------------------------


def test_version_option_does_not_need_distribution_metadata(tmp_path):
    # In a subprocess, so the reimported CLI module does not leak into other
    # tests. The metadata lookup is made to fail as for an uninstalled tree.
    import sys

    code = (
        "import importlib.metadata as m\n"
        "def missing(name):\n"
        "    raise m.PackageNotFoundError(name)\n"
        "m.version = missing\n"
        "from crucible.cli import cli\n"
        "cli(['--version'])\n"
    )
    src = Path(cli_module.__file__).parent.parent
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                            cwd=tmp_path, env={"PYTHONPATH": str(src), "HOME": str(tmp_path),
                                               "PATH": "/usr/bin:/bin"})
    assert result.returncode == 0, result.stderr
    assert "dev" in result.stdout


# -- 10. init/update report the registry once ------------------------------


def test_init_and_update_report_registration_once(tmp_path, registry, monkeypatch):
    root = make_root(tmp_path, monkeypatch)
    result = invoke("init", "--root", str(root))
    assert result.exit_code == 0, result.output
    assert result.output.count("registered") + result.output.count("Registered") == 1
    result = invoke("update")
    assert result.exit_code == 0, result.output
    assert result.output.count("Registry:") == 1


# -- 11. publish timeout ---------------------------------------------------


def test_publish_timeout_is_a_clean_error(project, monkeypatch):  # noqa: F811
    def slow(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs.get("timeout", 300))

    monkeypatch.setattr(subprocess, "run", slow)
    result = invoke("publish")
    assert result.exit_code == 1
    assert "300 seconds" in result.output
    assert not isinstance(result.exception, subprocess.TimeoutExpired)


# -- 12. vscode with a missing FILE ----------------------------------------


def test_vscode_with_a_missing_file_errors(project, monkeypatch):  # noqa: F811
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: calls.append(cmd))
    result = invoke("vscode", "concepts/no-such-article.org")
    assert result.exit_code == 1
    assert "No such file" in result.output
    assert calls == []
    result = invoke("vscode", "concepts/activation-energy.org")
    assert result.exit_code == 0, result.output
    assert calls and calls[-1][-1].endswith("activation-energy.org")
