"""Tests for shell tab completion: the completers and `crucible completion`."""

import pytest
from click.shell_completion import ShellComplete
from click.testing import CliRunner

from crucible.cli import cli
from crucible.completion import BASH_SCRIPT, RC_MARKER

ARTICLE = """#+TITLE: {title}
#+FILETAGS: :concept:{tag}:

* {title}
:PROPERTIES:
:ARTICLE_TYPE: concept
:STATUS: draft
:CREATED: 2026-01-01
:END:

Body.
"""


def complete(args, incomplete):
    comp = ShellComplete(cli, {}, "crucible", "_CRUCIBLE_COMPLETE")
    return [c.value for c in comp.get_completions(args, incomplete)]


@pytest.fixture
def project(tmp_path, registry, monkeypatch):
    root = tmp_path / "kb"
    root.mkdir()
    monkeypatch.chdir(root)
    assert CliRunner().invoke(cli, ["init", "--root", str(root)]).exit_code == 0
    wiki = root / ".crucible" / "wiki" / "concepts"
    (wiki / "alpha.org").write_text(ARTICLE.format(title="Alpha", tag="catalysis"))
    (wiki / "beta.org").write_text(ARTICLE.format(title="Beta", tag="kinetics"))
    note = root / "smith-notes.md"
    note.write_text("# Smith notes\n\nText.\n")
    r = CliRunner().invoke(cli, ["ingest", str(note), "--type", "notebook"])
    assert r.exit_code == 0, r.output
    assert CliRunner().invoke(cli, ["sync"]).exit_code == 0
    return root


def test_commands_and_options_complete():
    assert "distill" in complete([], "dis")
    assert complete(["search"], "--mo") == ["--mode", "--model"]


def test_source_keys(project):
    keys = complete(["distill"], "")
    assert keys and all(k.startswith("smith") for k in keys)
    assert complete(["source"], "smi") == keys
    assert complete(["distill"], "zzz") == []


def test_article_paths(project):
    assert complete(["related"], "concepts/") == ["concepts/alpha.org", "concepts/beta.org"]
    assert complete(["backlinks"], ".crucible/wiki/concepts/a") == [
        ".crucible/wiki/concepts/alpha.org"]


def test_concepts_registry_and_help(project):
    assert "catalysis" in complete(["concept"], "cat")
    assert "kb" in complete(["pull"], "k")
    assert complete(["help"], "or") == ["org-format"]


def test_outside_a_crucible_completes_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert complete(["distill"], "") == []
    assert complete(["related"], "") == []


def test_completion_never_writes(project):
    db = project / ".crucible" / "crucible.db"
    before = db.stat().st_mtime_ns
    complete(["distill"], "")
    complete(["related"], "")
    assert db.stat().st_mtime_ns == before


def test_bash_script_runs_on_bash_32():
    # compopt and `complete -o nosort` need bash 4; macOS has 3.2.
    assert "compopt" not in BASH_SCRIPT and "nosort" not in BASH_SCRIPT
    assert "_CRUCIBLE_COMPLETE=bash_complete" in BASH_SCRIPT


@pytest.mark.parametrize("shell", ["bash", "zsh", "fish"])
def test_prints_script(shell):
    r = CliRunner().invoke(cli, ["completion", shell])
    assert r.exit_code == 0
    assert "_CRUCIBLE_COMPLETE" in r.output


def test_shell_from_environment(monkeypatch):
    monkeypatch.setenv("SHELL", "/usr/bin/zsh")
    assert "#compdef crucible" in CliRunner().invoke(cli, ["completion"]).output
    monkeypatch.setenv("SHELL", "/bin/tcsh")
    r = CliRunner().invoke(cli, ["completion"])
    assert r.exit_code == 2 and "name it" in r.output


def test_install_bash_is_idempotent(tmp_path, monkeypatch):
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    (home / ".bashrc").write_text("export X=1")  # no trailing newline
    for _ in range(2):
        r = CliRunner().invoke(cli, ["completion", "bash", "--install"])
        assert r.exit_code == 0, r.output
    rc = (home / ".bashrc").read_text()
    assert rc.startswith("export X=1\n")
    assert rc.count(RC_MARKER) == 1
    assert ". ~/.crucible/completion.bash" in rc
    assert (home / ".crucible" / "completion.bash").read_text() == BASH_SCRIPT


def test_install_bash_uses_profile_that_skips_bashrc(tmp_path, monkeypatch):
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    home.mkdir(parents=True, exist_ok=True)
    (home / ".bash_profile").write_text("export PATH=/x:$PATH\n")
    assert CliRunner().invoke(cli, ["completion", "bash", "--install"]).exit_code == 0
    assert RC_MARKER in (home / ".bash_profile").read_text()
    assert not (home / ".bashrc").exists()


def test_install_fish(tmp_path, monkeypatch):
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    assert CliRunner().invoke(cli, ["completion", "fish", "--install"]).exit_code == 0
    assert (home / ".config" / "fish" / "completions" / "crucible.fish").exists()
