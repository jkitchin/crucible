"""Tests for crucible distill: agent commands, prompts and the run loop."""

import re
import shlex
import sys

import pytest
from click.testing import CliRunner

from crucible.cli import cli
from crucible.distill import PRESETS, agent_argv, describe_agent

# A stand-in agent: reads the prompt (argv[1], else stdin), records it,
# and writes a summary article citing the key the prompt names. MODE
# (argv[2] or "write") makes it misbehave instead.
FAKE_AGENT = r'''
import pathlib, re, sys
args = sys.argv[1:]
mode = "write"
if args and args[0] in ("write", "nothing", "fail"):
    mode = args.pop(0)
prompt = args[0] if args else sys.stdin.read()
pathlib.Path("agent-prompts.log").open("a").write(prompt + "\n=====\n")
if mode == "fail":
    sys.exit(3)
if mode == "nothing":
    sys.exit(0)
key = re.search(r"^Cite key: (\S+)$", prompt, re.M).group(1)
out = pathlib.Path(".crucible/wiki/summaries") / f"{key}.org"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(f"""#+TITLE: Summary of {key}
#+FILETAGS: :summary:

* Summary of {key}
:PROPERTIES:
:ARTICLE_TYPE: summary
:SOURCE_KEYS: {key}
:STATUS: draft
:CREATED: 2026-11-28
:MODIFIED: 2026-11-28
:END:

A summary citep:{key}.
""")
'''


@pytest.fixture
def project(tmp_path, registry, monkeypatch):
    root = tmp_path / "kb"
    root.mkdir()
    monkeypatch.chdir(root)
    monkeypatch.delenv("CRUCIBLE_AGENT", raising=False)
    r = CliRunner().invoke(cli, ["init", "--root", str(root)])
    assert r.exit_code == 0, r.output
    return root


@pytest.fixture
def fake(tmp_path):
    script = tmp_path / "fake_agent.py"
    script.write_text(FAKE_AGENT)
    base = f"{shlex.quote(sys.executable)} {shlex.quote(str(script))}"

    def agent(mode="write", stdin=False):
        return f"{base} {mode}" + ("" if stdin else " {prompt}")
    return agent


def ingest(project, name, text):
    f = project / name
    f.write_text(text)
    r = CliRunner().invoke(cli, ["ingest", str(f), "--type", "notebook"])
    assert r.exit_code == 0, r.output
    return re.search(r"Cite key: (\S+)", r.output).group(1)


def undigested(project):
    return CliRunner().invoke(cli, ["undigested"]).output


# -- agent commands -----------------------------------------------------------

def test_presets_pass_the_prompt_as_one_argument():
    prompt = "two words; $(rm -rf ~) `x`"
    argv, stdin = agent_argv("claude", prompt)
    assert argv[:3] == ["claude", "-p", prompt] and stdin is None
    assert "Bash(crucible *)" in argv and "Write" in argv
    for name in PRESETS:
        argv, stdin = agent_argv(name, prompt)
        assert argv.count(prompt) == 1 and stdin is None


def test_template_without_placeholder_reads_stdin():
    assert agent_argv("my-agent --fast", "hi") == (["my-agent", "--fast"], "hi")
    assert describe_agent("my-agent") == "my-agent < <prompt>"


def test_bad_templates():
    with pytest.raises(ValueError):
        agent_argv("", "x")
    with pytest.raises(ValueError):
        agent_argv("agent 'unclosed", "x")


# -- the command --------------------------------------------------------------

def test_distill_one_source(project, fake):
    key = ingest(project, "notes.md", "# Notes\n\nZeolite cracking results.\n")
    assert "notes" in undigested(project).lower()
    r = CliRunner().invoke(cli, ["distill", key, "--agent", fake()])
    assert r.exit_code == 0, r.output
    assert "Distilled into 1 article(s)." in r.output
    assert "Distilled 1 of 1 source(s)." in r.output
    assert undigested(project).strip() == "All sources have been distilled."
    prompt = (project / "agent-prompts.log").read_text()
    assert f"Cite key: {key}" in prompt
    assert "non-interactive" in prompt and "crucible sync" in prompt
    assert "SKILL.md" in prompt


def test_prompt_on_stdin(project, fake):
    key = ingest(project, "notes.md", "# Notes\n\nSomething.\n")
    r = CliRunner().invoke(cli, ["distill", key, "--agent", fake(stdin=True)])
    assert r.exit_code == 0, r.output
    assert f"Cite key: {key}" in (project / "agent-prompts.log").read_text()


def test_agent_from_environment(project, fake, monkeypatch):
    key = ingest(project, "notes.md", "# Notes\n\nSomething.\n")
    monkeypatch.setenv("CRUCIBLE_AGENT", fake())
    assert CliRunner().invoke(cli, ["distill", key]).exit_code == 0


def test_agent_that_writes_nothing_fails(project, fake):
    key = ingest(project, "notes.md", "# Notes\n\nSomething.\n")
    r = CliRunner().invoke(cli, ["distill", key, "--agent", fake("nothing")])
    assert r.exit_code == 1
    assert "no article cites it yet" in r.output


def test_agent_failure_is_reported(project, fake):
    key = ingest(project, "notes.md", "# Notes\n\nSomething.\n")
    r = CliRunner().invoke(cli, ["distill", key, "--agent", fake("fail")])
    assert r.exit_code == 1
    assert "agent exited with 3" in r.output


def test_missing_agent_command(project):
    key = ingest(project, "notes.md", "# Notes\n\nSomething.\n")
    r = CliRunner().invoke(cli, ["distill", key, "--agent",
                                 "no-such-agent-xyz {prompt}"])
    assert r.exit_code != 0
    assert "Agent command not found: no-such-agent-xyz" in r.output


def test_dry_run_runs_nothing(project, fake):
    key = ingest(project, "notes.md", "# Notes\n\nSomething.\n")
    r = CliRunner().invoke(cli, ["distill", key, "--dry-run", "--agent", fake()])
    assert r.exit_code == 0, r.output
    assert f"Cite key: {key}" in r.output
    assert not (project / "agent-prompts.log").exists()


def test_all_asks_before_several(project, fake):
    ingest(project, "a.md", "# A\n\nAlpha.\n")
    ingest(project, "b.md", "# B\n\nBeta.\n")
    r = CliRunner().invoke(cli, ["distill", "--all", "--agent", fake()], input="n\n")
    assert "Distill these 2 sources?" in r.output and "Cancelled." in r.output
    assert not (project / "agent-prompts.log").exists()
    r = CliRunner().invoke(cli, ["distill", "--all", "-y", "--agent", fake()])
    assert r.exit_code == 0, r.output
    assert "Distilled 2 of 2 source(s)." in r.output
    r = CliRunner().invoke(cli, ["distill", "--all", "--agent", fake()])
    assert "All sources have been distilled." in r.output


def test_needs_sources_or_all(project):
    r = CliRunner().invoke(cli, ["distill"])
    assert r.exit_code == 2
    assert "pass --all" in r.output


def test_unknown_source(project, fake):
    r = CliRunner().invoke(cli, ["distill", "nope", "--agent", fake()])
    assert r.exit_code == 1
    assert "Source not found: nope" in r.output


def test_ingest_then_distill(project, fake):
    f = project / "paper.md"
    f.write_text("# Paper\n\nNudged elastic band results.\n")
    r = CliRunner().invoke(cli, ["ingest", str(f), "--type", "notebook",
                                 "--distill", "--agent", fake()])
    assert r.exit_code == 0, r.output
    assert "Next: `crucible distill" not in r.output
    assert "Distilled 1 of 1 source(s)." in r.output
    assert undigested(project).strip() == "All sources have been distilled."


def test_ingest_suggests_distill(project):
    f = project / "paper.md"
    f.write_text("# Paper\n\nText.\n")
    r = CliRunner().invoke(cli, ["ingest", str(f), "--type", "notebook"])
    assert "Next: `crucible distill " in r.output
