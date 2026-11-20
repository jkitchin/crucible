"""Sealed sources and `crucible record`."""

import json

from click.testing import CliRunner

from crucible.cli import cli
from crucible.ingest import file_sha256, parse_bib_file

from test_sources import cdir, ingest, invoke, project  # noqa: F401


def bib(root):
    return {e["cite_key"]: e for e in parse_bib_file(cdir(root) / "references.bib")}


def lint_issues(kind):
    result = CliRunner().invoke(cli, ["lint", "--json"])
    return [i for i in json.loads(result.stdout) if i["type"] == kind], result.exit_code


def test_ingest_seals_the_stored_file(project, tmp_path):  # noqa: F811
    key = ingest(project, tmp_path, "notes.org", "#+TITLE: Notes\n\nA result.\n")
    entry = bib(project)[key]
    assert entry["sha256"] == file_sha256(project / entry["file"])
    assert lint_issues("modified_source") == ([], 0)


def test_lint_reports_an_edited_source(project, tmp_path):  # noqa: F811
    key = ingest(project, tmp_path, "notes.org", "#+TITLE: Notes\n\nA result.\n")
    stored = project / bib(project)[key]["file"]
    stored.write_text("#+TITLE: Notes\n\nA different result.\n")
    issues, code = lint_issues("modified_source")
    assert [i["key"] for i in issues] == [key]
    assert issues[0]["severity"] == "error"
    assert code == 1


def test_sync_keeps_the_seal(project, tmp_path):  # noqa: F811
    key = ingest(project, tmp_path, "notes.org", "#+TITLE: Notes\n\nA result.\n")
    sealed = bib(project)[key]["sha256"]
    invoke("sync")
    assert bib(project)[key]["sha256"] == sealed


def test_record_from_stdin(project):  # noqa: F811
    result = CliRunner().invoke(
        cli, ["record", "-t", "Rate onset below 400 K", "-a", "claude-opus-5-5", "--json"],
        input="The rate doubled at 350 K.\n",
    )
    assert result.exit_code == 0, result.output
    out = json.loads(result.stdout)
    assert out["path"].startswith(".crucible/sources/notebooks/")
    text = (project / out["path"]).read_text()
    assert text.startswith("#+TITLE: Rate onset below 400 K\n#+AUTHOR: claude-opus-5-5\n#+DATE: ")
    assert "The rate doubled at 350 K." in text
    entry = bib(project)[out["cite_key"]]
    assert entry["sha256"] == file_sha256(project / out["path"])
    assert "claude-opus-5-5" in entry["author"]
    undigested = json.loads(invoke("undigested", "--json").stdout)
    assert out["path"] in {s["path"] for s in undigested}


def test_record_keeps_the_contents_own_keywords(project, tmp_path):  # noqa: F811
    src = tmp_path / "idea.org"
    src.write_text("#+TITLE: Own title\n#+AUTHOR: Jane\n\n* Hypothesis\nX.\n")
    out = json.loads(invoke("record", str(src), "--json").stdout)
    text = (project / out["path"]).read_text()
    assert text.count("#+TITLE:") == 1 and text.count("#+AUTHOR:") == 1
    assert "#+DATE: " in text
    assert out["title"] == "Own title"


def test_record_needs_title_and_author(project):  # noqa: F811
    r = CliRunner().invoke(cli, ["record", "-a", "me"], input="text\n")
    assert r.exit_code != 0 and "title" in r.output
    r = CliRunner().invoke(cli, ["record", "-t", "T"], input="text\n")
    assert r.exit_code != 0 and "author" in r.output


def test_record_amends(project):  # noqa: F811
    first = json.loads(CliRunner().invoke(
        cli, ["record", "-t", "First", "-a", "me", "--json"], input="Claim.\n").stdout)
    r = CliRunner().invoke(
        cli, ["record", "-t", "Correction", "-a", "me", "--amends", first["cite_key"],
              "--json"], input="The claim was wrong.\n")
    assert r.exit_code == 0, r.output
    second = json.loads(r.stdout)
    assert bib(project)[second["cite_key"]]["amends"] == first["cite_key"]
    assert f"#+AMENDS: {first['cite_key']}" in (project / second["path"]).read_text()
    r = CliRunner().invoke(cli, ["record", "-t", "X", "-a", "me", "--amends", "nope"],
                           input="y\n")
    assert r.exit_code != 0 and "nope" in r.output


def test_seal_backfills_unsealed_entries(project, tmp_path):  # noqa: F811
    key = ingest(project, tmp_path, "notes.org", "#+TITLE: Notes\n\nA result.\n")
    bib_path = cdir(project) / "references.bib"
    text = bib_path.read_text()
    sha = bib(project)[key]["sha256"]
    bib_path.write_text("\n".join(l for l in text.splitlines() if "sha256" not in l) + "\n")
    assert "sha256" not in bib(project)[key]

    out = json.loads(invoke("seal", "--json").stdout)
    assert out["sealed"] == [key]
    assert bib(project)[key]["sha256"] == sha
    # Never resealed: an edited file stays reported.
    (project / bib(project)[key]["file"]).write_text("changed\n")
    out = json.loads(invoke("seal", key, "--json").stdout)
    assert out["sealed"] == [] and out["skipped"][0]["reason"] == "already sealed"
    assert lint_issues("modified_source")[0]


def test_record_key_names_the_record_not_the_author(project, monkeypatch):  # noqa: F811
    monkeypatch.delenv("AI_AGENT", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    keys = []
    for _ in range(2):
        r = CliRunner().invoke(
            cli, ["record", "-t", "The rate onset at 350 K", "-a", "claude-opus-5-5",
                  "--json"], input=f"Run {len(keys)}.\n")
        assert r.exit_code == 0, r.output
        keys.append(json.loads(r.stdout)["cite_key"])
    day = keys[0][:8]
    assert day.isdigit()
    assert keys == [f"{day}-rate-onset-350", f"{day}-rate-onset-350a"]
    text = (project / bib(project)[keys[0]]["file"]).read_text()
    assert "#+RECORDED_WITH:" not in text


def test_record_captures_the_agent_session(project, monkeypatch):  # noqa: F811
    monkeypatch.setenv("AI_AGENT", "claude-code_2-1-329_agent")
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "b34b5691")
    out = json.loads(CliRunner().invoke(
        cli, ["record", "-t", "T", "-a", "claude-opus-5-5", "--json"],
        input="Body.\n").stdout)
    text = (project / out["path"]).read_text()
    assert "#+AUTHOR: claude-opus-5-5\n" in text
    assert "#+RECORDED_WITH: claude-code_2-1-329_agent, session b34b5691\n" in text
