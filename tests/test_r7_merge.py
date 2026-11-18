"""Round-7 merge and uninstall fixes."""

import errno
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from crucible.cli import cli
from crucible.database import CrucibleDB
from test_merge import make_crucible, project  # noqa: F401

SUMMARY = """#+TITLE: {title}

* {title}
:PROPERTIES:
:ARTICLE_TYPE: summary
:SOURCE_KEYS: {key}
:STATUS: draft
:CREATED: 2026-01-01
:END:

body
"""

CONCEPT = """#+TITLE: {title}
#+FILETAGS: :concept:

* {title}
:PROPERTIES:
:ARTICLE_TYPE: concept
:SOURCE_KEYS:
:STATUS: draft
:CREATED: 2026-01-01
:END:

{text} See [[file:other.org][other]].
"""


def invoke(*args):
    return CliRunner().invoke(cli, list(args))


def synced(root, monkeypatch):
    monkeypatch.chdir(root)
    result = invoke("sync")
    assert result.exit_code == 0, result.output


def article_sources(root):
    db = CrucibleDB(root / ".crucible/crucible.db")
    try:
        return sorted(tuple(row.values()) for row in db.conn.execute(
            "select a.path as article, s.path as source from articles a "
            "join article_sources x on x.article_id = a.id "
            "join sources s on s.id = x.source_id"
        ).fetchall())
    finally:
        db.close()


def move_external(root, elsewhere):
    """Keep root's sources/external/ in elsewhere, linked from the crucible."""
    external = root / ".crucible/sources/external"
    external.rename(elsewhere)
    try:
        external.symlink_to(elsewhere, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")


def ingest_pdf(root, tmp_path, monkeypatch, *extra):
    pdf = tmp_path / "paper.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake paper")
    monkeypatch.chdir(root)
    result = invoke("ingest", str(pdf), "--type", "pdf", "--title", "A Paper",
                    "--date", "2026-01-01", *extra)
    assert result.exit_code == 0, result.output
    synced(root, monkeypatch)


# -- 1. symlinked sources/external/ ----------------------------------------


@pytest.mark.parametrize("side", ["target", "source"])
def test_merge_accepts_a_symlinked_external_directory(
    project, tmp_path, monkeypatch, side  # noqa: F811
):
    source = make_crucible(tmp_path / "source")
    drive = tmp_path / "drive"
    drive.mkdir()
    if side == "source":
        move_external(source, drive / "external")
    ingest_pdf(source, tmp_path, monkeypatch, "--no-shareable")
    if side == "target":
        move_external(project, drive / "external")

    # The external directory is on another filesystem: renames fail there.
    real_rename = os.rename

    def rename(src, dst):
        if side == "target" and str(drive) in os.path.realpath(os.path.dirname(dst)):
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        return real_rename(src, dst)

    monkeypatch.setattr(os, "rename", rename)
    monkeypatch.chdir(project)
    result = invoke("merge", str(source), "--yes")
    if side == "source":
        # A clone keeps a committed symlink, so a shared crucible's
        # sources/external link could pull any file into the target.
        assert result.exit_code != 0
        assert "resolves outside the source crucible" in result.output
        assert not (project / ".crucible/sources/external/pdfs/paper.pdf").exists()
        return
    assert result.exit_code == 0, result.output
    landed = project / ".crucible/sources/external/pdfs/paper.pdf"
    assert landed.read_bytes() == b"%PDF-1.4 fake paper"
    if side == "target":
        assert (drive / "external/pdfs/paper.pdf").exists()
    assert "file = {.crucible/sources/external/pdfs/paper.pdf}" in (
        project / ".crucible/references.bib").read_text()


# -- 2 and 3. keys the target's disk scan gives ------------------------------


def test_merge_refuses_a_bib_only_entry_taking_a_target_data_key(
    project, tmp_path, monkeypatch  # noqa: F811
):
    data = project / ".crucible/sources/data/kinetics.csv"
    data.write_text("t,k\n")
    mine = project / ".crucible/wiki/summaries/mine.org"
    mine.write_text(SUMMARY.format(title="My data", key="kinetics"))
    synced(project, monkeypatch)

    source = make_crucible(tmp_path / "source")
    (source / ".crucible/references.bib").write_text(
        "@book{kinetics,\n  title = {Chemical Kinetics},\n  year = {1987},\n}\n")
    (source / ".crucible/wiki/summaries/book.org").write_text(
        SUMMARY.format(title="Book", key="kinetics"))
    synced(source, monkeypatch)

    monkeypatch.chdir(project)
    result = invoke("merge", str(source), "--yes")
    assert result.exit_code != 0
    assert "Cannot safely merge bibliography entry 'kinetics'" in result.output
    assert "kinetics" not in (project / ".crucible/references.bib").read_text()
    assert article_sources(project) == [
        (".crucible/wiki/summaries/mine.org", ".crucible/sources/data/kinetics.csv")
    ]


def test_merge_sees_unsynced_target_notebooks(project, tmp_path, monkeypatch):  # noqa: F811
    source = make_crucible(tmp_path / "source")
    (source / ".crucible/sources/notebooks/2026-11-01.org").write_text("colleague\n")
    (source / ".crucible/wiki/summaries/col.org").write_text(
        SUMMARY.format(title="Colleague", key="2026-11-01"))
    synced(source, monkeypatch)

    # Added to the target after its last sync.
    (project / ".crucible/sources/notebooks/2026-11-01.org").write_text("mine\n")
    (project / ".crucible/wiki/summaries/mine.org").write_text(
        SUMMARY.format(title="Mine", key="2026-11-01"))
    monkeypatch.chdir(project)
    for option in ("--dry-run", "--yes"):
        result = invoke("merge", str(source), option)
        assert result.exit_code != 0
        assert "citation key '2026-11-01'" in result.output
    assert not (project / ".crucible/sources/notebooks/2026-11-01_1.org").exists()
    synced(project, monkeypatch)
    assert article_sources(project) == [
        (".crucible/wiki/summaries/mine.org", ".crucible/sources/notebooks/2026-11-01.org")
    ]


# -- 4. round trips -----------------------------------------------------------


def test_round_trip_merges_do_not_grow_the_wiki(tmp_path, registry, monkeypatch):
    crucibles = {}
    for name in ("A", "B"):
        root = make_crucible(tmp_path / name)
        concepts = root / ".crucible/wiki/concepts"
        (concepts / "foo.org").write_text(CONCEPT.format(title=f"Foo {name}", text=name))
        (concepts / "other.org").write_text(CONCEPT.format(title=f"Other {name}", text=name))
        synced(root, monkeypatch)
        crucibles[name] = root

    def merge(into, frm):
        monkeypatch.chdir(crucibles[into])
        result = invoke("merge", str(crucibles[frm]), "--yes")
        assert result.exit_code == 0, result.output

    def listing(name):
        concepts = crucibles[name] / ".crucible/wiki/concepts"
        return {p.name: p.read_text() for p in concepts.iterdir()}

    merge("B", "A")
    merge("A", "B")
    names = ["foo.org", "foo_1.org", "other.org", "other_1.org"]
    assert sorted(listing("A")) == sorted(names)
    a = listing("A")
    assert "Foo A" in a["foo.org"] and "[[file:other.org]" in a["foo.org"]
    assert "Foo B" in a["foo_1.org"] and "[[file:other_1.org]" in a["foo_1.org"]
    before = (listing("A"), listing("B"))
    merge("B", "A")
    merge("A", "B")
    assert (listing("A"), listing("B")) == before


# -- 5. uninstall keeps the user's lines -------------------------------------


MARKER = "<!-- crucible-knowledge-base -->"


def test_install_and_uninstall_keep_the_users_lines(project):  # noqa: F811
    claude_md = Path.home() / ".claude/CLAUDE.md"
    claude_md.unlink(missing_ok=True)
    assert invoke("install").exit_code == 0
    text = claude_md.read_text()
    assert text.startswith(MARKER)   # no empty first line
    claude_md.write_text("- never use --no-verify\n" + text + "- merge commits\n")
    result = invoke("uninstall")
    assert result.exit_code == 0, result.output
    assert claude_md.read_text() == "- never use --no-verify\n- merge commits\n"


def test_uninstall_after_text_added_below_the_block(project):  # noqa: F811
    # Install puts a blank line before its block; uninstall takes it too,
    # so a line added after the block follows the user's text, as on main.
    claude_md = Path.home() / ".claude/CLAUDE.md"
    claude_md.parent.mkdir(parents=True, exist_ok=True)
    claude_md.write_text("# My rules\n\n- be nice\n")
    assert invoke("install").exit_code == 0
    claude_md.write_text(claude_md.read_text() + "- added later\n")
    assert invoke("uninstall").exit_code == 0
    assert claude_md.read_text() == "# My rules\n\n- be nice\n- added later\n"


# -- 6. a .TXT source is its own text -----------------------------------------


def test_merge_does_not_take_an_upper_case_txt_source_for_its_own_sidecar(
    project, tmp_path, monkeypatch  # noqa: F811
):
    source = make_crucible(tmp_path / "source")
    notes = source / ".crucible/sources/data/NOTES.TXT"
    notes.write_text("Plain notes.\n")
    if not notes.with_suffix(".txt").exists():
        pytest.skip("needs a case-insensitive filesystem")
    synced(source, monkeypatch)
    monkeypatch.chdir(project)
    result = invoke("merge", str(source), "--yes")
    assert result.exit_code == 0, result.output
    data = project / ".crucible/sources/data"
    assert [p.name for p in data.iterdir()] == ["NOTES.TXT"]
    assert (data / "NOTES.TXT").read_text() == "Plain notes.\n"
