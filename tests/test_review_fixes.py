"""Regression tests for the review of PR #39."""

import hashlib
import json

from click.testing import CliRunner

from crucible import cli as cli_module
from crucible import ingest as ingest_module
from crucible.browse import org_to_html
from crucible.cli import cli
from crucible.database import CrucibleDB
from crucible.orgparse import parse_org

from test_data_loss_ingest import kb, run, stored  # noqa: F401
from test_merge import CONCEPT, make_crucible
from test_merge import project as merge_project  # noqa: F401


# 1. a file-level drawer does not hide the article's heading drawer ----------

def test_file_level_drawer_and_heading_drawer_are_both_read():
    meta = parse_org(
        ":PROPERTIES:\n:ID: abc\n:END:\n#+TITLE: T\n\n"
        "* T\n:PROPERTIES:\n:ARTICLE_TYPE: summary\n:SOURCE_KEYS: smith2024\n:END:\n"
        "** Sub\n:PROPERTIES:\n:ARTICLE_TYPE: concept\n:END:\n"
    )
    assert meta.properties["ID"] == "abc"
    assert meta.properties["ARTICLE_TYPE"] == "summary"
    assert meta.properties["SOURCE_KEYS"] == "smith2024"


# 2. ingest claims its destination instead of check-then-copy ----------------

def test_ingest_does_not_overwrite_a_file_created_after_the_check(kb, tmp_path, monkeypatch):  # noqa: F811
    web = kb / ".crucible/sources/external/web"
    web.mkdir(parents=True, exist_ok=True)
    (web / "paper.md").write_text("another process wrote this")
    # The existence check misses the file, as it would in a race.
    monkeypatch.setattr(ingest_module, "_dest_taken", lambda path: False)
    (tmp_path / "paper.md").write_text("# Paper\nbody\n")
    result = run("ingest", str(tmp_path / "paper.md"))
    assert (web / "paper.md").read_text() == "another process wrote this"
    assert stored(result).endswith("paper_1.md")


# 3. merge rewrites prefix-less links and keeps ::search ---------------------

def test_merge_rewrites_dot_slash_links_to_renamed_articles(merge_project, tmp_path):  # noqa: F811
    source = make_crucible(tmp_path / "src")
    concepts = source / ".crucible/wiki/concepts"
    # Collides with the target's article, so it becomes activation-energy_1.org.
    (concepts / "activation-energy.org").write_text(CONCEPT.replace("barrier", "hill"))
    (concepts / "linker.org").write_text(
        CONCEPT.replace("Activation energy", "Linker")
        + "See [[./activation-energy.org::*Activation energy][it]] and "
          "[[file:activation-energy.org]].\n"
    )
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    text = (merge_project / ".crucible/wiki/concepts/linker.org").read_text()
    assert "[[./activation-energy_1.org::*Activation energy][it]]" in text
    assert "[[file:activation-energy_1.org]]" in text


# 4. install without a record does not flag an older shipped copy ------------

def test_install_without_record_accepts_an_older_shipped_skill(tmp_path, registry, monkeypatch):
    monkeypatch.chdir(tmp_path)
    old = b"an older SKILL.md crucible shipped\n"
    monkeypatch.setattr(cli_module, "_PRE_RECORD_SKILL_HASHES",
                        {"SKILL.md": frozenset({hashlib.sha256(old).hexdigest()})})
    skill_dir = registry.parent / ".claude/skills/crucible"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_bytes(old)
    result = CliRunner().invoke(cli, ["install"])
    assert result.exit_code == 0, result.output
    assert "local changes" not in result.output
    assert not list(skill_dir.glob("SKILL.md.bak*"))


def test_install_never_overwrites_an_earlier_backup(tmp_path, registry, monkeypatch):
    monkeypatch.chdir(tmp_path)
    skill_dir = registry.parent / ".claude/skills/crucible"
    assert CliRunner().invoke(cli, ["install"]).exit_code == 0
    for text in ("first edit", "second edit"):
        (skill_dir / "SKILL.md").write_text(text)
        assert CliRunner().invoke(cli, ["install"]).exit_code == 0
    assert (skill_dir / "SKILL.md.bak").read_text() == "first edit"
    assert (skill_dir / "SKILL.md.bak2").read_text() == "second edit"


def test_shipped_skill_hashes_include_the_current_one():
    # The list is frozen: the current SKILL.md is recognised through the
    # record, but an install from before the record must still match.
    assert cli_module._PRE_RECORD_SKILL_HASHES["SKILL.md"]


# 5. non-ASCII concepts are not hidden by an empty key -----------------------

def _concept_article(title, tags):
    return (f"#+TITLE: {title}\n#+FILETAGS: :concept:{tags}:\n\n* {title}\n"
            ":PROPERTIES:\n:ARTICLE_TYPE: concept\n:END:\n\nBody.\n")


def test_non_ascii_concept_needs_its_own_article(kb):  # noqa: F811
    concepts = kb / ".crucible/wiki/concepts"
    (concepts / "触媒.org").write_text(_concept_article("触媒", "触媒:酵素"))
    run("sync")
    db = CrucibleDB(kb / ".crucible/crucible.db")
    try:
        assert db.concepts_without_articles() == ["酵素"]
    finally:
        db.close()


# 6. delete --file keeps a .txt that is not the extracted text ---------------

def test_delete_file_keeps_a_users_txt(kb, tmp_path):  # noqa: F811
    (tmp_path / "results.csv").write_text("a,b\n1,2\n")
    run("ingest", str(tmp_path / "results.csv"))
    data = kb / ".crucible/sources/data"
    (data / "results.txt").write_text("my own notes about the results")
    result = run("delete", "results.csv", "--yes", "--file")
    assert not (data / "results.csv").exists()
    assert (data / "results.txt").read_text() == "my own notes about the results"
    assert "Kept" in result.output


def test_delete_file_removes_an_unrecorded_sidecar_that_matches(kb, tmp_path):  # noqa: F811
    (tmp_path / "results.csv").write_text("a,b\n1,2\n")
    run("ingest", str(tmp_path / "results.csv"))
    db = CrucibleDB(kb / ".crucible/crucible.db")
    try:
        # As for a source ingested before text hashes were recorded.
        db.conn.execute("UPDATE sources SET metadata = '{}'")
        db.conn.commit()
    finally:
        db.close()
    run("delete", "results.csv", "--yes", "--file")
    assert list((kb / ".crucible/sources/data").iterdir()) == []


# 7. keyword search leaves the embeddings table alone ------------------------

def _has_embeddings_table(root):
    db = CrucibleDB(root / ".crucible/crucible.db")
    try:
        return db.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'embeddings'"
        ).fetchone() is not None
    finally:
        db.close()


def test_fts_and_auto_search_do_not_create_embeddings(kb, tmp_path):  # noqa: F811
    (tmp_path / "notes.md").write_text("# Notes\ncatalysis\n")
    run("ingest", str(tmp_path / "notes.md"))
    assert not _has_embeddings_table(kb)
    for mode in ("fts", "auto"):
        run("search", "catalysis", "--mode", mode)
        assert not _has_embeddings_table(kb)


# 8. a stray drawer-like line does not swallow prose -------------------------

def test_drawer_does_not_cross_a_heading():
    _, h, _ = org_to_html(
        "* A\n:note:\nprose that must stay\n\n* B\n:LOGBOOK:\n- done\n:END:\n"
    )
    assert "prose that must stay" in h
    assert "<summary>Logbook</summary>" in h


# 9. concept is case-insensitive without a second lowercase -------------------

def test_concept_lookup_is_case_insensitive(kb):  # noqa: F811
    (kb / ".crucible/wiki/concepts/kinetics.org").write_text(
        _concept_article("Kinetics", "kinetics"))
    run("sync")
    result = CliRunner().invoke(cli, ["concept", "KINETICS", "--json"])
    assert result.exit_code == 0, result.output
    assert [a["title"] for a in json.loads(result.stdout)] == ["Kinetics"]


# Follow-ups from the re-review of 9b818ae -----------------------------------

def test_failure_right_after_claiming_leaves_no_placeholder(kb, tmp_path, monkeypatch):  # noqa: F811
    (tmp_path / "paper.md").write_text("# Paper\nbody\n")
    real = ingest_module.set_bib_field

    def set_bib_field(entry, name, value):
        if name == "file":  # the first call after the names are claimed
            raise RuntimeError("boom")
        return real(entry, name, value)
    monkeypatch.setattr(ingest_module, "set_bib_field", set_bib_field)
    result = run("ingest", str(tmp_path / "paper.md"), ok=False)
    assert result.exit_code != 0
    web = kb / ".crucible/sources/external/web"
    assert not web.exists() or list(web.iterdir()) == []


def test_delete_file_removes_a_legacy_sidecar_with_changed_spacing(kb, tmp_path):  # noqa: F811
    (tmp_path / "results.csv").write_text("a,b\n1,2\n")
    run("ingest", str(tmp_path / "results.csv"))
    data = kb / ".crucible/sources/data"
    db = CrucibleDB(kb / ".crucible/crucible.db")
    try:
        db.conn.execute("UPDATE sources SET metadata = '{}'")
        db.conn.commit()
    finally:
        db.close()
    (data / "results.txt").write_text("a,b\r\n1,2  \n\n")  # as an older extractor wrote it
    run("delete", "results.csv", "--yes", "--file")
    assert list(data.iterdir()) == []
