"""Merge attaches an incoming file to a bibliography-only entry in the target (#28)."""

from click.testing import CliRunner

from crucible.cli import cli
from crucible.database import CrucibleDB
from crucible.ingest import parse_bib_file

from test_merge import CONCEPT, _ingest_into_new_crucible, project  # noqa: F401


def target_entry(project, fields):
    bib = project / ".crucible/references.bib"
    with bib.open("a") as f:
        f.write("\n@article{synthetic2026,\n" + fields + "}\n")
    return bib


def test_merge_attaches_incoming_file_to_bibliography_only_entry(
    project, tmp_path, monkeypatch  # noqa: F811
):
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Imported.\n")
    bib = target_entry(
        project, "  title = {Synthetic {E}vidence},\n  journal = {J. Target},\n"
    )
    concept = project / ".crucible/wiki/concepts/activation-energy.org"
    concept.write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic2026"))
    monkeypatch.chdir(project)

    preview = CliRunner().invoke(cli, ["merge", str(source), "--dry-run"])
    assert preview.exit_code == 0, preview.output
    assert "existing entry gets file" in preview.output

    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert "Attached 1 source file(s)" in result.output

    (entry,) = [e for e in parse_bib_file(bib) if e["cite_key"] == "synthetic2026"]
    # The target's own entry is kept and gains the file field.
    assert entry["journal"] == "J. Target"
    path = entry["file"]
    assert (project / path).read_text() == "Imported.\n"
    assert bib.read_text().count("synthetic2026,") == 1

    def linked():
        db = CrucibleDB(project / ".crucible/crucible.db")
        try:
            rows = db.conn.execute(
                "SELECT s.path FROM article_sources x JOIN sources s ON s.id = x.source_id"
            ).fetchall()
        finally:
            db.close()
        return [r["path"] for r in rows]

    assert linked() == [path]
    # And the same after a rebuild from the committed files.
    for db_file in (project / ".crucible").glob("crucible.db*"):
        db_file.unlink()
    assert CliRunner().invoke(cli, ["sync"]).exit_code == 0
    assert linked() == [path]


def test_merge_refuses_attaching_when_dois_differ(project, tmp_path, monkeypatch):  # noqa: F811
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Imported.\n")
    in_bib = source / ".crucible/references.bib"
    in_bib.write_text(in_bib.read_text().replace(
        "  title =", "  doi = {10.1000/incoming},\n  title =", 1))
    bib = target_entry(project, "  doi = {https://doi.org/10.1000/OTHER},\n")
    before = bib.read_text()
    monkeypatch.chdir(project)
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code != 0
    assert "different DOIs" in result.output
    assert bib.read_text() == before


def test_merge_attaches_when_dois_match_up_to_spelling(project, tmp_path, monkeypatch):  # noqa: F811
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Imported.\n")
    in_bib = source / ".crucible/references.bib"
    in_bib.write_text(in_bib.read_text().replace(
        "  title =", "  doi = {10.1000/Same},\n  title =", 1))
    target_entry(project, "  doi = {https://doi.org/10.1000/same},\n")
    monkeypatch.chdir(project)
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output


def test_merge_refuses_attaching_when_titles_differ(project, tmp_path, monkeypatch):  # noqa: F811
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Imported.\n")
    target_entry(project, "  title = {Something else},\n")
    monkeypatch.chdir(project)
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code != 0
    assert "different titles" in result.output


def test_merge_still_refuses_when_target_entry_names_a_file(project, tmp_path, monkeypatch):  # noqa: F811
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Imported.\n")
    target_entry(project, "  file = {.crucible/sources/data/unsynced.md},\n")
    monkeypatch.chdir(project)
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code != 0
    assert "Give the target's entry a file field" in result.output


def test_attach_refuses_an_entry_that_gained_a_file_since_planning():
    import click
    import pytest

    from crucible.cli import _merged_bib_text, _MergePlan

    plan = _MergePlan(bib_file_fields={"k2020": ".crucible/sources/data/k.md"})
    assert "file = {.crucible/sources/data/k.md}" in _merged_bib_text(
        "@misc{k2020,\n  title = {T},\n}\n", plan
    )
    for text in ("@misc{k2020,\n  file = {x.md},\n}\n", ""):
        with pytest.raises(click.ClickException, match="changed while merging"):
            _merged_bib_text(text, plan)
