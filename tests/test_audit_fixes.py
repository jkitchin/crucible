"""Bugs found by the post-#36 audit of sources, the bibliography and merge."""

import json

from click.testing import CliRunner

from crucible.cli import cli
from crucible.database import CrucibleDB

from test_cli_json import CONCEPT, CONCEPT_PATH, project, run_json  # noqa: F401

DATA = ".crucible/sources/data"


def sync():
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    return result


def rebuild(root):
    for db_file in (root / ".crucible").glob("crucible.db*"):
        db_file.unlink()
    sync()


def sources(root):
    db = CrucibleDB(root / ".crucible/crucible.db")
    try:
        return sorted(
            (s["path"], json.loads(s["metadata"]).get("cite_key"))
            for s in db.list_sources()
        )
    finally:
        db.close()


def lint_types(*types):
    return [i for i in run_json("lint") if i["type"] in types]


# A2: two entries with one key -----------------------------------------------

def test_repeated_bib_key_keeps_a_row_per_file_and_is_linted(project):  # noqa: F811
    data = project / DATA
    data.mkdir(parents=True, exist_ok=True)
    (data / "a.csv").write_text("a")
    (data / "b.csv").write_text("b")
    (project / ".crucible/references.bib").write_text(
        f"@misc{{dup,\n  title = {{A}},\n  file = {{{DATA}/a.csv}},\n}}\n\n"
        f"@misc{{dup,\n  title = {{B}},\n  file = {{{DATA}/b.csv}},\n}}\n"
    )
    sync()
    sync()
    expected = [(f"{DATA}/a.csv", "dup"), (f"{DATA}/b.csv", "dup")]
    assert sources(project) == expected
    issues = lint_types("duplicate_bib_key", "duplicate_cite_key")
    assert sorted(i["type"] for i in issues) == ["duplicate_bib_key", "duplicate_cite_key"]
    rebuild(project)
    assert sources(project) == expected


def test_unclosed_bib_entry_is_linted(project):  # noqa: F811
    (project / ".crucible/references.bib").write_text(
        "@misc{ok,\n  title = {OK},\n}\n\n@misc{broken,\n  title = {never closed\n"
    )
    sync()
    issues = lint_types("malformed_bib_entry")
    assert [i["line"] for i in issues] == [5]


# A3: a key only in references.bib is taken ----------------------------------

def _ingest(root, path, *args):
    result = CliRunner().invoke(cli, ["ingest", str(path), "--type", "data", *args])
    assert result.exit_code == 0, result.output
    return result


def test_ingest_does_not_reuse_a_key_only_the_bib_has(project, tmp_path):  # noqa: F811
    bib = project / ".crucible/references.bib"
    bib.write_text("@book{smith2020,\n  title = {Someone else's book},\n}\n")
    src = tmp_path / "widgets.csv"
    src.write_text("x\n")
    _ingest(project, src, "--title", "Widgets", "--author", "Smith, J", "--date", "2020-02-03")
    from crucible.ingest import parse_bib_file
    entries = {e["cite_key"]: e for e in parse_bib_file(bib)}
    assert set(entries) == {"smith2020", "smith2020a"}
    assert "file" not in entries["smith2020"]
    assert entries["smith2020a"]["title"] == "Widgets"


# A7: an ingested row is the row a rebuild makes -----------------------------

def _rows(root):
    db = CrucibleDB(root / ".crucible/crucible.db")
    try:
        return sorted(
            (s["path"], s["title"], s["source_type"], s["authors"], s["date"], s["url"],
             json.loads(s["metadata"]).get("cite_key"))
            for s in db.list_sources()
        )
    finally:
        db.close()


def test_ingested_rows_survive_a_rebuild_unchanged(project, tmp_path):  # noqa: F811
    plain = tmp_path / "plain.csv"
    plain.write_text("p\n")
    _ingest(project, plain, "--title", "Plain", "--author", "Ann Smith",
            "--author", "Bo Jones", "--date", "2020-02-03")
    supplied = tmp_path / "supplied.csv"
    supplied.write_text("s\n")
    _ingest(project, supplied, "--title", "Supplied", "--author", "Cy Lee",
            "--date", "2021-05-06",
            "--bibtex", "@article{x, title={Supplied paper}, author={Lee, Cy}, year={2021} }")
    sync()
    ingested = _rows(project)
    titles = {r[1] for r in ingested}
    assert "Supplied paper" in titles  # the entry's title, as a rebuild reads it
    rebuild(project)
    assert _rows(project) == ingested


# A4: a deleted source stays deleted ------------------------------------------

def test_deleted_source_does_not_come_back_on_sync(project, tmp_path):  # noqa: F811
    src = tmp_path / "gone.csv"
    src.write_text("g\n")
    _ingest(project, src, "--title", "Gone", "--date", "2020-01-01")
    stored = f"{DATA}/gone.csv"
    assert stored in [p for p, _ in sources(project)]
    result = CliRunner().invoke(cli, ["delete", stored, "--yes", "--file"])
    assert result.exit_code == 0, result.output
    assert "Removed the file field" in result.output
    sync()
    assert stored not in [p for p, _ in sources(project)]
    from crucible.ingest import parse_bib_file
    entry = parse_bib_file(project / ".crucible/references.bib")[0]
    assert entry["title"] == "Gone" and "file" not in entry
    rebuild(project)
    assert stored not in [p for p, _ in sources(project)]


def test_delete_warns_when_the_scanned_file_stays(project, tmp_path):  # noqa: F811
    src = tmp_path / "kept.csv"
    src.write_text("k\n")
    _ingest(project, src, "--title", "Kept")
    result = CliRunner().invoke(cli, ["delete", f"{DATA}/kept.csv", "--yes"])
    assert result.exit_code == 0, result.output
    assert "next `crucible sync` adds it back" in result.output


def test_lint_reports_a_committed_source_whose_file_is_gone(project, tmp_path):  # noqa: F811
    src = tmp_path / "lost.csv"
    src.write_text("l\n")
    _ingest(project, src, "--title", "Lost")
    (project / DATA / "lost.csv").unlink()
    assert [i["path"] for i in lint_types("missing_source_file")] == [f"{DATA}/lost.csv"]


# A7: article dates ----------------------------------------------------------

def _dates(root, path):
    db = CrucibleDB(root / ".crucible/crucible.db")
    try:
        a = db.get_article_by_path(path)
        return a["created_at"], a["updated_at"]
    finally:
        db.close()


def _set_props(root, **props):
    text = (root / CONCEPT_PATH).read_text()
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith((":CREATED:", ":MODIFIED:"))]
    i = next(n for n, ln in enumerate(lines) if ln.strip() == ":PROPERTIES:")
    for name, value in props.items():
        lines.insert(i + 1, f":{name}: {value}")
    (root / CONCEPT_PATH).write_text("\n".join(lines) + "\n")


def test_modified_without_created_is_the_created_date_too(project):  # noqa: F811
    _set_props(project, MODIFIED="2024-03-04")
    sync()
    assert _dates(project, CONCEPT_PATH) == ("2024-03-04 00:00:00", "2024-03-04 00:00:00")
    rebuild(project)
    assert _dates(project, CONCEPT_PATH) == ("2024-03-04 00:00:00", "2024-03-04 00:00:00")


def test_lint_reports_unreadable_and_reversed_dates(project):  # noqa: F811
    _set_props(project, CREATED="last tuesday")
    sync()
    assert [i["path"] for i in lint_types("invalid_date")] == [CONCEPT_PATH]
    _set_props(project, CREATED="2024-05-01", MODIFIED="2024-01-01")
    sync()
    assert "earlier" in lint_types("invalid_date")[0]["message"]
    _set_props(project, CREATED="2024-01-01", MODIFIED="2024-05-01")
    sync()
    assert lint_types("invalid_date") == []


def test_init_ignores_lock_and_rebuild_files(project):  # noqa: F811
    ignored = (project / ".gitignore").read_text()
    for pattern in (".crucible/references.bib.lock", ".crucible/.crucible.db.rebuild-*.tmp",
                    ".crucible/crucible.db-journal"):
        assert pattern in ignored


def test_sync_records_an_old_ingest_date_in_the_bib(project, tmp_path):  # noqa: F811
    """Rows ingested before ingest wrote `date` keep it through a rebuild."""
    from crucible import bibtex
    src = tmp_path / "old.csv"
    src.write_text("o\n")
    _ingest(project, src, "--title", "Old", "--author", "Ann Smith", "--date", "2019-04-05")
    bib = project / ".crucible/references.bib"
    bib.write_text(bibtex.remove_field(bib.read_text(), "date"))
    sync()
    ingested = _rows(project)
    assert ingested[0][4] == "2019-04-05"
    assert "date = {2019-04-05}" in bib.read_text()
    rebuild(project)
    assert _rows(project) == ingested


# Review of #38 ----------------------------------------------------------------

def test_unclosed_bibtex_is_refused_before_anything_is_written(project, tmp_path):  # noqa: F811
    src = tmp_path / "x.csv"
    src.write_text("x\n")
    bib = project / ".crucible/references.bib"
    before = bib.read_text() if bib.exists() else ""
    result = CliRunner().invoke(cli, ["ingest", str(src), "--type", "data",
                                      "--bibtex", "@article{orig, title={T}, year={2024}"])
    assert result.exit_code != 0
    assert "not a complete BibTeX entry" in result.output
    assert (bib.read_text() if bib.exists() else "") == before
    assert not (project / DATA / "x.csv").exists()


def test_ingest_title_is_written_when_the_entry_has_none(project, tmp_path):  # noqa: F811
    src = tmp_path / "t.csv"
    src.write_text("t\n")
    _ingest(project, src, "--title", "My Title", "--author", "Ann Smith",
            "--bibtex", "@misc{x, year={2024}}")
    rows = _rows(project)
    assert rows[0][1] == "My Title"
    rebuild(project)
    assert _rows(project) == rows


def test_ingest_date_that_contradicts_the_year_is_not_written(project, tmp_path):  # noqa: F811
    src = tmp_path / "d.csv"
    src.write_text("d\n")
    _ingest(project, src, "--title", "Old paper", "--date", "2025-01-10",
            "--bibtex", "@article{x, title={Old paper}, author={Lee, Cy}, year={2019}}")
    from crucible.ingest import parse_bib_file
    entry = parse_bib_file(project / ".crucible/references.bib")[0]
    assert entry["year"] == "2019" and "date" not in entry
    rows = _rows(project)
    assert rows[0][4] == "2019"
    rebuild(project)
    assert _rows(project) == rows


def test_sync_does_not_record_the_ingest_day_as_a_date(project, tmp_path):  # noqa: F811
    from datetime import datetime

    from crucible import bibtex
    today = datetime.now().strftime("%Y-%m-%d")
    src = tmp_path / "today.csv"
    src.write_text("t\n")
    _ingest(project, src, "--title", "Today", "--author", "Ann Smith", "--date", today)
    bib = project / ".crucible/references.bib"
    bib.write_text(bibtex.remove_field(bib.read_text(), "date"))
    sync()
    assert "date =" not in bib.read_text()


def test_delete_drops_the_file_field_through_a_symlinked_sources_dir(project, tmp_path):  # noqa: F811
    real = tmp_path / "elsewhere"
    real.mkdir()
    data = project / DATA
    if data.exists():
        data.rmdir()
    data.parent.mkdir(parents=True, exist_ok=True)
    data.symlink_to(real, target_is_directory=True)
    src = tmp_path / "linked.csv"
    src.write_text("l\n")
    _ingest(project, src, "--title", "Linked")
    from crucible.ingest import drop_bib_file_fields
    bib = project / ".crucible/references.bib"
    text = bib.read_text().replace(f"{DATA}/linked.csv", str(real / "linked.csv"))
    bib.write_text(text)
    assert drop_bib_file_fields(project, f"{DATA}/linked.csv") != []
    assert "file =" not in bib.read_text()


def test_unreadable_created_is_not_also_reported_missing(project):  # noqa: F811
    _set_props(project, CREATED="2024/01/02")
    sync()
    assert [i["path"] for i in lint_types("invalid_date")] == [CONCEPT_PATH]
    assert not [i for i in lint_types("missing_property") if "MODIFIED" in i["message"]]
