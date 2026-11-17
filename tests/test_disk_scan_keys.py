"""A committed file named like a cite key does not capture that key (#29)."""

import json

from click.testing import CliRunner

from crucible.cli import cli
from crucible.database import CrucibleDB

from test_cli_json import CONCEPT, CONCEPT_PATH, project  # noqa: F401

DATA = ".crucible/sources/data"


def sync():
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    return result


def keys_by_path(root):
    db = CrucibleDB(root / ".crucible/crucible.db")
    try:
        return {s["path"]: json.loads(s["metadata"]).get("cite_key")
                for s in db.list_sources()}
    finally:
        db.close()


def linked(root):
    db = CrucibleDB(root / ".crucible/crucible.db")
    try:
        rows = db.conn.execute(
            "SELECT s.path FROM article_sources x JOIN sources s ON s.id = x.source_id"
        ).fetchall()
    finally:
        db.close()
    return sorted(r["path"] for r in rows)


def rebuild(root):
    for db_file in (root / ".crucible").glob("crucible.db*"):
        db_file.unlink()
    sync()


def cite(root, key):
    (root / CONCEPT_PATH).write_text(CONCEPT.replace(":SOURCE_KEYS:", f":SOURCE_KEYS: {key}"))


def test_entry_keeps_its_key_from_a_file_named_like_it(project):  # noqa: F811
    data = project / DATA
    data.mkdir(parents=True, exist_ok=True)
    (data / "smith2024.csv").write_text("u\n")
    (data / "other.csv").write_text("r\n")
    sync()
    (project / ".crucible/references.bib").write_text(
        f"@misc{{smith2024,\n  title = {{Smith data}},\n  file = {{{DATA}/other.csv}},\n}}\n"
    )
    cite(project, "smith2024")

    sync()   # an existing database, where smith2024.csv was keyed smith2024
    assert linked(project) == [f"{DATA}/other.csv"]
    keys = keys_by_path(project)
    assert keys[f"{DATA}/other.csv"] == "smith2024"
    assert keys[f"{DATA}/smith2024.csv"] == "smith2024-csv"
    issues = json.loads(CliRunner().invoke(cli, ["lint", "--json"]).stdout)
    assert not [i for i in issues if i["type"] in ("duplicate_cite_key", "ambiguous_source_key")]

    rebuild(project)
    assert linked(project) == [f"{DATA}/other.csv"]
    assert keys_by_path(project) == keys


def test_repairs_a_row_from_an_older_version(project):  # noqa: F811
    # Before the disk_scan mark, a scanned row was keyed by its stem alone.
    data = project / DATA
    data.mkdir(parents=True, exist_ok=True)
    (data / "smith2024.csv").write_text("u\n")
    (data / "other.csv").write_text("r\n")
    db = CrucibleDB(project / ".crucible/crucible.db")
    try:
        db.add_source(path=f"{DATA}/smith2024.csv", title="Smith2024",
                      source_type="data", shareable=True,
                      metadata={"cite_key": "smith2024"})
    finally:
        db.close()
    (project / ".crucible/references.bib").write_text(
        f"@misc{{smith2024,\n  title = {{Smith data}},\n  file = {{{DATA}/other.csv}},\n}}\n"
    )
    cite(project, "smith2024")
    sync()
    assert linked(project) == [f"{DATA}/other.csv"]
    assert keys_by_path(project)[f"{DATA}/smith2024.csv"] == "smith2024-csv"


def test_bibliography_only_entry_is_not_captured_by_a_file(project):  # noqa: F811
    # The entry names no file, so its key resolves to nothing (lint reports
    # it) rather than to whatever file happens to share its name.
    data = project / DATA
    data.mkdir(parents=True, exist_ok=True)
    (data / "smith2024.csv").write_text("u\n")
    (project / ".crucible/references.bib").write_text(
        "@misc{smith2024,\n  title = {Smith data},\n}\n"
    )
    cite(project, "smith2024")
    sync()
    assert linked(project) == []
    rebuild(project)
    assert linked(project) == []
    assert keys_by_path(project) == {f"{DATA}/smith2024.csv": "smith2024-csv"}


def test_fallback_key_avoids_other_taken_keys(project):  # noqa: F811
    data = project / DATA
    data.mkdir(parents=True, exist_ok=True)
    (data / "smith2024.csv").write_text("u\n")
    (data / "a.md").write_text("a\n")
    (data / "b.md").write_text("b\n")
    (project / ".crucible/references.bib").write_text(
        f"@misc{{smith2024,\n  file = {{{DATA}/a.md}},\n}}\n"
        f"@misc{{smith2024-csv,\n  file = {{{DATA}/b.md}},\n}}\n"
    )
    sync()
    assert keys_by_path(project)[f"{DATA}/smith2024.csv"] == "smith2024-csv-2"


def test_unclaimed_file_keeps_its_stem(project):  # noqa: F811
    data = project / DATA
    data.mkdir(parents=True, exist_ok=True)
    (data / "run.csv").write_text("x\n")
    cite(project, "run")
    sync()
    assert keys_by_path(project)[f"{DATA}/run.csv"] == "run"
    assert linked(project) == [f"{DATA}/run.csv"]
