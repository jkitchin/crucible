"""BibTeX entries may be delimited by braces or by parentheses (#26)."""

import json

from click.testing import CliRunner

from crucible.browse import parse_bibtex
from crucible.cli import cli
from crucible.database import CrucibleDB
from crucible.ingest import (
    append_bib_entry,
    bib_has_key,
    parse_bib_file,
    replace_cite_key,
    set_bib_field,
)

from test_cli_json import CONCEPT, CONCEPT_PATH, project, run_json  # noqa: F401

PAREN_ENTRY = """@article(smith2020,
  title = {A paper},
  author = {Smith, J},
  file = {.crucible/sources/data/smith.csv},
)
"""


def test_parse_bib_file_reads_parenthesised_entries(tmp_path):
    bib = tmp_path / "references.bib"
    bib.write_text(PAREN_ENTRY + "\n@misc{doe2021,\n  title = {Other},\n}\n")
    entries = {e["cite_key"]: e for e in parse_bib_file(bib)}
    assert set(entries) == {"smith2020", "doe2021"}
    assert entries["smith2020"]["entry_type"] == "article"
    assert entries["smith2020"]["title"] == "A paper"
    assert entries["smith2020"]["file"] == ".crucible/sources/data/smith.csv"


def test_entry_closer_must_match_its_opener(tmp_path):
    # A ")" line inside a brace entry does not end it.
    bib = tmp_path / "references.bib"
    bib.write_text("@misc{a2020,\n  note = {x\n)},\n  title = {T},\n}\n")
    (entry,) = parse_bib_file(bib)
    assert entry["title"] == "T"


def test_bib_has_key_and_append_see_parenthesised_entries(tmp_path):
    assert bib_has_key(PAREN_ENTRY, "smith2020")
    assert bib_has_key("@article( smith2020 ,\n title={X})", "smith2020")
    assert not bib_has_key(PAREN_ENTRY, "Smith")

    root = tmp_path
    (root / ".crucible").mkdir()
    bib = root / ".crucible" / "references.bib"
    bib.write_text(PAREN_ENTRY)
    append_bib_entry(root, "@misc{smith2020,\n  title = {Dup},\n}", "smith2020")
    assert bib.read_text() == PAREN_ENTRY


def test_key_and_field_edits_handle_parenthesised_entries():
    assert replace_cite_key(PAREN_ENTRY, "new2020").startswith("@article(new2020,")
    edited = set_bib_field(PAREN_ENTRY, "doi", "10.1/x")
    assert edited.startswith("@article(smith2020,\n  doi = {10.1/x},")


def test_browse_parses_parenthesised_entries():
    bib = parse_bibtex(PAREN_ENTRY)
    assert bib["smith2020"]["title"] == "A paper"
    assert bib["smith2020"]["_type"] == "article"


def test_rebuild_and_lint_use_parenthesised_entries(project):  # noqa: F811
    cdir = project / ".crucible"
    csv = cdir / "sources" / "data" / "smith.csv"
    csv.parent.mkdir(parents=True, exist_ok=True)
    csv.write_text("a,b\n1,2\n")
    (cdir / "references.bib").write_text(PAREN_ENTRY)
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: smith2020")
        + "\nAs shown in cite:&smith2020.\n"
    )
    for db_file in cdir.glob("crucible.db*"):
        db_file.unlink()

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    db = CrucibleDB(cdir / "crucible.db")
    try:
        rows = {s["path"]: s for s in db.list_sources()}
    finally:
        db.close()
    row = rows[".crucible/sources/data/smith.csv"]
    assert row["title"] == "A paper"
    assert json.loads(row["metadata"])["cite_key"] == "smith2020"

    sources = run_json("sources", CONCEPT_PATH)
    assert [s["path"] for s in sources] == [".crucible/sources/data/smith.csv"]
    issues = run_json("lint")
    assert not [i for i in issues if "smith2020" in i.get("message", "")]
