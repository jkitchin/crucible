"""One brace-balanced BibTeX reader for every part of crucible.

Entries that close on the same line, as doi.org returns them, used to run
on to the next line-initial closing brace and swallow the entries after
them.
"""

import json

from click.testing import CliRunner

from crucible import bibtex
from crucible.browse import parse_bibtex
from crucible.cli import cli
from crucible.database import CrucibleDB
from crucible.ingest import (
    _parse_bib_fields,
    bib_has_key,
    parse_bib_file,
    replace_cite_key,
    set_bib_field,
)

from test_cli_json import CONCEPT, CONCEPT_PATH, project  # noqa: F401

DATA = ".crucible/sources/data"

# Crossref style: one line, closing brace on the same line.
COMPACT = (
    "@article{alpha2024, title={Alpha paper}, volume={1}, DOI={10.1/a}, "
    "journal={J}, author={One, Kay}, year={2024} }\n\n"
    "@article{beta2023, title={Beta paper}, DOI={10.1/b}, author={Two, B}, year={2023} }\n\n"
    "@misc{gamma2022,\n  title = {Gamma},\n  year = {2022},\n}\n"
)


def test_compact_entries_end_on_their_own_line(tmp_path):
    bib = tmp_path / "references.bib"
    bib.write_text(COMPACT)
    entries = parse_bib_file(bib)
    assert [e["cite_key"] for e in entries] == ["alpha2024", "beta2023", "gamma2022"]
    assert entries[0]["title"] == "Alpha paper"
    assert entries[0]["doi"] == "10.1/a"
    assert entries[1]["title"] == "Beta paper"
    assert set(parse_bibtex(COMPACT)) == {"alpha2024", "beta2023", "gamma2022"}
    assert parse_bibtex(COMPACT)["alpha2024"]["title"] == "Alpha paper"


def test_field_values_nest_braces_quote_and_go_bare():
    entry = (
        '@article{a, title={The {DFT} method}, year=2021, month=jan, '
        'note="a {quoted} value", journal=jacs # " letters"}'
    )
    fields = _parse_bib_fields(entry)
    assert fields["title"] == "The {DFT} method"
    assert fields["year"] == "2021"
    assert fields["month"] == "jan"
    assert fields["note"] == "a {quoted} value"
    assert fields["journal"] == "jacs letters"
    assert parse_bibtex(entry)["a"]["title"] == "The {DFT} method"


def test_comments_strings_and_unclosed_entries_are_not_entries():
    text = (
        "% @article{commented, title={x}}\n"
        '@string{jacs = "J. Am. Chem. Soc."}\n'
        "@comment{ @misc{inside, title={n}} }\n"
        "@preamble{ \"\\newcommand{\\x}{y}\" }\n"
        "@book{broken, title={never closed\n"
        "@misc{last,\n  title = {Last},\n}\n"
    )
    entries, malformed = bibtex.scan(text)
    assert [e.key for e in entries] == ["last"]
    assert len(malformed) == 1
    assert replace_cite_key(text, "new").count("@misc{new,") == 1
    assert "% @article{commented," in replace_cite_key(text, "new")


def test_parenthesised_entry_closes_outside_quotes():
    text = '@misc(p, title = "a (b) c", note = {)})\n@misc{q, title={Q}}\n'
    assert bibtex.keys(text) == ["p", "q"]
    assert _parse_bib_fields(text)["title"] == "a (b) c"


def test_key_lookup_ignores_field_text():
    text = "@article{x, author = {Smith, J}}"
    assert bib_has_key(text, "x")
    assert not bib_has_key(text, "Smith")


def test_set_field_on_a_compact_entry_keeps_the_other_fields():
    entry = "@misc{k, title={T}, file={old}, year={2020} }"
    new = set_bib_field(entry, "file", "new path.pdf")
    assert _parse_bib_fields(new) == {"file": "new path.pdf", "title": "T", "year": "2020"}
    assert new.count("file") == 1
    assert _parse_bib_fields(set_bib_field("@misc{k}", "file", "f")) == {"file": "f"}


def _sync():
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output


def _sources(root):
    db = CrucibleDB(root / ".crucible/crucible.db")
    try:
        return sorted(
            (s["path"], json.loads(s["metadata"])["cite_key"], s["title"])
            for s in db.list_sources()
        )
    finally:
        db.close()


def test_compact_entries_keep_their_files_through_sync_and_rebuild(project):  # noqa: F811
    data = project / DATA
    data.mkdir(parents=True, exist_ok=True)
    for name in ("alpha.csv", "beta.csv", "gamma.csv"):
        (data / name).write_text(name)
    text = COMPACT
    for key, name in (("alpha2024", "alpha"), ("beta2023", "beta"), ("gamma2022", "gamma")):
        text = set_bib_field_in(text, key, f"{DATA}/{name}.csv")
    bib = project / ".crucible/references.bib"
    bib.write_text(text)
    (project / CONCEPT_PATH).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: alpha2024 beta2023 gamma2022")
    )

    _sync()
    _sync()
    after_sync = bib.read_text()
    # Every entry keeps its own file field.
    files = {e["cite_key"]: e.get("file") for e in parse_bib_file(bib)}
    assert files == {
        "alpha2024": f"{DATA}/alpha.csv",
        "beta2023": f"{DATA}/beta.csv",
        "gamma2022": f"{DATA}/gamma.csv",
    }
    incremental = _sources(project)
    assert incremental == [
        (f"{DATA}/alpha.csv", "alpha2024", "Alpha paper"),
        (f"{DATA}/beta.csv", "beta2023", "Beta paper"),
        (f"{DATA}/gamma.csv", "gamma2022", "Gamma"),
    ]

    for db_file in (project / ".crucible").glob("crucible.db*"):
        db_file.unlink()
    _sync()
    assert _sources(project) == incremental
    assert bib.read_text() == after_sync


def set_bib_field_in(text, key, path):
    return bibtex.map_entries(
        text, lambda e: set_bib_field(e.text, "file", path) if e.key == key else e.text
    )
