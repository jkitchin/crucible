"""The database must be regenerable from the files a project commits.

These tests build a project the normal way (ingest, write articles, sync),
then simulate a fresh clone: the database and the gitignored
sources/external/ are deleted, and the next command rebuilds the database.
The rebuilt database should match the one built incrementally.
"""

import shutil

import pytest
from click.testing import CliRunner

from crucible.cli import cli
from crucible.database import CrucibleDB

SOURCES = [
    # (file name, contents, ingest options)
    ("widget-kinetics.md", "# Widget Kinetics\n\nNotes.\n",
     ["--shareable", "--type", "notebook", "--author", "Doe", "--date", "2026-01-01"]),
    ("run.csv", "a,b\n1,2\n",
     ["--shareable", "--type", "data", "--title", "Run data", "--author", "Roe",
      "--date", "2025-01-01"]),
    ("paper.pdf", "%PDF-1.4 synthetic\n",
     ["--type", "pdf", "--title", "A Paper", "--author", "Poe", "--date", "2024-01-01"]),
]

CONCEPT = """#+TITLE: Widgets
#+FILETAGS: :concept:widgets:

* Widgets
:PROPERTIES:
:ARTICLE_TYPE: concept
:SOURCE_KEYS: {keys}
:STATUS: draft
:CREATED: 2026-01-01
:END:

See [[file:../summaries/notes.org][the notes]], cite:&{first}.
"""

SUMMARY = """#+TITLE: Widget notes
#+FILETAGS: :summary:widgets:

* Widget notes
:PROPERTIES:
:ARTICLE_TYPE: summary
:SOURCE_KEYS: {first}
:STATUS: draft
:CREATED: 2026-01-01
:END:

Back to [[file:../concepts/widgets.org][widgets]].
"""

QUERIES = {
    "articles": "SELECT path, title, article_type, status FROM articles",
    "concepts": """
        SELECT a.path, c.name FROM article_concepts x
        JOIN articles a ON a.id = x.article_id JOIN concepts c ON c.id = x.concept_id
    """,
    "links": """
        SELECT a.path AS from_path, b.path AS to_path FROM article_links x
        JOIN articles a ON a.id = x.from_id JOIN articles b ON b.id = x.to_id
    """,
    # The date is compared by year: references.bib only records the year.
    "sources": """
        SELECT path, source_type, title, authors, substr(date, 1, 4) AS year,
               shareable, json_extract(metadata, '$.cite_key') AS cite_key
        FROM sources
    """,
    # By path: after #6's rebuild the phantom rows carry the right cite keys,
    # but point at files that do not exist.
    "provenance": """
        SELECT a.path AS article, s.path AS source FROM article_sources x
        JOIN articles a ON a.id = x.article_id JOIN sources s ON s.id = x.source_id
    """,
}


def invoke(*args):
    result = CliRunner().invoke(cli, list(args))
    assert result.exit_code == 0, result.output
    return result


def snapshot(root, table):
    db = CrucibleDB(root / ".crucible" / "crucible.db")
    try:
        rows = db.conn.execute(QUERIES[table]).fetchall()
        # Rows are dicts keyed by column name: every column needs a unique name.
        return {tuple(dict(row).values()) for row in rows}
    finally:
        db.close()


@pytest.fixture
def rebuilt(tmp_path, registry, monkeypatch):
    """Snapshots of a project's database before and after a fresh-clone rebuild."""
    root = tmp_path / "kb"
    root.mkdir()
    monkeypatch.chdir(root)
    invoke("init", "--root", str(root))

    staging = tmp_path / "staging"
    staging.mkdir()
    for name, contents, options in SOURCES:
        (staging / name).write_text(contents)
        invoke("ingest", str(staging / name), *options)

    db = CrucibleDB(root / ".crucible" / "crucible.db")
    try:
        keys = sorted(
            row["cite_key"] for row in db.conn.execute(
                "SELECT json_extract(metadata, '$.cite_key') AS cite_key FROM sources"
            ).fetchall()
        )
    finally:
        db.close()
    wiki = root / ".crucible" / "wiki"
    (wiki / "concepts" / "widgets.org").write_text(
        CONCEPT.format(keys=" ".join(keys), first=keys[0])
    )
    (wiki / "summaries" / "notes.org").write_text(SUMMARY.format(first=keys[0]))
    invoke("sync")
    before = {table: snapshot(root, table) for table in QUERIES}

    # Fresh clone: no database, no gitignored external sources.
    for path in (root / ".crucible").glob("crucible.db*"):
        path.unlink()
    shutil.rmtree(root / ".crucible" / "sources" / "external")
    invoke("sync")
    after = {table: snapshot(root, table) for table in QUERIES}
    return before, after


@pytest.mark.parametrize("table", ["articles", "concepts", "links"])
def test_rebuild_reproduces_wiki_tables(rebuilt, table):
    before, after = rebuilt
    assert before[table]  # the fixture exercised this table
    assert after[table] == before[table]


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="#6: rebuild duplicates sources, links articles to phantom "
    ".crucible/bib/ paths, and loses titles, types and the shareable flag",
)
@pytest.mark.parametrize("table", ["sources", "provenance"])
def test_rebuild_reproduces_sources_and_provenance(rebuilt, table):
    before, after = rebuilt
    assert before[table]
    assert after[table] == before[table]
