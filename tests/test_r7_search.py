"""Regression tests for search: quoted phrases are searched as phrases, and
chemical subscripts (CO_2, TiO_{2}) match their plain spelling (CO2)."""

import http.client
import sqlite3
import threading

import pytest
from test_embeddings import FakeOllama, _make_project, invoke

from crucible.browse import start_server
from crucible.database import CrucibleDB, fts_text
from crucible.embeddings import EmbeddingIndex

DFT = ("DFT", "Density functional theory computes ground states.")
POLY = ("Polymers", "Polymer density depends on which functional groups are present.")
REV = ("Reversed", "A functional density estimate, then catalysis on oxides.")
CAPTURE = ("Capture", "Amines bind CO_2 strongly on TiO_{2} supports.")
PLAIN = ("Plain", "Amines bind CO2 strongly on TiO2 supports.")
SNAKE = ("Snake", "The file_2024_report and my_var hold H2O counts.")


@pytest.fixture
def db(tmp_path):
    d = CrucibleDB(tmp_path / "c.db")
    d.initialize()
    for i, (title, body) in enumerate((DFT, POLY, REV, CAPTURE, PLAIN, SNAKE)):
        d.add_article(path=f".crucible/wiki/concepts/a{i}.org", title=title,
                      article_type="concept", content=body)
    yield d
    d.close()


def titles(rows):
    return sorted(r["title"] for r in rows)


# -- 1. quoted phrases --------------------------------------------------------

def test_phrase_requires_adjacency(db):
    assert titles(db.search('"density functional"')) == ["DFT"]


def test_reversed_phrase(db):
    assert titles(db.search('"functional density"')) == ["Reversed"]


def test_phrase_plus_term(db):
    assert titles(db.search('"functional density" catalysis')) == ["Reversed"]
    assert titles(db.search('"density functional" groups')) == []


def test_phrase_plus_prefix_term(db):
    # catal* goes through the prefix post-filter and the broad fallback.
    assert titles(db.search('"functional density" catal*')) == ["Reversed"]
    assert titles(db.search('"density functional" catal*')) == []


def test_unbalanced_quote_is_literal(db):
    # A quote with no partner is ignored; the words are searched as words.
    assert titles(db.search('"catal*')) == ["Reversed"]
    for q in ('"density functional', 'density functional"', '"density',
              '"density" "functional'):
        assert "DFT" in titles(db.search(q)), q
    for q in ('"', '"""', '""', '"*"'):
        assert db.search(q) == [], q


def test_curly_quotes_are_a_phrase(db):
    assert titles(db.search("“density functional”")) == ["DFT"]


def test_phrase_in_cli_search(tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    _make_project(tmp_path / "kb", monkeypatch, {"dft.org": DFT, "poly.org": POLY})
    r = invoke("search", '"density functional"', "--mode", "fts")
    assert r.exit_code == 0, r.output
    assert "DFT" in r.output
    assert "Polymers" not in r.output


def test_phrase_in_hybrid_search(tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    fake = FakeOllama()
    try:
        root = _make_project(tmp_path / "kb", monkeypatch,
                             {"dft.org": DFT, "poly.org": POLY})
        assert invoke("embed", "--url", fake.url).exit_code == 0
        d = CrucibleDB(root / ".crucible" / "crucible.db")
        try:
            idx = EmbeddingIndex(d.conn, url=fake.url)
            hits = idx.hybrid_search('"density functional"', raw=True)
            via_fts = {h["title"] for h in hits
                       if any(m.startswith("fts") for m in h["methods"])}
            assert via_fts == {"DFT"}
        finally:
            d.close()
    finally:
        fake.server.shutdown()
        fake.server.server_close()


def test_phrase_in_browse_search(tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch,
                         {"dft.org": DFT, "poly.org": POLY})
    db_path = root / ".crucible" / "crucible.db"
    srv = start_server(root / ".crucible" / "wiki", db_path, [], port=0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        c = http.client.HTTPConnection("127.0.0.1", srv.server_address[1], timeout=5)
        c.request("GET", "/_search?q=%22density+functional%22",
                  headers={"Host": "localhost"})
        body = c.getresponse().read().decode()
        c.close()
    finally:
        srv.shutdown()
        srv.server_close()
    assert "dft.org" in body
    assert "poly.org" not in body


# -- 2. chemical subscripts ---------------------------------------------------

@pytest.mark.parametrize("text, want", [
    ("CO_2", "CO2"), ("TiO_{2}", "TiO2"), ("Fe_2O_3", "Fe2O3"),
    ("H_2O", "H2O"), ("(NH_4)_2SO_4", "(NH4)2SO4"), ("CO₂", "CO2"),
    ("file_2024_report", "file_2024_report"), ("my_var", "my_var"),
    ("x_2b", "x_2b"), ("_2", "_2"), ("a_{x}", "a_{x}"),
])
def test_fts_text(text, want):
    assert fts_text(text) == want
    assert fts_text(want) == want


@pytest.mark.parametrize("q", ["CO2", "TiO2", "CO_2", "TiO_{2}", "CO2*",
                               '"bind CO2 strongly"', '"bind CO_2 strongly"'])
def test_subscripts_match_both_spellings(db, q):
    assert titles(db.search(q)) == ["Capture", "Plain"]


def test_snake_case_is_not_mangled(db):
    assert titles(db.search("file_2024_report")) == ["Snake"]
    assert titles(db.search("my_var")) == ["Snake"]
    assert titles(db.search("H_2O")) == ["Snake"]


def _v2_db(path, content):
    """A schema v2 database whose FTS row holds the text unjoined."""
    d = CrucibleDB(path)
    d.initialize()
    d.add_article(path=".crucible/wiki/concepts/c.org", title="Capture",
                  article_type="concept", content="x")
    d.close()
    c = sqlite3.connect(path)
    c.execute("UPDATE article_fts SET content = ?", (content,))
    c.execute("DELETE FROM schema_version")
    c.execute("INSERT INTO schema_version (version) VALUES (2)")
    c.commit()
    c.close()


def test_old_database_is_reindexed(tmp_path):
    p = tmp_path / "old.db"
    _v2_db(p, CAPTURE[1])
    d = CrucibleDB(p)
    try:
        assert d.schema_version() == 2
        # Before the upgrade, a query written with subscripts still matches.
        assert titles(d.search("CO_2")) == ["Capture"]
        assert titles(d.search("TiO_{2}")) == ["Capture"]
        d.initialize()
        assert d.schema_version() >= 3
        assert d.get_article_fts(1)["content"] == fts_text(CAPTURE[1])
        assert titles(d.search("CO2")) == ["Capture"]
        assert titles(d.search("TiO2")) == ["Capture"]
        assert titles(d.search("TiO_{2}")) == ["Capture"]
    finally:
        d.close()


def test_sync_reindexes_old_rows_without_counting_an_edit(
        tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch, {"c.org": CAPTURE})
    db_path = root / ".crucible" / "crucible.db"
    d = CrucibleDB(db_path)
    try:
        aid = d.get_article_by_path(".crucible/wiki/concepts/c.org")["id"]
        row = d.get_article_fts(aid)
        assert "CO2" in row["content"]
        # Put the row back as an index built before subscripts were joined.
        d.conn.execute("UPDATE article_fts SET content = ? WHERE rowid = ?",
                       (row["content"].replace("CO2", "CO_2")
                        .replace("TiO2", "TiO_{2}"), aid))
        d.conn.commit()
        assert titles(d.search("CO2")) == []
    finally:
        d.close()
    edits = []
    monkeypatch.setattr(CrucibleDB, "delete_embedding",
                        lambda self, article_id: edits.append(article_id))
    assert invoke("sync").exit_code == 0
    assert edits == []  # an unchanged file keeps its embedding
    d = CrucibleDB(db_path)
    try:
        assert d.get_article_fts(aid) == row
        assert titles(d.search("CO2")) == ["Capture"]
    finally:
        d.close()
    assert invoke("sync").exit_code == 0
    assert edits == []
