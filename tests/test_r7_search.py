"""Regression tests for search: quoted phrases are searched as phrases, and
chemical subscripts (CO_2, TiO_{2}) match their plain spelling (CO2)."""

import http.client
import sqlite3
import threading

import pytest
from test_embeddings import FakeOllama, _make_project, invoke

from crucible.browse import start_server
from crucible.database import CrucibleDB, fts_joined, join_subscripts
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
def test_join_subscripts(text, want):
    assert join_subscripts(text) == want
    assert join_subscripts(want) == want


def test_fts_joined_holds_the_paragraphs_with_subscripts():
    # The title and content are indexed as written; the paragraphs that
    # change are indexed once more, joined, in a column of their own.
    text = "Amines bind CO_2 on TiO_{2}.\n\nNo subscript.\n\nThen H_2O."
    assert fts_joined("On CO_2", text) == (
        "On CO2\n\nAmines bind CO2 on TiO2.\n\nThen H2O.")
    assert fts_joined("Plain", "no subscripts here") == ""


def test_joined_column_does_not_change_the_ranking(tmp_path):
    # Rows are found and ranked by article_fts as on main: an article with
    # a subscript (sample_3) is not ranked by its joined copy as well.
    arts = [("Catalysis review", "Catalysis on oxide surfaces is reviewed here"
             " in some detail with examples."),
            ("Run log", "Catalysis on oxide surfaces is reviewed here in some"
             " detail with sample_3.")]
    arts += [(f"Filler {i}", "Unrelated words about polymers and heat.") for i in range(8)]
    d = CrucibleDB(tmp_path / "c.db")
    d.initialize()
    plain = sqlite3.connect(":memory:")
    plain.execute("CREATE VIRTUAL TABLE article_fts USING fts5("
                  "path, title, content, tokenize='porter unicode61')")
    try:
        for i, (t, c) in enumerate(arts, start=1):
            path = f".crucible/wiki/concepts/a{i}.org"
            d.add_article(path=path, title=t, article_type="concept", content=c)
            plain.execute("INSERT INTO article_fts (rowid, path, title, content)"
                          " VALUES (?, ?, ?, ?)", (i, path, t, c))
        for q in ("catalysis", "oxide surfaces", "detail"):
            want = [(arts[r[0] - 1][0], r[1]) for r in plain.execute(
                "SELECT rowid, rank FROM article_fts WHERE article_fts MATCH ? ORDER BY rank",
                ("{title content} : (" + q + ")",))]
            assert [(r["title"], r["rank"]) for r in d.search(q)] == want, q
        assert titles(d.search("sample3")) == ["Run log"]
    finally:
        plain.close()
        d.close()


def test_words_with_digit_subscripts_are_still_found_by_their_parts(
        tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch, {
        "a.org": ("Steps", "See step_1 of report_2024 and $x_2$.")})
    d = CrucibleDB(root / ".crucible" / "crucible.db")
    try:
        for q in ("step", "2024", "report 2024", "x", "step_1", "step1",
                  '"report_2024"'):
            assert titles(d.search(q)) == ["Steps"], q
    finally:
        d.close()


def test_phrase_with_joined_subscript_finds_text_written_with_one(
        tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch, {"c.org": CAPTURE})
    d = CrucibleDB(root / ".crucible" / "crucible.db")
    try:
        assert titles(d.search('"bind CO2 strongly"')) == ["Capture"]
        assert titles(d.search('"bind CO_2 strongly"')) == ["Capture"]
        assert titles(d.search('"strongly CO2"')) == []
    finally:
        d.close()


def test_sync_counts_an_edit_that_only_changes_a_subscript(
        tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch, {"c.org": CAPTURE})
    f = root / ".crucible" / "wiki" / "concepts" / "c.org"
    f.write_text(f.read_text().replace("CO_2", "CO2"))
    edits = []
    monkeypatch.setattr(CrucibleDB, "delete_embedding",
                        lambda self, article_id: edits.append(article_id))
    result = invoke("sync")
    assert result.exit_code == 0
    assert "1 updated" in result.output
    assert len(edits) == 1


@pytest.mark.parametrize("q", ["CO2", "TiO2", "CO_2", "TiO_{2}", "CO2*",
                               '"bind CO2 strongly"', '"bind CO_2 strongly"'])
def test_subscripts_match_both_spellings(db, q):
    assert titles(db.search(q)) == ["Capture", "Plain"]


def test_snake_case_is_not_mangled(db):
    assert titles(db.search("file_2024_report")) == ["Snake"]
    assert titles(db.search("my_var")) == ["Snake"]
    assert titles(db.search("H_2O")) == ["Snake"]


def _as_v2(path):
    """Make the database at PATH a schema v2 one, as main builds it, with
    no article_fts_joined."""
    c = sqlite3.connect(path)
    c.execute("DROP TABLE article_fts_joined")
    c.execute("DELETE FROM schema_version")
    c.execute("INSERT INTO schema_version (version) VALUES (2)")
    c.commit()
    c.close()


def _v2_db(path, content):
    """A schema v2 database with one article."""
    d = CrucibleDB(path)
    d.initialize()
    d.add_article(path=".crucible/wiki/concepts/c.org", title="Capture",
                  article_type="concept", content=content)
    d.close()
    _as_v2(path)


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
        row = d.get_article_fts(1)
        assert row["content"] == CAPTURE[1]
        assert row["joined"] == ("Capture", CAPTURE[1], fts_joined("Capture", CAPTURE[1]))
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
        assert "CO2" in row["joined"][2]
        # An article indexed without its row in article_fts_joined.
        d.conn.execute("DELETE FROM article_fts_joined WHERE rowid = ?", (aid,))
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


def test_prefix_on_a_snake_case_word(tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch, {
        "a.org": ("Runs", "See file_2024_report and run_2b, and CO_2 too.")})
    d = CrucibleDB(root / ".crucible" / "crucible.db")
    try:
        for q in ("file_2024*", "run_2*", "CO2*", "CO_2*", "file*"):
            assert titles(d.search(q)) == ["Runs"], q
    finally:
        d.close()


@pytest.mark.parametrize("written", ["CO_2", "CO_{2}", "CO\u2082", "CO2"])
@pytest.mark.parametrize("typed", ["CO2", "CO_2", "co\u2082"])
def test_phrase_finds_each_way_of_writing_a_subscript(
        tmp_path, registry, monkeypatch, written, typed):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch, {
        "a.org": ("Red", f"Electrochemical {written} reduction on Cu.")})
    d = CrucibleDB(root / ".crucible" / "crucible.db")
    try:
        assert titles(d.search(f'"{typed} reduction"')) == ["Red"]
    finally:
        d.close()


@pytest.fixture
def phrase_db(tmp_path):
    d = CrucibleDB(tmp_path / "p.db")
    d.initialize()
    for i, (title, body) in enumerate((
            ("Cobalt", "Solutions of Co 2+ ions, then Python 3 scripts and Table 2."),
            ("Apart", "We measured CO_2 uptake. Later, unrelated, H_2O was added."),
            ("End", "Only at the end is CO_2 mentioned"),
            ("Water", "Photocatalytic H_2O splitting on Fe_2O_3 rust."))):
        d.add_article(path=f".crucible/wiki/concepts/p{i}.org", title=title,
                      article_type="concept", content=body)
    yield d
    d.close()


@pytest.mark.parametrize("q", ['"CO2"', '"Python3"', '"Table2"'])
def test_phrase_with_digits_does_not_match_them_written_apart(phrase_db, q):
    assert titles(phrase_db.search(q)) == ([] if q != '"CO2"' else ["Apart", "End"])


@pytest.mark.parametrize("q", ['"CO2 H2O"', '"mentioned CO2"', '"mentioned CO_2"'])
def test_phrase_does_not_run_across_the_joined_copy(phrase_db, q):
    assert titles(phrase_db.search(q)) == []


@pytest.mark.parametrize("q", ['"H2O splitting"', '"H₂O splitting"',
                               '"Fe2O3 rust"', '"H_2O splitting"'])
def test_phrase_with_a_subscript_inside_the_word(phrase_db, q):
    assert titles(phrase_db.search(q)) == ["Water"]


def test_migration_after_sync_does_not_reindex_twice(
        tmp_path, registry, monkeypatch):
    # A v2 database is migrated by sync; a later migration (update, init)
    # must leave the rows alone, and sync must not count an edit.
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch, {"c.org": CAPTURE})
    db_path = root / ".crucible" / "crucible.db"
    d = CrucibleDB(db_path)
    try:
        aid = d.get_article_by_path(".crucible/wiki/concepts/c.org")["id"]
        row = d.get_article_fts(aid)
    finally:
        d.close()
    _as_v2(db_path)
    edits = []
    monkeypatch.setattr(CrucibleDB, "delete_embedding",
                        lambda self, article_id: edits.append(article_id))
    result = invoke("sync")
    assert result.exit_code == 0 and "0 updated" in result.output, result.output
    assert edits == []
    d = CrucibleDB(db_path)
    try:
        # Sync leaves the v2 index as it is, and searches it as such.
        assert "joined" not in d.get_article_fts(aid)
        assert titles(d.search("CO_2")) == ["Capture"]
        d.initialize()
        assert d.get_article_fts(aid) == row
        assert titles(d.search("CO2")) == ["Capture"]
    finally:
        d.close()
    result = invoke("sync")
    assert "0 updated" in result.output
    assert edits == []


# -- regressions against main found after the merge ------------------------------

def _plain_fts(arts):
    """main's article_fts, for ARTS (title, content), rowids from 1."""
    plain = sqlite3.connect(":memory:")
    plain.execute("CREATE VIRTUAL TABLE article_fts USING fts5("
                  "path, title, content, tokenize='porter unicode61')")
    for i, (t, c) in enumerate(arts, start=1):
        plain.execute("INSERT INTO article_fts (rowid, path, title, content)"
                      " VALUES (?, ?, ?, ?)", (i, f"a{i}.org", t, c))
    return plain


@pytest.mark.parametrize("q", ["CO_2", "CO_{2}", "CO₂", "H_2O", "CO_2 reduction"])
def test_subscript_query_keeps_mains_rows_and_ranks_first(tmp_path, q):
    # A query with a subscript asks article_fts for it as typed, as main
    # did; the articles only its joined form finds (CO2, H2O) come after,
    # not above main's rows.
    arts = [("Literal", "CO_2 reduction and H_2O splitting, written out in a long"
             " sentence with many other words about electrodes and cells."),
            ("Unicode", "CO₂ reduction and H₂O splitting, written out in a long"
             " sentence with many other words about electrodes and cells."),
            ("Joined", "CO2 reduction. CO2. H2O. H2O.")]
    arts += [(f"Filler {i}", "Unrelated words about polymers and heat.") for i in range(8)]
    d = CrucibleDB(tmp_path / "c.db")
    d.initialize()
    plain = _plain_fts(arts)
    try:
        for i, (t, c) in enumerate(arts, start=1):
            d.add_article(path=f"a{i}.org", title=t, article_type="concept", content=c)
        match = "{title content} : (" + " AND ".join(
            '"' + w + '"' for w in q.split()) + ")"
        want = [(arts[r[0] - 1][0], r[1]) for r in plain.execute(
            "SELECT rowid, rank FROM article_fts WHERE article_fts MATCH ?"
            " ORDER BY rank", (match,))]
        got = [(r["title"], r["rank"]) for r in d.search(q)]
        assert got[:len(want)] == want
        assert "Joined" in [t for t, _ in got[len(want):]]
    finally:
        plain.close()
        d.close()


def test_search_all_puts_joined_only_rows_after_every_crucibles_rows(tmp_path):
    # Ranks from article_fts_joined do not compare with article_fts's, so
    # a local row only the joined form finds must not take a peer's place.
    def crucible(name, arts):
        root = tmp_path / name
        (root / ".crucible").mkdir(parents=True)
        d = CrucibleDB(root / ".crucible/crucible.db")
        d.initialize()
        for t, c in arts:
            d.add_article(path=f"{t}.org", title=t, article_type="concept", content=c)
        return root, d
    _, local = crucible("loc", [("plain", "CO2 emissions."), ("sub", "CO_2 capture.")]
                        + [(f"w{i}", "H_2O water vapour.") for i in range(6)])
    peer_root, peer = crucible("peer", [
        (f"p{i}", "CO2 mentioned once among many other filler words " * 3)
        for i in range(5)])
    peer.close()
    peers = [{"name": "peer", "path": str(peer_root)}]
    try:
        assert [r["title"] for r in local.search_all("CO2", limit=5, peers=peers)] == [
            "plain", "p0", "p1", "p2", "p3"]
        assert [r["title"] for r in local.search_all("CO2", limit=7, peers=peers)][-1] == "sub"
    finally:
        local.close()


@pytest.mark.parametrize("q", ["CO2*", "CO2* capture"])
def test_unmigrated_database_finds_subscripts_after_mains_rows(tmp_path, q):
    # With no article_fts_joined (a peer never updated), a prefix query's
    # broad candidates are still checked against the joined forms, after
    # the rows found as typed. (Plain CO2 needs `crucible update`.)
    _v2_db(tmp_path / "c.db", "Amines bind CO_2 strongly; CO_2 capture.")
    d = CrucibleDB(tmp_path / "c.db")
    try:
        d.add_article(path="p.org", title="Plain", article_type="concept",
                      content="CO2 capture with amines.")
        assert [r["title"] for r in d.search(q)] == ["Plain", "Capture"]
    finally:
        d.close()
