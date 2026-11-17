"""Regression tests for the fourth review: browse escaping and links,
schema migrations, FTS query building, hybrid search across a peer named
"local", DOT export and concept lookup."""

import http.client
import re
import sqlite3
import threading

import pytest
from test_embeddings import FakeOllama, _make_project, invoke

from crucible import browse
from crucible.browse import (
    _build_index_html,
    _build_nav_html,
    _build_topics_html,
    org_to_html,
    start_server,
)
from crucible.database import SCHEMA_VERSION, CrucibleDB, _fts_query
from crucible.embeddings import EmbeddingIndex

XSS = '<img src=x onerror=alert(1)>'


# -- 1. stored XSS through article_type / status ------------------------------

def _evil_articles():
    return [
        {"path": ".crucible/wiki/concepts/a.org", "title": "A",
         "article_type": XSS, "status": XSS},
        {"path": ".crucible/wiki/concepts/b.org", "title": "B",
         "article_type": "concept", "status": XSS},
    ]


@pytest.mark.parametrize("build", [_build_nav_html, _build_index_html])
def test_article_type_and_status_are_escaped(build):
    page = build(_evil_articles())
    assert "<img" not in page.lower()
    assert "&lt;img" in page.lower()


def test_topics_escape_article_type():
    page = _build_topics_html(
        [{"name": "t", "article_count": 1}],
        {"t": [{"path": "concepts/a.org", "title": "A", "article_type": XSS}]})
    assert "<img" not in page
    assert "&lt;img" in page


def _write(root, name, title, body, atype="concept", status="draft"):
    (root / ".crucible" / "wiki" / "concepts" / name).write_text(
        f"#+TITLE: {title}\n#+FILETAGS: :{atype}:\n\n* {title}\n:PROPERTIES:\n"
        f":ARTICLE_TYPE: {atype}\n:STATUS: {status}\n:CREATED: 2026-01-01\n"
        f":END:\n\n{body}\n")


@pytest.fixture
def kb(tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch, {})
    _write(root, "evil.org", "Evil", "Body.",
           atype="<script>alert(1)</script>", status="<b>x</b>")
    _write(root, "ok.org", "Ok", "Body.", status="<b>x</b>")
    assert invoke("sync").exit_code == 0
    return root


@pytest.fixture
def server(kb):
    db_path = kb / ".crucible" / "crucible.db"
    db = CrucibleDB(db_path)
    articles = db.list_articles()
    db.close()
    srv = start_server(kb / ".crucible" / "wiki", db_path, articles, port=0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv
    srv.shutdown()
    srv.server_close()


def get(srv, path):
    c = http.client.HTTPConnection("127.0.0.1", srv.server_address[1], timeout=5)
    c.request("GET", path, headers={"Host": "localhost"})
    r = c.getresponse()
    body = r.read().decode("utf-8")
    c.close()
    return r.status, body


@pytest.mark.parametrize("path", ["/", "/_topics", "/concepts/ok.org"])
def test_served_pages_escape_db_values(server, path):
    status, body = get(server, path)
    assert status == 200
    assert "<script>alert(1)" not in body.lower()
    assert "<b>x</b>" not in body.lower()


def test_graph_failure_is_an_error_page(server, monkeypatch):
    import crucible.cli

    def boom(*a, **kw):
        raise RuntimeError("graph broke")

    monkeypatch.setattr(crucible.viz, "viz_html", boom)
    status, body = get(server, "/_graph")
    assert status == 500
    assert "graph broke" in body
    assert get(server, "/_graph")[0] == 500  # server still answers


# -- 2. migrations run on an older database -----------------------------------

def test_v1_database_is_migrated(tmp_path):
    p = tmp_path / "v1.db"
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY,"
              " applied_at TEXT NOT NULL DEFAULT (datetime('now')))")
    c.execute("INSERT INTO schema_version (version) VALUES (1)")
    c.execute("CREATE TABLE peers (name TEXT, path TEXT)")
    c.commit()
    c.close()
    db = CrucibleDB(p)
    try:
        db.initialize()
        assert db.schema_version() == SCHEMA_VERSION
        assert db.conn.execute(
            "SELECT name FROM sqlite_master WHERE name = 'peers'").fetchone() is None
    finally:
        db.close()


def test_unversioned_old_database_is_migrated(tmp_path):
    p = tmp_path / "old.db"
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE peers (name TEXT, path TEXT)")
    c.commit()
    c.close()
    db = CrucibleDB(p)
    try:
        db.initialize()
        assert db.schema_version() == SCHEMA_VERSION
        assert db.conn.execute(
            "SELECT name FROM sqlite_master WHERE name = 'peers'").fetchone() is None
    finally:
        db.close()


def test_new_database_runs_no_migration(tmp_path, monkeypatch):
    ran = []
    monkeypatch.setattr(CrucibleDB, "_migrate_v2", lambda self: ran.append(1))
    db = CrucibleDB(tmp_path / "new.db")
    try:
        db.initialize()
        assert ran == []
        assert db.schema_version() == SCHEMA_VERSION
        db.initialize()  # idempotent on a current database
        assert ran == []
    finally:
        db.close()


# -- 3. FTS: punctuation tokens and prefixes ----------------------------------

@pytest.fixture
def fts_db(tmp_path):
    db = CrucibleDB(tmp_path / "c.db")
    db.initialize()
    for i, (title, text) in enumerate([
        ("Catalysis", "Heterogeneous catalysis of heat and mass transfer."),
        ("Catalyst", "A catalyst lowers the barrier."),
        ("Catastrophe", "A catastrophe with a catapult."),
    ]):
        db.add_article(f".crucible/wiki/concepts/{i}.org", title, "concept",
                       content=text)
    yield db
    db.close()


@pytest.mark.parametrize("q", ["heat & mass", "heat - mass", "heat / mass",
                               "heat – mass", "heat . mass", "heat -- mass"])
def test_punctuation_tokens_are_ignored(fts_db, q):
    assert [r["title"] for r in fts_db.search(q)] == ["Catalysis"]


def test_punctuation_only_query_is_empty():
    assert _fts_query("& - / – .") == ""
    assert _fts_query("café &") == '{title content} : ("café")'


def test_prefix_does_not_overmatch(fts_db):
    titles = {r["title"] for r in fts_db.search("catal*")}
    assert titles == {"Catalysis", "Catalyst"}
    assert fts_db.search("catapu*")[0]["title"] == "Catastrophe"


# -- 4. hybrid search across a peer named "local" -----------------------------

def test_peer_named_local_does_not_collide(tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    fake = FakeOllama()
    try:
        peer = _make_project(tmp_path / "peer", monkeypatch, {
            "p.org": ("Peer platinum", "Platinum electrodes in fuel cells.")})
        assert invoke("embed", "--url", fake.url).exit_code == 0
        local = _make_project(tmp_path / "kb", monkeypatch, {
            "l.org": ("Local platinum", "Platinum nanoparticles on carbon.")})
        assert invoke("embed", "--url", fake.url).exit_code == 0
        db = CrucibleDB(local / ".crucible" / "crucible.db")
        try:
            idx = EmbeddingIndex(db.conn, url=fake.url)
            peers = [{"name": "local", "path": str(peer)}]
            for raw in (True, False):
                hits = idx.hybrid_search_all("platinum", peers=peers, raw=raw)
                assert sorted(h["title"] for h in hits) == [
                    "Local platinum", "Peer platinum"], raw
                assert {h["_crucible"] for h in hits} == {"local"}
        finally:
            db.close()
    finally:
        fake.server.shutdown()
        fake.server.server_close()


# -- 5. prefix-less file links render as wiki links ---------------------------

@pytest.mark.parametrize("link, href", [
    ("[[./b.org][B]]", "/concepts/b.org"),
    ("[[../methods/y.org][Y]]", "/methods/y.org"),
    ("[[b.org]]", "/concepts/b.org"),
    ("[[file:b.org][B]]", "/concepts/b.org"),
])
def test_prefixless_file_links(link, href):
    _, body, _ = org_to_html(f"See {link}.", "concepts/a.org")
    assert f'<a href="{href}">' in body


@pytest.mark.parametrize("link", ["[[javascript:alert(1)][x]]",
                                  "[[javascript:alert(1).org][x]]",
                                  "[[data:text/html,hi][x]]"])
def test_other_schemes_stay_inert(link):
    _, body, _ = org_to_html(f"See {link}.", "concepts/a.org")
    assert "href=\"javascript" not in body
    assert "href=\"data" not in body


# -- 6. a cite inside a link target is literal --------------------------------

def test_cite_in_link_target_is_literal():
    _, body, _ = org_to_html(
        "Go [[https://example.com/ cite:foo][desc]] and cite:bar.", "a.org")
    assert '<a href="https://example.com/ cite:foo" target="_blank">desc</a>' in body
    assert 'href="/_references#bar"' in body
    assert "_references#foo" not in body
    # Every href attribute is a single well-formed value.
    for attr in re.findall(r'href="([^"]*)"', body):
        assert "<" not in attr


def test_bracketed_citation_still_renders():
    _, body, _ = org_to_html("As [[cite:&smith2024][Smith]] shows.", "a.org")
    assert 'href="/_references#smith2024"' in body
    assert "link-unknown" not in body


# -- 7. graph_dot ---------------------------------------------------------------

def test_graph_dot_escapes_and_keeps_same_titles_apart(tmp_path):
    db = CrucibleDB(tmp_path / "g.db")
    db.initialize()
    try:
        a = db.add_article("a.org", "Back\\", "concept")
        b = db.add_article("b.org", 'Quote "q"', "concept")
        c1 = db.add_article("c1.org", "Same", "concept")
        c2 = db.add_article("c2.org", "Same", "concept")
        db.add_article_link(a, b)
        db.add_article_link(c1, c2)
        dot = db.graph_dot()
    finally:
        db.close()
    assert 'label="Back\\\\"' in dot
    assert 'label="Quote \\"q\\""' in dot
    assert f"a{c1} -> a{c2};" in dot
    assert dot.count('label="Same"') == 2
    # Every quoted string closes: strip escapes, then quotes must pair up.
    for line in dot.splitlines():
        assert re.sub(r'\\.', "", line).count('"') % 2 == 0, line


# -- concept_articles caches its key scan --------------------------------------

def test_concept_articles_caches_and_sees_writes(tmp_path):
    p = tmp_path / "c.db"
    db = CrucibleDB(p)
    db.initialize()
    try:
        art = db.add_article("a.org", "A", "concept")
        db.link_article_concept(art, db.add_concept("activation_energy"))
        assert [r["title"] for r in db.concept_articles("Activation Energy")] == ["A"]
        cache = db._concept_key_cache
        db.concept_articles("activation-energy")
        assert db._concept_key_cache is cache  # no rescan without writes
        # A write on this connection is seen.
        art2 = db.add_article("b.org", "B", "concept")
        db.link_article_concept(art2, db.add_concept("Activation-Energy"))
        assert [r["title"] for r in db.concept_articles("activation energy")] == ["A", "B"]
        # A write from another connection is seen.
        other = CrucibleDB(p)
        try:
            art3 = other.add_article("c.org", "C", "concept")
            other.link_article_concept(art3, other.add_concept("ACTIVATION ENERGY"))
        finally:
            other.close()
        assert [r["title"] for r in db.concept_articles("activation energy")] == [
            "A", "B", "C"]
    finally:
        db.close()


def test_browse_module_uses_orgparse_link_pattern():
    from crucible.orgparse import FILE_LINK_RE
    assert browse._FILE_LINK_RE is FILE_LINK_RE


# -- review of #41: one concept cache, cleared by this connection's writes --

def test_concept_cache_is_defined_once_and_follows_own_writes(tmp_path):
    import inspect

    import crucible.database as dbmod
    source = inspect.getsource(dbmod.CrucibleDB)
    assert source.count("def _concept_ids_by_key") == 1
    db = CrucibleDB(tmp_path / "c.db")
    db.initialize()
    try:
        aid = db.add_article(path="a.org", title="A", article_type="concept")
        cid = db.add_concept("activation_energy")
        db.link_article_concept(aid, cid)
        assert [a["title"] for a in db.concept_articles("activation energy")] == ["A"]
        # Written on this connection without going through add_concept.
        db.conn.execute("INSERT INTO concepts (name) VALUES ('rate law')")
        new = db.conn.execute("SELECT id FROM concepts WHERE name = 'rate law'").fetchone()["id"]
        db.link_article_concept(aid, new)
        assert [a["title"] for a in db.concept_articles("rate-law")] == ["A"]
    finally:
        db.close()


def test_graph_dot_lists_only_linked_articles(tmp_path):
    db = CrucibleDB(tmp_path / "g.db")
    db.initialize()
    try:
        a = db.add_article(path="a.org", title="A", article_type="concept")
        b = db.add_article(path="b.org", title="B", article_type="concept")
        db.add_article(path="c.org", title="Lonely", article_type="concept")
        db.add_article_link(a, b)
        dot = db.graph_dot()
        assert f"a{a} -> a{b};" in dot
        assert "Lonely" not in dot
    finally:
        db.close()
