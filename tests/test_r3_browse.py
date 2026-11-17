"""Regression tests for the third review: viz, browse rendering and serving,
FTS search and `crucible embed` model resolution."""

import http.client
import json
import re
import threading

import pytest
from click.testing import CliRunner

from crucible.browse import org_to_html, start_server
from crucible.cli import _VIZ_TEMPLATE, cli
from crucible.database import CrucibleDB, _fts_query
from crucible.embeddings import embedding_text

from test_embeddings import PT, FakeOllama, _article, _make_project, _rows

EVIL = "Evil </script><script>alert(1)</script>"


def invoke(*args):
    return CliRunner().invoke(cli, list(args))


@pytest.fixture
def kb(tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    return _make_project(tmp_path / "kb", monkeypatch, {
        "evil.org": (EVIL, "Body cite:k1."),
        "pt.org": ("Platinum", PT),
        "bio.org": ("Enzymes", "Enzymes fold proteins in living cells."),
    })


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
    body = r.read()
    headers = dict(r.getheaders())
    c.close()
    return r.status, headers, body


def _graph_data(page: str) -> dict:
    m = re.search(r"^const data = (.*);$", page, re.MULTILINE)
    return json.loads(m.group(1))


# -- 1-4: knowledge graph ------------------------------------------------------

def test_viz_json_cannot_break_out_of_the_script(kb):
    r = invoke("viz")
    assert r.exit_code == 0, r.output
    page = (kb / ".crucible/wiki/viz.html").read_text()
    assert "</script><script>alert(1)" not in page
    # Exactly the template's own script elements close.
    assert page.count("</script>") == _VIZ_TEMPLATE.count("</script>")
    # The escaped JSON is still the same data.
    labels = {n["label"] for n in _graph_data(page)["nodes"]}
    assert EVIL in labels


def test_served_graph_cannot_break_out_of_the_script(server):
    status, _, body = get(server, "/_graph")
    page = body.decode()
    assert status == 200
    assert "<script>alert(1)</script>" not in page
    assert page.count("</script>") == _VIZ_TEMPLATE.count("</script>")


def test_viz_tooltip_does_not_insert_html():
    assert ".html(" not in _VIZ_TEMPLATE
    assert "innerHTML" not in _VIZ_TEMPLATE
    assert 'tooltip.append("strong").text(d.label)' in _VIZ_TEMPLATE


def test_viz_colours_have_no_duplicate_key_and_match_legend(kb):
    m = re.search(r"const colors = \{(.*?)\};", _VIZ_TEMPLATE, re.DOTALL)
    pairs = re.findall(r'"?(\w+)"?:\s*"(#[0-9a-f]{6})"', m.group(1))
    keys = [k for k, _ in pairs]
    assert len(keys) == len(set(keys)), keys
    colors = dict(pairs)
    legend = dict(re.findall(
        r'<span style="background:(#[0-9a-f]{6})"></span>([^<]+)</div>', _VIZ_TEMPLATE))
    assert legend[colors["concept"]] == "Concept article"
    assert legend[colors["topic"]] == "Topic tag"
    assert colors["concept"] != colors["topic"]
    # Topic (concept-tag) nodes are grouped as topics, articles by type.
    invoke("viz")
    data = _graph_data((kb / ".crucible/wiki/viz.html").read_text())
    groups = {n["group"] for n in data["nodes"] if "path" not in n}
    assert groups <= {"topic"}
    assert {n["group"] for n in data["nodes"] if "path" in n} == {"concept"}


def test_served_graph_is_live_and_clicks_open_browse_pages(kb, server):
    status, _, body = get(server, "/_graph")
    assert 'const linkMode = "browse";' in body.decode()
    (kb / ".crucible/wiki/concepts/new.org").write_text(_article("Newcomer", "x"))
    assert invoke("sync").exit_code == 0
    status, _, body = get(server, "/_graph")
    assert status == 200
    assert "Newcomer" in body.decode()


def test_viz_file_clicks_open_vscode(kb):
    invoke("viz")
    assert 'const linkMode = "vscode";' in (kb / ".crucible/wiki/viz.html").read_text()


def test_vscode_and_browse_refresh_a_stale_viz(kb, monkeypatch, tmp_path):
    viz = kb / ".crucible/wiki/viz.html"
    viz.write_text("stale")
    empty = tmp_path / "empty-bin"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))  # no `code`
    invoke("vscode")
    assert "Platinum" in viz.read_text()

    viz.write_text("stale")

    def no_server(*a, **kw):  # browse returns after writing the graph
        raise OSError(98, "Address in use")

    monkeypatch.setattr("crucible.browse.start_server", no_server)
    r = invoke("browse", "--no-open", "-p", "1")
    assert r.exit_code == 1
    assert "Platinum" in viz.read_text()


# -- 5, 9, 10: rendering -------------------------------------------------------

NESTED = [
    "Energy ~$E_a$~ here.",
    "Use =[[https://x.org]]= syntax.",
    "Write ~[[file:x.org][X]]~ for links.",
    "In ~see cite:k1~ form.",
    "Ollama at =http://localhost:11434/api/embed= by default.",
    "Display $$a [[file:x.org][X]] b$$ math.",
    "Inline $x$ with ~code $y$~ and =cite:k1= and [[https://a.org][~b~]].",
    "#+BEGIN_QUOTE\nsaid:\n#+BEGIN_SRC python\nprint(1)\n#+END_SRC\n#+END_QUOTE\n",
    "* H\n:RESULTS:\n#+BEGIN_EXAMPLE\nout 42\n#+END_EXAMPLE\n:END:\n",
    "#+BEGIN_QUOTE\n#+BEGIN_EXAMPLE\nx\n#+END_EXAMPLE\n| cite:k1 | ~c~ |\n#+END_QUOTE\n",
    "| ~[[file:x.org]]~ | =$a$= |\n",
    "Raw \x00BLOCK0\x00 and \x01I0\x01 bytes in the source.",
    "* Heading with ~code~ and $m$ :tag:\n- item =v= cite:k1\n",
]


@pytest.mark.parametrize("src", NESTED)
def test_no_placeholder_reaches_the_output(src):
    _, out, _ = org_to_html(src, "concepts/a.org", bib={"k1": {"title": "T"}})
    assert "\x00" not in out and "\x01" not in out, repr(out)


@pytest.mark.parametrize("src, expected", [
    ("Energy ~$E_a$~ here.", "<code>$E_a$</code>"),
    ("Use =[[https://x.org]]= syntax.", "<code>[[https://x.org]]</code>"),
    ("Write ~[[file:x.org][X]]~ for links.", "<code>[[file:x.org][X]]</code>"),
    ("In ~see cite:k1~ form.", "<code>see cite:k1</code>"),
    ("At =http://x.org/a= now.", "<code>http://x.org/a</code>"),
])
def test_code_and_verbatim_are_literal(src, expected):
    _, out, _ = org_to_html(src, "concepts/a.org")
    assert expected in out


def test_src_block_inside_quote_is_rendered():
    src = "#+BEGIN_QUOTE\nsaid:\n#+BEGIN_SRC python\nprint(1)\n#+END_SRC\n#+END_QUOTE\n"
    _, out, _ = org_to_html(src, "concepts/a.org")
    assert "<blockquote>" in out
    assert '<pre><code class="language-python">print(1)' in out


def test_example_inside_drawer_keeps_its_content():
    src = "* H\n:RESULTS:\n#+BEGIN_EXAMPLE\nout 42\n#+END_EXAMPLE\n:END:\n"
    _, out, _ = org_to_html(src, "concepts/a.org")
    assert "out 42" in out


def test_table_cells_get_inline_markup():
    src = "| a | b |\n|---+---|\n| cite:k1 | [[file:x.org][X]] |\n| <b> | ~c~ |\n"
    _, out, _ = org_to_html(src, "concepts/a.org", bib={"k1": {"title": "T"}})
    assert 'class="citation" href="/_references#k1"' in out
    assert '<a href="/concepts/x.org">X</a>' in out
    assert "&lt;b&gt;" in out and "<b>" not in out
    assert "<code>c</code>" in out


def test_deep_headings_cap_at_h6():
    _, out, _ = org_to_html("***** deep\ntext\n****** deeper\n")
    assert '<h6 id="deep">deep</h6>' in out
    assert '<h6 id="deeper">deeper</h6>' in out


def test_heading_tags_are_not_part_of_title_or_id():
    _, out, _ = org_to_html("* Results :noexport:draft:\nbody")
    assert 'id="results"' in out
    assert ":noexport:" not in out
    assert '<span class="tag">noexport</span>' in out


def test_indented_star_bullets_are_a_list():
    _, out, _ = org_to_html("Items:\n  * one\n  * two\n")
    assert "<ul><li>one</li><li>two</li></ul>" in out


def test_comments_are_not_rendered():
    src = ("# a comment line\ntext\n#\n"
           "#+begin_comment\nhidden block\n#+end_comment\nafter\n")
    _, out, _ = org_to_html(src)
    assert "comment" not in out and "hidden" not in out
    assert "text" in out and "after" in out


# -- 11: images ------------------------------------------------------------------

def test_image_links_render_as_img():
    _, out, _ = org_to_html("See [[file:fig.png]] and [[file:fig.png][the figure]].",
                            "concepts/a.org")
    assert '<img src="/concepts/fig.png" alt="fig.png">' in out
    assert '<a href="/concepts/fig.png">the figure</a>' in out


def test_images_in_the_wiki_are_served(kb, server):
    concepts = kb / ".crucible/wiki/concepts"
    (concepts / "fig.png").write_bytes(b"\x89PNG\r\n")
    (concepts / "fig.svg").write_text("<svg xmlns='http://www.w3.org/2000/svg'/>")
    (concepts / "notes.txt").write_text("secret")
    status, headers, body = get(server, "/concepts/fig.png")
    assert status == 200 and body == b"\x89PNG\r\n"
    assert headers["Content-Type"] == "image/png"
    status, headers, _ = get(server, "/concepts/fig.svg")
    assert status == 200
    assert headers["Content-Type"] == "image/svg+xml"
    assert "sandbox" in headers["Content-Security-Policy"]
    assert get(server, "/concepts/notes.txt")[0] == 403
    assert get(server, "/concepts/missing.png")[0] == 404


def test_image_symlink_out_of_the_wiki_is_refused(kb, server, tmp_path):
    outside = tmp_path / "outside.png"
    outside.write_bytes(b"\x89PNG secret")
    (kb / ".crucible/wiki/concepts/link.png").symlink_to(outside)
    assert get(server, "/concepts/link.png")[0] == 403
    assert get(server, "/../../outside.png")[0] in (400, 403, 404)


# -- 6, 12: keyword search ---------------------------------------------------------

@pytest.mark.parametrize("q", ["wiki", "crucible", "org", "concepts", "summaries"])
def test_path_words_do_not_match_every_article(tmp_path, q):
    db = CrucibleDB(tmp_path / "c.db")
    db.initialize()
    try:
        db.add_article(".crucible/wiki/concepts/a.org", "Catalysis", "concept",
                       content="Heterogeneous catalysis on Pt(111).")
        db.add_article(".crucible/wiki/summaries/b.org", "DFT", "summary",
                       content="Density functional theory.")
        db.conn.commit()
        assert db.search(q) == []
        assert [r["title"] for r in db.search("catalysis")] == ["Catalysis"]
    finally:
        db.close()


def test_prefix_search(kb):
    db = CrucibleDB(kb / ".crucible" / "crucible.db")
    try:
        assert [r["title"] for r in db.search("catal*")] == ["Platinum"]
        assert [r["title"] for r in db.search("cataly*")] == ["Platinum"]
        assert [r["title"] for r in db.search("plat* hydrog*")] == ["Platinum"]
        # Still no FTS syntax injection.
        for q in ["*", "-*", '"*', "(*", "NOT*", "a* OR*", ")*", "*catal",
                  "AND", "catal* AND", "OR catal*", "NEAR(a b)"]:
            db.search(q)
    finally:
        db.close()
    assert _fts_query("*") == ""
    assert _fts_query('a"b*') == '{title content} : ("a""b"*)'


def test_search_all_skips_path_words_in_peers(tmp_path, registry, monkeypatch):
    peer = _make_project(tmp_path / "peer", monkeypatch, {"p.org": ("Peer", PT)})
    local = _make_project(tmp_path / "kb", monkeypatch, {"l.org": ("Local", PT)})
    db = CrucibleDB(local / ".crucible" / "crucible.db")
    try:
        peers = [{"name": "peer", "path": str(peer)}]
        assert db.search_all("wiki", peers=peers) == []
        hits = db.search_all("catal*", peers=peers)
        assert {r["_crucible"] for r in hits} == {"local", "peer"}
    finally:
        db.close()


# -- 7: embed keeps the stored model ----------------------------------------------

def test_plain_embed_keeps_the_stored_model(kb):
    fake = FakeOllama(dims={"mxbai": 1024})
    try:
        r = invoke("embed", "-m", "mxbai", "--url", fake.url)
        assert r.exit_code == 0, r.output
        bio = kb / ".crucible/wiki/concepts/bio.org"
        bio.write_text(bio.read_text() + "\nMore text.\n")
        assert invoke("sync").exit_code == 0
        r = invoke("embed", "--url", fake.url)
        assert r.exit_code == 0, r.output
        assert "Embedding with mxbai" in r.output
        assert {row["model"] for row in _rows(kb)} == {"mxbai"}
        assert set(fake.models()) == {"mxbai"}
    finally:
        fake.server.shutdown()
        fake.server.server_close()


def test_first_embed_uses_the_default_model(kb):
    fake = FakeOllama()
    try:
        r = invoke("embed", "--url", fake.url)
        assert r.exit_code == 0, r.output
        assert {row["model"] for row in _rows(kb)} == {"nomic-embed-text"}
    finally:
        fake.server.shutdown()
        fake.server.server_close()


# -- 8: embedding text keeps the body ----------------------------------------------

def test_embedding_text_does_not_swallow_body_before_a_later_drawer():
    doc = ("#+TITLE: T\n\n* Intro\nKey terms:\n:catalysis:\n"
           "The main body about platinum catalysis goes here.\n\n"
           "* Log\n:LOGBOOK:\n- note\n:END:\n")
    text = embedding_text(doc)
    assert "The main body about platinum catalysis" in text
    assert "- note" not in text
