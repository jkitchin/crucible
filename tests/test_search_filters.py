"""`crucible search --tag/--type`: filtering search results by tags and types."""

import json

import pytest
from test_embeddings import FakeOllama, _make_project, invoke

from crucible.mcp_server import CrucibleService, Served, ServeError


@pytest.fixture
def ollama():
    fake = FakeOllama()
    yield fake
    fake.server.shutdown()
    fake.server.server_close()


def _tagged(title, body, tags=(), atype="concept"):
    tagline = ":".join((atype, *tags))
    return f"""#+TITLE: {title}
#+FILETAGS: :{tagline}:

* {title}
:PROPERTIES:
:ARTICLE_TYPE: {atype}
:STATUS: draft
:CREATED: 2026-01-01
:END:

{body}
"""


# (file, title, body, tags, type). The untagged articles say "barrier"
# more often, so they outrank the tagged ones without a filter.
ARTICLES = [
    ("loud1.org", "Loud one", "barrier barrier barrier barrier height", (), "concept"),
    ("loud2.org", "Loud two", "barrier barrier barrier barrier height", (), "concept"),
    ("loud3.org", "Loud three", "barrier barrier barrier barrier height", (), "concept"),
    ("dft.org", "DFT barriers", "A barrier height from DFT.", ("dft",), "concept"),
    ("dftz.org", "DFT zeolite barriers", "A barrier in a zeolite pore.",
     ("dft", "zeolite"), "method"),
    ("ea.org", "Activation barriers", "The barrier sets the rate.",
     ("activation_energy",), "summary"),
]


def _write(root, articles):
    wiki = root / ".crucible" / "wiki" / "concepts"
    for name, title, body, tags, atype in articles:
        (wiki / name).write_text(_tagged(title, body, tags, atype))
    r = invoke("sync")
    assert r.exit_code == 0, r.output


@pytest.fixture
def kb(tmp_path, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch, {})
    _write(root, ARTICLES)
    return root


def titles(*args):
    r = invoke("search", *args, "--json")
    assert r.exit_code == 0, r.output
    return [x["title"] for x in json.loads(r.stdout)]


def test_tag_filters_before_the_limit(kb):
    assert "DFT barriers" not in titles("barrier", "-n", "2")
    assert titles("barrier", "-n", "1", "--tag", "dft") in (
        ["DFT barriers"], ["DFT zeolite barriers"])
    assert set(titles("barrier", "--tag", "dft")) == {"DFT barriers",
                                                      "DFT zeolite barriers"}


def test_several_tags_are_all_required(kb):
    assert titles("barrier", "-t", "dft", "-t", "zeolite") == ["DFT zeolite barriers"]


def test_tags_match_like_concept_names(kb):
    assert titles("barrier", "--tag", "Activation Energy") == ["Activation barriers"]
    assert titles("barrier", "--tag", "DFT", "--tag", "activation-energy") == []


def test_type_filter_allows_any_of_several(kb):
    assert titles("barrier", "--type", "method") == ["DFT zeolite barriers"]
    assert set(titles("barrier", "--type", "Method", "--type", "summary")) == {
        "DFT zeolite barriers", "Activation barriers"}
    assert titles("barrier", "--type", "method", "--tag", "activation_energy") == []
    r = invoke("search", "barrier", "--type", "essay")
    assert r.exit_code == 2


def test_unknown_tag_warns(kb):
    r = invoke("search", "barrier", "--tag", "dtf")
    assert r.exit_code == 0
    assert "No results." in r.stdout
    assert "no article is tagged 'dtf'" in r.stderr
    r = invoke("search", "barrier", "--tag", "dft")
    assert "no article is tagged" not in r.stderr


@pytest.mark.parametrize("mode", ["semantic", "hybrid"])
def test_semantic_modes_filter(kb, ollama, mode):
    assert invoke("embed", "--url", ollama.url).exit_code == 0
    found = titles("barrier height", "--mode", mode, "--url", ollama.url,
                   "--raw", "--tag", "dft")
    assert set(found) == {"DFT barriers", "DFT zeolite barriers"}
    found = titles("barrier height", "--mode", mode, "--url", ollama.url,
                   "--raw", "--type", "summary")
    assert found == ["Activation barriers"]


def test_all_filters_every_crucible(kb, tmp_path, monkeypatch):
    peer = _make_project(tmp_path / "peer", monkeypatch, {})
    _write(peer, [
        ("p1.org", "Peer DFT", "A barrier from DFT.", ("dft",), "concept"),
        ("p2.org", "Peer loud", "barrier barrier barrier", (), "concept"),
    ])
    monkeypatch.chdir(kb)
    found = titles("barrier", "--all", "--tag", "dft")
    assert set(found) == {"DFT barriers", "DFT zeolite barriers", "Peer DFT"}
    # With --all a tag only a peer carries is no typo.
    r = invoke("search", "barrier", "--all", "--tag", "dft")
    assert "no article is tagged" not in r.stderr


@pytest.mark.parametrize("mode", ["semantic", "hybrid"])
def test_semantic_all_filters_peers(kb, ollama, tmp_path, monkeypatch, mode):
    peer = _make_project(tmp_path / "peer", monkeypatch, {})
    _write(peer, [
        ("p1.org", "Peer DFT", "A barrier height from DFT.", ("dft",), "concept"),
        ("p2.org", "Peer loud", "barrier height barrier", (), "concept"),
    ])
    assert invoke("embed", "--url", ollama.url).exit_code == 0
    monkeypatch.chdir(kb)
    assert invoke("embed", "--url", ollama.url).exit_code == 0
    found = titles("barrier height", "--all", "--mode", mode, "--url", ollama.url,
                   "--raw", "--tag", "dft")
    assert set(found) == {"DFT barriers", "DFT zeolite barriers", "Peer DFT"}


def test_mcp_search_filters(kb):
    s = CrucibleService([Served("kb", kb)])
    out = s.search("barrier", tags="dft zeolite")
    assert [r["title"] for r in out["results"]] == ["DFT zeolite barriers"]
    out = s.search("barrier", tags=["dft"], types="method, summary")
    assert [r["title"] for r in out["results"]] == ["DFT zeolite barriers"]
    out = s.search("barrier", limit=1, tags="activation_energy")
    assert [r["title"] for r in out["results"]] == ["Activation barriers"]
    with pytest.raises(ServeError):
        s.search("barrier", types="essay")


# -- the tag hierarchy (.crucible/taxonomy.org) ------------------------------

TAXONOMY = """#+TITLE: Taxonomy
#+TAGS: [ methods : electronic_structure molecular_dynamics ]
#+TAGS: [ electronic_structure : dft(d) coupled_cluster ]
#+TAGS: [ materials : porous ] { catalysts : zeolite }
#+TAGS: [ porous : zeolite mof {P@.+} ]
"""

HIER = [
    ("h1.org", "DFT barrier", "A barrier from DFT.", ("dft",), "concept"),
    ("h2.org", "CC barrier", "A barrier from coupled cluster.", ("coupled_cluster",),
     "concept"),
    ("h3.org", "Zeolite barrier", "A barrier in a zeolite.", ("zeolite",), "concept"),
    ("h4.org", "Enzyme barrier", "An enzyme lowers the barrier.", ("enzymes",),
     "concept"),
    ("h5.org", "Method barrier", "Methods compute a barrier.", ("methods",), "method"),
]


def test_parse_taxonomy():
    from crucible.taxonomy import parse_taxonomy
    pairs, warnings = parse_taxonomy(TAXONOMY)
    assert pairs == [
        ("methods", "electronic_structure"), ("methods", "molecular_dynamics"),
        ("electronic_structure", "dft"), ("electronic_structure", "coupled_cluster"),
        ("materials", "porous"), ("catalysts", "zeolite"),
        ("porous", "zeolite"), ("porous", "mof"),
    ]
    assert len(warnings) == 1 and "{P@.+}" in warnings[0]
    # Plain tags and mutually exclusive groups without a parent carry no
    # hierarchy; a #+TAGS: in a block of text is not one.
    assert parse_taxonomy("#+TAGS: dft zeolite { @home @work }\n"
                          "Some text #+TAGS: [ a : b ]\n") == ([], [])
    pairs, warnings = parse_taxonomy("#+tags: [a : b]\n#+TAGS: [ b : a B ]\n")
    assert pairs == [("a", "b"), ("b", "a")]
    assert any("under itself" in w for w in warnings)
    assert any("cycle" in w for w in warnings)


@pytest.fixture
def hier(tmp_path, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "hier", monkeypatch, {})
    (root / ".crucible" / "taxonomy.org").write_text(TAXONOMY)
    _write(root, HIER)
    return root


def test_tag_matches_the_tags_under_it(hier):
    assert set(titles("barrier", "--tag", "electronic_structure")) == {
        "DFT barrier", "CC barrier"}
    assert set(titles("barrier", "--tag", "Methods")) == {
        "DFT barrier", "CC barrier", "Method barrier"}
    assert titles("barrier", "--tag", "materials") == ["Zeolite barrier"]
    # A tag with two parents is found under both.
    assert titles("barrier", "--tag", "catalysts") == ["Zeolite barrier"]
    assert titles("barrier", "--tag", "methods", "--type", "method") == [
        "Method barrier"]


def test_exact_matches_only_the_tag(hier):
    assert titles("barrier", "--tag", "methods", "--exact") == ["Method barrier"]
    r = invoke("search", "barrier", "--tag", "electronic_structure", "--exact")
    assert "No results." in r.stdout
    assert "no article is tagged 'electronic_structure'" in r.stderr
    # Without --exact, a group tag no article carries is no typo.
    r = invoke("search", "barrier", "--tag", "electronic_structure")
    assert "no article is tagged" not in r.stderr


def test_semantic_search_expands_tags(hier, ollama):
    assert invoke("embed", "--url", ollama.url).exit_code == 0
    found = titles("barrier", "--mode", "semantic", "--url", ollama.url,
                   "--tag", "electronic_structure")
    assert set(found) == {"DFT barrier", "CC barrier"}


def test_sync_reloads_and_clears_the_taxonomy(hier):
    tax = hier / ".crucible" / "taxonomy.org"
    tax.write_text("#+TAGS: [ methods : dft ]\n")
    r = invoke("sync")
    assert "1 tag hierarchy links" in r.output
    assert set(titles("barrier", "--tag", "methods")) == {"DFT barrier", "Method barrier"}
    tax.unlink()
    assert invoke("sync").exit_code == 0
    assert titles("barrier", "--tag", "methods") == ["Method barrier"]


def test_cycles_do_not_hang(hier):
    (hier / ".crucible" / "taxonomy.org").write_text(
        "#+TAGS: [ dft : coupled_cluster ] [ coupled_cluster : dft ]\n")
    r = invoke("sync")
    assert "Tag cycle" in r.stderr
    assert set(titles("barrier", "--tag", "dft")) == {"DFT barrier", "CC barrier"}
    tree = json.loads(invoke("concepts", "--tree", "--json").stdout)["tree"]
    assert [n["name"] for n in tree] == ["dft"]
    assert tree[0]["children"][0]["name"] == "coupled_cluster"
    assert tree[0]["children"][0]["children"] == []


def test_each_crucible_uses_its_own_taxonomy(hier, tmp_path, monkeypatch):
    peer = _make_project(tmp_path / "peer", monkeypatch, {})
    (peer / ".crucible" / "taxonomy.org").write_text("#+TAGS: [ methods : md ]\n")
    _write(peer, [("p.org", "Peer MD barrier", "A barrier from MD.", ("md",), "concept"),
                  ("q.org", "Peer DFT barrier", "A barrier from DFT.", ("dft",),
                   "concept")])
    monkeypatch.chdir(hier)
    assert set(titles("barrier", "--all", "--tag", "methods")) == {
        "DFT barrier", "CC barrier", "Method barrier", "Peer MD barrier"}


def test_peer_database_without_the_hierarchy_table(hier, tmp_path, monkeypatch):
    import sqlite3
    peer = _make_project(tmp_path / "old", monkeypatch, {})
    _write(peer, [("p.org", "Old DFT barrier", "A barrier from DFT.", ("dft",),
                   "concept")])
    con = sqlite3.connect(peer / ".crucible" / "crucible.db")
    con.execute("DROP TABLE tag_hierarchy")
    con.commit()
    con.close()
    monkeypatch.chdir(hier)
    r = invoke("search", "barrier", "--all", "--tag", "dft", "--json")
    assert r.exit_code == 0, r.output
    assert "Old DFT barrier" in {x["title"] for x in json.loads(r.stdout)}


def test_concepts_tree(hier):
    r = invoke("concepts", "--tree")
    assert r.exit_code == 0, r.output
    lines = r.stdout.splitlines()
    assert lines[:5] == [
        "  methods (3 articles)",
        "    electronic_structure (2 articles)",
        "      dft (1 article)",
        "      coupled_cluster (1 article)",
        "    molecular_dynamics (0 articles)",
    ]
    assert "Not in the taxonomy:" in lines
    assert lines[-1] == "  enzymes (1 article)"
    data = json.loads(invoke("concepts", "--tree", "--json").stdout)
    roots = {n["name"]: n for n in data["tree"]}
    assert set(roots) == {"methods", "materials", "catalysts"}
    assert roots["methods"]["direct_count"] == 1
    assert [n["name"] for n in data["unplaced"]] == ["enzymes"]
    r = invoke("concepts", "--tree", "--all")
    assert r.exit_code == 2


def test_concepts_tree_without_a_taxonomy(kb):
    r = invoke("concepts", "--tree")
    assert "No tag hierarchy" in r.stdout
    assert "  dft (2 articles)" in r.stdout


def test_mcp_search_expands_tags(hier):
    s = CrucibleService([Served("hier", hier)])
    out = s.search("barrier", tags="electronic_structure")
    assert {r["title"] for r in out["results"]} == {"DFT barrier", "CC barrier"}
    assert s.search("barrier", tags="electronic_structure", exact=True)["results"] == []
