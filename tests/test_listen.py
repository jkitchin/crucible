"""Tests for crucible listen: keyword extraction, related() and the endpoint."""

import http.client
import json
import threading

import pytest
from click.testing import CliRunner

from crucible.browse import start_server
from crucible.cli import cli
from crucible.database import CrucibleDB
from crucible.listen import keywords, related

ARTICLE = """#+TITLE: {title}
#+FILETAGS: :concept:

* {title}
:PROPERTIES:
:ARTICLE_TYPE: concept
:STATUS: draft
:CREATED: 2026-01-01
:END:

{body}
"""

ARTICLES = {
    "dft": ("Density Functional Theory",
            "Density functional theory approximates the exchange correlation "
            "energy of electrons with a functional of the density."),
    "neb": ("Nudged Elastic Band",
            "The nudged elastic band method finds minimum energy paths and "
            "transition states between two configurations."),
    "zeolite": ("Zeolite Catalysis",
                "Zeolites are microporous aluminosilicates used as acid "
                "catalysts for cracking hydrocarbons."),
}


@pytest.fixture
def project(tmp_path, registry, monkeypatch):
    root = tmp_path / "kb"
    root.mkdir()
    monkeypatch.chdir(root)
    r = CliRunner().invoke(cli, ["init", "--root", str(root)])
    assert r.exit_code == 0, r.output
    for stem, (title, body) in ARTICLES.items():
        (root / ".crucible" / "wiki" / "concepts" / f"{stem}.org").write_text(
            ARTICLE.format(title=title, body=body))
    assert CliRunner().invoke(cli, ["sync"]).exit_code == 0
    return root


@pytest.fixture
def db(project):
    d = CrucibleDB(project / ".crucible" / "crucible.db")
    yield d
    d.close()


@pytest.fixture
def server(project):
    db_path = project / ".crucible" / "crucible.db"
    d = CrucibleDB(db_path)
    articles = d.list_articles()
    d.close()
    srv = start_server(project / ".crucible" / "wiki", db_path, articles, port=0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv
    srv.shutdown()
    srv.server_close()


def post(srv, path, body, headers=None):
    c = http.client.HTTPConnection("127.0.0.1", srv.server_address[1], timeout=5)
    data = body if isinstance(body, bytes) else json.dumps(body).encode()
    c.request("POST", path, body=data,
              headers={"Content-Type": "application/json", **(headers or {})})
    r = c.getresponse()
    out = r.status, json.loads(r.read() or b"{}")
    c.close()
    return out


# -- keywords -----------------------------------------------------------------

def test_keywords_drop_fillers_and_keep_acronyms():
    kws = keywords("um so basically I think we should, you know, run DFT "
                   "on the zeolite and maybe the NEB too")
    assert "DFT" in kws and "NEB" in kws and "zeolite" in kws
    for filler in ("basically", "think", "should", "maybe", "know", "the"):
        assert filler not in [k.lower() for k in kws]


def test_keywords_favor_recent_speech():
    kws = keywords("catalysis catalysis " + "filler words here " * 3 + "adsorption adsorption")
    assert kws.index("adsorption") < kws.index("catalysis")


def test_keywords_of_empty_text():
    assert keywords("") == []
    assert keywords("um uh yeah okay") == []
    assert keywords("don't we'll they're") == []
    assert sorted(keywords("the catalyst's surface")) == ["catalyst", "surface"]


# -- related ------------------------------------------------------------------

def test_long_rambling_speech_still_finds_articles(db):
    """Plain keyword search ANDs every word; listening must not."""
    speech = ("So I was talking to the group yesterday about how we could "
              "find the transition states for this reaction, and somebody "
              "suggested the nudged elastic band approach instead of guessing.")
    out = related(db.conn, speech)
    assert out["mode"] == "fts"
    assert out["results"][0]["title"] == "Nudged Elastic Band"
    assert out["results"][0]["score"] == 1.0
    assert "elastic" in out["results"][0]["terms"]


def test_snippet_has_no_org_noise(db):
    out = related(db.conn, "zeolite catalysts cracking")
    snip = out["results"][0]["snippet"]
    assert snip.startswith("Zeolites are microporous")
    assert ":PROPERTIES:" not in snip and "#+TITLE" not in snip


def test_unrelated_speech_finds_nothing(db):
    assert related(db.conn, "what time is lunch, is anyone ordering pizza")["results"] == []
    assert related(db.conn, "")["results"] == []


# -- HTTP ---------------------------------------------------------------------

def test_related_endpoint(server, project):
    status, data = post(server, "/_api/related",
                        {"text": "the density functional for exchange correlation"})
    assert status == 200
    top = data["results"][0]
    assert top["title"] == "Density Functional Theory"
    assert top["url"] == "/concepts/dft.org"
    assert top["file"] == str(project / ".crucible" / "wiki" / "concepts" / "dft.org")


def test_related_endpoint_refuses_cross_origin_and_foreign_host(server):
    assert post(server, "/_api/related", {"text": "zeolite"},
                {"Origin": "https://evil.example.com"})[0] == 403
    assert post(server, "/_api/related", {"text": "zeolite"},
                {"Host": "evil.example.com"})[0] == 403
    port = server.server_address[1]
    assert post(server, "/_api/related", {"text": "zeolite"},
                {"Origin": f"http://localhost:{port}"})[0] == 200


def test_related_endpoint_bad_requests(server):
    assert post(server, "/_api/related", b"not json")[0] == 400
    assert post(server, "/_api/related", {"text": 3})[0] == 400
    assert post(server, "/_api/nope", {"text": "x"})[0] == 404
    assert post(server, "/_api/related", b"x" * (65 * 1024))[0] == 413


def test_listen_page_is_served_and_linked(server):
    c = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
    c.request("GET", "/_listen")
    r = c.getresponse()
    body = r.read().decode()
    assert r.status == 200
    assert "webkitSpeechRecognition" in body and "/_api/related" in body
    assert 'href="/_listen"' in body  # sidebar link


def test_listen_command_help():
    r = CliRunner().invoke(cli, ["listen", "--help"])
    assert r.exit_code == 0
    assert "--interval" in r.output and "--window" in r.output
