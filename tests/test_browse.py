"""Tests for the wiki browser: security, org rendering and live reload."""

import http.client
import socket
import threading

import pytest
from click.testing import CliRunner

from crucible.browse import (
    _build_index_html,
    _build_nav_html,
    _build_references_html,
    _build_search_html,
    _build_topics_html,
    org_to_html,
    start_server,
)
from crucible.cli import cli

ARTICLE = """#+TITLE: {title}
#+FILETAGS: :concept:

* {title}
:PROPERTIES:
:ARTICLE_TYPE: concept
:STATUS: draft
:CREATED: 2026-01-01
:END:

Body of {title}.
"""


@pytest.fixture
def project(tmp_path, registry, monkeypatch):
    root = tmp_path / "kb"
    root.mkdir()
    monkeypatch.chdir(root)
    r = CliRunner().invoke(cli, ["init", "--root", str(root)])
    assert r.exit_code == 0, r.output
    (root / ".crucible" / "wiki" / "concepts" / "alpha.org").write_text(
        ARTICLE.format(title="Alpha"))
    assert CliRunner().invoke(cli, ["sync"]).exit_code == 0
    return root


@pytest.fixture
def server(project):
    from crucible.database import CrucibleDB
    db_path = project / ".crucible" / "crucible.db"
    db = CrucibleDB(db_path)
    articles = db.list_articles()
    db.close()
    srv = start_server(project / ".crucible" / "wiki", db_path, articles, port=0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv
    srv.shutdown()
    srv.server_close()


def get(srv, path, host=None):
    port = srv.server_address[1]
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    headers = {"Host": host} if host is not None else {}
    c.request("GET", path, headers=headers)
    r = c.getresponse()
    body = r.read().decode("utf-8", "replace")
    c.close()
    return r.status, body


# -- security -----------------------------------------------------------------

def test_sibling_directory_is_not_served(project, server):
    sib = project / ".crucible" / "wiki-old"
    sib.mkdir()
    (sib / "secret.org").write_text("#+TITLE: SECRET\n")
    for path in ("/../wiki-old/secret.org", "/%2e%2e/wiki-old/secret.org"):
        status, body = get(server, path)
        assert status == 403
        assert "SECRET" not in body


def test_only_org_files_are_served(project, server):
    (project / ".crucible" / "wiki" / "notes.txt").write_text("private")
    status, body = get(server, "/notes.txt")
    assert status == 403
    assert "private" not in body
    assert get(server, "/concepts/alpha.org")[0] == 200


def test_foreign_host_header_is_rejected(server):
    assert get(server, "/", host="evil.example.com")[0] == 403
    assert get(server, "/", host="evil.example.com:%d" % server.server_address[1])[0] == 403
    assert get(server, "/", host="localhost:%d" % server.server_address[1])[0] == 200
    assert get(server, "/", host="127.0.0.1")[0] == 200


def test_non_utf8_file_gets_a_response(project, server):
    (project / ".crucible" / "wiki" / "bin.org").write_bytes(b"#+TITLE: Bin\n\xff\xfe junk\n")
    status, body = get(server, "/bin.org")
    assert status == 200
    assert "Bin" in body


def test_article_paths_are_escaped_in_hrefs():
    arts = [
        {"path": '.crucible/wiki/concepts/x" onmouseover="alert(1).org',
         "title": "Evil", "article_type": "concept", "status": "draft"},
        {"path": ".crucible/wiki/concepts/c#-lang.org", "title": "C#",
         "article_type": "concept", "status": "draft"},
    ]
    pages = [
        _build_nav_html(arts),
        _build_index_html(arts),
        _build_search_html("q", arts),
        _build_topics_html([{"name": "t"}], {"t": arts}),
    ]
    for page in pages:
        assert 'onmouseover="alert' not in page
        assert 'href="/concepts/c%23-lang.org"' in page
    _, h, _ = org_to_html("[[file:c#-lang.org][C]]", "concepts/a.org")
    assert 'href="/concepts/c%23-lang.org"' in h


def test_only_http_bib_urls_are_linked():
    page = _build_references_html({
        "bad": {"_type": "misc", "url": "javascript:alert(document.domain)"},
        "good": {"_type": "misc", "url": "https://example.com/x"},
    })
    assert 'href="javascript:' not in page
    assert "javascript:alert(document.domain)" in page  # shown as text
    assert 'href="https://example.com/x"' in page


# -- org rendering ---------------------------------------------------------------

def test_plain_lists():
    _, h, _ = org_to_html("Intro:\n- one\n- two\n  continued\n  - nested\n\n1. first\n2) second\n")
    assert "<p>Intro:</p>" in h
    assert "<ul><li>one</li><li>two continued<ul><li>nested</li></ul></li></ul>" in h
    assert "<ol><li>first</li><li>second</li></ol>" in h


def test_description_and_checkbox_items():
    _, h, _ = org_to_html("- term :: meaning\n- [X] done\n")
    assert "<strong>term</strong>: meaning" in h
    assert "&#9745; done" in h


def test_indented_table():
    _, h, _ = org_to_html("  | a | b |\n  |---+---|\n  | 1 | 2 |\n")
    assert "<th>a</th>" in h and "<td>2</td>" in h
    assert "|" not in h


def test_other_drawers_are_not_shown_raw():
    _, h, _ = org_to_html("* H\n:LOGBOOK:\nCLOCK: [2026-01-01]\n:END:\ntext\n")
    assert ":LOGBOOK:" not in h and ":END:" not in h
    assert "<summary>Logbook</summary>" in h
    assert "<p>text</p>" in h


def test_keywords_inside_src_blocks_are_kept():
    title, h, _ = org_to_html(
        "#+TITLE: Real\n\n#+BEGIN_SRC org\n#+TITLE: inside\n#+OPTIONS: toc:nil\n#+END_SRC\n")
    assert title == "Real"
    assert "#+TITLE: inside" in h and "#+OPTIONS: toc:nil" in h


# -- live reload -----------------------------------------------------------------

def test_newly_synced_articles_appear(project, server):
    assert "Beta" not in get(server, "/")[1]
    (project / ".crucible" / "wiki" / "concepts" / "beta.org").write_text(
        ARTICLE.format(title="Beta"))
    assert CliRunner().invoke(cli, ["sync"]).exit_code == 0
    status, body = get(server, "/")
    assert status == 200
    assert 'href="/concepts/beta.org"' in body


# -- browse command ------------------------------------------------------------

def test_browse_port_in_use_is_a_clean_error_and_graph_is_made_in_process(
        project, monkeypatch):
    monkeypatch.setenv("PATH", "/nonexistent")  # no `crucible` on PATH
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen(1)
    port = s.getsockname()[1]
    try:
        r = CliRunner().invoke(cli, ["browse", "--no-open", "-p", str(port)])
    finally:
        s.close()
    assert r.exit_code == 1
    assert f"Cannot serve on port {port}" in r.output
    assert (project / ".crucible" / "wiki" / "viz.html").exists()
