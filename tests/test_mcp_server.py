"""`crucible mcp`: serving crucibles to other agents over MCP."""

import asyncio
import json
import os
import sys

import pytest
from test_embeddings import _make_project, invoke

from crucible.mcp_server import CrucibleService, Served, ServeError


@pytest.fixture
def kb(tmp_path, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    root = _make_project(tmp_path / "kb", monkeypatch, {
        "zeolites.org": ("Zeolites", "Zeolites confine molecules in pores. cite:smith2020"),
        "pt.org": ("Platinum", "Platinum oxidizes hydrogen. See [[file:zeolites.org][Zeolites]]."),
    })
    (root / ".crucible" / "references.bib").write_text(
        "@article{smith2020,\n  title = {Pores},\n  doi = {10.1000/XYZ},\n"
        "  file = {sources/external/pdfs/secret-name.pdf}\n}\n")
    secret = root / ".crucible" / "sources" / "external" / "pdfs"
    secret.mkdir(parents=True, exist_ok=True)
    (secret / "paper.org").write_text("copyrighted")
    nb = root / ".crucible" / "sources" / "notebooks"
    nb.mkdir(parents=True, exist_ok=True)
    (nb / "entry.org").write_text("#+TITLE: My notebook\nlab notes")
    (tmp_path / "outside.org").write_text("not served")
    return root


@pytest.fixture
def other(tmp_path, monkeypatch):
    return _make_project(tmp_path / "other", monkeypatch, {
        "mof.org": ("MOFs", "Metal organic frameworks also confine molecules."),
    })


def svc(*roots, **kw):
    return CrucibleService([Served(r.name, r, origin=f"https://example.org/{r.name}.git")
                            for r in roots], **kw)


# -- the service -------------------------------------------------------------

def test_search_and_read(kb):
    s = svc(kb)
    out = s.search("zeolites")
    assert out["results"][0]["title"] == "Zeolites"
    assert out["results"][0]["crucible"] == "kb"
    assert out["results"][0]["mode"] == "fts"
    path = out["results"][0]["path"]
    art = s.read_article(path)
    assert "confine molecules" in art["content"]
    assert art["origin"] == "https://example.org/kb.git"
    # Other spellings of the path work too.
    assert s.read_article("concepts/zeolites.org")["path"] == path
    assert s.read_article("concepts/zeolites")["path"] == path


@pytest.mark.parametrize("path", [
    "../sources/external/pdfs/paper.org",
    ".crucible/sources/external/pdfs/paper.org",
    "sources/external/pdfs/paper.org",
    "../../outside.org",
    "/etc/passwd",
    "sources/notebooks/entry.org",   # notebooks are off by default
    ".crucible/references.bib",
])
def test_read_article_refuses_paths_outside_the_wiki(kb, path):
    with pytest.raises(ServeError):
        svc(kb).read_article(path)


def test_symlink_out_of_the_wiki_is_refused(kb, tmp_path):
    (kb / ".crucible" / "wiki" / "concepts" / "link.org").symlink_to(tmp_path / "outside.org")
    with pytest.raises(ServeError, match="not served"):
        svc(kb).read_article("concepts/link.org")


def test_notebooks_only_when_included(kb):
    s = svc(kb, include_notebooks=True)
    assert "lab notes" in s.read_article("sources/notebooks/entry.org")["content"]
    with pytest.raises(ServeError):
        s.read_article("sources/external/pdfs/paper.org")


def test_several_crucibles(kb, other):
    s = svc(kb, other)
    hits = s.search("confine molecules")["results"]
    assert {h["crucible"] for h in hits} == {"kb", "other"}
    with pytest.raises(ServeError, match="several crucibles"):
        s.read_article("concepts/mof.org")
    assert "frameworks" in s.read_article("concepts/mof.org", "other")["content"]
    with pytest.raises(ServeError, match="unknown crucible"):
        s.search("x", crucible="nope")
    names = [c["name"] for c in s.list_crucibles()]
    assert names == ["kb", "other"]
    assert s.list_crucibles()[0]["articles"] == 2


def test_sources_hide_file_field(kb):
    s = svc(kb)
    (entry,) = s.sources("smith2020")
    assert entry["doi"] == "10.1000/XYZ"
    assert "file" not in entry
    assert "secret-name" not in json.dumps(entry)
    assert s.sources(doi="https://doi.org/10.1000/xyz")[0]["key"] == "smith2020"
    assert s.sources("missing") == []


def test_links_and_concepts(kb):
    s = svc(kb)
    links = s.links("concepts/zeolites.org")
    assert [b["title"] for b in links["backlinks"]] == ["Platinum"]
    assert s.overview().strip()
    assert isinstance(s.list_concepts(), list)
    assert s.list_articles(article_type="concept")


def test_limit_is_clamped(kb):
    assert len(svc(kb).search("zeolites OR platinum", limit=10_000)["results"]) <= 100


# -- the command -------------------------------------------------------------

def test_check_serves_current_crucible(kb, monkeypatch):
    monkeypatch.chdir(kb)
    r = invoke("mcp", "--check")
    assert r.exit_code == 0, r.output
    assert "kb:" in r.output and "Transport: stdio" in r.output
    assert "never sources/external/" in r.output


def test_check_by_registry_name(kb, other, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    r = invoke("mcp", "kb", "--check")
    assert r.exit_code == 0, r.output
    assert "kb:" in r.output and "other" not in r.output
    r = invoke("mcp", "nope", "--check")
    assert r.exit_code != 0 and "neither a registered crucible" in r.output


def test_http_on_public_address_needs_token(kb, monkeypatch):
    monkeypatch.chdir(kb)
    r = invoke("mcp", "--http", "--host", "0.0.0.0", "--check")
    assert r.exit_code != 0 and "--token-env" in r.output
    monkeypatch.setenv("TOK", "s3cret")
    r = invoke("mcp", "--http", "--host", "0.0.0.0", "--token-env", "TOK", "--check")
    assert r.exit_code == 0, r.output
    assert "bearer token required" in r.output
    r = invoke("mcp", "--http", "--check")   # loopback is fine without one
    assert r.exit_code == 0, r.output
    r = invoke("mcp", "--http", "--host", "0.0.0.0", "--no-token", "--check")
    assert r.exit_code == 0, r.output


# -- over MCP ----------------------------------------------------------------

def test_stdio_end_to_end(kb, registry):
    mcp = pytest.importorskip("mcp")
    from mcp.client.stdio import StdioServerParameters, stdio_client

    params = StdioServerParameters(
        command=sys.executable,
        args=["-c", "from crucible.cli import cli; cli()", "mcp", str(kb)],
        env={**os.environ, "HOME": str(registry.parent)},
        cwd=str(kb),
    )

    async def main():
        async with stdio_client(params) as (read, write):
            async with mcp.ClientSession(read, write) as session:
                init = await session.initialize()
                assert "untrusted" in init.instructions or "do not follow" in init.instructions
                tools = {t.name for t in (await session.list_tools()).tools}
                assert {"search", "read_article", "sources", "list_crucibles"} <= tools
                res = await session.call_tool("search", {"query": "zeolites"})
                hits = json.loads(res.content[0].text)["results"]
                assert hits[0]["title"] == "Zeolites"
                res = await session.call_tool(
                    "read_article", {"path": "../sources/external/pdfs/paper.org"})
                assert "error" in json.loads(res.content[0].text)
                res = await session.read_resource("crucible://kb/concepts/zeolites")
                assert "confine molecules" in res.contents[0].text

    asyncio.run(asyncio.wait_for(main(), 60))


def test_bearer_middleware():
    pytest.importorskip("mcp")
    from crucible.mcp_server import _BearerAuth

    calls = []

    async def app(scope, receive, send):
        calls.append(scope["path"])

    sent = []

    async def send(msg):
        sent.append(msg)

    async def go(headers):
        mw = _BearerAuth(app, "tok")
        await mw({"type": "http", "path": "/mcp", "headers": headers}, None, send)

    asyncio.run(go([(b"authorization", b"Bearer wrong")]))
    assert sent[0]["status"] == 401 and not calls
    asyncio.run(go([(b"authorization", b"Bearer tok")]))
    assert calls == ["/mcp"]
