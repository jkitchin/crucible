"""Tests for embeddings and the embed/search commands.

The embedding service is a small in-process HTTP server that speaks
ollama's /api/embed protocol, so ollama is not needed.
"""

import hashlib
import http.server
import json
import re
import socket
import struct
import threading

import libsql
import pytest
from click.testing import CliRunner

from crucible.cli import cli
from crucible.database import CrucibleDB
from crucible.embeddings import (
    EmbeddingError,
    EmbeddingIndex,
    embed_texts,
    embedding_text,
)


def _bag_of_words(text: str, dim: int) -> list[float]:
    """A hashing bag-of-words vector: similar texts get similar vectors."""
    vec = [0.0] * dim
    for word in re.findall(r"\w+", text.lower()):
        vec[int(hashlib.md5(word.encode()).hexdigest(), 16) % dim] += 1.0
    return vec


class FakeOllama:
    """An ollama-like embed endpoint; dims maps model -> dimension."""

    def __init__(self, dims=None, missing=()):
        self.dims = dims or {}
        self.missing = set(missing)
        self.requests: list[dict] = []
        fake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                fake.requests.append(req)
                if req["model"] in fake.missing:
                    body = json.dumps({"error": f"model '{req['model']}' not found"}).encode()
                    self.send_response(404)
                else:
                    dim = fake.dims.get(req["model"], 768)
                    body = json.dumps({"embeddings": [
                        _bag_of_words(t, dim) for t in req["input"]]}).encode()
                    self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        self.server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/api/embed"

    def models(self):
        return [r["model"] for r in self.requests]


@pytest.fixture
def ollama():
    fake = FakeOllama()
    yield fake
    fake.server.shutdown()
    fake.server.server_close()


@pytest.fixture
def dead_url():
    """A URL on a port nothing listens on."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"http://127.0.0.1:{port}/api/embed"


def _article(title, body, atype="concept", status="draft"):
    return f"""#+TITLE: {title}
#+FILETAGS: :{atype}:

* {title}
:PROPERTIES:
:ARTICLE_TYPE: {atype}
:STATUS: {status}
:CREATED: 2026-01-01
:END:

{body}
"""


PT = ("Platinum catalysis of hydrogen oxidation proceeds through adsorbed "
      "intermediates on the platinum surface with low activation barriers.")


def _make_project(root, monkeypatch, articles):
    root.mkdir()
    monkeypatch.chdir(root)
    runner = CliRunner()
    r = runner.invoke(cli, ["init", "--root", str(root)])
    assert r.exit_code == 0, r.output
    wiki = root / ".crucible" / "wiki" / "concepts"
    for name, (title, body) in articles.items():
        (wiki / name).write_text(_article(title, body))
    r = runner.invoke(cli, ["sync"])
    assert r.exit_code == 0, r.output
    return root


@pytest.fixture
def project(tmp_path, registry, monkeypatch):
    monkeypatch.delenv("CRUCIBLE_OLLAMA_URL", raising=False)
    return _make_project(tmp_path / "kb", monkeypatch, {
        "pt.org": ("Platinum", PT),
        "pt-copy.org": ("Platinum again", PT),
        "bio.org": ("Enzymes", "Enzymes fold proteins in living cells."),
    })


def invoke(*args, **kw):
    return CliRunner().invoke(cli, list(args), **kw)


def _rows(project):
    db = CrucibleDB(project / ".crucible" / "crucible.db")
    try:
        return [dict(r) for r in db.conn.execute(
            "SELECT article_id, model, dimensions FROM embeddings")]
    finally:
        db.close()


# -- dimensions and migration -------------------------------------------------

@pytest.mark.parametrize("dim", [384, 1024])
def test_models_of_any_dimension_embed_and_search(project, ollama, dim):
    ollama.dims["m"] = dim
    r = invoke("embed", "-m", "m", "--url", ollama.url)
    assert r.exit_code == 0, r.output
    assert {row["dimensions"] for row in _rows(project)} == {dim}
    r = invoke("search", "platinum", "--mode", "semantic", "--url", ollama.url)
    assert r.exit_code == 0, r.output
    assert "Platinum" in r.output


def test_old_fixed_768_table_is_migrated(project, ollama):
    db_path = project / ".crucible" / "crucible.db"
    db = CrucibleDB(db_path)
    aid = db.conn.execute("SELECT id FROM articles LIMIT 1").fetchone()["id"]
    db.conn.execute("""CREATE TABLE embeddings (
        article_id INTEGER PRIMARY KEY REFERENCES articles(id) ON DELETE CASCADE,
        model TEXT NOT NULL, dimensions INTEGER NOT NULL,
        vector F32_BLOB(768),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')))""")
    db.conn.execute("CREATE INDEX idx_embeddings_vector "
                    "ON embeddings(libsql_vector_idx(vector))")
    db.conn.execute("INSERT INTO embeddings (article_id, model, dimensions, vector)"
                    " VALUES (?, 'old', 768, ?)",
                    (aid, struct.pack("768f", *([1.0] * 768))))
    db.conn.commit()
    db.close()

    ollama.dims["big"] = 1024
    r = invoke("embed", "-m", "big", "--url", ollama.url)
    assert r.exit_code == 0, r.output
    db = CrucibleDB(db_path)
    sql = db.conn.execute("SELECT sql FROM sqlite_master WHERE name='embeddings'"
                          ).fetchone()["sql"]
    assert "F32_BLOB" not in sql
    assert db.conn.execute("SELECT 1 FROM sqlite_master WHERE name="
                           "'idx_embeddings_vector'").fetchone() is None
    db.close()
    assert {(r["model"], r["dimensions"]) for r in _rows(project)} == {("big", 1024)}


def test_migration_keeps_rows():
    conn = libsql.connect(":memory:")
    from crucible.database import DictConnection
    c = DictConnection(conn)
    c.execute("CREATE TABLE articles (id INTEGER PRIMARY KEY)")
    c.execute("INSERT INTO articles VALUES (1)")
    c.execute("CREATE TABLE embeddings (article_id INTEGER PRIMARY KEY, model TEXT NOT NULL,"
              " dimensions INTEGER NOT NULL, vector F32_BLOB(3),"
              " updated_at TEXT NOT NULL DEFAULT (datetime('now')))")
    c.execute("INSERT INTO embeddings (article_id, model, dimensions, vector)"
              " VALUES (1, 'x', 3, ?)", (struct.pack("3f", 1, 2, 3),))
    c.commit()
    idx = EmbeddingIndex(c)
    idx.initialize()
    row = c.execute("SELECT model, vector FROM embeddings").fetchone()
    assert row["model"] == "x"
    assert struct.unpack("3f", row["vector"]) == (1.0, 2.0, 3.0)
    assert idx.model == "x"  # defaults to the stored model


# -- models -------------------------------------------------------------------

def test_switching_models_reembeds_and_search_uses_stored_model(project, ollama):
    assert invoke("embed", "-m", "a", "--url", ollama.url).exit_code == 0
    r = invoke("embed", "-m", "b", "--url", ollama.url)
    assert r.exit_code == 0, r.output
    assert "Embedded 3 new" in r.output
    assert {row["model"] for row in _rows(project)} == {"b"}
    ollama.requests.clear()
    r = invoke("search", "platinum", "--url", ollama.url)
    assert r.exit_code == 0, r.output
    assert ollama.models() == ["b"]


def test_search_with_a_model_without_embeddings(project, ollama):
    assert invoke("embed", "-m", "a", "--url", ollama.url).exit_code == 0
    # auto: no embeddings from c, so keyword search
    r = invoke("search", "platinum", "-m", "c", "--url", ollama.url)
    assert r.exit_code == 0 and "Platinum" in r.output
    r = invoke("search", "platinum", "-m", "c", "--mode", "semantic", "--url", ollama.url)
    assert r.exit_code == 1
    assert "No embeddings from c" in r.output


def test_dimension_change_for_same_model_is_a_clear_error(project, ollama):
    assert invoke("embed", "-m", "a", "--url", ollama.url).exit_code == 0
    ollama.dims["a"] = 384
    r = invoke("search", "platinum", "--mode", "hybrid", "--url", ollama.url)
    assert r.exit_code == 1
    assert "768 dimensions" in r.output and "--force" in r.output
    assert r.exception is None or isinstance(r.exception, SystemExit)
    r = invoke("embed", "-m", "a", "--force", "--url", ollama.url)
    assert r.exit_code == 0, r.output
    assert {row["dimensions"] for row in _rows(project)} == {384}


# -- service failures ----------------------------------------------------------

def test_embed_texts_raises_embedding_error(dead_url, ollama):
    with pytest.raises(EmbeddingError, match="Cannot reach"):
        embed_texts(["x"], url=dead_url)
    ollama.missing.add("nope")
    with pytest.raises(EmbeddingError, match="ollama pull nope"):
        embed_texts(["x"], model="nope", url=ollama.url)


def test_auto_search_falls_back_to_fts_when_service_is_down(project, ollama, dead_url):
    assert invoke("embed", "--url", ollama.url).exit_code == 0
    r = invoke("search", "platinum", "--url", dead_url)
    assert r.exit_code == 0, r.output
    assert "Platinum" in r.stdout
    assert "Falling back to keyword search" in r.stderr


@pytest.mark.parametrize("mode", ["semantic", "hybrid"])
def test_explicit_semantic_modes_fail_cleanly(project, ollama, dead_url, mode):
    assert invoke("embed", "--url", ollama.url).exit_code == 0
    r = invoke("search", "platinum", "--mode", mode, "--url", dead_url)
    assert r.exit_code == 1
    assert "Cannot reach the embedding service" in r.output
    assert "Traceback" not in r.output


def test_embed_fails_cleanly(project, ollama, dead_url):
    r = invoke("embed", "--url", dead_url)
    assert r.exit_code == 1
    assert "Cannot reach the embedding service" in r.output
    ollama.missing.add("nope")
    r = invoke("embed", "-m", "nope", "--url", ollama.url)
    assert r.exit_code == 1
    assert "HTTP 404" in r.output


def test_search_honours_url_option_and_env_var(project, ollama, monkeypatch):
    assert invoke("embed", "--url", ollama.url).exit_code == 0
    ollama.requests.clear()
    r = invoke("search", "platinum", "--mode", "semantic", "--url", ollama.url)
    assert r.exit_code == 0, r.output
    assert len(ollama.requests) == 1
    monkeypatch.setenv("CRUCIBLE_OLLAMA_URL", ollama.url)
    r = invoke("search", "platinum", "--mode", "semantic")
    assert r.exit_code == 0, r.output
    assert len(ollama.requests) == 2
    # embed honours it too
    (project / ".crucible" / "wiki" / "concepts" / "new.org").write_text(
        _article("New", "Something new."))
    assert invoke("sync").exit_code == 0
    r = invoke("embed")
    assert r.exit_code == 0, r.output
    assert "Embedded 1 new" in r.output


# -- batching and text ---------------------------------------------------------

def test_embed_batches_requests(project, ollama):
    r = invoke("embed", "--url", ollama.url)
    assert r.exit_code == 0, r.output
    assert len(ollama.requests) == 1
    assert len(ollama.requests[0]["input"]) == 3


def test_embed_missing_uses_batch_size(project, ollama):
    db = CrucibleDB(project / ".crucible" / "crucible.db")
    try:
        idx = EmbeddingIndex(db.conn, model="m", url=ollama.url)
        idx.initialize()
        arts = db.list_articles()
        assert idx.embed_missing(arts, batch_size=2) == 3
        assert [len(r["input"]) for r in ollama.requests] == [2, 1]
        assert idx.embed_missing(arts) == 0
    finally:
        db.close()


def test_embedding_text_strips_org_header():
    text = embedding_text(_article("Title here", "Body " * 3000))
    assert text.startswith("Title here\n\n* Title here\n\nBody")
    assert ":PROPERTIES:" not in text and "#+FILETAGS" not in text
    assert len(text) == 8000


# -- search across crucibles ---------------------------------------------------

def test_hybrid_all_dedups_unless_raw_and_warns_on_other_model_peer(
        project, ollama, tmp_path, monkeypatch):
    ollama.dims["other"] = 384
    peer = _make_project(tmp_path / "peer", monkeypatch, {
        "p.org": ("Peer platinum", PT)})
    assert invoke("embed", "-m", "other", "--url", ollama.url).exit_code == 0
    monkeypatch.chdir(project)
    assert invoke("embed", "--url", ollama.url).exit_code == 0

    r = invoke("search", "platinum", "--all", "--url", ollama.url, "--json")
    assert r.exit_code == 0, r.output
    results = json.loads(r.stdout)
    titles = [x["title"] for x in results]
    # The two identical local articles are clustered into one result
    assert len({"Platinum", "Platinum again"} & set(titles)) == 1
    assert any(x.get("dedup_absorbed") for x in results)
    assert "Peer 'peer'" in r.stderr and "384 dims" in r.stderr
    # The peer still contributes keyword hits
    assert "Peer platinum" in titles

    r = invoke("search", "platinum", "--all", "--raw", "--url", ollama.url, "--json")
    titles = [x["title"] for x in json.loads(r.stdout)]
    assert {"Platinum", "Platinum again"} <= set(titles)
    assert peer.exists()
