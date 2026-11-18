"""Regression tests: generated files never write through links, and wiki
paths through a symlink alias resolve everywhere."""

import http.client
import os
import threading

import pytest
from click.testing import CliRunner
from test_data_loss_ingest import kb, run  # noqa: F401

from crucible.browse import start_server
from crucible.cli import cli
from crucible.database import CrucibleDB


def article(title, props="", body=""):
    return (f"#+TITLE: {title}\n#+FILETAGS: :concept:\n\n* {title}\n"
            f":PROPERTIES:\n:ARTICLE_TYPE: concept\n{props}:END:\n\n{body}\n")


def wiki(root):
    return root / ".crucible" / "wiki"


def authored_home(root):
    home = wiki(root) / "home.org"
    text = article("Home", body="My hand-written landing page.")
    home.write_text(text)
    return home, text


# -- Bug 1: generated files replace a link instead of writing through it ------

def test_sync_does_not_overwrite_article_symlinked_as_index(kb):  # noqa: F811
    home, text = authored_home(kb)
    index = wiki(kb) / "index.org"
    index.unlink(missing_ok=True)
    os.symlink("home.org", index)
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert home.read_text() == text
    assert not index.is_symlink() and "Home" in index.read_text()
    assert "was a link" in result.output


def test_sync_does_not_overwrite_article_hard_linked_as_index(kb):  # noqa: F811
    home, text = authored_home(kb)
    index = wiki(kb) / "index.org"
    index.unlink(missing_ok=True)
    os.link(home, index)
    run("sync")
    assert home.read_text() == text
    assert home.stat().st_nlink == 1


def test_manifest_does_not_overwrite_symlinked_article(kb):  # noqa: F811
    home, text = authored_home(kb)
    manifest = wiki(kb) / "MANIFEST.md"
    manifest.unlink(missing_ok=True)
    os.symlink("home.org", manifest)
    run("manifest")
    assert home.read_text() == text
    assert not manifest.is_symlink()


def test_viz_does_not_overwrite_symlinked_article(kb):  # noqa: F811
    home, text = authored_home(kb)
    viz = wiki(kb) / "viz.html"
    viz.unlink(missing_ok=True)
    os.symlink("home.org", viz)
    run("viz")
    assert home.read_text() == text
    assert not viz.is_symlink()


def test_publish_config_does_not_overwrite_symlinked_article(kb):  # noqa: F811
    home, text = authored_home(kb)
    config = wiki(kb) / ".org-publish.json"
    os.symlink("home.org", config)
    run("publish", "--init-only")
    assert home.read_text() == text
    assert not config.is_symlink()


# -- Bug 2: wiki-relative paths through a symlink alias ------------------------

def alias_kb(root):
    w = wiki(root)
    (w / "concepts" / "density-functional-theory.org").write_text(
        article("DFT", body="See [[file:basis.org][basis]]."))
    (w / "concepts" / "basis.org").write_text(article("Basis"))
    os.symlink("concepts/density-functional-theory.org", w / "dft.org")
    (w / "summaries" / "s.org").write_text(
        article("S", ":DERIVED_FROM: wiki/dft.org\n", "[[file:../dft.org]]"))
    run("sync")


def test_wiki_relative_derived_from_through_alias_resolves(kb):  # noqa: F811
    alias_kb(kb)
    db = CrucibleDB(kb / ".crucible" / "crucible.db")
    try:
        upstream = db.derived_from(".crucible/wiki/summaries/s.org")
    finally:
        db.close()
    assert [r["path"] for r in upstream] == [
        ".crucible/wiki/concepts/density-functional-theory.org"]
    lint = run("lint", ok=False).output
    assert "DERIVED_FROM entry 'wiki/dft.org'" not in lint


def test_article_argument_through_alias_resolves(kb):  # noqa: F811
    alias_kb(kb)
    result = run("backlinks", "dft.org")
    assert "summaries/s.org" in result.output
    result = run("history", "wiki/dft.org")
    assert "summaries/s.org" in result.output


@pytest.mark.parametrize("spelling", [
    "dft.org", "wiki/dft.org", ".crucible/wiki/dft.org", "ABS", "CWD"])
def test_delete_through_alias_refuses(kb, spelling, monkeypatch):  # noqa: F811
    alias_kb(kb)
    real = wiki(kb) / "concepts" / "density-functional-theory.org"
    given = {"ABS": str(wiki(kb) / "dft.org"), "CWD": "dft.org"}.get(
        spelling, spelling)
    if spelling == "CWD":
        monkeypatch.chdir(wiki(kb))
    result = run("delete", given, "--yes", "--file", ok=False)
    assert result.exit_code != 0
    assert "symlink alias" in result.output
    assert real.exists() and (wiki(kb) / "dft.org").is_symlink()
    db = CrucibleDB(kb / ".crucible" / "crucible.db")
    try:
        assert db.get_article_by_path(
            ".crucible/wiki/concepts/density-functional-theory.org")
    finally:
        db.close()
    # The article's own path still deletes it.
    monkeypatch.chdir(kb)
    run("delete", "concepts/density-functional-theory.org", "--yes", "--file")
    assert not real.exists()


def test_browse_resolves_relative_links_of_article_served_through_alias(kb):  # noqa: F811
    alias_kb(kb)
    db_path = kb / ".crucible" / "crucible.db"
    db = CrucibleDB(db_path)
    articles = db.list_articles()
    db.close()
    srv = start_server(wiki(kb), db_path, articles, port=0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        c = http.client.HTTPConnection("127.0.0.1", srv.server_address[1], timeout=5)
        c.request("GET", "/dft.org")
        body = c.getresponse().read().decode()
        c.close()
    finally:
        srv.shutdown()
        srv.server_close()
    assert '<a href="/concepts/basis.org">basis</a>' in body


def test_delete_real_article_through_another_spelling_of_the_root(
        kb, tmp_path, monkeypatch):  # noqa: F811
    # /tmp for /private/tmp: the root through a symlink, not an alias.
    alias_kb(kb)
    other = tmp_path / "other-root"
    os.symlink(kb, other)
    real = wiki(kb) / "concepts" / "basis.org"
    run("delete", str(other / ".crucible/wiki/concepts/basis.org"), "--yes", "--file")
    assert not real.exists()
    # The alias spelled through the other root is still refused.
    result = run("delete", str(other / ".crucible/wiki/dft.org"), "--yes", "--file",
                 ok=False)
    assert result.exit_code != 0 and "symlink alias" in result.output
    assert (wiki(kb) / "concepts" / "density-functional-theory.org").exists()


def test_delete_refuses_an_alias_reached_through_a_link_outside_the_project(
        kb, tmp_path):  # noqa: F811
    # ~/wiki -> .crucible/wiki: dft.org under it is still the alias.
    alias_kb(kb)
    (wiki(kb) / "concepts" / "other.org").write_text(article("Other"))
    os.symlink("concepts", wiki(kb) / "topics")
    run("sync")
    outside = tmp_path / "outlink"
    os.symlink(wiki(kb), outside)
    for spelling in (outside / "dft.org", outside / "topics" / "other.org"):
        result = run("delete", str(spelling), "--yes", "--file", ok=False)
        assert result.exit_code != 0 and "symlink alias" in result.output, spelling
    assert (wiki(kb) / "concepts" / "density-functional-theory.org").exists()
    assert (wiki(kb) / "concepts" / "other.org").exists()
    # The real article through the outside link is deleted.
    run("delete", str(outside / "concepts" / "other.org"), "--yes", "--file")
    assert not (wiki(kb) / "concepts" / "other.org").exists()


def test_delete_in_a_wiki_that_is_a_symlink_into_the_project(
        kb, monkeypatch):  # noqa: F811
    # .crucible/wiki -> ../wiki-real: the real files are no aliases.
    real_wiki = kb / "wiki-real"
    os.rename(wiki(kb), real_wiki)
    os.symlink("../wiki-real", wiki(kb))
    (real_wiki / "concepts" / "a.org").write_text(article("A"))
    (real_wiki / "concepts" / "b.org").write_text(article("B"))
    run("sync")
    run("delete", str(real_wiki / "concepts" / "b.org"), "--yes", "--file")
    assert not (real_wiki / "concepts" / "b.org").exists()
    monkeypatch.chdir(real_wiki / "concepts")
    run("delete", "a.org", "--yes", "--file")
    assert not (real_wiki / "concepts" / "a.org").exists()


def test_sync_in_a_read_only_wiki_directory_rewrites_its_files_in_place(kb):  # noqa: F811
    (wiki(kb) / "index.org").unlink(missing_ok=True)
    os.chmod(wiki(kb), 0o555)
    try:
        # index.org cannot be created there: a clear error, not a traceback.
        result = run("sync", ok=False)
        assert result.exit_code == 1
        assert "directory is not writable" in result.output
        os.chmod(wiki(kb), 0o755)
        run("sync")
        (wiki(kb) / "concepts" / "two.org").write_text(article("Two"))
        os.chmod(wiki(kb), 0o555)
        result = run("sync")
    finally:
        os.chmod(wiki(kb), 0o755)
    assert result.exit_code == 0, result.output
    assert "1 added" in result.output
    assert "Two" in (wiki(kb) / "index.org").read_text()
