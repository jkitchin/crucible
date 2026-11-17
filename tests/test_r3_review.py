"""Regression tests for the review of PR #40."""

import os

import pytest
from click.testing import CliRunner

from crucible.cli import cli
from crucible.database import CrucibleDB, _concept_key

from test_merge import make_crucible
from test_merge import project as merge_project  # noqa: F401
from test_r3_browse import get, kb, server  # noqa: F401


def article(title, body=""):
    return (f"#+TITLE: {title}\n#+FILETAGS: :concept:\n\n* {title}\n"
            f":PROPERTIES:\n:ARTICLE_TYPE: concept\n:END:\n\n{body}\n")


def merge(source):
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    return result


def org_names(directory):
    return sorted(p.name for p in directory.glob("*.org"))


# 1. a repeat merge recognises articles whose links the first merge rewrote ----

def test_repeat_merge_skips_articles_with_rewritten_links(merge_project, tmp_path):  # noqa: F811
    concepts = merge_project / ".crucible/wiki/concepts"
    (concepts / "a.org").write_text(article("Target A"))
    (concepts / "b.org").write_text(article("Target B"))
    peer = make_crucible(tmp_path / "peer")
    peer_concepts = peer / ".crucible/wiki/concepts"
    (peer_concepts / "a.org").write_text(article("Peer A", "See [[file:b.org][B]]."))
    (peer_concepts / "b.org").write_text(article("Peer B", "See [[file:a.org][A]]."))

    merge(peer)
    assert "[[file:b_1.org][B]]" in (concepts / "a_1.org").read_text()
    before = org_names(concepts)
    merge(peer)
    assert org_names(concepts) == before


# 2. links follow an article an earlier merge stored under another name ------

def test_new_article_links_to_the_renamed_copy_of_a_skipped_one(merge_project, tmp_path):  # noqa: F811
    concepts = merge_project / ".crucible/wiki/concepts"
    (concepts / "x.org").write_text(article("Target X"))
    peer = make_crucible(tmp_path / "peer")
    peer_concepts = peer / ".crucible/wiki/concepts"
    (peer_concepts / "x.org").write_text(article("Peer X"))
    merge(peer)
    assert (concepts / "x_1.org").exists()

    (peer_concepts / "y.org").write_text(article("Peer Y", "See [[file:x.org][X]]."))
    merge(peer)
    assert not (concepts / "x_2.org").exists()
    assert "[[file:x_1.org][X]]" in (concepts / "y.org").read_text()


# 3. merge does not copy files that symlinks bring in from outside the wiki --

def test_merge_skips_symlinks_out_of_the_wiki(merge_project, tmp_path):  # noqa: F811
    peer = make_crucible(tmp_path / "peer")
    figures = peer / ".crucible/wiki/figures"
    figures.mkdir()
    secret = tmp_path / "id_rsa"
    secret.write_text("PRIVATE KEY")
    (figures / "plot.png").write_bytes(b"\x89PNG")
    try:
        (figures / "key").symlink_to(secret)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    merge(peer)
    target = merge_project / ".crucible/wiki/figures"
    assert (target / "plot.png").exists()
    assert not (target / "key").exists()


@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0,
                    reason="needs POSIX permissions and a non-root user")
def test_merge_reports_an_unreadable_wiki_directory(merge_project, tmp_path):  # noqa: F811
    peer = make_crucible(tmp_path / "peer")
    locked = peer / ".crucible/wiki/figures"
    locked.mkdir()
    locked.chmod(0)
    try:
        result = CliRunner().invoke(cli, ["merge", str(peer), "--yes"])
    finally:
        locked.chmod(0o755)
    assert result.exit_code == 1
    assert "Cannot read" in result.output
    assert not isinstance(result.exception, PermissionError)


# 4. a prefix query matches the prefix, not a shorter one -------------------

def test_prefix_does_not_match_a_shorter_unrelated_prefix(tmp_path):
    db = CrucibleDB(tmp_path / "c.db")
    db.initialize()
    try:
        db.add_article("a.org", "Catalysis", "concept", content="Heterogeneous catalysis.")
        db.add_article("b.org", "Catapult", "concept", content="A catapult and a catalog.")
        db.add_article("c.org", "Cats", "concept", content="Cathedral cats.")
        db.conn.commit()
        assert [r["title"] for r in db.search("catal*")] == ["Catalysis", "Catapult"]
        assert [r["title"] for r in db.search("catap*")] == ["Catapult"]
        assert db.search("cata*") and all(r["title"] != "Cats" for r in db.search("cata*"))
        assert [r["title"] for r in db.search("cats*")] == ["Cats"]
    finally:
        db.close()


# 5. concept keys ignore spacing and dashes, not all punctuation ------------

def test_concept_key_keeps_distinct_tags_apart():
    assert len({_concept_key(t) for t in ("c", "c++", "c#")}) == 3
    same = {"activation_energy", "activation-energy", "Activation Energy",
            "Michaelis–Menten", "michaelis-menten"}
    assert len({_concept_key(t) for t in same}) == 2
    assert _concept_key("Le Chatelier’s principle") == _concept_key("le-chateliers-principle")


def test_concept_lookup_sees_concepts_another_connection_adds(tmp_path):
    path = tmp_path / "c.db"
    db = CrucibleDB(path)
    db.initialize()
    other = CrucibleDB(path)
    try:
        first = db.add_article("a.org", "A", "concept")
        db.link_article_concept(first, db.add_concept("kinetics"))
        db.conn.commit()
        assert [a["title"] for a in db.concept_articles("Kinetics")] == ["A"]
        assert db.concept_articles("catalysis") == []
        second = other.add_article("b.org", "B", "concept")
        other.link_article_concept(second, other.add_concept("catalysis"))
        other.conn.commit()
        assert [a["title"] for a in db.concept_articles("catalysis")] == ["B"]
    finally:
        other.close()
        db.close()


# 6. /_graph reports a failure instead of dropping the connection -----------

def test_graph_page_reports_errors(server, monkeypatch):  # noqa: F811
    def broken(*args, **kwargs):
        raise RuntimeError("database is locked")
    monkeypatch.setattr("crucible.viz.viz_html", broken)
    status, _, body = get(server, "/_graph")
    assert status == 500
    assert b"database is locked" in body


def test_browse_and_embeddings_do_not_reach_into_the_cli():
    import crucible.browse
    import crucible.embeddings
    browse = open(crucible.browse.__file__, encoding="utf-8").read()
    embeddings = open(crucible.embeddings.__file__, encoding="utf-8").read()
    assert "crucible.cli" not in browse
    assert "crucible.browse" not in embeddings


# 7. source refuses an ambiguous fragment, as delete does -------------------

def test_source_refuses_an_ambiguous_fragment(merge_project, tmp_path):  # noqa: F811
    for name in ("smith-2020.csv", "smith-2024.csv"):
        (tmp_path / name).write_text(f"{name}\n1\n")
        assert CliRunner().invoke(cli, ["ingest", str(tmp_path / name)]).exit_code == 0
    result = CliRunner().invoke(cli, ["source", "smith"])
    assert result.exit_code == 1
    assert "smith-2020.csv" in result.output and "smith-2024.csv" in result.output
    result = CliRunner().invoke(cli, ["source", "smith-2024"])
    assert result.exit_code == 0, result.output
    assert "smith-2024.csv" in result.output
