"""Regression tests for the fifth review round (after #41)."""

import errno
import os

import pytest
from click.testing import CliRunner
from test_data_loss_ingest import kb, run  # noqa: F401
from test_merge import make_crucible

from crucible import cli as cli_module
from crucible import ingest as ingest_module
from crucible.browse import org_to_html
from crucible.cli import cli
from crucible.database import CrucibleDB
from crucible.orgparse import parse_org

DOI = "10.1021/acscatal.9b01234"
FETCHED = ("@article{Smith_2019, title={Cobalt oxide catalysts for OER}, "
           "DOI={%s}, author={Smith, Alice and Jones, Bob}, year={2019}}" % DOI)


def db_of(root):
    return CrucibleDB(root / ".crucible" / "crucible.db")


def article(title, body="", tags="concept", keys=""):
    props = f":SOURCE_KEYS: {keys}\n" if keys else ""
    return (f"#+TITLE: {title}\n#+FILETAGS: :{tags}:\n\n* {title}\n"
            f":PROPERTIES:\n:ARTICLE_TYPE: concept\n{props}:END:\n\n{body}\n")


@pytest.fixture
def doi_lookup(monkeypatch):
    monkeypatch.setattr(ingest_module, "fetch_bibtex_from_doi",
                        lambda doi, timeout=10.0: FETCHED if doi == DOI else None)


def bib(root):
    return (root / ".crucible/references.bib").read_text(errors="replace")


# 1. a document that cites a DOI is not filed as the cited paper --------------

@pytest.mark.parametrize("name", ["group-meeting.pptx", "report.docx"])
def test_decks_and_word_files_never_guess_a_doi(kb, tmp_path, doi_lookup, monkeypatch, name):  # noqa: F811
    monkeypatch.setattr(ingest_module, "extract_text",
                        lambda path: f"Cobalt oxide catalysts for OER (doi:{DOI})")
    (tmp_path / name).write_bytes(b"PK")
    run("ingest", str(tmp_path / name))
    assert DOI not in bib(kb) and "smith2019" not in bib(kb)


def test_pdf_citing_a_doi_without_its_title_is_not_that_paper(kb, tmp_path, doi_lookup, monkeypatch):  # noqa: F811
    monkeypatch.setattr(ingest_module, "extract_text",
                        lambda path: f"PhD thesis: Zeolites.\nAs shown in doi:{DOI}, ...")
    (tmp_path / "thesis.pdf").write_bytes(b"%PDF-1.4")
    result = run("ingest", str(tmp_path / "thesis.pdf"))
    assert "smith2019" not in bib(kb)
    assert "Ignored DOI" in result.output


def test_pdf_with_its_own_title_takes_the_fetched_entry(kb, tmp_path, doi_lookup, monkeypatch):  # noqa: F811
    monkeypatch.setattr(ingest_module, "extract_text", lambda path: (
        f"ACS Catal. 2019\nCobalt oxide\ncatalysts for OER\nA. Smith, B. Jones\ndoi:{DOI}"))
    (tmp_path / "paper.pdf").write_bytes(b"%PDF-1.4")
    run("ingest", str(tmp_path / "paper.pdf"))
    assert "@article{smith2019" in bib(kb)


# 2. --url and --doi survive a rebuild ---------------------------------------

def test_url_and_doi_are_written_to_the_entry(kb, tmp_path):  # noqa: F811
    (tmp_path / "paper.md").write_text("# Paper\nbody\n")
    run("ingest", str(tmp_path / "paper.md"),
        "--bibtex", "@article{x, title={Real Title}, author={Doe, Jane}, year={2020}}",
        "--url", "https://example.org/paper", "--doi", "10.1234/abc")
    (kb / ".crucible/crucible.db").unlink()
    run("sync")
    db = db_of(kb)
    try:
        [src] = db.list_sources()
        assert src["url"] == "https://example.org/paper"
        assert '"doi": "10.1234/abc"' in src["metadata"]
    finally:
        db.close()


# 3. a key that articles still cite is not reused ----------------------------

def test_new_source_does_not_take_a_key_articles_cite(kb, tmp_path):  # noqa: F811
    (kb / ".crucible/wiki/summaries").mkdir(parents=True, exist_ok=True)
    (kb / ".crucible/wiki/summaries/old.org").write_text(
        article("Old summary", "See cite:smith2020.", keys="smith2020"))
    (tmp_path / "zeolite.md").write_text("# Zeolite paper\nbody\n")
    result = run("ingest", str(tmp_path / "zeolite.md"), "-a", "Smith", "--date", "2020")
    assert "smith2020a" in result.output


# 4. prefix search finds words the stemmer shortens ---------------------------

def test_prefix_search_finds_stemmed_words(tmp_path):
    db = CrucibleDB(tmp_path / "c.db")
    db.initialize()
    try:
        db.add_article("a.org", "Oxidation", "concept",
                       content="oxidation generation catalysis activation")
        db.add_article("b.org", "Other", "concept", content="oxen generous")
        db.conn.commit()
        for query in ("oxidat*", "generat*", "catalys*", "activat*", "oxidation*"):
            assert [r["title"] for r in db.search(query)] == ["Oxidation"], query
        assert sorted(r["title"] for r in db.search("ox*")) == ["Other", "Oxidation"]
    finally:
        db.close()


# 5. a bare URL ends where a rendered link or citation starts -----------------

@pytest.mark.parametrize("text", [
    "https://x.org/p[[file:b.org][B]]",
    "(see https://example.com)cite:smith2020",
])
def test_bare_url_does_not_swallow_the_next_link(text):
    _, html, _ = org_to_html(f"* A\n{text}\n")
    assert 'href="https://x.org/p<' not in html and 'href="https://example.com<' not in html
    assert html.count("<a ") == 2


# 6. FILETAGS separated by spaces --------------------------------------------

def test_filetags_split_on_spaces_and_colons():
    assert parse_org("#+FILETAGS: :catalysis: :dft:\n").filetags == ["catalysis", "dft"]
    assert parse_org("#+FILETAGS: concept catalysis\n").filetags == ["concept", "catalysis"]
    assert parse_org("#+FILETAGS: :a:b:\n").filetags == ["a", "b"]


# 7. a symlinked file in the wiki is an alias, not a second article -----------

def test_file_alias_is_not_a_second_article(kb):  # noqa: F811
    concepts = kb / ".crucible/wiki/concepts"
    (concepts / "density-functional-theory.org").write_text(article("DFT", "body"))
    try:
        (concepts / "dft.org").symlink_to("density-functional-theory.org")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    (concepts / "b.org").write_text(article("B", "See [[file:dft.org][DFT]]."))
    run("sync")
    db = db_of(kb)
    try:
        assert sorted(a["path"].rsplit("/", 1)[1] for a in db.list_articles()) == [
            "b.org", "density-functional-theory.org"]
    finally:
        db.close()
    result = run("backlinks", ".crucible/wiki/concepts/density-functional-theory.org")
    assert "b.org" in result.output


# 8. merge with a bibliography that is not UTF-8 -----------------------------

LATIN1 = "@article{muller2020,\n  title = {Müller paper},\n  year = {2020}\n}\n".encode("latin-1")


@pytest.mark.parametrize("side", ["source", "target"])
def test_merge_keeps_a_latin1_bibliography(tmp_path, monkeypatch, side):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    target, peer = make_crucible(tmp_path / "t"), make_crucible(tmp_path / "peer")
    (peer / ".crucible/wiki/concepts/z.org").write_text(article("Z"))
    latin = target if side == "target" else peer
    (latin / ".crucible/references.bib").write_bytes(LATIN1)
    other = peer if side == "target" else target
    (other / ".crucible/references.bib").write_text(
        "@article{kim2021,\n  title = {Kim},\n  year = {2021}\n}\n")
    monkeypatch.chdir(target)
    result = CliRunner().invoke(cli, ["merge", str(peer), "--yes"])
    assert result.exit_code == 0, result.output
    data = (target / ".crucible/references.bib").read_bytes()
    assert "Müller".encode("latin-1") in data and b"kim2021" in data


# 9. merge into a wiki on another filesystem ----------------------------------

def test_merge_moves_files_across_filesystems(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    target, peer = make_crucible(tmp_path / "t"), make_crucible(tmp_path / "peer")
    (peer / ".crucible/wiki/concepts/z.org").write_text(article("Z", "zzz"))
    real_rename = os.rename

    def rename(src, dst):
        if ".merge-staging" in str(src):
            raise OSError(errno.EXDEV, "Cross-device link")
        return real_rename(src, dst)
    monkeypatch.setattr(cli_module.os, "rename", rename)
    monkeypatch.chdir(target)
    result = CliRunner().invoke(cli, ["merge", str(peer), "--yes"])
    assert result.exit_code == 0, result.output
    assert "zzz" in (target / ".crucible/wiki/concepts/z.org").read_text()
    leftovers = [p.name for p in (target / ".crucible/wiki/concepts").iterdir()
                 if p.name.startswith(".crucible-merge-")]
    assert leftovers == []


# 10. ingest: directories, unreadable files and non-UTF-8 bibliographies -----

def test_ingest_a_directory_is_a_clean_error(kb, tmp_path):  # noqa: F811
    (tmp_path / "papers").mkdir()
    result = run("ingest", str(tmp_path / "papers"), ok=False)
    assert result.exit_code == 1
    assert "is a directory" in result.output
    assert not isinstance(result.exception, OSError)


@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0,
                    reason="needs POSIX permissions and a non-root user")
def test_ingest_an_unreadable_file_is_a_clean_error(kb, tmp_path):  # noqa: F811
    secret = tmp_path / "secret.md"
    secret.write_text("# Secret\n")
    secret.chmod(0)
    try:
        result = run("ingest", str(secret), ok=False)
    finally:
        secret.chmod(0o644)
    assert result.exit_code == 1
    assert result.output.strip() == f"Error: Cannot ingest {secret}: Permission denied"


def test_bibtex_file_in_windows_1252(kb, tmp_path):  # noqa: F811
    (tmp_path / "paper.md").write_text("# Über Katalyse\nbody\n")
    export = tmp_path / "export.bib"
    export.write_bytes("% Encoding: windows-1252\n@article{x, title={Über Katalyse}, "
                       "author={Müller, Hans}, year={2020}}\n".encode("cp1252"))
    run("ingest", str(tmp_path / "paper.md"), "--bibtex", f"@{export}")
    assert "Müller, Hans" in (kb / ".crucible/references.bib").read_text(encoding="utf-8")


def test_ingest_keeps_a_latin1_references_bib(kb, tmp_path):  # noqa: F811
    (kb / ".crucible/references.bib").write_bytes(LATIN1)
    (tmp_path / "paper.md").write_text("# Paper\nbody\n")
    run("ingest", str(tmp_path / "paper.md"))
    run("sync")
    data = (kb / ".crucible/references.bib").read_bytes()
    assert "Müller".encode("latin-1") in data
    assert data.count(b"@") == 2


# --- review of #42 ------------------------------------------------------------

from crucible import database as database_module  # noqa: E402
from crucible.ingest import (  # noqa: E402
    FRONT_MATTER_CHARS, _keys_cited_in_wiki, _restore_mismatch, _title_on_first_page,
    extract_doi_from_text)


def test_cited_keys_are_found_through_a_symlinked_wiki_directory(kb, tmp_path):  # noqa: F811
    elsewhere = tmp_path / "dropbox-summaries"
    elsewhere.mkdir()
    (elsewhere / "old.org").write_text(article("Old", "See cite:smith2020.", keys="smith2020"))
    try:
        (kb / ".crucible/wiki/linked").symlink_to(elsewhere, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    (tmp_path / "zeolite.md").write_text("# Zeolite paper\nbody\n")
    result = run("ingest", str(tmp_path / "zeolite.md"), "-a", "Smith", "--date", "2020")
    assert "smith2020a" in result.output


def test_cited_keys_parse_each_article_once(kb, monkeypatch):  # noqa: F811
    concepts = kb / ".crucible/wiki/concepts"
    (concepts / "a.org").write_text(article("A", "cite:a2020"))
    (concepts / "b.org").write_text(article("B", "cite:b2020"))
    parsed = []
    real = ingest_module.parse_org_file
    monkeypatch.setattr(ingest_module, "parse_org_file",
                        lambda path: parsed.append(path.name) or real(path))
    assert {"a2020", "b2020"} <= _keys_cited_in_wiki(kb)
    assert {"a2020", "b2020"} <= _keys_cited_in_wiki(kb)
    assert sorted(parsed) == ["a.org", "b.org"]
    (concepts / "b.org").write_text(article("B", "cite:c2021 and more"))
    assert "c2021" in _keys_cited_in_wiki(kb)
    assert sorted(parsed) == ["a.org", "b.org", "b.org"]


def test_restore_check_ignores_a_doi_a_deck_cites(tmp_path):
    src = {"path": ".crucible/sources/external/web/deck.pptx", "title": "Group meeting",
           "metadata": '{"cite_key": "group2026", "doi": "10.1021/aaa111"}'}
    problem = _restore_mismatch(tmp_path, src, tmp_path / "deck.pptx", "web",
                                "Group meeting", None, "Slide 1: see doi:10.1021/bbb222")
    assert problem is None


@pytest.mark.parametrize("text", [
    "ACS Catal.\nElectro-\ncatalytic Ammonia Synthesis\nA. Smith",
    "ACS Catal.\nElectro-\n  catalytic ammonia synthesis",
    "Electrocatalytic NH3 Synthesis on Ru",
])
def test_title_check_tolerates_hyphenation_and_one_missing_word(text):
    assert _title_on_first_page("Electrocatalytic Ammonia Synthesis", text)


def test_title_check_still_rejects_a_document_without_the_title():
    assert not _title_on_first_page("Electrocatalytic Ammonia Synthesis",
                                    "PhD thesis: Zeolite synthesis\nAs shown in doi:10.1/x")


def test_front_matter_window_is_shared():
    import inspect
    for func in (extract_doi_from_text, _title_on_first_page):
        assert inspect.signature(func).parameters["scan_chars"].default == FRONT_MATTER_CHARS


@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0,
                    reason="needs POSIX permissions and a non-root user")
def test_ingest_write_failure_names_what_could_not_be_written(kb, tmp_path):  # noqa: F811
    (tmp_path / "paper.md").write_text("# Paper\nbody\n")
    store = kb / ".crucible/sources/external/web"
    store.mkdir(parents=True, exist_ok=True)
    store.chmod(0o555)
    try:
        result = run("ingest", str(tmp_path / "paper.md"), ok=False)
    finally:
        store.chmod(0o755)
    assert result.exit_code == 1
    assert "Cannot read" not in result.output
    assert "Permission denied" in result.output and str(store) in result.output


def test_file_link_to_a_file_sync_does_not_collect_is_indexed(kb):  # noqa: F811
    wiki = kb / ".crucible/wiki"
    (wiki / "drafts").mkdir()
    (wiki / "drafts/dft.org.bak").write_text(article("DFT draft", "body"))
    try:
        (wiki / "concepts/dft.org").symlink_to("../drafts/dft.org.bak")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    run("sync")
    db = db_of(kb)
    try:
        assert [a["path"] for a in db.list_articles()] == [".crucible/wiki/concepts/dft.org"]
    finally:
        db.close()


def test_link_through_a_directory_alias_reaches_the_article(kb):  # noqa: F811
    wiki = kb / ".crucible/wiki"
    topics = wiki / "concepts/dft-topics"
    topics.mkdir()
    (topics / "basis-sets.org").write_text(article("Basis sets", "body"))
    try:
        (wiki / "dft").symlink_to("concepts/dft-topics", target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    (wiki / "concepts/b.org").write_text(article("B", "See [[file:../dft/basis-sets.org][sets]]."))
    run("sync")
    result = run("backlinks", ".crucible/wiki/concepts/dft-topics/basis-sets.org")
    assert "b.org" in result.output
    lint = run("lint", ok=False)
    assert "broken" not in lint.output.lower()


class _CountingConn:
    """A connection that records the MATCH expressions fts_rows sends."""

    def __init__(self, conn):
        self.conn, self.matches = conn, []

    def execute(self, sql, params=()):
        if "MATCH" in sql:
            self.matches.append(params[0])
        return self.conn.execute(sql, params)


def test_broad_prefix_query_runs_only_when_needed(tmp_path):
    db = CrucibleDB(tmp_path / "c.db")
    db.initialize()
    try:
        for i in range(3):
            db.add_article(f"c{i}.org", f"Cobalt {i}", "concept", content="cobalt oxide")
        db.add_article("o.org", "Oxidation", "concept", content="oxidation")
        db.conn.commit()
        conn = _CountingConn(db.conn)
        assert len(database_module.fts_rows(conn, "coba*", 2)) == 2
        assert conn.matches == [database_module._fts_query("coba*")]
        conn.matches.clear()
        assert [r["title"] for r in database_module.fts_rows(conn, "oxidat*", 5)] == ["Oxidation"]
        assert len(conn.matches) == 2
    finally:
        db.close()


def test_merge_of_a_latin1_entry_with_a_file(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    target, peer = make_crucible(tmp_path / "t"), make_crucible(tmp_path / "peer")
    monkeypatch.chdir(peer)
    (tmp_path / "m.md").write_text("# M\nbody\n")
    result = CliRunner().invoke(cli, ["ingest", str(tmp_path / "m.md"), "--bibtex",
                                      "@article{muller2020, title={Müller paper}, "
                                      "author={Müller, Hans}, year={2020}}"])
    assert result.exit_code == 0, result.output
    peer_bib = peer / ".crucible/references.bib"
    peer_bib.write_bytes(peer_bib.read_text(encoding="utf-8").encode("latin-1"))
    monkeypatch.chdir(target)
    result = CliRunner().invoke(cli, ["merge", str(peer), "--yes"])
    assert result.exit_code == 0, result.output
    assert "Müller".encode("latin-1") in (target / ".crucible/references.bib").read_bytes()
    db = db_of(target)
    try:
        [src] = db.list_sources()
        assert src["title"].endswith("ller paper")
    finally:
        db.close()
    assert CliRunner().invoke(cli, ["source", "muller2020"]).exit_code == 0
