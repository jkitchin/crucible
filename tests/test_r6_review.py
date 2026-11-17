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
    assert "Cannot read" in result.output


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
