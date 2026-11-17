"""Regression tests for data-loss bugs in settings, the registry, ingest, delete and merge."""

import http.server
import json
import multiprocessing
import threading

import pytest
from click.testing import CliRunner

from crucible import bibtex
from crucible import ingest as ingest_module
from crucible import registry as registry_module
from crucible.cli import _ensure_settings, cli
from crucible.database import CrucibleDB


@pytest.fixture
def kb(tmp_path, registry, monkeypatch):
    """An initialized crucible; cwd is its root. DOI lookups are offline."""
    root = tmp_path / "kb"
    root.mkdir()
    monkeypatch.chdir(root)
    monkeypatch.setattr(ingest_module, "fetch_bibtex_from_doi", lambda doi, timeout=10.0: None)
    result = CliRunner().invoke(cli, ["init", "--root", str(root)])
    assert result.exit_code == 0, result.output
    return root


def run(*args, input=None, ok=True):
    result = CliRunner().invoke(cli, list(args), input=input)
    if ok:
        assert result.exit_code == 0, result.output
    return result


def bib_keys(root):
    return bibtex.keys((root / ".crucible/references.bib").read_text())


def cite_key(result):
    return next(line.split(":", 1)[1].strip()
                for line in result.output.splitlines() if "Cite key:" in line)


def stored(result):
    return next(line.split(":", 1)[1].strip()
                for line in result.output.splitlines() if "Stored:" in line)


# 1. settings.json --------------------------------------------------------------

def test_unparseable_settings_are_left_alone(tmp_path):
    settings = tmp_path / ".claude" / "settings.json"
    settings.parent.mkdir()
    settings.write_text('{"model": "opus", // comment\n}')
    assert _ensure_settings(tmp_path) is False
    assert settings.read_text() == '{"model": "opus", // comment\n}'


@pytest.mark.parametrize("content", ['{"permissions": []}', "[]",
                                     '{"permissions": {"allow": "x"}}'])
def test_wrongly_shaped_settings_are_left_alone(tmp_path, content):
    settings = tmp_path / ".claude" / "settings.json"
    settings.parent.mkdir()
    settings.write_text(content)
    assert _ensure_settings(tmp_path) is False
    assert settings.read_text() == content


def test_settings_merge_keeps_other_keys(tmp_path):
    settings = tmp_path / ".claude" / "settings.json"
    settings.parent.mkdir()
    settings.write_text('{"model": "opus", "permissions": {"deny": ["X"]}}')
    assert _ensure_settings(tmp_path) is True
    data = json.loads(settings.read_text())
    assert data["model"] == "opus" and data["permissions"]["deny"] == ["X"]
    assert "Bash(crucible *)" in data["permissions"]["allow"]


# 2-4. registry --------------------------------------------------------------------

def test_corrupt_registry_is_not_erased(registry, tmp_path):
    registry.mkdir()
    reg = registry / "registry.json"
    reg.write_text('{"version": 1, "instances": {"a": {"path": "/x"}')  # truncated
    with pytest.raises(registry_module.RegistryError):
        registry_module.register("b", str(tmp_path))
    assert reg.read_text().startswith('{"version": 1')
    backups = list(registry.glob("registry.json.corrupt-*"))
    assert len(backups) == 1
    # A second attempt does not pile up copies.
    assert registry_module.register_or_warn("b", str(tmp_path), lambda m: None) is None
    assert len(list(registry.glob("registry.json.corrupt-*"))) == 1


@pytest.mark.parametrize("content", ["[]", '{"instances": []}',
                                     '{"instances": {"a": {"description": "no path"}}}'])
def test_wrongly_shaped_registry(registry, tmp_path, content):
    registry.mkdir()
    (registry / "registry.json").write_text(content)
    assert registry_module.list_instances() == []
    with pytest.raises(registry_module.RegistryError):
        registry_module.unregister("a")
    assert (registry / "registry.json").read_text() == content


def _register_worker(args):
    reg_dir, i = args
    from pathlib import Path
    registry_module.REGISTRY_DIR = Path(reg_dir)
    registry_module.REGISTRY_PATH = Path(reg_dir) / "registry.json"
    registry_module.register(f"n{i}", f"{reg_dir}/fake{i}")


def test_parallel_registrations_are_all_kept(registry):
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(8) as pool:
        pool.map(_register_worker, [(str(registry), i) for i in range(16)])
    assert len(registry_module.list_instances()) == 16


def test_registry_add_renames_registered_path(kb):
    result = run("registry", "add", "mykb", str(kb))
    assert "taken" not in result.output
    names = {i["name"]: i["path"] for i in registry_module.list_instances()}
    assert names == {"mykb": str(kb.resolve())}


def test_registry_add_name_taken_by_other_path(kb, tmp_path):
    other = tmp_path / "other"
    other.mkdir()
    registry_module.register("mykb", str(other))
    result = run("registry", "add", "mykb", str(kb))
    assert "stays registered as 'kb'" in result.output
    names = {i["name"] for i in registry_module.list_instances()}
    assert names == {"kb", "mykb"}


def test_registry_remove_missing_name_fails(kb):
    result = run("registry", "remove", "nope", ok=False)
    assert result.exit_code != 0 and "not found" in result.output


# 5, 7. text sidecars --------------------------------------------------------------

def test_sidecar_never_overwrites_a_txt_source(kb, tmp_path):
    (tmp_path / "notes.txt").write_text("original notes")
    (tmp_path / "notes.md").write_text("# Notes MD\nmarkdown body\n")
    run("ingest", str(tmp_path / "notes.txt"))
    result = run("ingest", str(tmp_path / "notes.md"))
    web = kb / ".crucible/sources/external/web"
    assert (web / "notes.txt").read_text() == "original notes"
    assert stored(result).endswith("notes_1.md")
    assert (web / "notes_1.txt").read_text().startswith("# Notes MD")


def test_same_stem_data_files_get_separate_sidecars(kb, tmp_path):
    (tmp_path / "x.csv").write_text("a,b\n1,2\n")
    (tmp_path / "x.tsv").write_text("a\tb\n3\t4\n")
    run("ingest", str(tmp_path / "x.csv"))
    run("ingest", str(tmp_path / "x.tsv"))
    data = kb / ".crucible/sources/data"
    assert (data / "x.txt").read_text() == "a,b\n1,2\n"
    assert (data / "x_1.txt").read_text() == "a\tb\n3\t4\n"


def test_txt_source_does_not_take_a_sidecar_name(kb, tmp_path):
    (tmp_path / "notes.md").write_text("# Notes\nbody\n")
    (tmp_path / "notes.txt").write_text("plain notes")
    run("ingest", str(tmp_path / "notes.md"))
    result = run("ingest", str(tmp_path / "notes.txt"))
    assert stored(result).endswith("notes_1.txt")
    web = kb / ".crucible/sources/external/web"
    assert (web / "notes.txt").read_text() == "# Notes\nbody\n"


def test_delete_file_removes_sidecar(kb, tmp_path):
    (tmp_path / "results.csv").write_text("a,b\n1,2\n")
    run("ingest", str(tmp_path / "results.csv"))
    data = kb / ".crucible/sources/data"
    assert (data / "results.txt").exists()
    run("delete", "results.csv", "--yes", "--file")
    run("sync")
    assert list(data.iterdir()) == []
    db = CrucibleDB(kb / ".crucible/crucible.db")
    try:
        assert db.list_sources() == []
    finally:
        db.close()


# 6. non-ASCII DOI, cleanup on failure ---------------------------------------------

def test_doi_regex_is_ascii_only():
    assert ingest_module.extract_doi_from_text("doi: 10.1021/jp123–456 x") == "10.1021/jp123"


def test_fetch_doi_with_bad_characters_returns_none(monkeypatch):
    def fake_urlopen(req, timeout):
        assert req.full_url.isascii()
        raise ValueError("bad url")
    monkeypatch.setattr(ingest_module, "urlopen", fake_urlopen)
    assert ingest_module.fetch_bibtex_from_doi("10.1021/jp123–456") is None


def test_failed_ingest_leaves_no_files(kb, tmp_path, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("lookup exploded")
    monkeypatch.setattr(ingest_module, "fetch_bibtex_from_doi", boom)
    (tmp_path / "paper.md").write_text("# Paper\ndoi: 10.1021/abc.def\n")
    result = run("ingest", str(tmp_path / "paper.md"), ok=False)
    assert result.exit_code != 0
    assert not list((kb / ".crucible/sources").rglob("paper*"))


def test_failed_db_write_undoes_files_and_bib(kb, tmp_path, monkeypatch):
    def boom(self, **k):
        raise RuntimeError("db exploded")
    monkeypatch.setattr(CrucibleDB, "add_source", boom)
    (tmp_path / "a.org").write_text("#+TITLE: A\nbody\n")
    result = run("ingest", str(tmp_path / "a.org"), ok=False)
    assert result.exit_code != 0
    assert not list((kb / ".crucible/sources").rglob("a*"))
    bib = kb / ".crucible/references.bib"
    assert not bib.exists() or bib_keys(kb) == []


# 8. key chosen under the lock ------------------------------------------------------

def test_unique_key_append_never_shares_a_key(kb):
    db = CrucibleDB(kb / ".crucible/crucible.db")
    try:
        entry = "@misc{x,\n  title = {T},\n}"
        k1 = ingest_module.append_bib_entry_unique_key(kb, db, "smith2024", entry)
        k2 = ingest_module.append_bib_entry_unique_key(kb, db, "smith2024", entry)
    finally:
        db.close()
    assert (k1, k2) == ("smith2024", "smith2024a")
    assert bib_keys(kb) == ["smith2024", "smith2024a"]


def test_append_bib_entry_reports_collision(kb):
    assert ingest_module.append_bib_entry(kb, "@misc{k,\n}", "k") is True
    assert ingest_module.append_bib_entry(kb, "@misc{k,\n}", "k") is False


# 9. key from the supplied entry ------------------------------------------------------

def test_key_uses_bibtex_author_and_year(kb, tmp_path):
    (tmp_path / "download_123.org").write_text("x")
    result = run("ingest", str(tmp_path / "download_123.org"), "--bibtex",
                 "@article{orig, author={Nørskov, Jens and Doe, J.}, "
                 "title={Real Title}, year={2019}}")
    assert cite_key(result) == "norskov2019"


def test_key_uses_bibtex_title_and_year_without_author(kb, tmp_path):
    (tmp_path / "download_123.org").write_text("x")
    result = run("ingest", str(tmp_path / "download_123.org"), "--bibtex",
                 "@article{orig, title={Catalysis Today}, year={2019}}")
    assert cite_key(result) == "catalysis2019"


def test_cli_metadata_still_wins_for_key(kb, tmp_path):
    (tmp_path / "a.org").write_text("x")
    result = run("ingest", str(tmp_path / "a.org"), "-a", "Jane Smith", "--date", "2021",
                 "--bibtex", "@article{orig, author={Doe, J.}, title={T}, year={2019}}")
    # The author given wins; the year is the entry's, since a contradicting
    # --date is not written to it (and is warned about).
    assert cite_key(result) == "smith2019"
    assert "disagrees" in result.output


# 10. non-ASCII titles and names ----------------------------------------------------

def test_non_ascii_title_gets_a_visible_filename(kb):
    result = run("ingest", "-", "-t", "日本語のノート", input="some body text\n")
    name = stored(result).rsplit("/", 1)[1]
    assert not name.startswith(".") and name.startswith("stdin-")


def test_accented_title_is_transliterated(kb):
    result = run("ingest", "-", "-t", "Ünïcödé Notes", input="body\n")
    assert stored(result).endswith("/unicode-notes.md")
    assert cite_key(result).startswith("unicode")


def test_accented_author_is_transliterated():
    assert ingest_module.generate_cite_key("t", ["Müller, K."], "2020") == "muller2020"
    assert ingest_module.generate_cite_key("t", ["Jens Nørskov"], "2020") == "norskov2020"


# 11. date validation ---------------------------------------------------------------

@pytest.mark.parametrize("date", ["May 2024", "2024/05/01", "2024-13-01"])
def test_bad_date_is_rejected(kb, tmp_path, date):
    (tmp_path / "a.org").write_text("a")
    result = run("ingest", str(tmp_path / "a.org"), "--date", date, ok=False)
    assert result.exit_code != 0 and "YYYY" in result.output
    assert not list((kb / ".crucible/sources").rglob("a.org"))


@pytest.mark.parametrize("date", ["2024", "2024-05", "2024-05-01"])
def test_iso_dates_are_accepted(kb, tmp_path, date):
    (tmp_path / "a.org").write_text("a")
    result = run("ingest", str(tmp_path / "a.org"), "-a", "Jane Smith", "--date", date)
    assert cite_key(result) == "smith2024"


# 12. URL ingest ----------------------------------------------------------------------

@pytest.fixture
def server(tmp_path):
    root = tmp_path / "srv"
    root.mkdir()

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **k):
            super().__init__(*a, directory=str(root), **k)

        def log_message(self, *a):
            pass

        def guess_type(self, path):
            if str(path).endswith("latin.html"):
                return "text/html; charset=iso-8859-1"
            return super().guess_type(path)

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield root, f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def test_url_pdf_is_saved_as_pdf_bytes(kb, server):
    root, base = server
    payload = b"%PDF-1.4\n\x00\x01\x02\xff binary\n"
    (root / "paper.pdf").write_bytes(payload)
    result = run("ingest", f"{base}/paper.pdf")
    path = kb / stored(result)
    assert path.suffix == ".pdf" and path.parent.name == "pdfs"
    assert path.read_bytes() == payload


def test_url_html_title_entities_and_charset(kb, server):
    root, base = server
    (root / "latin.html").write_bytes(
        "<html><head><title>Q&amp;A: Café &lt;111&gt;</title></head>"
        "<body>hi</body></html>".encode("latin-1"))
    result = run("ingest", f"{base}/latin.html")
    assert "Ingested: Q&A: Café <111>" in result.output
    path = kb / stored(result)
    assert path.suffix == ".html"
    assert "Café" in path.read_text(encoding="utf-8")


def test_malformed_url_is_a_clean_error(kb):
    result = run("ingest", "http://[::1", ok=False)
    assert result.exit_code == 1 and "Failed to fetch URL" in result.output


def test_url_temp_dir_removed_on_failure(kb, server, monkeypatch, tmp_path):
    root, base = server
    (root / "p.html").write_text("<title>T</title>")
    tmp = tmp_path / "tmpdirs"
    tmp.mkdir()
    monkeypatch.setattr("tempfile.tempdir", str(tmp))

    def boom(**kwargs):
        raise RuntimeError("ingest failed")
    monkeypatch.setattr("crucible.cli.ingest_source", boom)
    run("ingest", f"{base}/p.html", ok=False)
    assert list(tmp.iterdir()) == []


# 13-14. merge --------------------------------------------------------------------

ARTICLE = """#+TITLE: {title}
* {title}
:PROPERTIES:
:ARTICLE_TYPE: concept
:END:
{body}
"""


def test_merge_keeps_strings_and_search_links(kb, tmp_path, registry):
    other = tmp_path / "other"
    other.mkdir()
    run("init", "--root", str(other))
    # Different articles at the same path, so the incoming one is renamed.
    for root in (kb, other):
        (root / ".crucible/wiki/concepts/foo.org").write_text(
            ARTICLE.format(title="Foo", body=f"body of {root.name}"))
    (other / ".crucible/wiki/concepts/bar.org").write_text(ARTICLE.format(
        title="Bar", body="See [[file:foo.org::*Foo][Foo heading]] and [[file:foo.org][Foo]]."))
    (kb / ".crucible/references.bib").write_text('@string{jcp = "Target JCP"}\n')
    (other / ".crucible/references.bib").write_text(
        '@string{jcp = "J. Chem. Phys."}\n@string{jacs = "JACS"}\n'
        '@preamble{"\\newcommand{\\x}{x}"}\n\n'
        "@article{k1, title={T}, journal = jacs, year={2020}}\n")
    run("sync")
    import os
    cwd = os.getcwd()
    os.chdir(other)
    try:
        run("sync")
    finally:
        os.chdir(cwd)
    run("merge", str(other), "--yes")
    text = (kb / ".crucible/references.bib").read_text()
    assert text.count("@string{jcp") == 1 and "Target JCP" in text
    assert '@string{jacs = "JACS"}' in text
    assert text.index("@string{jacs") < text.index("@article{k1")
    assert "@preamble" in text
    bar = (kb / ".crucible/wiki/concepts/bar.org").read_text()
    assert "[[file:foo_1.org::*Foo][Foo heading]]" in bar
    assert "[[file:foo_1.org][Foo]]" in bar


def test_macro_blocks():
    text = '@string{ JCP = "x"}\n@preamble{"p"}\n@article{k, title={t}}\n'
    assert [(k, n) for k, n, _ in bibtex.macro_blocks(text)] == [
        ("string", "jcp"), ("preamble", None)]


# 15. --bibtex parsing -------------------------------------------------------------

def test_paren_bibtex_entry_is_not_a_path(kb, tmp_path):
    (tmp_path / "a.org").write_text("a")
    result = run("ingest", str(tmp_path / "a.org"), "--bibtex",
                 '@article(x, title = "Paren", year = 2020)')
    assert cite_key(result) == "paren2020"


def test_bibtex_from_file_still_works(kb, tmp_path):
    (tmp_path / "a.org").write_text("a")
    (tmp_path / "e.bib").write_text("@article{x, title={From File}, year={2018}}")
    result = run("ingest", str(tmp_path / "a.org"), "--bibtex", f"@{tmp_path / 'e.bib'}")
    assert cite_key(result) == "from2018"


def test_multiple_bibtex_entries_rejected(kb, tmp_path):
    (tmp_path / "c.org").write_text("c")
    result = run("ingest", str(tmp_path / "c.org"), "--bibtex",
                 "@article{mine, title={C}, year={2022}}\n"
                 "@article{doe2021, title={Other}, year={2021}}", ok=False)
    assert result.exit_code != 0 and "more than one" in result.output
    assert not list((kb / ".crucible/sources").rglob("c.org"))
