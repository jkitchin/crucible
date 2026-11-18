"""Regression tests for the seventh review round: ingest."""

import json
import signal
import threading

import pytest
from test_data_loss_ingest import kb, run  # noqa: F401

from crucible import cli as cli_module
from crucible import ingest as ingest_module
from crucible.database import CrucibleDB

DOI = "10.1021/acscatal.5b00001"
FETCHED = ("@article{Smith_2015, title={Oxygen evolution on perovskite oxides}, "
           "author={Smith, Jane and Doe, John}, journal={ACS Catal.}, year={2015}, "
           "doi={%s}}" % DOI)
WEB = ".crucible/sources/external/web"


@pytest.fixture
def doi_lookup(monkeypatch):
    monkeypatch.setattr(ingest_module, "fetch_bibtex_from_doi",
                        lambda doi, timeout=10.0: FETCHED if doi == DOI else None)


def bib(root):
    return (root / ".crucible/references.bib").read_text()


def rows(root):
    db = CrucibleDB(root / ".crucible" / "crucible.db")
    try:
        return {src["path"]: src for src in db.list_sources()}
    finally:
        db.close()


@pytest.fixture
def alarm():
    """Fail, rather than hang, if a command loops."""
    def boom(*_):
        raise TimeoutError("still running after 10 s")
    old = signal.signal(signal.SIGALRM, boom)
    signal.alarm(10)
    yield
    signal.alarm(0)
    signal.signal(signal.SIGALRM, old)


def case_insensitive(path):
    probe = path / "CaseProbe"
    probe.write_text("")
    try:
        return (path / "caseprobe").exists()
    finally:
        probe.unlink()


# 1. a markdown note that cites a DOI is not filed as the cited paper ----------

NOTE_BODY = (f"We compared our OER data with {DOI} and found lower "
             "overpotentials.\n")


@pytest.mark.parametrize("text", [
    "---\ntitle: Group meeting 2026-11-10\n---\n\n## Discussion\n\n" + NOTE_BODY,
    "## Discussion\n\n" + NOTE_BODY,
    NOTE_BODY,
])
def test_markdown_note_citing_a_doi_is_not_that_paper(kb, tmp_path, doi_lookup, text):  # noqa: F811
    (tmp_path / "group-meeting-notes.md").write_text(text)
    result = run("ingest", str(tmp_path / "group-meeting-notes.md"))
    assert "Ignored DOI" in result.output
    assert DOI not in bib(kb) and "smith2015" not in bib(kb).lower()
    assert "Oxygen evolution" not in bib(kb)


def test_stdin_note_citing_a_doi_is_not_that_paper(kb, doi_lookup):  # noqa: F811
    result = run("ingest", "-", input=NOTE_BODY)
    assert "Ignored DOI" in result.output
    assert DOI not in bib(kb) and "Oxygen evolution" not in bib(kb)


def test_web_page_with_its_title_by_its_doi_takes_the_fetched_entry(kb, tmp_path, doi_lookup):  # noqa: F811
    (tmp_path / "page.md").write_text(
        f"Oxygen evolution on perovskite oxides\n\nJane Smith, John Doe\n\ndoi: {DOI}\n")
    result = run("ingest", str(tmp_path / "page.md"))
    assert "Ignored DOI" not in result.output
    assert DOI in bib(kb) and "Smith" in bib(kb)


TITLE = "Oxygen evolution on perovskite oxides"


@pytest.mark.parametrize("name, text", [
    ("article.html",
     f"<html><head><title>{TITLE} | ACS Catalysis</title>"
     f'<meta name="citation_doi" content="{DOI}"></head><body>'
     f"<h1>{TITLE}</h1><p>https://doi.org/{DOI}</p></body></html>"),
    ("article2.htm",
     f"<html><head><meta name='dc.identifier' content='doi:{DOI.upper()}'>"
     f"<title>ACS Catalysis - {TITLE}</title></head><body>doi:{DOI}</body></html>"),
    ("abs.md", f"Computer Science\n\n# Title:{TITLE}\n\nhttps://doi.org/{DOI}\n"),
])
def test_page_that_declares_its_own_doi_keeps_it(kb, tmp_path, doi_lookup, name, text):  # noqa: F811
    p = tmp_path / name
    p.write_text(text)
    result = run("ingest", str(p))
    assert result.exit_code == 0, result.output
    assert DOI in bib(kb)


@pytest.mark.parametrize("name, text", [
    ("hn.html", f"<html><head><title>{TITLE} | Hacker News</title></head>"
                f"<body><a href='https://doi.org/{DOI}'>{TITLE}</a></body></html>"),
    ("yt.html", f"<html><head><title>{TITLE} - YouTube</title></head>"
                f"<body>https://doi.org/{DOI}</body></html>"),
    ("summary.md", f"# Paper summary - {TITLE}\n\nhttps://doi.org/{DOI}\n"),
    ("other.html", f"<html><head><title>Something else</title>"
                   f'<meta name="citation_doi" content="10.1000/other"></head>'
                   f"<body>https://doi.org/{DOI}</body></html>"),
])
def test_page_about_a_paper_does_not_take_its_doi(kb, tmp_path, doi_lookup, name, text):  # noqa: F811
    p = tmp_path / name
    p.write_text(text)
    result = run("ingest", str(p))
    assert result.exit_code == 0, result.output
    assert DOI not in bib(kb)


def test_note_titled_about_a_paper_does_not_take_its_doi(kb, tmp_path, doi_lookup):  # noqa: F811
    p = tmp_path / "note.md"
    p.write_text(f"# Notes on {TITLE}\n\nSee https://doi.org/{DOI}\n")
    result = run("ingest", str(p))
    assert result.exit_code == 0, result.output
    assert DOI not in bib(kb)
    assert "Ignored DOI" in result.output


def test_note_citing_a_missing_sources_doi_is_not_restored_as_it(kb, tmp_path, doi_lookup):  # noqa: F811
    # The row for web/notes.md was rebuilt from its entry (a fresh clone):
    # a different notes.md that only cites that paper's DOI is not it.
    (kb / ".crucible/references.bib").write_text(
        "@article{smith2015, title={Oxygen evolution on perovskite oxides}, "
        f"author={{Smith, Jane}}, year={{2015}}, doi={{{DOI}}}, file={{{WEB}/notes.md}}}}\n")
    run("sync")
    assert f"{WEB}/notes.md" in rows(kb)
    (tmp_path / "notes.md").write_text(NOTE_BODY)
    result = run("ingest", str(tmp_path / "notes.md"), ok=False)
    assert result.exit_code != 0
    assert not (kb / WEB / "notes.md").exists()


# 2. an upper-case .TXT is its own text ---------------------------------------

def test_upper_case_txt_is_ingested_without_hanging(kb, tmp_path, alarm):  # noqa: F811
    (tmp_path / "NOTES.TXT").write_text("Field notes from the reactor run.\n")
    run("ingest", str(tmp_path / "NOTES.TXT"))
    stored = sorted(p.name for p in (kb / WEB).iterdir())
    assert stored == ["NOTES.TXT"]
    assert (kb / WEB / "NOTES.TXT").read_text() == "Field notes from the reactor run.\n"


def test_upper_case_txt_is_not_its_own_sidecar(kb, tmp_path):  # noqa: F811
    if not case_insensitive(tmp_path):
        pytest.skip("needs a case-insensitive filesystem")
    disk = kb / WEB / "NOTES.TXT"
    disk.parent.mkdir(parents=True, exist_ok=True)
    disk.write_text("Field notes.\n")
    db = CrucibleDB(kb / ".crucible" / "crucible.db")
    try:
        src = {"path": f"{WEB}/NOTES.TXT", "metadata": json.dumps({})}
        assert not cli_module._is_own_sidecar(kb, db, src, disk, disk.with_suffix(".txt"))
    finally:
        db.close()


def test_name_search_is_bounded(kb, tmp_path, monkeypatch, alarm):  # noqa: F811
    monkeypatch.setattr(ingest_module, "_MAX_NAME_TRIES", 5)
    monkeypatch.setattr(ingest_module, "_claim", lambda path, created: False)
    (tmp_path / "paper.md").write_text("# A paper\n\nbody\n")
    result = run("ingest", str(tmp_path / "paper.md"), ok=False)
    assert result.exit_code != 0
    assert "free name" in result.output


# 3. a sync during an ingest does not lose the source ---------------------------

def test_sync_during_ingest_waits_for_the_row(kb, tmp_path, monkeypatch):  # noqa: F811
    real = ingest_module.append_bib_entry_unique_key
    syncs = []

    def sync():
        other = CrucibleDB(kb / ".crucible" / "crucible.db")
        try:
            ingest_module.reconcile_sources(kb, other)
        finally:
            other.close()

    def append_then_sync(root, db, base, entry):
        key = real(root, db, base, entry)
        # A concurrent `crucible sync`, given a second to run in the window
        # between the entry and the row.
        thread = threading.Thread(target=sync)
        thread.start()
        thread.join(timeout=1.0)
        syncs.append(thread)
        return key

    monkeypatch.setattr(ingest_module, "append_bib_entry_unique_key", append_then_sync)
    (tmp_path / "paper.md").write_text("# A study of things\n\nbody\n")
    run("ingest", str(tmp_path / "paper.md"))
    for thread in syncs:
        thread.join(timeout=15)
        assert not thread.is_alive()
    monkeypatch.setattr(ingest_module, "append_bib_entry_unique_key", real)

    row = rows(kb)[f"{WEB}/paper.md"]
    key = json.loads(row["metadata"])["cite_key"]
    assert key in bib(kb)
    assert (kb / WEB / "paper.md").exists()
    run("sync")
    assert f"{WEB}/paper.md" in rows(kb) and key in bib(kb)


def test_disk_scan_row_made_during_ingest_is_adopted(kb, tmp_path, monkeypatch):  # noqa: F811
    # A sync after the notebook is copied but before its entry is written
    # indexes it from the disk scan; ingest takes that row over.
    real = ingest_module.append_bib_entry_unique_key

    def sync_then_append(root, db, base, entry):
        other = CrucibleDB(root / ".crucible" / "crucible.db")
        try:
            ingest_module.reconcile_sources(root, other)
        finally:
            other.close()
        return real(root, db, base, entry)

    monkeypatch.setattr(ingest_module, "append_bib_entry_unique_key", sync_then_append)
    (tmp_path / "notes.org").write_text("#+TITLE: Lab notes\n\nfirst run\n")
    result = run("ingest", str(tmp_path / "notes.org"), "--type", "notebook")
    monkeypatch.setattr(ingest_module, "append_bib_entry_unique_key", real)

    path = ".crucible/sources/notebooks/notes.org"
    (row,) = [r for p, r in rows(kb).items() if p == path]
    metadata = json.loads(row["metadata"])
    assert "content_hash" in metadata and "disk_scan" not in metadata
    assert metadata["cite_key"] in bib(kb)
    assert f"citep:{metadata['cite_key']}" in result.output


def test_restore_into_a_row_without_a_bib_entry_is_refused(kb, tmp_path):  # noqa: F811
    (kb / ".crucible/references.bib").write_text(
        "@misc{study2026, title={A study of things}, year={2026}, "
        f"file={{{WEB}/paper.md}}}}\n")
    run("sync")
    assert f"{WEB}/paper.md" in rows(kb)
    # The entry is removed (an interrupted ingest, or by hand) before a
    # sync drops the row: restoring into it would leave a source with no
    # entry, which the next sync removes.
    (kb / ".crucible/references.bib").write_text("")
    (tmp_path / "paper.md").write_text("# A study of things\n\nbody\n")
    result = run("ingest", str(tmp_path / "paper.md"), ok=False)
    assert result.exit_code != 0
    assert "no entry" in result.output
    assert not (kb / WEB / "paper.md").exists()
