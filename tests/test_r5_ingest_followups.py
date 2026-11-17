"""Follow-ups from the review of #41: re-ingest attribution and bib-driven pruning."""

import json
from contextlib import contextmanager

import pytest
from click.testing import CliRunner
from test_data_loss_ingest import kb, run  # noqa: F401

from crucible import ingest as ingest_module
from crucible.cli import cli
from crucible.database import CrucibleDB

BIB = ".crucible/references.bib"


def db_of(root):
    return CrucibleDB(root / ".crucible" / "crucible.db")


def ingest(root, path, **kwargs):
    kwargs.setdefault("date", "2024-01-02")
    db = db_of(root)
    try:
        return ingest_module.ingest_source(root, db, path, **kwargs)
    finally:
        db.close()


def rows(root):
    """(path, cite key) of every source row."""
    db = db_of(root)
    try:
        return sorted(
            (s["path"], json.loads(s["metadata"] or "{}").get("cite_key"))
            for s in db.list_sources()
        )
    finally:
        db.close()


def rebuild(root):
    (root / ".crucible" / "crucible.db").unlink()
    run("sync")


def fresh_clone_of(root, first):
    """Remove the stored (gitignored) copy and its sidecar, then rebuild."""
    stored = root / first["relative_path"]
    stored.unlink()
    stored.with_suffix(".txt").unlink(missing_ok=True)
    rebuild(root)
    return stored


# -- 1. re-ingest by name must be the same source ---------------------------


def test_a_different_file_with_the_same_name_is_refused(kb, tmp_path):  # noqa: F811
    old = tmp_path / "old" / "paper.md"
    old.parent.mkdir()
    old.write_text("# Catalysis on Platinum\nbody\n")
    first = ingest(kb, old)
    stored = fresh_clone_of(kb, first)
    bib, before = (kb / BIB).read_text(), rows(kb)

    new = tmp_path / "new" / "paper.md"
    new.parent.mkdir()
    new.write_text("# Something Else Entirely\nother body\n")
    with pytest.raises(ValueError, match="--restore"):
        ingest(kb, new)
    assert not stored.exists()
    assert (kb / BIB).read_text() == bib
    assert rows(kb) == before


def test_restore_flag_overrides_the_check(kb, tmp_path):  # noqa: F811
    old = tmp_path / "old" / "paper.md"
    old.parent.mkdir()
    old.write_text("# Catalysis on Platinum\nbody\n")
    first = ingest(kb, old)
    stored = fresh_clone_of(kb, first)

    # Same paper, retitled copy: the user says it is that source.
    new = tmp_path / "new" / "paper.md"
    new.parent.mkdir()
    new.write_text("# Catalysis on Pt (preprint)\nbody\n")
    again = ingest(kb, new, restore=True)
    assert again.get("restored")
    assert again["cite_key"] == first["cite_key"]
    assert stored.read_bytes() == new.read_bytes()


def test_a_disagreeing_doi_is_refused_and_a_matching_one_restores(kb, tmp_path):  # noqa: F811
    old = tmp_path / "old" / "paper.md"
    old.parent.mkdir()
    old.write_text("# Catalysis on Platinum\nbody\n")
    first = ingest(kb, old, doi="10.1234/abcd.1")
    stored = fresh_clone_of(kb, first)

    other = tmp_path / "other" / "paper.md"
    other.parent.mkdir()
    other.write_text("# Catalysis on Platinum\nbody\n")
    with pytest.raises(ValueError, match="DOI"):
        ingest(kb, other, doi="10.1234/zzzz.9")
    assert not stored.exists()

    # The DOI decides even when the title the file declares changed.
    same = tmp_path / "same" / "paper.md"
    same.parent.mkdir()
    same.write_text("# Platinum catalysis, revised\nbody\n")
    again = ingest(kb, same, doi="https://doi.org/10.1234/ABCD.1")
    assert again.get("restored")
    assert again["cite_key"] == first["cite_key"]


def test_cli_refusal_is_a_clean_error_and_restore_flag_works(kb, tmp_path):  # noqa: F811
    old = tmp_path / "old" / "paper.md"
    old.parent.mkdir()
    old.write_text("# Catalysis on Platinum\nbody\n")
    first = ingest(kb, old)
    stored = fresh_clone_of(kb, first)

    new = tmp_path / "new" / "paper.md"
    new.parent.mkdir()
    new.write_text("# Unrelated\nother\n")
    result = CliRunner().invoke(cli, ["ingest", str(new)])
    assert result.exit_code == 1
    assert "--restore" in result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert not stored.exists()

    result = CliRunner().invoke(cli, ["ingest", str(new), "--restore"])
    assert result.exit_code == 0, result.output
    assert stored.read_bytes() == new.read_bytes()


# -- 2. sync's release of unlisted rows -------------------------------------


def _two_sources(root, tmp_path):
    results = []
    for name, body in (("a.md", "# Alpha\nbody\n"), ("b.md", "# Beta\nbody\n")):
        p = tmp_path / name
        p.write_text(body)
        results.append(ingest(root, p))
    return results


def test_a_malformed_bib_keeps_rows_and_warns(kb, tmp_path):  # noqa: F811
    a, b = _two_sources(kb, tmp_path)
    run("sync")
    before = rows(kb)
    assert len(before) == 2
    # Half-edited: b's entry loses its closing brace, so it no longer parses.
    bib = (kb / BIB).read_text()
    start = bib.index("@", bib.index(b["cite_key"]) - 20)
    broken = bib[:start] + bib[start:].rstrip().rstrip("}") + "\n"
    (kb / BIB).write_text(broken)

    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    assert rows(kb) == before
    assert "malformed" in result.output

    # Fixed by removing the entry for real: now b's row is released.
    (kb / BIB).write_text(bib[:start].rstrip() + "\n")
    run("sync")
    assert [k for _, k in rows(kb)] == [a["cite_key"]]


def test_an_ingest_finishing_before_the_transaction_is_not_released(kb, tmp_path):  # noqa: F811
    """The bib is read inside the write transaction: an ingest that appends
    its entry and adds its row just before reconcile starts keeps its row."""
    alpha = tmp_path / "a.md"
    alpha.write_text("# Alpha\nbody\n")
    first = ingest(kb, alpha)
    late = tmp_path / "late.md"
    late.write_text("# Late arrival\nbody\n")

    db = db_of(kb)
    original = db.conn.transaction
    done = []

    @contextmanager
    def transaction_after_an_ingest():
        if not done:
            done.append(ingest(kb, late))   # another process, in effect
        with original():
            yield

    db.conn.transaction = transaction_after_an_ingest
    try:
        ingest_module.reconcile_sources(kb, db)
    finally:
        db.conn.transaction = original
        db.close()
    keys = [k for _, k in rows(kb)]
    assert done and done[0]["cite_key"] in keys
    assert first["cite_key"] in keys
    md = None
    db = db_of(kb)
    try:
        for s in db.list_sources():
            meta = json.loads(s["metadata"] or "{}")
            if meta.get("cite_key") == done[0]["cite_key"]:
                md = meta
    finally:
        db.close()
    assert md and "content_hash" in md
