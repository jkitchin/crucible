"""Round-4 fixes in the CLI: link checks, init, merge, delete, install, source."""

import json
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from crucible import cli as cli_module
from crucible.cli import cli
from crucible.database import CrucibleDB


def invoke(*args, input=None):
    return CliRunner().invoke(cli, list(args), input=input)


def ok(*args, input=None):
    result = invoke(*args, input=input)
    assert result.exit_code == 0, result.output
    return result


def make_project(tmp_path, monkeypatch, name="kb"):
    root = tmp_path / name
    root.mkdir()
    monkeypatch.chdir(root)
    ok("init", "--root", str(root))
    return root


ARTICLE = """#+TITLE: {title}
#+FILETAGS: concept x
* {title}
:PROPERTIES:
:ARTICLE_TYPE: concept
:STATUS: draft
:CREATED: 2026-01-01
{extra}:END:
{body}
"""


def article(title, body="text", extra=""):
    return ARTICLE.format(title=title, body=body, extra=extra)


def concepts(root: Path) -> Path:
    return root / ".crucible" / "wiki" / "concepts"


def db_rows(root: Path, sql: str) -> list[tuple]:
    db = CrucibleDB(root / ".crucible" / "crucible.db")
    try:
        return [tuple(r.values()) if isinstance(r, dict) else tuple(r)
                for r in db.conn.execute(sql).fetchall()]
    finally:
        db.close()


LINKS_SQL = (
    "SELECT a.path AS a_path, b.path AS b_path FROM article_links l "
    "JOIN articles a ON a.id = l.from_id JOIN articles b ON b.id = l.to_id"
)
DERIVATIONS_SQL = (
    "SELECT a.path AS a_path, b.path AS b_path FROM article_derivations d "
    "JOIN articles a ON a.id = d.derived_id JOIN articles b ON b.id = d.source_id"
)


# -- 1. an overlong link target is a broken link, not a crash --------------


def test_overlong_link_target_does_not_crash_sync_or_lint(tmp_path, registry, monkeypatch):
    root = make_project(tmp_path, monkeypatch)
    long = "x" * 300
    (concepts(root) / "c.org").write_text(article("C", f"see [[file:{long}.org][x]]"))
    ok("sync")
    assert db_rows(root, "SELECT path FROM articles") == [(".crucible/wiki/concepts/c.org",)]
    result = invoke("lint", "--json")
    assert result.exception is None or isinstance(result.exception, SystemExit), result.output
    issues = json.loads(result.stdout)
    assert any(i["type"] == "broken_link" for i in issues)


def test_overlong_link_target_does_not_break_auto_rebuild(tmp_path, registry, monkeypatch):
    root = make_project(tmp_path, monkeypatch)
    (concepts(root) / "c.org").write_text(article("C", f"[[file:{'y' * 300}.org][y]]"))
    ok("sync")
    for f in (root / ".crucible").glob("crucible.db*"):
        f.unlink()
    result = ok("stats", "--json")
    assert json.loads(result.stdout)["articles"] == 1


# -- 2. init in a fresh clone rebuilds the database from the wiki ----------


def test_init_in_a_fresh_clone_rebuilds_the_database(tmp_path, registry, monkeypatch):
    root = make_project(tmp_path, monkeypatch)
    (concepts(root) / "a.org").write_text(article("A", "see [[file:b.org][b]]"))
    (concepts(root) / "b.org").write_text(article("B"))
    ok("sync")
    manifest = root / ".crucible" / "wiki" / "MANIFEST.md"
    assert "- 2 articles" in manifest.read_text()
    committed = manifest.read_text()
    for f in (root / ".crucible").glob("crucible.db*"):
        f.unlink()

    ok("init", "--root", str(root))
    assert len(db_rows(root, "SELECT path FROM articles")) == 2
    assert db_rows(root, LINKS_SQL) == [
        (".crucible/wiki/concepts/a.org", ".crucible/wiki/concepts/b.org")
    ]
    # The manifest still counts the articles (only its timestamp may differ).
    def counts(text):
        return [line for line in text.splitlines() if any(ch.isdigit() for ch in line)
                and "Generated" not in line and "Updated" not in line]
    assert counts(manifest.read_text()) == counts(committed)


def test_init_of_a_new_project_still_creates_an_empty_database(tmp_path, registry, monkeypatch):
    root = make_project(tmp_path, monkeypatch)
    assert db_rows(root, "SELECT path FROM articles") == []


# -- 8. install replaces the first of several directive blocks -------------


def test_install_collapses_duplicate_directive_blocks(tmp_path, registry, monkeypatch):
    make_project(tmp_path, monkeypatch)
    claude_md = Path(os.environ["HOME"]) / ".claude" / "CLAUDE.md"
    claude_md.parent.mkdir(parents=True, exist_ok=True)
    marker = "<!-- crucible-knowledge-base -->"
    claude_md.write_text(
        "# Mine\n\n"
        f"{marker}\nold one\n{marker}\n\n"
        "middle text\n\n"
        f"{marker}\nold two\n{marker}\n\n"
        "tail text\n"
    )
    ok("install")
    text = claude_md.read_text()
    assert text.count(marker) == 2
    assert text.count("## Crucible Knowledge Base") == 1
    assert "old one" not in text and "old two" not in text
    assert text.startswith("# Mine\n\n" + marker)
    assert "middle text" in text and "tail text" in text
    # Idempotent.
    ok("install")
    assert claude_md.read_text() == text


# -- merge helpers ---------------------------------------------------------


def two_projects(tmp_path, monkeypatch):
    target = make_project(tmp_path, monkeypatch, "target")
    source = make_project(tmp_path, monkeypatch, "source")
    monkeypatch.chdir(target)
    return target, source


def sync_in(root, monkeypatch):
    monkeypatch.chdir(root)
    ok("sync")


def merge_into(target, source, monkeypatch):
    monkeypatch.chdir(target)
    return ok("merge", str(source), "--yes")


# -- 3. DERIVED_FROM follows renamed wiki files -----------------------------


@pytest.mark.parametrize("ref, expected", [
    ("b.org", "b_1.org"),
    ("../concepts/b.org", "b_1.org"),  # respelled, as links are
    ("concepts/b.org", "concepts/b_1.org"),          # relative to the wiki
    ("wiki/concepts/b.org", "wiki/concepts/b_1.org"),
])
def test_merge_rewrites_derived_from_for_renamed_articles(
    tmp_path, registry, monkeypatch, ref, expected
):
    target, source = two_projects(tmp_path, monkeypatch)
    (concepts(target) / "b.org").write_text(article("Target B", "target"))
    sync_in(target, monkeypatch)
    (concepts(source) / "b.org").write_text(article("Incoming B", "incoming"))
    (concepts(source) / "c.org").write_text(
        article("Incoming C", "derived", extra=f":DERIVED_FROM: {ref}\n"))
    sync_in(source, monkeypatch)

    merge_into(target, source, monkeypatch)
    text = (concepts(target) / "c.org").read_text()
    assert f":DERIVED_FROM: {expected}\n" in text
    assert db_rows(target, DERIVATIONS_SQL) == [
        (".crucible/wiki/concepts/c.org", ".crucible/wiki/concepts/b_1.org")
    ]


def test_merge_leaves_derived_from_of_unrenamed_articles(tmp_path, registry, monkeypatch):
    target, source = two_projects(tmp_path, monkeypatch)
    (concepts(target) / "b.org").write_text(article("Target B"))
    sync_in(target, monkeypatch)
    (concepts(source) / "a.org").write_text(article("A"))
    (concepts(source) / "c.org").write_text(
        article("C", extra=":DERIVED_FROM: a.org concepts/a.org\n"))
    sync_in(source, monkeypatch)
    merge_into(target, source, monkeypatch)
    assert ":DERIVED_FROM: a.org concepts/a.org\n" in (concepts(target) / "c.org").read_text()


# -- 4. links into a symlinked wiki are rewritten ---------------------------


def test_merge_rewrites_links_when_the_target_wiki_is_a_symlink(tmp_path, registry, monkeypatch):
    target, source = two_projects(tmp_path, monkeypatch)
    dropbox = tmp_path / "Dropbox"
    dropbox.mkdir()
    shutil.move(str(target / ".crucible/wiki"), str(dropbox / "wiki"))
    os.symlink(dropbox / "wiki", target / ".crucible/wiki")
    (concepts(target) / "b.org").write_text(article("Target B"))
    sync_in(target, monkeypatch)
    (concepts(source) / "b.org").write_text(article("Incoming B"))
    (concepts(source) / "c.org").write_text(
        article("Incoming C", "see [[file:b.org][b]]", extra=":DERIVED_FROM: b.org\n"))
    sync_in(source, monkeypatch)

    merge_into(target, source, monkeypatch)
    text = (concepts(target) / "c.org").read_text()
    assert "[[file:b_1.org][b]]" in text
    assert ":DERIVED_FROM: b_1.org\n" in text
    assert db_rows(target, LINKS_SQL) == [
        (".crucible/wiki/concepts/c.org", ".crucible/wiki/concepts/b_1.org")
    ]


# -- 6. a source crucible without a database can be merged ------------------


def test_merge_from_a_fresh_clone_without_a_database(tmp_path, registry, monkeypatch):
    target, source = two_projects(tmp_path, monkeypatch)
    (concepts(source) / "c.org").write_text(article("C"))
    data = source / ".crucible/sources/data/numbers.csv"
    data.write_text("a,b\n1,2\n")
    sync_in(source, monkeypatch)
    for f in (source / ".crucible").glob("crucible.db*"):
        f.unlink()
    before = sorted(p.name for p in (source / ".crucible").iterdir())

    result = merge_into(target, source, monkeypatch)
    assert "Merged: 1 sources, 1 articles" in result.output
    assert (concepts(target) / "c.org").exists()
    assert (target / ".crucible/sources/data/numbers.csv").exists()
    # Nothing was written into the source.
    assert sorted(p.name for p in (source / ".crucible").iterdir()) == before


def test_merge_warns_about_a_stale_source_database(tmp_path, registry, monkeypatch):
    target, source = two_projects(tmp_path, monkeypatch)
    (concepts(source) / "c.org").write_text(article("C"))
    sync_in(source, monkeypatch)
    (source / ".crucible/sources/data/late.csv").write_text("x\n1\n")
    db_before = (source / ".crucible/crucible.db").read_bytes()

    monkeypatch.chdir(target)
    result = invoke("merge", str(source), "--dry-run")
    assert result.exit_code == 0, result.output
    assert "lacks 1 source file" in result.output
    assert (source / ".crucible/crucible.db").read_bytes() == db_before


# -- 9/10. repeat merges add nothing; links follow existing renamed copies --


def test_repeat_merge_with_renamed_articles_adds_nothing(tmp_path, registry, monkeypatch):
    target, source = two_projects(tmp_path, monkeypatch)
    (concepts(target) / "b.org").write_text(article("Target B"))
    sync_in(target, monkeypatch)
    (concepts(source) / "a.org").write_text(
        article("A", "see [[file:b.org][b]]", extra=":DERIVED_FROM: b.org\n"))
    (concepts(source) / "b.org").write_text(article("Incoming B", "see [[file:a.org][a]]"))
    sync_in(source, monkeypatch)

    merge_into(target, source, monkeypatch)
    files = sorted(p.name for p in concepts(target).iterdir())
    assert files == ["a.org", "b.org", "b_1.org"]
    assert "[[file:b_1.org][b]]" in (concepts(target) / "a.org").read_text()

    result = merge_into(target, source, monkeypatch)
    assert sorted(p.name for p in concepts(target).iterdir()) == files
    assert "Merged: 0 sources, 0 articles" in result.output


def test_new_article_links_follow_a_copy_an_earlier_merge_renamed(
    tmp_path, registry, monkeypatch
):
    target, source = two_projects(tmp_path, monkeypatch)
    (concepts(target) / "b.org").write_text(article("Target B"))
    sync_in(target, monkeypatch)
    (concepts(source) / "b.org").write_text(article("Incoming B"))
    sync_in(source, monkeypatch)
    merge_into(target, source, monkeypatch)
    assert (concepts(target) / "b_1.org").exists()

    # The source gains an article linking to b.org; merging again copies
    # only it, and its link goes to the copy that holds the source's b.org.
    (concepts(source) / "d.org").write_text(
        article("D", "see [[file:b.org][b]]", extra=":DERIVED_FROM: b.org\n"))
    sync_in(source, monkeypatch)
    merge_into(target, source, monkeypatch)
    assert sorted(p.name for p in concepts(target).iterdir()) == ["b.org", "b_1.org", "d.org"]
    text = (concepts(target) / "d.org").read_text()
    assert "[[file:b_1.org][b]]" in text
    assert ":DERIVED_FROM: b_1.org\n" in text


# -- 11. wiki files linking outside the wiki are not copied -----------------


def test_merge_skips_wiki_symlinks_that_point_outside_the_wiki(tmp_path, registry, monkeypatch):
    target, source = two_projects(tmp_path, monkeypatch)
    secret = tmp_path / "id_rsa"
    secret.write_text("PRIVATE KEY")
    figures = source / ".crucible/wiki/concepts/figures"
    figures.mkdir()
    (figures / "plot.png").write_bytes(b"png")
    os.symlink(secret, figures / "key.png")
    os.symlink(figures / "plot.png", figures / "alias.png")  # inside: kept
    sync_in(source, monkeypatch)

    result = merge_into(target, source, monkeypatch)
    assert "links outside the wiki" in result.output
    copied = sorted(p.name for p in (concepts(target) / "figures").iterdir())
    assert copied == ["alias.png", "plot.png"]


def test_unreadable_wiki_directory_is_a_clean_error(tmp_path, registry, monkeypatch):
    if os.geteuid() == 0:
        pytest.skip("root can read any directory")
    target, source = two_projects(tmp_path, monkeypatch)
    locked = source / ".crucible/wiki/concepts/locked"
    locked.mkdir()
    locked.chmod(0)
    try:
        monkeypatch.chdir(target)
        result = invoke("merge", str(source), "--yes")
    finally:
        locked.chmod(0o755)
    assert result.exit_code != 0
    assert "Cannot read" in result.output
    assert not isinstance(result.exception, PermissionError)


def test_wiki_other_files_scandir_errors_are_clean(tmp_path, monkeypatch):
    wiki = tmp_path / "wiki"
    (wiki / "concepts").mkdir(parents=True)
    real_scandir = os.scandir

    def broken(path):
        if str(path).endswith("concepts"):
            raise PermissionError(13, "Permission denied")
        return real_scandir(path)

    monkeypatch.setattr(cli_module.os, "scandir", broken)
    with pytest.raises(cli_module.click.ClickException, match="Cannot read"):
        cli_module._wiki_other_files(wiki)


# -- 5. delete --file keeps files outside the project -----------------------


def test_delete_file_keeps_a_source_stored_outside_the_project(tmp_path, registry, monkeypatch):
    root = make_project(tmp_path, monkeypatch)
    library = tmp_path / "Zotero"
    library.mkdir()
    paper = library / "smith.csv"
    paper.write_text("a,b\n1,2\n")
    sidecar = library / "smith.txt"
    sidecar.write_text("a b 1 2")
    (root / ".crucible/references.bib").write_text(
        "@article{smith2024,\n  title={Smith},\n  year={2024},\n"
        f"  file={{{paper}}}\n}}\n")
    ok("sync")
    assert db_rows(root, "SELECT path FROM sources") == [(str(paper),)]

    result = ok("delete", "smith2024", "--file", "--yes")
    assert "outside this project" in result.output
    assert paper.exists() and sidecar.exists()
    assert db_rows(root, "SELECT path FROM sources") == []
    assert "file" not in (root / ".crucible/references.bib").read_text().split("year")[1]


def test_delete_file_still_deletes_a_source_inside_the_project(tmp_path, registry, monkeypatch):
    root = make_project(tmp_path, monkeypatch)
    data = root / ".crucible/sources/data/numbers.csv"
    data.write_text("a,b\n1,2\n")
    ok("sync")
    ok("delete", ".crucible/sources/data/numbers.csv", "--file", "--yes")
    assert not data.exists()
    assert db_rows(root, "SELECT path FROM sources") == []


# -- 7. a failed database delete leaves the file and the bib entry ---------


def test_failed_source_delete_leaves_file_and_bib_field(tmp_path, registry, monkeypatch):
    root = make_project(tmp_path, monkeypatch)
    pdf = root / ".crucible/sources/external/pdfs/paper.pdf"
    pdf.write_bytes(b"%PDF-1.4 x")
    bib = root / ".crucible/references.bib"
    bib.write_text(
        "@article{paper2024,\n  title={Paper},\n  year={2024},\n"
        "  file={.crucible/sources/external/pdfs/paper.pdf}\n}\n")
    ok("sync")
    bib_before = bib.read_text()

    def locked(self, source_id):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(CrucibleDB, "delete_source", locked)
    result = invoke("delete", "paper2024", "--file", "--yes")
    assert result.exit_code != 0
    assert pdf.exists()
    assert bib.read_text() == bib_before
    monkeypatch.undo()
    assert db_rows(root, "SELECT path FROM sources") == [
        (".crucible/sources/external/pdfs/paper.pdf",)
    ]


def test_failed_article_delete_leaves_the_file(tmp_path, registry, monkeypatch):
    root = make_project(tmp_path, monkeypatch)
    path = concepts(root) / "c.org"
    path.write_text(article("C"))
    ok("sync")

    def locked(self, article_id):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(CrucibleDB, "delete_article", locked)
    result = invoke("delete", ".crucible/wiki/concepts/c.org", "--file", "--yes")
    assert result.exit_code != 0
    assert path.exists()


# -- 12. source refuses an ambiguous title or partial path -----------------


def test_source_lists_candidates_for_an_ambiguous_fragment(tmp_path, registry, monkeypatch):
    root = make_project(tmp_path, monkeypatch)
    for name in ("alpha-run.csv", "beta-run.csv"):
        (root / ".crucible/sources/data" / name).write_text("x\n1\n")
    ok("sync")
    result = invoke("source", "run")
    assert result.exit_code != 0
    assert "matches 2 sources" in result.output
    assert "alpha-run.csv" in result.output and "beta-run.csv" in result.output

    result = ok("source", "beta-run")
    assert "Path: .crucible/sources/data/beta-run.csv" in result.output
