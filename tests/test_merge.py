"""Tests for `crucible merge`: source identity, sidecars and bibliography paths."""

import json
import re
import shutil

import pytest
from click.testing import CliRunner

from crucible.cli import cli
from crucible.database import CrucibleDB

# An article whose :SOURCE_KEYS: tests fill in to link it to a merged source.
CONCEPT = """#+TITLE: Activation energy
#+FILETAGS: :concept:kinetics:

* Activation energy
:PROPERTIES:
:ARTICLE_TYPE: concept
:SOURCE_KEYS:
:STATUS: draft
:CREATED: 2026-01-01
:END:

The energy barrier for a reaction.
"""


def make_crucible(root):
    """Initialize an empty crucible at root and return root."""
    root.mkdir()
    result = CliRunner().invoke(cli, ["init", "--root", str(root)])
    assert result.exit_code == 0, result.output
    return root


def copy_without_database(root, dest):
    """Copy a crucible's files but not its database, as a fresh clone would."""
    shutil.copytree(
        root / ".crucible",
        dest / ".crucible",
        ignore=shutil.ignore_patterns("crucible.db", "crucible.db-wal", "crucible.db-shm"),
    )
    return dest


def claim_in_target_bib(root, *rel_paths):
    """Give target files bibliography entries, so the disk scan does not key
    them by file name and an incoming source with that name is no key collision."""
    bib = root / ".crucible/references.bib"
    text = bib.read_text() if bib.exists() else ""
    for i, rel_path in enumerate(rel_paths):
        text += f"\n@misc{{target{i}2026,\n  file = {{{rel_path}}},\n}}\n"
    bib.write_text(text)


def run_json(*args):
    result = CliRunner().invoke(cli, [*args, "--json"])
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)


@pytest.fixture
def project(tmp_path, registry, monkeypatch):
    """The merge target: an initialized, synced crucible; cwd is its root."""
    root = make_crucible(tmp_path / "kb")
    monkeypatch.chdir(root)
    (root / ".crucible/wiki/concepts/activation-energy.org").write_text(CONCEPT)
    result = CliRunner().invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    return root


@pytest.mark.parametrize(
    ("absolute_file", "collision", "alias", "double_space", "bib_alias", "registered"),
    [(False, False, False, False, False, False), (True, False, False, False, False, False),
     (False, True, False, False, False, False), (True, True, False, False, False, False),
     (False, False, True, False, False, False), (False, True, False, True, False, False),
     (False, True, False, False, True, False), (False, True, True, False, True, False),
     (False, True, False, False, False, True)],
)
def test_merge_preserves_imported_source_after_database_rebuild(
    project, tmp_path, monkeypatch, absolute_file, collision, alias, double_space, bib_alias,
    registered
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()

    filename = "evidence  notes.md" if double_space else "evidence notes.md"
    original = tmp_path / filename
    original.write_text("Imported source content.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(
        cli,
        ["ingest", str(original), "--type", "data",
         "--title", "Synthetic Evidence", "--date", "2026-01-01"],
    )
    assert result.exit_code == 0, result.output
    source_path = f".crucible/sources/data/{filename}"
    if bib_alias:
        try:
            (source / ".crucible/alias.md").symlink_to(f"sources/data/{filename}")
        except (OSError, NotImplementedError) as exc:
            pytest.skip(f"Symlinks unavailable: {exc}")
        file_value = ".crucible/alias.md"
    else:
        file_value = str(source / source_path) if absolute_file else source_path
    file_field = f'"{file_value}"' if absolute_file else f"{{{file_value}}}"
    bib = source / ".crucible" / "references.bib"
    bib.write_text(bib.read_text().replace("\n}", f"\n  file = {file_field},\n}}", 1))
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(
        CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic2026")
    )
    if alias:
        try:
            (source / ".crucible/sources/data/alias.md").symlink_to(filename)
        except (OSError, NotImplementedError) as exc:
            pytest.skip(f"Symlinks unavailable: {exc}")
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    unrelated = f"@misc{{other2026,\n  file = {{{tmp_path / 'other.pdf'}}},\n}}"
    bib.write_text(bib.read_text() + "\n" + unrelated + "\n")

    monkeypatch.chdir(project)
    if collision:
        (project / source_path).write_text("Unrelated target content.\n")
    if registered:
        result = runner.invoke(cli, ["sync"])
        assert result.exit_code == 0, result.output
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert unrelated in (project / ".crucible" / "references.bib").read_text()
    renamed_path = f".crucible/sources/data/{original.stem}_1.md" if collision else source_path
    assert [row["path"] for row in run_json("sources", article)] == [renamed_path]

    rebuilt = copy_without_database(project, tmp_path / "rebuilt")
    monkeypatch.chdir(rebuilt)
    assert [row["path"] for row in run_json("sources", article)] == [renamed_path]
    assert (rebuilt / renamed_path).read_text() == "Imported source content.\n"


@pytest.mark.parametrize(
    ("filename", "bib_filename"),
    [
        ("long name  notes.md", "long\n    name  notes.md"),
        ("literal\nname.md", "literal\nname.md"),
    ],
)
def test_merge_preserves_wrapped_and_literal_newline_bib_paths(
    project, tmp_path, monkeypatch, filename, bib_filename
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    original = tmp_path / filename
    original.write_text("Imported source content.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(
        cli,
        ["ingest", str(original), "--type", "data",
         "--title", "Synthetic Evidence", "--date", "2026-01-01"],
    )
    assert result.exit_code == 0, result.output
    source_path = f".crucible/sources/data/{filename}"
    bib = source / ".crucible/references.bib"
    bib.write_text(bib.read_text().replace(
        "\n}", f"\n  file = {{.crucible/sources/data/{bib_filename}}},\n}}", 1
    ))
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic2026"))
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    rebuilt_source = copy_without_database(source, tmp_path / "rebuilt-source")
    monkeypatch.chdir(rebuilt_source)
    assert [row["path"] for row in run_json("sources", article)] == [source_path]

    monkeypatch.chdir(project)
    (project / source_path).write_text("Existing target content.\n")
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    renamed_path = f".crucible/sources/data/{original.stem}_1.md"
    rebuilt_target = copy_without_database(project, tmp_path / "rebuilt-target")
    monkeypatch.chdir(rebuilt_target)
    assert [row["path"] for row in run_json("sources", article)] == [renamed_path]


def test_merge_preserves_punctuated_bib_keys_after_rebuild(project, tmp_path, monkeypatch):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    original = tmp_path / "evidence.md"
    original.write_text("Imported source content.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(
        cli,
        ["ingest", str(original), "--type", "data",
         "--title", "Synthetic Evidence", "--date", "2026-01-01"],
    )
    assert result.exit_code == 0, result.output
    source_path = ".crucible/sources/data/evidence.md"
    bib = source / ".crucible/references.bib"
    bib.write_text(
        bib.read_text()
        .replace("{synthetic2026,", "{synthetic-2026,")
        .replace("\n}", f"\n  file = {{{source_path}}},\n}}", 1)
    )
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.conn.execute(
            "UPDATE sources SET metadata = ?",
            (json.dumps({"cite_key": "synthetic-2026"}),),
        )
        db.conn.commit()
    finally:
        db.close()
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic-2026"))
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    (project / source_path).write_text("Existing target content.\n")
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert "synthetic-2026" in (project / ".crucible/references.bib").read_text()
    rebuilt = copy_without_database(project, tmp_path / "rebuilt")
    monkeypatch.chdir(rebuilt)
    assert [row["path"] for row in run_json("sources", article)] == [
        ".crucible/sources/data/evidence_1.md"
    ]


def test_merge_prefers_matching_citation_key_over_direct_alias(project, tmp_path, monkeypatch):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    original = tmp_path / "paper.md"
    original.write_text("Imported source content.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(
        cli,
        ["ingest", str(original), "--type", "data",
         "--title", "Synthetic Evidence", "--date", "2026-01-01"],
    )
    assert result.exit_code == 0, result.output
    try:
        (source / ".crucible/sources/data/alias.md").symlink_to("paper.md")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    bib = source / ".crucible/references.bib"
    bib.write_text(bib.read_text().replace(
        "\n}", "\n  file = {.crucible/sources/data/alias.md},\n}", 1
    ))
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic2026"))
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    (project / ".crucible/sources/data/paper.md").write_text("Existing target content.\n")
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert "file = {.crucible/sources/data/paper_1.md}" in (
        project / ".crucible/references.bib"
    ).read_text()
    rebuilt = copy_without_database(project, tmp_path / "rebuilt")
    monkeypatch.chdir(rebuilt)
    assert [row["path"] for row in run_json("sources", article)] == [
        ".crucible/sources/data/paper_1.md"
    ]


@pytest.mark.parametrize("source_exists", [False, True])
def test_merge_rejects_unindexed_bibliography_source_before_copy(
    project, tmp_path, monkeypatch, source_exists
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    rel_path = ".crucible/sources/data/paper.md"
    if source_exists:
        (source / rel_path).write_text("Incoming content.\n")
    (source / ".crucible/references.bib").write_text(
        f"@misc{{paper2026,\n  file = {{{rel_path}}},\n}}\n"
    )
    (project / rel_path).write_text("Unrelated target content.\n")
    target_bib = project / ".crucible/references.bib"
    before = target_bib.read_text() if target_bib.exists() else None
    monkeypatch.chdir(project)
    for option in ("--dry-run", "--yes"):
        result = runner.invoke(cli, ["merge", str(source), option])
        assert result.exit_code != 0
        assert "bibliography" in result.output.lower()
        assert (project / rel_path).read_text() == "Unrelated target content.\n"
        assert (target_bib.read_text() if target_bib.exists() else None) == before


def test_merge_rejects_duplicate_incoming_bib_keys_before_copying(project, tmp_path, monkeypatch):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    source_dir = source / ".crucible/sources/data"
    (source_dir / "first.md").write_text("First source.\n")
    (source_dir / "second.md").write_text("Second source.\n")
    (source / ".crucible/references.bib").write_text(
        "@misc{duplicate2026,\n  file = {.crucible/sources/data/first.md},\n}\n"
        "@misc{duplicate2026,\n  file = {.crucible/sources/data/second.md},\n}\n"
    )
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    target_bib = project / ".crucible/references.bib"
    before = target_bib.read_text() if target_bib.exists() else None
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "duplicate" in result.output.lower()
        assert not (project / ".crucible/sources/data/first.md").exists()
        assert not (project / ".crucible/sources/data/second.md").exists()
        assert (target_bib.read_text() if target_bib.exists() else None) == before


def test_merge_reserves_filenames_for_multiple_imported_sources(project, tmp_path, monkeypatch):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    source_dir = source / ".crucible/sources/data"
    (source_dir / "evidence.md").write_text("First imported source.\n")
    (source_dir / "evidence_1.md").write_text("Second imported source.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    target_dir = project / ".crucible/sources/data"
    (target_dir / "evidence.md").write_text("Existing target source.\n")
    claim_in_target_bib(project, ".crucible/sources/data/evidence.md")
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    assert preview.exit_code == 0, preview.output
    planned = {
        line.split(" -> ", 1)[1]
        for line in preview.output.splitlines()
        if line.startswith("  source: ")
    }
    assert len(planned) == 2
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (target_dir / "evidence.md").read_text() == "Existing target source.\n"
    assert {str(path.relative_to(project)) for path in target_dir.iterdir()
            if path.name != "evidence.md"} == planned
    assert {(project / path).read_text() for path in planned} == {
        "First imported source.\n", "Second imported source.\n"
    }


def test_merge_reserves_filenames_across_symlinked_target_directories(
    project, tmp_path, monkeypatch
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    (source / ".crucible/sources/data/paper.md").write_text("Imported data.\n")
    (source / ".crucible/sources/notebooks/paper_1.md").write_text("Imported notebook.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    target_data = project / ".crucible/sources/data"
    (target_data / "paper.md").write_text("Existing target.\n")
    target_notebooks = project / ".crucible/sources/notebooks"
    target_notebooks.rmdir()
    try:
        target_notebooks.symlink_to("data", target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    claim_in_target_bib(project, ".crucible/sources/data/paper.md",
                        ".crucible/sources/notebooks/paper.md")
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    assert preview.exit_code == 0, preview.output
    planned = [
        project / line.split(" -> ", 1)[1]
        for line in preview.output.splitlines()
        if line.startswith("  source: ")
    ]
    assert len(planned) == len({path.resolve() for path in planned}) == 2
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (target_data / "paper.md").read_text() == "Existing target.\n"
    assert {path.read_text() for path in planned} == {"Imported data.\n", "Imported notebook.\n"}


@pytest.mark.parametrize("existing_sidecar", [False, True])
def test_merge_does_not_overwrite_existing_or_planned_text_sidecars(
    project, tmp_path, monkeypatch, existing_sidecar
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    source_dir = source / ".crucible/sources/data"
    (source_dir / "paper.pdf").write_bytes(b"Imported PDF")
    (source_dir / "paper.txt").write_text("Imported PDF text.\n")
    (source_dir / "paper_1.txt").write_text("Independent imported text.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    target_dir = project / ".crucible/sources/data"
    (target_dir / "paper.pdf").write_bytes(b"Existing PDF")
    claim_in_target_bib(project, ".crucible/sources/data/paper.pdf")
    if existing_sidecar:
        (target_dir / "paper_1.txt").write_text("Existing target text.\n")
        claim_in_target_bib(project, ".crucible/sources/data/paper_1.txt")
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    assert preview.exit_code == 0, preview.output
    planned = {
        line.split(" -> ", 1)[1]
        for line in preview.output.splitlines()
        if line.startswith("  source: ")
    }
    assert len(planned) == 2
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (target_dir / "paper.pdf").read_bytes() == b"Existing PDF"
    if existing_sidecar:
        assert (target_dir / "paper_1.txt").read_text() == "Existing target text.\n"
    imported_pdf = next(project / path for path in planned if path.endswith(".pdf"))
    imported_text = next(project / path for path in planned if path.endswith(".txt"))
    assert imported_pdf.read_bytes() == b"Imported PDF"
    assert imported_pdf.with_suffix(".txt").read_text() == "Imported PDF text.\n"
    assert imported_text.read_text() == "Independent imported text.\n"


def test_merge_does_not_write_through_dangling_target_symlinks(project, tmp_path, monkeypatch):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    source_dir = source / ".crucible/sources/data"
    (source_dir / "paper.pdf").write_bytes(b"Imported PDF")
    (source_dir / "paper.txt").write_text("Imported text.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    target_dir = project / ".crucible/sources/data"
    try:
        (target_dir / "paper.pdf").symlink_to("missing-pdf")
        (target_dir / "paper_1.txt").symlink_to("missing-text")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    assert preview.exit_code == 0, preview.output
    assert "-> .crucible/sources/data/paper_2.pdf" in preview.output
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (target_dir / "paper.pdf").is_symlink()
    assert (target_dir / "paper_1.txt").is_symlink()
    assert not (target_dir / "missing-pdf").exists()
    assert not (target_dir / "missing-text").exists()
    assert (target_dir / "paper_2.pdf").read_bytes() == b"Imported PDF"
    assert (target_dir / "paper_2.txt").read_text() == "Imported text.\n"


@pytest.mark.parametrize("private_part", ["source", "sidecar", "outside-crucible"])
def test_merge_refuses_private_files_linked_into_shareable_sources(
    project, tmp_path, monkeypatch, private_part
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    data = source / ".crucible/sources/data"
    private = source / ".crucible/sources/external/pdfs"
    (private / "private.pdf").write_bytes(b"Private PDF")
    (private / "private.txt").write_text("Private extracted text.\n")
    try:
        if private_part == "source":
            (data / "paper.pdf").symlink_to("../external/pdfs/private.pdf")
        elif private_part == "outside-crucible":
            (source / "private.pdf").write_bytes(b"Private project file")
            (data / "paper.pdf").symlink_to("../../../private.pdf")
        else:
            (data / "paper.pdf").write_bytes(b"Public PDF")
            (data / "paper.txt").symlink_to("../external/pdfs/private.txt")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "private" in result.output.lower() or "outside" in result.output.lower()
        assert not (project / ".crucible/sources/data/paper.pdf").exists()
        assert not (project / ".crucible/sources/data/paper.txt").exists()


@pytest.mark.parametrize("reuse", [False, True])
def test_merge_refuses_private_metadata_in_shareable_source_paths(
    project, tmp_path, monkeypatch, reuse
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    data = ".crucible/sources/data"
    (source / data / "paper.pdf").write_bytes(b"Private source.")
    (source / data / "paper.txt").write_text("Private text.\n")
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.add_source(path=f"{data}/paper.pdf", title="Paper", source_type="pdf",
                      shareable=False, metadata={"cite_key": "paper2026"})
    finally:
        db.close()
    if reuse:
        (project / data / "paper.pdf").write_bytes(b"Private source.")
        db = CrucibleDB(project / ".crucible/crucible.db")
        try:
            db.add_source(path=f"{data}/paper.pdf", title="Paper", source_type="pdf",
                          shareable=True, metadata={"cite_key": "paper2026"})
        finally:
            db.close()

    monkeypatch.chdir(project)
    for option in ("--dry-run", "--yes"):
        result = runner.invoke(cli, ["merge", str(source), option])
        assert result.exit_code != 0
        assert "private" in result.output.lower()
        assert "move it under sources/external/" in result.output
        assert not (project / data / "paper.txt").exists()
        if not reuse:
            assert not (project / data / "paper.pdf").exists()


@pytest.mark.parametrize("outside_crucible", [False, True])
def test_merge_refuses_target_directory_symlinks_outside_the_crucible(
    project, tmp_path, monkeypatch, outside_crucible
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    (source / ".crucible/sources/data/paper.md").write_text("Imported source.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    outside = tmp_path / "outside"
    outside.mkdir()
    if outside_crucible:
        outside = project
    target_data = project / ".crucible/sources/data"
    target_data.rmdir()
    try:
        target_data.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "outside" in result.output.lower()
        assert not (outside / "paper.md").exists()


@pytest.mark.parametrize(
    "target_state", ["same", "different", "missing", "other-path", "same-other-path"]
)
def test_merge_registered_source_with_same_citation_key(
    project, tmp_path, monkeypatch, target_state
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    inputs = []
    for name, content in (
        ("incoming", "Imported content.\n"),
        ("existing", "Imported content.\n" if target_state in {"same", "same-other-path"}
         else "Other content.\n"),
    ):
        folder = tmp_path / name
        folder.mkdir()
        filename = (
            "other.md" if name == "existing" and target_state.endswith("other-path")
            else "evidence.md"
        )
        path = folder / filename
        path.write_text(content)
        inputs.append(path)
    ingest_args = ["--type", "data", "--title", "Synthetic Evidence", "--date", "2026-01-01"]
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["ingest", str(inputs[0]), *ingest_args])
    assert result.exit_code == 0, result.output
    source_path = ".crucible/sources/data/evidence.md"
    bib = source / ".crucible/references.bib"
    bib.write_text(bib.read_text().replace(
        "\n}", f'\n  file = "{source / source_path}",\n}}', 1
    ))
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: synthetic2026"))
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output

    monkeypatch.chdir(project)
    result = runner.invoke(cli, ["ingest", str(inputs[1]), *ingest_args])
    assert result.exit_code == 0, result.output
    target_path = project / source_path
    if target_state == "missing":
        target_path.unlink()
    if target_state.endswith("other-path"):
        target_path.write_text("Different registered source.\n")
        result = runner.invoke(cli, ["sync"])
        assert result.exit_code == 0, result.output
    target_bib = project / ".crucible/references.bib"
    target_bib.write_text("")
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    if target_state not in {"same", "same-other-path"}:
        for outcome in (preview, result):
            assert outcome.exit_code != 0
            assert "Cannot safely merge source with citation key 'synthetic2026'" in outcome.output
        assert not (project / article).exists()
        assert target_bib.read_text() == ""
        if target_state == "different":
            assert target_path.read_text() == "Other content.\n"
        elif target_state == "other-path":
            assert target_path.read_text() == "Different registered source.\n"
        else:
            assert not target_path.exists()
        return

    assert preview.exit_code == 0, preview.output
    assert result.exit_code == 0, result.output
    assert "Skipped 1 existing sources" in result.output
    expected_path = (
        ".crucible/sources/data/other.md" if target_state == "same-other-path" else source_path
    )
    assert [row["path"] for row in run_json("sources", article)] == [expected_path]
    rebuilt = copy_without_database(project, tmp_path / "rebuilt")
    monkeypatch.chdir(rebuilt)
    assert [row["path"] for row in run_json("sources", article)] == [expected_path]
    assert (rebuilt / expected_path).read_text() == "Imported content.\n"


@pytest.mark.parametrize("target_sidecar", ["missing", "same", "different", "private"])
def test_merge_keeps_text_sidecars_when_reusing_identical_sources(
    project, tmp_path, monkeypatch, target_sidecar
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    rel_path = ".crucible/sources/data/paper.pdf"
    for root in (source, project):
        (root / rel_path).write_bytes(b"Identical source.")
        db = CrucibleDB(root / ".crucible/crucible.db")
        try:
            db.add_source(path=rel_path, title="Paper", source_type="pdf",
                          shareable=True, metadata={"cite_key": "paper2026"})
        finally:
            db.close()
    source_text = source / ".crucible/sources/data/paper.txt"
    if target_sidecar == "private":
        private_text = source / ".crucible/sources/external/paper.txt"
        private_text.parent.mkdir(parents=True, exist_ok=True)
        private_text.write_text("Private text.\n")
        try:
            source_text.symlink_to(private_text)
        except (OSError, NotImplementedError) as exc:
            pytest.skip(f"Symlinks unavailable: {exc}")
    else:
        source_text.write_text("Incoming text.\n")
    target_text = project / ".crucible/sources/data/paper.txt"
    if target_sidecar in {"same", "different"}:
        target_text.write_text(
            "Incoming text.\n" if target_sidecar == "same" else "Existing text.\n"
        )

    monkeypatch.chdir(project)
    preview = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    if target_sidecar in {"different", "private"}:
        assert preview.exit_code != 0
        assert ("sidecar" if target_sidecar == "different" else "private") in preview.output.lower()
        result = runner.invoke(cli, ["merge", str(source), "--yes"])
        assert result.exit_code != 0
        if target_sidecar == "different":
            assert target_text.read_text() == "Existing text.\n"
        else:
            assert not target_text.exists()
    else:
        assert preview.exit_code == 0, preview.output
        if target_sidecar == "missing":
            assert "sidecar" in preview.output.lower()
            assert not target_text.exists()
        result = runner.invoke(cli, ["merge", str(source), "--yes"])
        assert result.exit_code == 0, result.output
        assert target_text.read_text() == "Incoming text.\n"


@pytest.mark.parametrize("target_state", ["source-collision", "sidecar-collision", "reuse"])
def test_merge_rewrites_bibliography_paths_for_text_sidecars(
    project, tmp_path, monkeypatch, target_state
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    data = ".crucible/sources/data"
    (source / data / "paper.pdf").write_bytes(b"Incoming source.")
    (source / data / "paper.txt").write_text("Incoming text.\n")
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.add_source(path=f"{data}/paper.pdf", title="Paper", source_type="pdf",
                      shareable=True, metadata={"cite_key": "paper2026"})
    finally:
        db.close()
    (source / ".crucible/references.bib").write_text(
        f"@misc{{paper2026,\n  file = {{{data}/paper.txt}},\n}}\n"
    )
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: paper2026"))

    if target_state == "reuse":
        (project / data / "different.pdf").write_bytes(b"Incoming source.")
        db = CrucibleDB(project / ".crucible/crucible.db")
        try:
            db.add_source(path=f"{data}/different.pdf", title="Paper", source_type="pdf",
                          shareable=True, metadata={"cite_key": "paper2026"})
        finally:
            db.close()
        expected = f"{data}/different.txt"
    else:
        collision = "paper.pdf" if target_state == "source-collision" else "paper.txt"
        (project / data / collision).write_text("Existing target content.\n")
        expected = f"{data}/paper_1.txt"

    monkeypatch.chdir(project)
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (project / expected).read_text() == "Incoming text.\n"
    assert f"file = {{{expected}}}" in (project / ".crucible/references.bib").read_text()

    rebuilt = copy_without_database(project, tmp_path / "rebuilt")
    monkeypatch.chdir(rebuilt)
    assert [row["path"] for row in run_json("sources", article)] == [expected]


@pytest.mark.parametrize("option", ["--dry-run", "--yes"])
def test_merge_rejects_nonfile_text_sidecar_before_copy(project, tmp_path, monkeypatch, option):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    data = ".crucible/sources/data"
    (source / data / "paper.pdf").write_bytes(b"Incoming source.")
    (source / data / "paper.txt").mkdir()
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.add_source(path=f"{data}/paper.pdf", title="Paper", source_type="pdf",
                      shareable=True, metadata={"cite_key": "paper2026"})
    finally:
        db.close()
    monkeypatch.chdir(project)
    result = runner.invoke(cli, ["merge", str(source), option])
    assert result.exit_code != 0
    assert "not a regular file" in result.output.lower()
    assert not (project / data / "paper.pdf").exists()


def test_merge_refuses_duplicate_incoming_citation_keys(project, tmp_path, monkeypatch):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    for name in ("first.md", "second.md"):
        (source / ".crucible/sources/data" / name).write_text(name)
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    db = CrucibleDB(source / ".crucible/crucible.db")
    for row in db.list_sources():
        db.conn.execute(
            "UPDATE sources SET metadata = ? WHERE id = ?",
            (json.dumps({"cite_key": "duplicate2026"}), row["id"]),
        )
    db.conn.commit()
    db.close()

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "Cannot safely merge source with citation key 'duplicate2026'" in result.output
        assert not (project / ".crucible/sources/data/first.md").exists()
        assert not (project / ".crucible/sources/data/second.md").exists()


@pytest.mark.parametrize("source_indexed", [False, True])
def test_merge_keeps_bibliography_only_entries_without_copying_phantom_files(
    project, tmp_path, monkeypatch, source_indexed
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    (source / ".crucible/references.bib").write_text(
        "@misc{legacy2026,\n  title = {Legacy entry},\n}\n"
    )
    article = ".crucible/wiki/concepts/probe.org"
    (source / article).write_text(CONCEPT.replace(":SOURCE_KEYS:", ":SOURCE_KEYS: legacy2026"))
    monkeypatch.chdir(source)
    if source_indexed:
        result = runner.invoke(cli, ["sync"])
        assert result.exit_code == 0, result.output
    assert not (source / ".crucible/bib/legacy2026").exists()

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code == 0, result.output
        assert "bibliography-only" in result.output.lower()
    assert "legacy2026" in (project / ".crucible/references.bib").read_text()
    assert (project / article).exists()
    assert not (project / ".crucible/bib/legacy2026").exists()


def test_merge_warns_when_an_indexed_source_lacks_a_durable_bib_mapping(
    project, tmp_path, monkeypatch
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    rel_path = ".crucible/sources/data/paper.md"
    (source / rel_path).write_text("Incoming content.\n")
    (source / ".crucible/references.bib").write_text(
        "@misc{paper2026,\n  title = {Paper},\n}\n"
    )
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.add_source(path=rel_path, title="Paper", source_type="data",
                      shareable=True, metadata={"cite_key": "paper2026"})
    finally:
        db.close()
    monkeypatch.chdir(project)
    for option in ("--dry-run", "--yes"):
        result = runner.invoke(cli, ["merge", str(source), option])
        assert result.exit_code == 0, result.output
        assert "no durable file mapping" in result.output.lower()
    assert (project / rel_path).read_text() == "Incoming content.\n"


def test_merge_rejects_duplicate_keys_even_when_one_source_is_outside_crucible(
    project, tmp_path, monkeypatch
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    local = source / ".crucible/sources/data/local.md"
    local.write_text("Local source.\n")
    outside = tmp_path / "outside.md"
    outside.write_text("External source.\n")
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        for path in (".crucible/sources/data/local.md", str(outside)):
            db.add_source(path=path, title="Source", source_type="data",
                          metadata={"cite_key": "duplicate2026"})
    finally:
        db.close()

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "duplicate2026" in result.output
        assert not (project / ".crucible/sources/data/local.md").exists()


@pytest.mark.parametrize("invalid_in", ["source", "target"])
def test_merge_reports_invalid_source_metadata_before_copying(
    project, tmp_path, monkeypatch, invalid_in
):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    (source / ".crucible/sources/data/incoming.md").write_text("Incoming source.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    db_root = source if invalid_in == "source" else project
    db = CrucibleDB(db_root / ".crucible/crucible.db")
    try:
        if invalid_in == "target":
            db.add_source(path=".crucible/sources/data/legacy.md", title="Legacy",
                          source_type="data")
        db.conn.execute("UPDATE sources SET metadata = ?", ("{invalid",))
        db.conn.commit()
    finally:
        db.close()

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "invalid metadata" in result.output.lower()
        assert not (project / ".crucible/sources/data/incoming.md").exists()


def test_merge_refuses_missing_incoming_files(project, tmp_path, monkeypatch):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    missing = source / ".crucible/sources/data/evidence.md"
    missing.write_text("Imported source.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    missing.unlink()

    monkeypatch.chdir(project)
    for args in (["--dry-run"], ["--yes"]):
        result = runner.invoke(cli, ["merge", str(source), *args])
        assert result.exit_code != 0
        assert "source file is missing" in result.output
        assert not (project / ".crucible/sources/data/evidence.md").exists()


def test_merge_reports_ambiguous_bibliography_source(project, tmp_path, monkeypatch):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    original = tmp_path / "evidence.md"
    original.write_text("Imported source content.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["ingest", str(original), "--type", "data"])
    assert result.exit_code == 0, result.output
    try:
        (source / ".crucible/sources/data/alias.md").symlink_to("evidence.md")
        (source / ".crucible/link.md").symlink_to("sources/data/evidence.md")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks unavailable: {exc}")
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    bib = source / ".crucible/references.bib"
    bib.write_text(
        bib.read_text()
        + "\n@misc{unmatched2026,\n  file = {.crucible/link.md},\n}\n"
    )
    (source / ".crucible/wiki/concepts/probe.org").write_text(CONCEPT)

    monkeypatch.chdir(project)
    target_bib = project / ".crucible/references.bib"
    original_bib = "@misc{existing2026,\n  title = {Existing entry},\n}\n"
    target_bib.write_text(original_bib)
    result = runner.invoke(cli, ["merge", str(source), "--dry-run"])
    assert result.exit_code != 0
    assert "Cannot safely map bibliography entry 'unmatched2026'" in result.output
    assert "may already have copied files" not in result.output
    assert not (project / ".crucible/sources/data/evidence.md").exists()
    assert not (project / ".crucible/wiki/concepts/probe.org").exists()
    assert target_bib.read_text() == original_bib

    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code != 0
    assert "Cannot safely map bibliography entry 'unmatched2026'" in result.output
    assert "may already have copied files" not in result.output
    assert not (project / ".crucible/sources/data/evidence.md").exists()
    assert not (project / ".crucible/wiki/concepts/probe.org").exists()
    assert target_bib.read_text() == original_bib


def _ingest_into_new_crucible(tmp_path, monkeypatch, name, content):
    """Create a crucible holding one ingested data source keyed synthetic2026."""
    root = make_crucible(tmp_path / name)
    runner = CliRunner()
    original = tmp_path / f"{name}-evidence.md"
    original.write_text(content)
    monkeypatch.chdir(root)
    result = runner.invoke(
        cli,
        ["ingest", str(original), "--type", "data",
         "--title", "Synthetic Evidence", "--date", "2026-01-01"],
    )
    assert result.exit_code == 0, result.output
    assert "{synthetic2026," in (root / ".crucible/references.bib").read_text()
    return root


def test_merge_lists_every_problem_at_once(project, tmp_path, monkeypatch):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    first = source / ".crucible/sources/data/first.md"
    second = source / ".crucible/sources/data/second.md"
    first.write_text("First.\n")
    second.write_text("Second.\n")
    monkeypatch.chdir(source)
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0, result.output
    first.unlink()
    second.unlink()

    monkeypatch.chdir(project)
    result = runner.invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code != 0
    assert "2 problems" in result.output
    assert "first.md" in result.output and "second.md" in result.output


def test_merge_refuses_before_asking_for_confirmation(project, tmp_path, monkeypatch):
    source = make_crucible(tmp_path / "source")
    runner = CliRunner()
    missing = source / ".crucible/sources/data/evidence.md"
    missing.write_text("Imported source.\n")
    monkeypatch.chdir(source)
    assert runner.invoke(cli, ["sync"]).exit_code == 0
    missing.unlink()

    monkeypatch.chdir(project)
    result = runner.invoke(cli, ["merge", str(source)], input="y\n")
    assert result.exit_code != 0
    assert "source file is missing" in result.output
    assert "Proceed with merge?" not in result.output


def test_merge_refuses_key_already_in_unsynced_target_bibliography(
    project, tmp_path, monkeypatch
):
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Imported.\n")
    # The target's bibliography has the key, but its database has no source
    # for it, so merge cannot detect the clash from the database alone.
    target_bib = project / ".crucible/references.bib"
    with target_bib.open("a") as f:
        f.write("\n@misc{synthetic2026,\n  title = {Something else},\n}\n")
    before = target_bib.read_text()

    monkeypatch.chdir(project)
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code != 0
    assert "target bibliography already has an entry" in result.output
    assert not list((project / ".crucible/sources/data").glob("*evidence*"))
    assert target_bib.read_text() == before


def test_merge_reuses_identical_source_with_same_key(project, tmp_path, monkeypatch):
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Shared bytes.\n")
    target = _ingest_into_new_crucible(tmp_path, monkeypatch, "target", "Shared bytes.\n")
    before = sorted(p.name for p in (target / ".crucible/sources/data").iterdir())

    monkeypatch.chdir(target)
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert "Skipped 1 existing sources" in result.output
    assert sorted(p.name for p in (target / ".crucible/sources/data").iterdir()) == before


def test_merge_does_not_mistake_field_text_for_a_target_key(project, tmp_path, monkeypatch):
    source = _ingest_into_new_crucible(tmp_path, monkeypatch, "source", "Imported.\n")
    # `{synthetic2026,` appears in a field of another entry, not as a key.
    target_bib = project / ".crucible/references.bib"
    with target_bib.open("a") as f:
        f.write("\n@misc{doe2020,\n  note = {synthetic2026, a later study},\n}\n")

    monkeypatch.chdir(project)
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    # The incoming entry was appended, not dropped as a duplicate.
    assert re.search(r"@\w+\{synthetic2026,", target_bib.read_text())


def test_merge_reports_sources_outside_the_crucible_separately(
    project, tmp_path, monkeypatch
):
    source = make_crucible(tmp_path / "source")
    outside = tmp_path / "outside.md"
    outside.write_text("Stored outside the crucible.\n")
    db = CrucibleDB(source / ".crucible/crucible.db")
    try:
        db.add_source(path=str(outside), title="Outside", source_type="data")
    finally:
        db.close()

    monkeypatch.chdir(project)
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert "Skipped 1 source(s) stored outside .crucible/" in result.output
    assert "existing sources" not in result.output


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("@article{smith2020,\n  title = {X},\n}", True),
        ("@Article{ smith2020 ,\n  title = {X}}", True),
        ("@misc{doe2020,\n  author = {smith2020, John},\n}", False),
        ("@misc{smith2020a,\n  title = {X},\n}", False),
    ],
)
def test_bib_has_key_matches_entry_headers_only(text, expected):
    from crucible.ingest import bib_has_key

    assert bib_has_key(text, "smith2020") is expected


def test_merge_copies_an_index_article_in_a_subdirectory(project, tmp_path):
    """Only the top-level wiki/index.org is generated; concepts/index.org is an article."""
    source = make_crucible(tmp_path / "other")
    wiki = source / ".crucible/wiki"
    (wiki / "concepts/index.org").write_text(
        CONCEPT.replace("Activation Energy", "Index").replace("activation-energy", "index")
    )
    (wiki / "index.org").write_text("#+TITLE: Generated index\n")
    target_index = (project / ".crucible/wiki/index.org")
    before = target_index.read_text() if target_index.exists() else None
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    assert (project / ".crucible/wiki/concepts/index.org").exists()
    after = target_index.read_text() if target_index.exists() else None
    assert after is None or "Generated index" not in after or before == after


def test_merge_keeps_non_utf8_article_bytes(project, tmp_path):
    """A badly encoded incoming article neither aborts the merge nor is mangled."""
    source = make_crucible(tmp_path / "src")
    wiki = source / ".crucible/wiki/concepts"
    # Same name as the target's article, so it is renamed and its links rewritten.
    (wiki / "activation-energy.org").write_bytes(
        CONCEPT.encode() + b"See [[file:other.org][other]]. Caf\xe9.\n"
    )
    (wiki / "other.org").write_text(CONCEPT.replace("Activation energy", "Other"))
    result = CliRunner().invoke(cli, ["merge", str(source), "--yes"])
    assert result.exit_code == 0, result.output
    merged = [p for p in (project / ".crucible/wiki/concepts").glob("activation-energy*.org")
              if b"Caf\xe9" in p.read_bytes()]
    assert len(merged) == 1
    assert b"Caf\xe9." in merged[0].read_bytes()
