"""Source ingestion logic for Crucible.

Handles copying sources to the right location, extracting text,
and registering them in the database.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from filelock import FileLock

from crucible.database import CrucibleDB
from crucible.orgparse import parse_org_file


def detect_source_type(path: Path) -> str:
    """Detect source type from file extension."""
    suffix = path.suffix.lower()
    type_map = {
        ".pdf": "pdf",
        ".org": "notebook",
        ".md": "web",
        ".html": "web",
        ".htm": "web",
        ".docx": "web",
        ".pptx": "web",
        ".csv": "data",
        ".json": "data",
        ".xlsx": "data",
        ".xls": "data",
        ".tsv": "data",
        ".hdf5": "data",
        ".h5": "data",
        ".npy": "data",
        ".npz": "data",
    }
    return type_map.get(suffix, "other")


CRUCIBLE_DIR = ".crucible"


def destination_dir(root: Path, source_type: str, shareable: bool) -> Path:
    """Determine where to store a source file."""
    base = root / CRUCIBLE_DIR / "sources"
    if shareable:
        if source_type == "notebook":
            return base / "notebooks"
        elif source_type == "data":
            return base / "data"
        else:
            return base / "data"
    else:
        if source_type == "pdf":
            return base / "external" / "pdfs"
        elif source_type == "web":
            return base / "external" / "web"
        else:
            return base / "external" / "web"


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8"):
    """Write content to path atomically via temp file + rename."""
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        os.write(fd, content.encode(encoding))
        os.close(fd)
        fd = -1
        os.replace(tmp_path, str(path))
    except Exception:
        if fd >= 0:
            os.close(fd)
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def extract_text(path: Path) -> str:
    """Extract plain text from a source file."""
    suffix = path.suffix.lower()

    if suffix == ".pdf":
        return _extract_pdf(path)
    elif suffix in (".html", ".htm"):
        return _extract_via_pandoc(path, input_format="html")
    elif suffix == ".docx":
        return _extract_via_pandoc(path, input_format="docx")
    elif suffix == ".pptx":
        return _extract_pptx(path)
    elif suffix in (".xlsx", ".xls"):
        return _extract_excel(path)
    elif suffix in (".org", ".md", ".txt"):
        return path.read_text(encoding="utf-8", errors="replace")
    elif suffix in (".csv", ".tsv"):
        return path.read_text(encoding="utf-8", errors="replace")
    else:
        return ""


def _extract_via_pandoc(path: Path, input_format: str) -> str:
    """Extract text via pandoc for formats it supports."""
    if not shutil.which("pandoc"):
        return path.read_text(encoding="utf-8", errors="replace") if input_format == "html" else ""
    result = subprocess.run(
        ["pandoc", "-f", input_format, "-t", "plain", "--wrap=none", str(path)],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode == 0:
        return result.stdout
    return ""


def _extract_pdf(path: Path) -> str:
    """Extract text from PDF using pdftotext or pandoc fallback."""
    if shutil.which("pdftotext"):
        result = subprocess.run(
            ["pdftotext", "-layout", str(path), "-"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode == 0:
            return result.stdout
    return _extract_via_pandoc(path, input_format="pdf")


def _extract_pptx(path: Path) -> str:
    """Extract text from PowerPoint files."""
    try:
        from pptx import Presentation
    except ImportError:
        return ""
    prs = Presentation(str(path))
    lines = []
    for i, slide in enumerate(prs.slides, 1):
        lines.append(f"--- Slide {i} ---")
        for shape in slide.shapes:
            if shape.has_text_frame:
                for para in shape.text_frame.paragraphs:
                    text = para.text.strip()
                    if text:
                        lines.append(text)
            if shape.has_table:
                for row in shape.table.rows:
                    cells = [cell.text.strip() for cell in row.cells]
                    lines.append(" | ".join(cells))
        lines.append("")
    return "\n".join(lines)


def _extract_excel(path: Path) -> str:
    """Extract text from Excel files."""
    try:
        from openpyxl import load_workbook
    except ImportError:
        return ""
    wb = load_workbook(str(path), read_only=True, data_only=True)
    lines = []
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        lines.append(f"--- Sheet: {sheet_name} ---")
        for row in ws.iter_rows(values_only=True):
            cells = [str(c) if c is not None else "" for c in row]
            if any(cells):
                lines.append(" | ".join(cells))
        lines.append("")
    wb.close()
    return "\n".join(lines)


def extract_title(path: Path, text: str) -> str:
    """Try to extract a title from the source content."""
    suffix = path.suffix.lower()

    if suffix == ".org":
        meta = parse_org_file(path)
        if meta.title:
            return meta.title

    if suffix == ".md":
        for line in text.split("\n"):
            line = line.strip()
            if line.startswith("# "):
                return line[2:].strip()

    if suffix in (".html", ".htm"):
        import re
        m = re.search(r"<title>(.*?)</title>",
                       path.read_text(encoding="utf-8", errors="replace"),
                       re.IGNORECASE | re.DOTALL)
        if m:
            return m.group(1).strip()

    # Fallback to filename
    return path.stem.replace("-", " ").replace("_", " ").title()


def generate_cite_key(title: str, authors: list[str] | None, date: str | None) -> str:
    """Generate a BibTeX cite key from source metadata.

    Format: firstauthor_surname + year, e.g. smith2024.
    Falls back to title slug + year.
    """
    import re as _re
    year = (date or "")[:4] or "nd"

    if authors and authors[0] and authors[0].strip():
        # Take last name of first author
        first = authors[0]
        # Handle "Last, First" or "First Last"
        if "," in first:
            surname = first.split(",")[0].strip()
        else:
            parts = first.strip().split()
            surname = parts[-1] if parts else ""
        surname = _re.sub(r"[^a-zA-Z]", "", surname).lower()
        if surname:
            return f"{surname}{year}"
        # No usable surname (e.g. all-punctuation author); fall through to title.

    # Fallback to first significant word of title
    words = _re.sub(r"[^a-zA-Z\s]", "", title.lower()).split()
    stop = {"the", "a", "an", "of", "on", "in", "for", "and", "to", "with"}
    word = next((w for w in words if w not in stop), words[0] if words else "unknown")
    return f"{word}{year}"


def _bib_value(value: str) -> str:
    """Sanitize a string for use inside a BibTeX ``{field}`` value.

    Removes braces, which would otherwise unbalance the field and break both
    our own parser and downstream LaTeX/bibtex compilation, and collapses
    newlines/whitespace. These values come from extracted titles and user
    input rather than hand-written BibTeX, so literal braces are never intended
    as grouping.
    """
    return " ".join(value.replace("{", "").replace("}", "").split())


def generate_bib_entry(cite_key: str, title: str, authors: list[str] | None,
                       date: str | None, url: str | None,
                       source_type: str, doi: str | None = None,
                       file: str | None = None) -> str:
    """Generate a minimal BibTeX entry string.

    Used as a fallback when no DOI lookup or user-provided bibtex is available.
    Contains only the bibliographic fields we know — no crucible-internal
    metadata — plus the standard `file` field naming the stored copy, which
    lets a fresh clone tie the entry back to its file.
    """
    year = (date or "")[:4] or ""

    entry_type = {
        "pdf": "article",
        "web": "misc",
        "notebook": "misc",
        "data": "misc",
    }.get(source_type, "misc")

    lines = [f"@{entry_type}{{{cite_key},"]
    lines.append(f"  title = {{{_bib_value(title)}}},")
    if authors:
        author_str = " and ".join(_bib_value(a) for a in authors)
        lines.append(f"  author = {{{author_str}}},")
    if year:
        lines.append(f"  year = {{{_bib_value(year)}}},")
    if date and re.fullmatch(r"\d{4}-\d{2}(-\d{2})?", date):
        # biblatex's date field keeps the full date, which a rebuild restores
        lines.append(f"  date = {{{date}}},")
    if doi:
        lines.append(f"  doi = {{{_bib_value(doi)}}},")
    if url:
        lines.append(f"  url = {{{_bib_value(url)}}},")
    if file:
        lines.append(f"  file = {{{_bib_value(file)}}},")
    lines.append("}")
    return "\n".join(lines)


_CITE_KEY_RE = re.compile(r"(@\w+\s*\{\s*)([^,\s]+)")


def replace_cite_key(bibtex: str, new_key: str) -> str:
    """Replace the citation key of the first @entry in a bibtex string."""
    return _CITE_KEY_RE.sub(lambda m: m.group(1) + new_key, bibtex, count=1)


_ENTRY_HEAD_RE = re.compile(r"@\w+\s*\{\s*[^,\s]+\s*,")


def set_bib_field(bibtex: str, name: str, value: str) -> str:
    """Set a field on the first @entry in a bibtex string, replacing any existing one."""
    field_re = re.compile(
        rf"\n?[ \t]*\b{re.escape(name)}\s*=\s*"
        r"(?:\{(?:[^{}]|\{[^{}]*\})*\}|\"[^\"]*\")\s*,?",
        re.IGNORECASE,
    )
    head = _ENTRY_HEAD_RE.search(bibtex)
    if head is None:
        return bibtex
    body = field_re.sub("", bibtex[head.end():])
    line = f"\n  {name} = {{{_bib_value(value)}}},"
    return bibtex[:head.end()] + line + body


_BIB_ENTRY_RE = re.compile(r"@(\w+)\s*\{\s*([^,\s]+)\s*,(.*?)\n\}", re.DOTALL)
_BIB_FIELD_RE = re.compile(
    r"(\w+)\s*=\s*(?:\{((?:[^{}]|\{[^{}]*\})*)\}|\"([^\"]*)\")",
    re.DOTALL,
)


def _parse_bib_fields(body: str) -> dict[str, str]:
    """Parse the field body of a BibTeX entry into a dict."""
    fields: dict[str, str] = {}
    for m in _BIB_FIELD_RE.finditer(body):
        name = m.group(1).lower()
        value = m.group(2) if m.group(2) is not None else m.group(3) or ""
        fields[name] = " ".join(value.split())
    return fields


def parse_bib_file(bib_path: Path) -> list[dict]:
    """Parse a BibTeX file into a list of entries.

    Each entry dict has 'cite_key', 'entry_type', and parsed fields
    (title, author, year, doi, url, file, ...).
    """
    if not bib_path.exists():
        return []
    text = bib_path.read_text(encoding="utf-8", errors="replace")
    entries = []
    for m in _BIB_ENTRY_RE.finditer(text):
        entry = {
            "entry_type": m.group(1).lower(),
            "cite_key": m.group(2),
        }
        entry.update(_parse_bib_fields(m.group(3)))
        entries.append(entry)
    return entries


def _authors_from_bib(author_field: str) -> list[str]:
    """Split a BibTeX author field on ' and '."""
    if not author_field:
        return []
    return [a.strip() for a in re.split(r"\s+and\s+", author_field) if a.strip()]


def source_type_for_path(rel_path: str, entry_type: str | None = None) -> str:
    """The source_type of a stored source, from the directory ingest put it in.

    destination_dir() files sources by type and shareability, so the
    directory is the reliable record: a markdown notebook in
    sources/notebooks/ is a notebook, not "web" as its extension suggests.
    Shareable PDFs are stored in sources/data/, so a .pdf there is a pdf.
    """
    parts = Path(rel_path).parts
    if len(parts) >= 3 and parts[0] == CRUCIBLE_DIR and parts[1] == "sources":
        area = parts[2]
        if area == "external" and len(parts) >= 4:
            if parts[3] == "pdfs":
                return "pdf"
            if parts[3] == "web":
                return "web"
        if area == "notebooks":
            return "notebook"
        if area == "data":
            return "pdf" if Path(rel_path).suffix.lower() == ".pdf" else "data"
    detected = detect_source_type(Path(rel_path))
    if detected != "other":
        return detected
    return {
        "article": "pdf",
        "inproceedings": "pdf",
        "book": "pdf",
        "phdthesis": "pdf",
        "mastersthesis": "pdf",
    }.get(entry_type or "", "other")


def upsert_sources_from_disk(root: Path, db) -> int:
    """Create sources rows for files in sources/notebooks/ and sources/data/.

    These directories are committed to git (unlike sources/external/), so a
    fresh clone has the files but no sources table. Scans both directories
    and creates a source row for each file, using the filename stem as the
    cite_key in metadata. Skips hidden files, extracted text sidecars
    (*.txt next to a non-txt source), and entries whose path already exists
    in the sources table.

    Returns the number of new sources inserted.
    """
    existing_paths: set[str] = {src["path"] for src in db.list_sources()}
    inserted = 0

    subdirs = [
        root / CRUCIBLE_DIR / "sources" / "notebooks",
        root / CRUCIBLE_DIR / "sources" / "data",
    ]

    for subdir in subdirs:
        if not subdir.is_dir():
            continue
        for path in sorted(subdir.rglob("*")):
            if not path.is_file() or path.name.startswith("."):
                continue
            # Skip *.txt sidecars from text extraction if a non-txt source
            # with the same stem exists
            if path.suffix.lower() == ".txt":
                siblings = [p for p in path.parent.glob(path.stem + ".*")
                            if p != path and p.is_file()]
                if siblings:
                    continue

            try:
                rel_path = str(path.relative_to(root.resolve()))
            except ValueError:
                rel_path = str(path)

            if rel_path in existing_paths:
                continue

            cite_key = path.stem
            title = path.stem.replace("-", " ").replace("_", " ").title()
            if path.suffix.lower() == ".org":
                try:
                    meta = parse_org_file(path)
                    if meta.title:
                        title = meta.title
                except Exception:
                    pass

            source_type = source_type_for_path(rel_path)

            metadata = {"cite_key": cite_key}

            try:
                mtime_iso = datetime.fromtimestamp(
                    path.stat().st_mtime
                ).strftime("%Y-%m-%d")
            except OSError:
                mtime_iso = None

            try:
                db.add_source(
                    path=rel_path,
                    title=title,
                    source_type=source_type,
                    shareable=True,
                    url=None,
                    authors=None,
                    date=mtime_iso,
                    metadata=metadata,
                )
                existing_paths.add(rel_path)
                inserted += 1
            except Exception:
                continue

    return inserted


PLACEHOLDER_PREFIX = f"{CRUCIBLE_DIR}/bib/"


def _bib_file_path(root: Path, file_field: str) -> str:
    """A bib entry's file field as a path relative to the project root."""
    candidate = Path(file_field)
    abs_path = candidate if candidate.is_absolute() else root / candidate
    try:
        return str(abs_path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(abs_path)


def _from_ingest(src: dict) -> bool:
    """Whether a source row was created by ingest (or copied by merge).

    Those rows carry a content hash and the metadata the user gave; rows
    rebuilt from references.bib or the disk scan do not.
    """
    return "content_hash" in _source_metadata(src)


def _from_disk_scan(src: dict) -> bool:
    """Whether a source row came from upsert_sources_from_disk (keyed by file stem)."""
    return (
        not _from_ingest(src)
        and _source_metadata(src).get("cite_key") == Path(src["path"]).stem
        and src["path"].startswith((
            f"{CRUCIBLE_DIR}/sources/notebooks/", f"{CRUCIBLE_DIR}/sources/data/",
        ))
    )


def _fields_from_bib(entry: dict, rel_path: str) -> dict:
    metadata: dict = {"cite_key": entry["cite_key"]}
    if entry.get("doi"):
        metadata["doi"] = entry["doi"]
    return {
        "path": rel_path,
        "title": entry.get("title") or entry["cite_key"],
        "source_type": source_type_for_path(rel_path, entry["entry_type"]),
        "shareable": not rel_path.startswith(f"{CRUCIBLE_DIR}/sources/external/"),
        "url": entry.get("url"),
        "authors": _authors_from_bib(entry.get("author", "")),
        "date": entry.get("date") or entry.get("year") or None,
        "metadata": metadata,
    }


def reconcile_sources(root: Path, db) -> int:
    """Make the sources table agree with references.bib and the files on disk.

    The database must be regenerable from committed files (#6). A bib
    entry's `file` field ties its cite key to the stored file, so:

    1. Rows created by ingest are authoritative. Their paths are written
       into the `file` field of entries that lack one or have a stale one,
       which upgrades projects ingested before ingest recorded `file`.
    2. Each entry with a `file` field owns the row at that path: a row the
       disk scan created for the file, or a placeholder or stale row under
       its cite key, takes the entry's metadata (duplicates are merged, and
       their article links moved), or a row is inserted.
    3. An entry without a `file` field gets no row. A placeholder path would
       capture the article links; instead `crucible lint` reports the key.
       Placeholder rows from earlier versions are removed.
    4. Files in sources/notebooks/ and sources/data/ that no entry claims
       get a row from the disk scan.

    Returns the number of source rows inserted.
    """
    bib_path = root / CRUCIBLE_DIR / "references.bib"
    entries = parse_bib_file(bib_path)
    bib_keys = {entry["cite_key"] for entry in entries}
    inserted = 0

    with db.conn.transaction():
        sources = db.list_sources()
        by_path = {src["path"]: src for src in sources}
        by_key: dict[str, dict] = {}
        for src in sources:
            key = _source_metadata(src).get("cite_key")
            if isinstance(key, str) and (key not in by_key or _from_ingest(src)):
                by_key[key] = src

        # 1. Record ingested paths in the bibliography.
        file_updates = {}
        for entry in entries:
            src = by_key.get(entry["cite_key"])
            if src is None or not _from_ingest(src):
                continue
            if not entry.get("file") or _bib_file_path(root, entry["file"]) != src["path"]:
                file_updates[entry["cite_key"]] = src["path"]
                entry["file"] = src["path"]
        set_bib_file_fields(root, file_updates)

        # 2. Entries with a file own the row for it.
        for entry in entries:
            if not entry.get("file"):
                continue
            key = entry["cite_key"]
            rel_path = _bib_file_path(root, entry["file"])
            row_key = by_key.get(key)
            row_path = by_path.get(rel_path)
            if row_key is not None and row_key is not row_path and _from_disk_scan(row_key):
                row_key = None  # a different file whose name matches this key
            if row_key is not None and _from_ingest(row_key):
                continue  # step 1 made the entry's file match this row
            if row_path is not None and row_path is not row_key:
                other = _source_metadata(row_path).get("cite_key")
                if _from_ingest(row_path) or (other != key and other in bib_keys):
                    continue  # the file belongs to another source's entry
            fields = _fields_from_bib(entry, rel_path)
            if row_path is not None:
                if row_key is not None and row_key is not row_path:
                    db.merge_source_into(row_key["id"], row_path["id"])
                    by_path.pop(row_key["path"], None)
                db.update_source(row_path["id"], **fields)
                target_id = row_path["id"]
            elif row_key is not None:
                by_path.pop(row_key["path"], None)
                db.update_source(row_key["id"], **fields)
                target_id = row_key["id"]
            else:
                target_id = db.add_source(**fields)
                inserted += 1
            updated = db.get_source(target_id)
            by_path[rel_path] = updated
            by_key[key] = updated

        # 3. Placeholder rows from earlier versions never point at a file.
        for src in list(by_path.values()):
            if src["path"].startswith(PLACEHOLDER_PREFIX):
                db.delete_source(src["id"])

    # 4. Committed files no entry claims.
    inserted += upsert_sources_from_disk(root, db)
    return inserted


def fetch_bibtex_from_doi(doi: str, timeout: float = 10.0) -> str | None:
    """Fetch a full BibTeX entry for a DOI via doi.org content negotiation.

    Returns the entry string on success, or None if the lookup fails.
    """
    doi = doi.strip()
    if doi.lower().startswith("doi:"):
        doi = doi[4:]
    if doi.startswith("http"):
        url = doi
    else:
        url = f"https://doi.org/{doi}"

    req = Request(url, headers={
        "Accept": "application/x-bibtex; charset=utf-8",
        "User-Agent": "crucible (+https://github.com/jkitchin/crucible)",
    })
    try:
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8", errors="replace").strip()
    except (URLError, OSError, TimeoutError):
        return None
    if not data.startswith("@"):
        return None
    return data


# DOIs are 10.<registrant>/<suffix>. Stop at whitespace/quotes/brackets and
# strip trailing sentence punctuation that commonly abuts a DOI in running text.
_DOI_RE = re.compile(r"\b10\.\d{4,9}/[^\s\"'<>}\]]+", re.IGNORECASE)


def extract_doi_from_text(text: str | None, scan_chars: int = 4000) -> str | None:
    """Best-effort extraction of a document's own DOI from its text.

    Only the first ``scan_chars`` characters are scanned (title page / header),
    where a paper's own DOI normally appears, to avoid picking up a DOI from the
    reference list. Returns the cleaned DOI string, or None if none is found.
    """
    if not text:
        return None
    m = _DOI_RE.search(text[:scan_chars])
    if not m:
        return None
    doi = m.group(0).rstrip(".,;:)]}>")
    return doi or None


def append_bib_entry(root: Path, entry: str, cite_key: str):
    """Append a BibTeX entry to .crucible/references.bib if not already present.

    Uses file locking and atomic writes to prevent concurrent writers
    from clobbering each other's entries.
    """
    bib_path = root / CRUCIBLE_DIR / "references.bib"
    lock_path = bib_path.with_suffix(".bib.lock")

    with FileLock(lock_path, timeout=10):
        if bib_path.exists():
            existing = bib_path.read_text(encoding="utf-8")
            if f"{{{cite_key}," in existing:
                return
            new_content = existing.rstrip() + "\n\n" + entry + "\n"
        else:
            new_content = entry + "\n"
        atomic_write_text(bib_path, new_content)


def set_bib_file_fields(root: Path, paths: dict[str, str]) -> None:
    """Set the `file` field of existing entries in references.bib (cite_key -> path)."""
    if not paths:
        return
    bib_path = root / CRUCIBLE_DIR / "references.bib"
    lock_path = bib_path.with_suffix(".bib.lock")
    with FileLock(lock_path, timeout=10):
        if not bib_path.exists():
            return
        text = bib_path.read_text(encoding="utf-8")

        def repl(m: re.Match) -> str:
            path = paths.get(m.group(2))
            return set_bib_field(m.group(0), "file", path) if path else m.group(0)

        new_text = _BIB_ENTRY_RE.sub(repl, text)
        if new_text != text:
            atomic_write_text(bib_path, new_text)


def file_sha256(path: Path) -> str:
    """Return the SHA-256 hex digest of a file's bytes, read in chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _source_metadata(src: dict) -> dict:
    """Parse a source row's metadata JSON into a dict (empty on any error).

    Malformed metadata that parses to something other than an object, such
    as a list or null, also gives an empty dict.
    """
    raw = src.get("metadata") or "{}"
    try:
        md = json.loads(raw) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        return {}
    return md if isinstance(md, dict) else {}


def _find_duplicate_source(db: CrucibleDB, root: Path, content_hash: str) -> dict | None:
    """Return an existing source row with the same content, or None.

    Matches on the stored ``content_hash`` metadata when present; for sources
    ingested before hashing existed, falls back to hashing the stored file on
    disk so re-ingesting an old source is still recognized as a duplicate.
    """
    for src in db.list_sources():
        md = _source_metadata(src)
        stored = md.get("content_hash")
        if stored is None:
            src_file = root / src["path"]
            if src_file.exists():
                try:
                    stored = file_sha256(src_file)
                except OSError:
                    continue
        if stored == content_hash:
            return src
    return None


def _unique_cite_key(base: str, db: CrucibleDB) -> str:
    """Return ``base``, or a suffixed variant, that no existing source uses.

    Disambiguates colliding generated keys (e.g. two different smith2024
    papers) as smith2024, smith2024a, smith2024b, ... so a new source never
    silently reuses another's cite key (and its bib entry).
    """
    used = {
        k for src in db.list_sources()
        if (k := _source_metadata(src).get("cite_key"))
    }
    if base not in used:
        return base
    for suffix in "abcdefghijklmnopqrstuvwxyz":
        if f"{base}{suffix}" not in used:
            return f"{base}{suffix}"
    i = 1
    while f"{base}{i}" in used:
        i += 1
    return f"{base}{i}"


def ingest_source(
    root: Path,
    db: CrucibleDB,
    source_path: Path,
    title: str | None = None,
    source_type: str | None = None,
    shareable: bool | None = None,
    url: str | None = None,
    date: str | None = None,
    authors: list[str] | None = None,
    doi: str | None = None,
    bibtex: str | None = None,
) -> dict:
    """Ingest a source file into Crucible.

    Returns a dict with source_id, stored_path, extracted_text_path, cite_key, and metadata.
    """
    source_path = source_path.resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Source not found: {source_path}")

    # If this exact file (same content hash) was already ingested, skip it:
    # don't copy, append bib, or add a duplicate DB row. Return the existing
    # source so callers can report it.
    content_hash = file_sha256(source_path)
    existing = _find_duplicate_source(db, root, content_hash)
    if existing is not None:
        md = _source_metadata(existing)
        return {
            "source_id": existing["id"],
            "title": existing["title"],
            "cite_key": md.get("cite_key", ""),
            "source_type": existing["source_type"],
            "shareable": bool(existing["shareable"]),
            "stored_path": str(root / existing["path"]),
            "relative_path": existing["path"],
            "text_path": None,
            "text_length": 0,
            "date": existing.get("date"),
            "duplicate": True,
        }

    # Detect type if not specified
    if source_type is None:
        source_type = detect_source_type(source_path)

    # Determine shareability
    if shareable is None:
        shareable = source_type in ("notebook", "data")

    # Copy to destination
    dest_dir = destination_dir(root, source_type, shareable)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / source_path.name

    # Handle name collisions
    if dest_path.exists() and not dest_path.samefile(source_path):
        stem = source_path.stem
        suffix = source_path.suffix
        counter = 1
        while dest_path.exists():
            dest_path = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    if not dest_path.exists():
        shutil.copy2(str(source_path), str(dest_path))

    # Extract text
    text = extract_text(dest_path)

    # Save extracted text alongside source
    text_path = dest_path.with_suffix(".txt")
    if text:
        atomic_write_text(text_path, text)

    # Determine title
    if title is None:
        title = extract_title(dest_path, text)

    # Use today if no date given
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    # Relative path for database
    rel_path = str(dest_path.relative_to(root))

    # If no DOI was supplied, try to recover one from the document text so we
    # can fetch a complete, correctly-typed entry instead of a bare @misc.
    if not doi:
        doi = extract_doi_from_text(text)

    # Generate cite key and bib entry. Preference order:
    #   1. user-provided --bibtex string
    #   2. fetched from doi.org content negotiation (real entry type + fields)
    #   3. minimal entry from known metadata
    cite_key = _unique_cite_key(generate_cite_key(title, authors, date), db)
    bib_entry = None
    if bibtex and bibtex.strip().startswith("@"):
        bib_entry = replace_cite_key(bibtex.strip(), cite_key)
    elif doi:
        fetched = fetch_bibtex_from_doi(doi)
        if fetched:
            bib_entry = replace_cite_key(fetched, cite_key)
    if bib_entry is None:
        bib_entry = generate_bib_entry(
            cite_key, title, authors, date, url, source_type, doi=doi,
        )
    # Record the stored copy, so a fresh clone can tie the entry to its file.
    bib_entry = set_bib_field(bib_entry, "file", rel_path)
    append_bib_entry(root, bib_entry, cite_key)

    # Register in database (store cite_key and content hash in metadata)
    metadata = {"cite_key": cite_key, "content_hash": content_hash}
    if doi:
        metadata["doi"] = doi
    source_id = db.add_source(
        path=rel_path,
        title=title,
        source_type=source_type,
        shareable=shareable,
        url=url,
        authors=authors,
        date=date,
        metadata=metadata,
    )

    return {
        "source_id": source_id,
        "title": title,
        "cite_key": cite_key,
        "source_type": source_type,
        "shareable": shareable,
        "stored_path": str(dest_path),
        "relative_path": rel_path,
        "text_path": str(text_path) if text else None,
        "text_length": len(text),
        "date": date,
        "duplicate": False,
    }
