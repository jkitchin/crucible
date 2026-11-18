"""Source ingestion logic for Crucible.

Handles copying sources to the right location, extracting text,
and registering them in the database.
"""

import hashlib
import html
import http.client
import json
import os
import re
import shutil
import subprocess
import tempfile
import unicodedata
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from urllib.error import URLError
from glob import escape as glob_escape
from urllib.parse import quote
from urllib.request import Request, urlopen

from filelock import FileLock

from crucible import bibtex as bibtex_mod
from crucible.database import CrucibleDB
from crucible.orgparse import parse_org_file
from crucible.wikiwalk import wiki_article_files


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


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8",
                      errors: str = "strict"):
    """Write content to path atomically via temp file + rename."""
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        os.write(fd, content.encode(encoding, errors))
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
    """Try to extract a title from the source content, else use the filename."""
    return declared_title(path, text) or path.stem.replace("-", " ").replace("_", " ").title()


def _title_key(title: str) -> str:
    """A title reduced to its letters and digits, casefolded, for comparison.

    BibTeX braces, LaTeX accents and punctuation differ between a fetched
    entry and the title a file declares without making it another paper.
    """
    return "".join(ch for ch in title.casefold() if ch.isalnum())


# arXiv's abstract pages label the title: "Title:Oxygen evolution ...".
_TITLE_LABEL = re.compile(r"^\s*title\s*:\s*", re.IGNORECASE)

# The metadata a publisher's page carries for its own article. A page
# that only discusses a paper (a forum thread, a video, a blog post)
# does not declare its DOI this way.
_META_TAG = re.compile(r"<meta\s[^>]*>", re.IGNORECASE)
_META_ATTR = re.compile(r"""([\w:.-]+)\s*=\s*("[^"]*"|'[^']*'|[^\s"'>]+)""")
_OWN_DOI_NAMES = frozenset({"citation_doi", "dc.identifier", "prism.doi"})
_DOI_PREFIX = re.compile(r"^\s*(?:doi:\s*|info:doi/|https?://(?:dx\.)?doi\.org/)",
                         re.IGNORECASE)


def _declares_title(declared: str, title: str) -> bool:
    """Whether a file declaring `declared` names the paper titled `title`.

    The titles must be the same, apart from an arXiv "Title:" label. A
    site or journal name beside the title ("Title | ACS Catalysis") is
    not stripped: "Title | Hacker News" and "Paper summary - Title" are
    pages about the paper. A publisher's page is recognised by its
    citation_doi metadata instead (see _page_declares_doi).
    """
    want = _title_key(title)
    return bool(want) and _title_key(_TITLE_LABEL.sub("", declared)) == want


def _page_declares_doi(path: Path, doi: str) -> bool:
    """Whether an HTML page's citation metadata names `doi` as its own."""
    if path.suffix.lower() not in (".html", ".htm", ".xhtml"):
        return False
    try:
        head = path.read_bytes()[:200_000].decode("utf-8", "replace")
    except OSError:
        return False
    for tag in _META_TAG.finditer(head):
        attrs = {k.casefold(): v.strip("\"'")
                 for k, v in _META_ATTR.findall(tag.group(0))}
        if (attrs.get("name") or attrs.get("property") or "").casefold() not in _OWN_DOI_NAMES:
            continue
        own = _DOI_PREFIX.sub("", attrs.get("content", "")).strip().rstrip(".")
        if own and own.casefold() == doi.casefold():
            return True
    return False


# Files that never carry their own DOI: a DOI in a deck or a Word document
# is one it cites.
_NO_OWN_DOI_SUFFIXES = frozenset({".docx", ".pptx"})

# How much of a document's text is its front matter: where a paper prints
# its own title and DOI, before any reference list.
FRONT_MATTER_CHARS = 4000


def may_carry_own_doi(source_type: str, path: Path) -> bool:
    """Whether a DOI in this file's text may be the file's own.

    Only a paper (a PDF) or a web page carries its own DOI near the top; a
    notebook, data file, slide deck or Word document that mentions a DOI is
    citing someone else's paper, and must not become that paper.
    """
    return (source_type in ("pdf", "web")
            and path.suffix.lower() not in _NO_OWN_DOI_SUFFIXES)


def _words(text: str) -> set[str]:
    return set(re.findall(r"[^\W_]{4,}", text.casefold()))


def _title_on_first_page(title: str, text: str | None,
                         scan_chars: int = FRONT_MATTER_CHARS) -> bool:
    """Whether `title` appears near the start of `text`, where a paper prints it.

    The window is the one extract_doi_from_text scans. Words are compared,
    not the whole string: a fetched title carries markup (CO<sub>2</sub>,
    {\\"u}) and a PDF's text line breaks and hyphenation. A word broken
    across lines (Electro-/catalytic) is read both joined and as two words.
    Most of the title's words of four or more letters must be there, and a
    title of three or more such words may miss one (a word set in Greek or
    math, say). A title with no such words is accepted.
    """
    title = re.sub(r"<[^>]*>|\\[a-zA-Z]+|[{}]", " ", title)
    words = _words(title)
    if not words:
        return True
    front = (text or "")[:scan_chars]
    page = _words(front) | _words(re.sub(r"(?<=\w)-\s*\n\s*(?=\w)", "", front))
    found = len(words & page)
    return found >= 0.7 * len(words) or (len(words) >= 3 and found >= len(words) - 1)


def declared_title(path: Path, text: str) -> str | None:
    """The title the file itself declares (#+TITLE, a # heading, <title>), or None."""
    suffix = path.suffix.lower()

    if suffix == ".org":
        meta = parse_org_file(path)
        if meta.title:
            return meta.title

    if suffix == ".md":
        front = re.match(r"\A\ufeff?---[ \t]*\r?\n(.*?)\r?\n(?:---|\.\.\.)[ \t]*(?:\r?\n|\Z)",
                         text or "", re.DOTALL)
        if front:
            m = re.search(r"^title\s*:\s*(.+?)\s*$", front.group(1), re.MULTILINE)
            if m and m.group(1).strip("\"' "):
                return m.group(1).strip().strip("\"'").strip()
        for line in text.split("\n"):
            line = line.strip()
            if line.startswith("# "):
                return line[2:].strip()

    if suffix in (".html", ".htm"):
        m = re.search(r"<title>(.*?)</title>",
                       path.read_text(encoding="utf-8", errors="replace"),
                       re.IGNORECASE | re.DOTALL)
        if m:
            return m.group(1).strip() or None

    return None


# Letters NFKD does not decompose into an ASCII base letter.
_FOLD = str.maketrans({
    "ø": "o", "Ø": "O", "ß": "ss", "æ": "ae", "Æ": "AE", "œ": "oe", "Œ": "OE",
    "ł": "l", "Ł": "L", "đ": "d", "Đ": "D", "ð": "d", "Ð": "D", "þ": "th",
    "Þ": "Th", "ı": "i",
})


def ascii_fold(text: str) -> str:
    """Transliterate to ASCII: Müller -> Muller, Nørskov -> Norskov.

    Characters with no ASCII equivalent (e.g. CJK) are dropped.
    """
    decomposed = unicodedata.normalize("NFKD", text.translate(_FOLD))
    return decomposed.encode("ascii", "ignore").decode("ascii")


def slugify(text: str, max_len: int = 60) -> str:
    """A filename-safe slug of `text`, or "" if nothing usable remains."""
    slug = ascii_fold(html.unescape(text)).lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug).strip("-")
    return slug[:max_len].rstrip("-")


_DATE_RE = re.compile(r"\d{4}(?:-\d{2}(?:-\d{2})?)?")


def validate_date(date: str | None) -> str | None:
    """Return `date` if it is ISO YYYY, YYYY-MM or YYYY-MM-DD, else raise ValueError."""
    if date is None:
        return None
    date = date.strip()
    ok = _DATE_RE.fullmatch(date) is not None
    if ok and len(date) > 4:
        fmt = {7: "%Y-%m", 10: "%Y-%m-%d"}[len(date)]
        try:
            datetime.strptime(date, fmt)
        except ValueError:
            ok = False
    if not ok:
        raise ValueError(
            f"Invalid date {date!r}: use ISO format YYYY, YYYY-MM or YYYY-MM-DD"
        )
    return date


def generate_cite_key(title: str, authors: list[str] | None, date: str | None) -> str:
    """Generate a BibTeX cite key from source metadata.

    Format: firstauthor_surname + year, e.g. smith2024.
    Falls back to title slug + year. Accented letters are transliterated
    (Müller -> muller).
    """
    year = (date or "")[:4]
    if not re.fullmatch(r"\d{4}", year):
        year = "nd"

    if authors and authors[0] and authors[0].strip():
        # Take last name of first author
        first = authors[0]
        # Handle "Last, First" or "First Last"
        if "," in first:
            surname = first.split(",")[0].strip()
        else:
            parts = first.strip().split()
            surname = parts[-1] if parts else ""
        surname = re.sub(r"[^a-zA-Z]", "", ascii_fold(surname)).lower()
        if surname:
            return f"{surname}{year}"
        # No usable surname (e.g. all-punctuation author); fall through to title.

    # Fallback to first significant word of title
    words = re.sub(r"[^a-zA-Z\s]", "", ascii_fold(title or "").lower()).split()
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


def _braces_balanced(value: str) -> bool:
    """Whether every brace in value is closed, and closed in order."""
    depth = 0
    for c in value:
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def _bib_file_value(path: str) -> str:
    """A file path for a ``file = {...}`` field, written verbatim.

    Balanced braces are kept: BibTeX allows them inside a braced value, and
    the reader returns them unchanged, so ``run{2}.csv`` names the file that
    is stored. Collapsing whitespace would name a different file too.
    Unbalanced braces cannot be written at all; they are dropped (ingest
    stores such a file under the name without them, see _storage_name).
    """
    if _braces_balanced(path):
        return path
    return path.replace("{", "").replace("}", "")


def _storage_name(name: str) -> str:
    """The name a source is stored under: its own, minus unbalanced braces.

    The bib `file` field can only hold balanced braces, and the field must
    name the stored file exactly.
    """
    return name if _braces_balanced(name) else name.replace("{", "").replace("}", "")


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
        "web": "online",  # biblatex; lets a rebuild tell web from data
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
        lines.append(f"  file = {{{_bib_file_value(file)}}},")
    lines.append("}")
    return "\n".join(lines)


def replace_cite_key(bibtex: str, new_key: str) -> str:
    """Replace the citation key of the first @entry in a bibtex string."""
    return bibtex_mod.replace_key(bibtex, new_key)


def set_bib_field(bibtex: str, name: str, value: str) -> str:
    """Set a field on the first @entry in a bibtex string, replacing any existing one."""
    if name.lower() == "file":
        value = _bib_file_value(value)
    else:
        value = _bib_value(value)
    return bibtex_mod.set_field(bibtex, name, value)


def _parse_bib_fields(entry_text: str) -> dict[str, str]:
    """The fields of the first entry in entry_text, as a dict."""
    entry = bibtex_mod.first_entry(entry_text)
    return entry.as_dict() if entry else {}


def _resolve_bib_file(root: Path, value: str) -> Path:
    """Prefer a literal file path, then try folding wrapped lines."""
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = root / candidate
    if candidate.exists() or not re.search(r"[\r\n]", value):
        return candidate

    unfolded = re.sub(r"[ \t]*(?:\r\n|\r|\n)[ \t]*", " ", value)
    alternative = Path(unfolded)
    if not alternative.is_absolute():
        alternative = root / alternative
    return alternative if alternative.exists() else candidate


def parse_bib_file(bib_path: Path) -> list[dict]:
    """Parse a BibTeX file into a list of entries.

    Each entry dict has 'cite_key', 'entry_type', and parsed fields
    (title, author, year, doi, url, file, ...).
    """
    if not bib_path.exists():
        return []
    text = bib_path.read_text(encoding="utf-8", errors="replace")
    parsed = []
    for e in bibtex_mod.entries(text):
        entry = {"entry_type": e.entry_type, "cite_key": e.key}
        entry.update(e.as_dict())
        parsed.append(entry)
    return parsed


def _read_bib_entries(root: Path, lock: bool = True) -> tuple[list[dict], int]:
    """references.bib parsed as parse_bib_file does, read under the bib lock,
    plus the number of malformed entries (unbalanced braces, no key).

    lock=False reads without the lock (see reconcile_sources).
    """
    bib_path = root / CRUCIBLE_DIR / "references.bib"
    with _bib_lock(root) if lock else nullcontext():
        if not bib_path.exists():
            return [], 0
        text = bib_path.read_text(encoding="utf-8", errors="replace")
    found, malformed = bibtex_mod.scan(text)
    parsed = []
    for e in found:
        entry = {"entry_type": e.entry_type, "cite_key": e.key}
        entry.update(e.as_dict())
        parsed.append(entry)
    return parsed, len(malformed)


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
    Shareable PDFs and web sources are also stored in sources/data/, so a
    .pdf there is a pdf, and an entry of type @online (which ingest writes
    for web sources) is web.
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
            # Shareable web sources and PDFs are stored here too.
            if entry_type == "online":
                return "web"
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


def _disk_scan_files(root: Path) -> list[Path]:
    """The committed source files the disk scan indexes, in a stable order.

    Files in sources/notebooks/ and sources/data/, skipping hidden files and
    extracted-text sidecars (a *.txt beside a non-txt file with its stem).
    """
    files = []
    for subdir in (
        root / CRUCIBLE_DIR / "sources" / "notebooks",
        root / CRUCIBLE_DIR / "sources" / "data",
    ):
        if not subdir.is_dir():
            continue
        for path in sorted(subdir.rglob("*")):
            if not path.is_file() or path.name.startswith("."):
                continue
            if path.suffix.lower() == ".txt":
                siblings = [p for p in path.parent.glob(glob_escape(path.stem) + ".*")
                            if p != path and p.is_file()]
                if siblings:
                    continue
            files.append(path)
    return files


def _disk_scan_key(path: Path, taken: set[str]) -> str:
    """A cite key for a disk-scanned file: its stem, unless that is taken.

    A stem that names a bib entry or a source not from the scan would
    capture that key's article links, so fall back to stem-ext, then
    stem-ext-2, ...
    """
    if path.stem not in taken:
        return path.stem
    base = f"{path.stem}-{path.suffix.lstrip('.').lower() or 'file'}"
    key, n = base, 2
    while key in taken:
        key, n = f"{base}-{n}", n + 1
    return key


def _disk_scan_fields(path: Path, rel_path: str) -> dict:
    """The columns (but metadata) the disk scan gives a file's row."""
    title = path.stem.replace("-", " ").replace("_", " ").title()
    if path.suffix.lower() == ".org":
        try:
            meta = parse_org_file(path)
            if meta.title:
                title = meta.title
        except Exception:
            pass
    try:
        mtime_iso = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d")
    except OSError:
        mtime_iso = None
    return {
        "title": title,
        "source_type": source_type_for_path(rel_path),
        "shareable": True,
        "url": None,
        "authors": None,
        "date": mtime_iso,
    }


def upsert_sources_from_disk(root: Path, db) -> int:
    """Create and key source rows for files in sources/notebooks/ and sources/data/.

    These directories are committed to git (unlike sources/external/), so a
    fresh clone has the files but no sources table. Each file that no other
    row (from ingest or a bib entry) claims gets a row keyed by its filename
    stem, marked ``disk_scan`` in its metadata.

    A stem is not used as the key when a bib entry or a row that is not from
    the scan already has that key: the file would capture that key's
    article links (#29). Such a file gets a key that collides with none of
    those (see _disk_scan_key). Keys are assigned over every scanned file,
    existing rows included, so an existing database is repaired to the keys
    a fresh rebuild gives. Two scanned files with the same stem still share
    it; sync links both and lint reports the duplicate (#11).

    Returns the number of new sources inserted.
    """
    files = _disk_scan_files(root)
    rows = db.list_sources()
    by_path = {src["path"]: src for src in rows}
    entries = parse_bib_file(root / CRUCIBLE_DIR / "references.bib")
    # A file a bib entry names belongs to that entry, even when its row
    # looks like a scanned one (an entry keyed by its file's stem does).
    entry_files = {_bib_file_path(root, e["file"]) for e in entries if e.get("file")}
    scanned_rows = {
        src["path"] for src in rows
        if _from_disk_scan(src) and src["path"] not in entry_files
    }

    taken = {entry["cite_key"] for entry in entries}
    for src in rows:
        key = _source_metadata(src).get("cite_key")
        if isinstance(key, str) and src["path"] not in scanned_rows:
            taken.add(key)

    inserted = 0
    for path in files:
        rel_path = _project_rel(root, path)

        existing = by_path.get(rel_path)
        if rel_path in entry_files or (
            existing is not None and rel_path not in scanned_rows
        ):
            continue  # ingest or a bib entry owns this file

        cite_key = _disk_scan_key(path, taken)

        if existing is not None:
            metadata = _source_metadata(existing)
            if metadata.get("cite_key") != cite_key or not metadata.get("disk_scan"):
                metadata.update(cite_key=cite_key, disk_scan=True)
                db.update_source(existing["id"], metadata=metadata)
            continue

        try:
            db.add_source(
                path=rel_path,
                **_disk_scan_fields(path, rel_path),
                metadata={"cite_key": cite_key, "disk_scan": True},
            )
            by_path[rel_path] = None
            inserted += 1
        except Exception:
            continue

    return inserted


PLACEHOLDER_PREFIX = f"{CRUCIBLE_DIR}/bib/"


def _project_rel(root: Path, path: Path) -> str:
    """`path` relative to the project root, as ingest stores it.

    The path is normalised lexically (``a/../b`` is ``b``) and compared with
    the root as given and as resolved, without resolving symlinks inside the
    project: with sources/notebooks/ a symlink to a directory elsewhere, a
    file in it is still .crucible/sources/notebooks/<name>, the path ingest
    recorded. Only a path that is not lexically under the root (one through
    a symlink to the project, say) is resolved. A path outside the project
    stays absolute.
    """
    lexical = Path(os.path.normpath(path))
    bases = [Path(os.path.normpath(root))]
    try:
        bases.append(root.resolve())
    except OSError:
        pass
    for base in bases:
        try:
            return str(lexical.relative_to(base))
        except ValueError:
            pass
    try:
        resolved = lexical.resolve()
    except OSError:
        return str(lexical)
    for base in bases:
        try:
            return str(resolved.relative_to(base))
        except ValueError:
            pass
    return str(lexical)


def _bib_file_path(root: Path, file_field: str) -> str:
    """A bib entry's file field as a path relative to the project root.

    _resolve_bib_file handles a path that a BibTeX formatter wrapped across
    lines; _project_rel makes it relative the way ingest stores paths.
    """
    return _project_rel(root, _resolve_bib_file(root, file_field))


def _is_ingest_day(row_date: str, ingested_at: str | None) -> bool:
    """Whether a row date is (within a day of) the day its row was ingested.

    ingested_at is UTC and ingest's default date was local, so they can
    differ by a day.
    """
    if not ingested_at or len(row_date) != 10:
        return False
    try:
        day = datetime.strptime(row_date, "%Y-%m-%d").date()
        ingested = datetime.strptime(ingested_at[:10], "%Y-%m-%d").date()
    except ValueError:
        return False
    return abs((day - ingested).days) <= 1


def row_date_for_bib(src: dict, entry_date: str | None,
                     entry_year: str | None) -> tuple[str | None, str | None]:
    """Whether an ingested row's date belongs in its bib entry.

    Rows ingested before ingest wrote `date` know a full date the entry may
    lack; recording it lets a rebuild keep it. Returns (date to write, date
    withheld). A date whose year disagrees with the entry's is not written:
    the entry wins. Nor is one within a day of the ingest when the entry has
    a year: a source ingested without --date got the day of ingest, which is
    not a publication date, and the row cannot say which it was. That date is
    returned as withheld so lint can report it.
    """
    row_date = src.get("date") or ""
    if (not _from_ingest(src) or entry_date
            or not re.fullmatch(r"\d{4}-\d{2}(-\d{2})?", row_date)):
        return None, None
    if entry_year and entry_year != row_date[:4]:
        return None, None
    if entry_year and _is_ingest_day(row_date, src.get("ingested_at")):
        return None, row_date
    return row_date, None


def _from_ingest(src: dict) -> bool:
    """Whether a source row was created by ingest (or copied by merge).

    Those rows carry a content hash and the metadata the user gave; rows
    rebuilt from references.bib or the disk scan do not.
    """
    return "content_hash" in _source_metadata(src)


def _knows_its_file(root: Path, src: dict, bib_keys: set[str]) -> bool:
    """Whether a row can supply the file for its cite key's bib entry.

    Rows from ingest qualify. So do rows ingested before ingest stored a
    content hash (2026-06): any row whose file exists and whose cite key has
    a bib entry, unless it is a placeholder or came from the disk scan.
    """
    if _from_ingest(src):
        return True
    return (
        not src["path"].startswith(PLACEHOLDER_PREFIX)
        and not _from_disk_scan(src)
        and _source_metadata(src).get("cite_key") in bib_keys
        and (root / src["path"]).is_file()
    )


def _from_disk_scan(src: dict) -> bool:
    """Whether a source row came from upsert_sources_from_disk.

    Such rows are marked ``disk_scan``; rows from versions before the mark
    are recognised by a cite key equal to the file stem.
    """
    md = _source_metadata(src)
    return (
        not _from_ingest(src)
        and (md.get("disk_scan") is True or md.get("cite_key") == Path(src["path"]).stem)
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


def _release_unlisted_sources(root: Path, db, bib_keys: set[str]) -> None:
    """Give ingested rows whose bib entry is gone the treatment a rebuild gives.

    A rebuild makes rows only from bib entries with a `file` field and from
    the disk scan, so once an ingested source's entry is removed its row has
    no claim to the file: another entry may name it, and otherwise a file
    the disk scan indexes (sources/notebooks/, sources/data/) becomes a
    disk-scan row, which the scan then keys as it would on a rebuild. Any
    other such row (an external file, a file that is gone) is dropped along
    with its article links.

    Only rows made by ingest are released. Rows with no content hash (from
    the disk scan, or from versions before ingest wrote bib entries) are
    left to step 3b and the scan.
    """
    scanned = {_project_rel(root, p): p for p in _disk_scan_files(root)}
    for src in db.list_sources():
        if not _from_ingest(src):
            continue
        metadata = _source_metadata(src)
        key = metadata.get("cite_key")
        if isinstance(key, str) and key in bib_keys:
            continue
        path = scanned.get(src["path"])
        if path is None:
            db.delete_source(src["id"])
            continue
        # The metadata a disk-scan row has (the scan sets its key), plus the
        # sidecar's hash, which lets delete tell the sidecar from a user's
        # own <stem>.txt.
        new_md = {"cite_key": key if isinstance(key, str) else path.stem, "disk_scan": True}
        if "text_hash" in metadata:
            new_md["text_hash"] = metadata["text_hash"]
        db.update_source(src["id"], **_disk_scan_fields(path, src["path"]), metadata=new_md)


def reconcile_sources(root: Path, db, on_warning=None, lock: bool = True) -> int:
    """Make the sources table agree with references.bib and the files on disk.

    The database must be regenerable from committed files (#6). A bib
    entry's `file` field ties its cite key to the stored file, so:

    1. Rows created by ingest are authoritative. Their paths are written
       into the `file` field of entries that lack one or have a stale one,
       which upgrades projects ingested before ingest recorded `file`. Rows
       ingested before content hashes existed fill in a missing field too
       (see _knows_its_file).
    2. Each entry with a `file` field owns the row at that path: a row the
       disk scan created for the file, or a placeholder or stale row under
       its cite key, takes the entry's metadata (duplicates are merged, and
       their article links moved), or a row is inserted.
    3. An entry without a `file` field gets no row. A placeholder path would
       capture the article links; instead `crucible lint` reports the key.
       Placeholder rows from earlier versions are removed.
    4. Files in sources/notebooks/ and sources/data/ that no entry claims
       get a row from the disk scan.

    Before all this, an ingested row whose cite key has no bib entry loses
    its claim to its file (see _release_unlisted_sources), as it has none on
    a rebuild, unless the bibliography has malformed entries: then the
    missing key may be one that failed to parse, so no row is released and
    on_warning (if given) is called with a message.

    lock=False skips the bib lock, which creates references.bib.lock next
    to the bibliography. Only for a database that root's own commands never
    see, such as the one merge builds in a temporary directory to plan from
    a source crucible: such a database knows no files, so nothing is
    written to the bibliography, and the source may be read-only.

    Returns the number of source rows inserted.
    """
    inserted = 0

    # The bib lock is taken before the database write lock, the order merge
    # uses, so a sync and a merge never each hold one lock while waiting
    # for the other. Ingest appends its entry and adds its row under the
    # bib lock, so every ingested row seen here has its entry in what is
    # read, and no entry is seen whose ingest has yet to add its row.
    with _bib_lock(root) if lock else nullcontext(), db.conn.transaction():
        entries, malformed = _read_bib_entries(root, lock=lock)
        bib_keys = {entry["cite_key"] for entry in entries}
        key_counts: dict[str, int] = {}
        for entry in entries:
            key_counts[entry["cite_key"]] = key_counts.get(entry["cite_key"], 0) + 1
        if malformed:
            # A half-edited or partly unparseable bibliography would release
            # every row whose entry was lost; keep them until it is fixed.
            if on_warning is not None:
                on_warning(
                    f"references.bib has {malformed} malformed "
                    f"entr{'y' if malformed == 1 else 'ies'}; sources whose entry "
                    "is missing were kept (run `crucible lint` to find them)"
                )
        else:
            _release_unlisted_sources(root, db, bib_keys)
        sources = db.list_sources()
        by_path = {src["path"]: src for src in sources}
        by_key: dict[str, dict] = {}
        for src in sources:
            key = _source_metadata(src).get("cite_key")
            if isinstance(key, str) and (key not in by_key or _from_ingest(src)):
                by_key[key] = src

        # 1. Record ingested paths in the bibliography: fill in a missing
        # file field from any row that knows its file, and correct a stale
        # one (e.g. a source merge renamed) only from ingest's own rows, so a
        # user's edit to an entry is not undone by a row rebuilt from it.
        file_updates = {}
        date_updates = {}
        withheld: dict[str, str] = {}
        for entry in entries:
            src = by_key.get(entry["cite_key"])
            # A repeated key is ambiguous: lint reports it, and which entry
            # the row belongs to is not guessed.
            if src is None or key_counts[entry["cite_key"]] > 1:
                continue
            missing = not entry.get("file")
            stale = (not missing and _from_ingest(src)
                     and _bib_file_path(root, entry["file"]) != src["path"])
            if stale or (missing and _knows_its_file(root, src, bib_keys)):
                file_updates[entry["cite_key"]] = src["path"]
                entry["file"] = src["path"]
            # Rows ingested before ingest wrote `date` know a full date the
            # entry lacks; record it so a rebuild keeps it. A date whose
            # year disagrees with the entry's is not written: the entry wins.
            row_date, held = row_date_for_bib(src, entry.get("date"), entry.get("year"))
            if held:
                withheld[entry["cite_key"]] = held
            if row_date:
                date_updates[entry["cite_key"]] = row_date
                entry["date"] = row_date
        set_bib_file_fields(root, file_updates)
        set_bib_fields(root, "date", date_updates)

        # 2. Entries with a file own the row for it. A row one entry claimed
        # is not handed to a later entry with the same key: both files keep
        # a row and lint reports the shared key.
        claimed: set[int] = set()
        for entry in entries:
            if not entry.get("file"):
                continue
            key = entry["cite_key"]
            rel_path = _bib_file_path(root, entry["file"])
            row_key = by_key.get(key)
            row_path = by_path.get(rel_path)
            if row_key is not None and row_key is not row_path and _from_disk_scan(row_key):
                row_key = None  # a different file whose name matches this key
            if row_key is not None and row_key["id"] in claimed and row_key is not row_path:
                row_key = None  # an earlier entry with this key owns that row
            if row_key is not None and _from_ingest(row_key):
                if key_counts[key] == 1 or row_key["path"] == rel_path:
                    # Step 1 made the entry's file match this row. The entry
                    # describes the source, as it will after a rebuild; the
                    # row keeps ingest's own metadata (its content hash).
                    claimed.add(row_key["id"])
                    if row_key["path"] == rel_path:
                        fields = _fields_from_bib(entry, rel_path)
                        fields["metadata"] = {**_source_metadata(row_key), **fields["metadata"]}
                        # The row's date becomes the entry's; a full date
                        # sync would not write to the bib is kept here so
                        # lint can report it, until the entry has a date.
                        if key in withheld:
                            fields["metadata"]["unrecorded_date"] = withheld[key]
                        elif entry.get("date"):
                            fields["metadata"].pop("unrecorded_date", None)
                        del fields["path"]
                        db.update_source(row_key["id"], **fields)
                    continue
                row_key = None  # another entry with this key has the ingested file
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
            claimed.add(target_id)
            by_path[rel_path] = updated
            by_key[key] = updated

        # 3. Placeholder rows from earlier versions never point at a file.
        for src in list(by_path.values()):
            if src["path"].startswith(PLACEHOLDER_PREFIX):
                db.delete_source(src["id"])

        # 3b. A row whose bib entry and file are both gone would not be
        # rebuilt, so drop it (its article links go with it) and keep the
        # table equal to a rebuild's. A moved file keeps its entry's row
        # (step 2 moved it) or gets a new one from the disk scan below.
        for src in db.list_sources():
            if src["path"].startswith(PLACEHOLDER_PREFIX):
                continue
            key = _source_metadata(src).get("cite_key")
            if isinstance(key, str) and key in bib_keys:
                continue
            if not (root / src["path"]).exists():
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
        url = quote(doi, safe=":/?#[]@!$&'()*+,;=%~")
    else:
        url = "https://doi.org/" + quote(doi, safe="/:;()[]<>~")

    req = Request(url, headers={
        "Accept": "application/x-bibtex; charset=utf-8",
        "User-Agent": "crucible (+https://github.com/jkitchin/crucible)",
    })
    try:
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8", errors="replace").strip()
    except (URLError, OSError, TimeoutError, ValueError, http.client.HTTPException):
        # ValueError covers malformed URLs (and UnicodeError, its subclass).
        return None
    if not data.startswith("@"):
        return None
    return data


# DOIs are 10.<registrant>/<suffix>. Stop at whitespace, quotes, ']' and '}'.
# Only printable ASCII: a non-ASCII character (an en dash typed for a hyphen,
# say) ends the match instead of producing a URL that cannot be requested.
# Brackets of all kinds may occur inside a DOI (10.1061/(ASCE)0733-9399(2008)
# 134:1(1), SICI DOIs with <...>), so closing ones are cut later only where
# they do not balance.
_DOI_RE = re.compile(r"\b10\.\d{4,9}/[!#-&(-\\^-|~]+", re.IGNORECASE)

_DOI_PAIRS = {")": "(", "]": "[", ">": "<"}


def _trim_doi(doi: str) -> str:
    """Cut a matched DOI at its first unbalanced closing bracket, then drop
    trailing punctuation and unclosed opening brackets.

    The bracket that closes text around the DOI (``(doi 10.1/x)``,
    ``[10.1/x](url)``) has no partner inside it. An HTML or XML tag
    (``</doi>``, ``<br>``, ``<br/>``) ends the DOI too: a ``<`` followed by
    a letter or ``/``. In a SICI DOI (``...35:4<513::AID-JBM12>3.0.CO;2-R``)
    the ``<`` is always followed by a digit.
    """
    doi = re.split(r"<(?=[A-Za-z/])", doi, maxsplit=1)[0]
    depth = {opener: 0 for opener in _DOI_PAIRS.values()}
    for i, c in enumerate(doi):
        if c in depth:
            depth[c] += 1
        elif c in _DOI_PAIRS:
            opener = _DOI_PAIRS[c]
            if depth[opener] == 0:
                doi = doi[:i]
                break
            depth[opener] -= 1
    while doi and doi[-1] in ".,;:([<":
        doi = doi[:-1]
    return doi


def extract_doi_from_text(text: str | None,
                          scan_chars: int = FRONT_MATTER_CHARS) -> str | None:
    """Best-effort extraction of a document's own DOI from its text.

    Only the first ``scan_chars`` characters are scanned (title page / header),
    where a paper's own DOI normally appears, to avoid picking up a DOI from the
    reference list. Returns the cleaned DOI string, or None if none is found.
    """
    if not text:
        return None
    for m in _DOI_RE.finditer(text[:scan_chars]):
        doi = _trim_doi(m.group(0))
        if "/" in doi and not doi.endswith("/"):
            return doi
    return None


def bib_has_key(text: str, cite_key: str) -> bool:
    """Return True if BibTeX text has an entry whose key is cite_key."""
    return bibtex_mod.has_key(text, cite_key)


_BIB_LOCKS: dict[str, FileLock] = {}


def _bib_lock(root: Path) -> FileLock:
    """The lock on root's references.bib, one object per lock file.

    A FileLock object can be re-acquired by the thread that holds it, but a
    second object for the same file waits for the first. Handing out one
    object lets sync hold the lock across its whole transaction while the
    bib reads and rewrites inside it take it again.

    Lock order: every writer takes this lock before the database write lock
    (sync's transaction, merge's row inserts). Ingest holds it while it
    appends its entry and adds its row, so a sync never sees one without
    the other.
    """
    lock_path = os.path.abspath(root / CRUCIBLE_DIR / "references.bib.lock")
    lock = _BIB_LOCKS.get(lock_path)
    if lock is None:
        lock = _BIB_LOCKS[lock_path] = FileLock(lock_path, timeout=10)
    return lock


def read_bib_text(bib_path: Path) -> str:
    """references.bib for rewriting: bytes that are not UTF-8 (an entry from a
    Latin-1 export) are kept as surrogates, so write_bib_text puts them back
    unchanged instead of the rewrite failing or replacing them."""
    return bib_path.read_text(encoding="utf-8", errors="surrogateescape")


def write_bib_text(bib_path: Path, text: str) -> None:
    """Write text read by read_bib_text (plus new entries) atomically."""
    atomic_write_text(bib_path, text, errors="surrogateescape")


def _append_bib_unlocked(root: Path, entry: str, cite_key: str) -> bool:
    bib_path = root / CRUCIBLE_DIR / "references.bib"
    if bib_path.exists():
        existing = read_bib_text(bib_path)
        if bib_has_key(existing, cite_key):
            return False
        new_content = existing.rstrip() + "\n\n" + entry + "\n"
    else:
        new_content = entry + "\n"
    write_bib_text(bib_path, new_content)
    return True


def append_bib_entry(root: Path, entry: str, cite_key: str) -> bool:
    """Append a BibTeX entry to .crucible/references.bib if not already present.

    Uses file locking and atomic writes to prevent concurrent writers
    from clobbering each other's entries. Returns False, writing nothing,
    if an entry with `cite_key` already exists.
    """
    with _bib_lock(root):
        return _append_bib_unlocked(root, entry, cite_key)


def append_bib_entry_unique_key(root: Path, db: CrucibleDB, base_key: str,
                                entry: str) -> str:
    """Append `entry` under a cite key no source or bib entry uses yet.

    The key is chosen from `base_key` (see _unique_cite_key) while holding
    the bib lock, so two concurrent ingests never settle on the same key.
    Returns the key used.
    """
    with _bib_lock(root):
        cite_key = _unique_cite_key(base_key, db, root)
        _append_bib_unlocked(root, replace_cite_key(entry, cite_key), cite_key)
    return cite_key


def _drop_bib_entry(root: Path, cite_key: str, rel_path: str) -> None:
    """Remove the entry `cite_key` that names `rel_path` (undoing a failed ingest)."""
    def repl(entry: bibtex_mod.Entry) -> str:
        if entry.key == cite_key and (entry.get("file") or "").strip() == rel_path:
            return ""
        return entry.text
    _rewrite_bib(root, repl)


def set_bib_file_fields(root: Path, paths: dict[str, str]) -> None:
    """Set the `file` field of existing entries in references.bib (cite_key -> path)."""
    set_bib_fields(root, "file", paths)


def set_bib_fields(root: Path, name: str, values: dict[str, str]) -> None:
    """Set field `name` on existing entries in references.bib (cite_key -> value)."""
    if not values:
        return

    def repl(entry: bibtex_mod.Entry) -> str:
        value = values.get(entry.key)
        return set_bib_field(entry.text, name, value) if value else entry.text

    _rewrite_bib(root, repl)


def _rewrite_bib(root: Path, fn) -> None:
    """Replace each references.bib entry with fn(entry), under the bib lock."""
    bib_path = root / CRUCIBLE_DIR / "references.bib"
    with _bib_lock(root):
        if not bib_path.exists():
            return
        text = read_bib_text(bib_path)
        new_text = bibtex_mod.map_entries(text, fn)
        if new_text != text:
            write_bib_text(bib_path, new_text)


def drop_bib_file_fields(root: Path, rel_path: str) -> list[str]:
    """Remove the `file` field of every entry that names rel_path.

    The entries themselves stay: they are bibliography, and articles may
    still cite them. Returns the keys of the entries changed.
    """
    changed: list[str] = []
    # Both sides are resolved: with a symlinked sources directory the
    # entry's path and the row's would otherwise never match.
    target = (root / rel_path).resolve()

    def repl(entry: bibtex_mod.Entry) -> str:
        value = entry.get("file")
        if value and _resolve_bib_file(root, value).resolve() == target:
            changed.append(entry.key)
            return bibtex_mod.remove_field(entry.text, "file")
        return entry.text

    _rewrite_bib(root, repl)
    return changed


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


def _find_duplicate_source(db: CrucibleDB, root: Path, content_hash: str,
                           size: int | None = None) -> dict | None:
    """Return an existing source row with the same content, or None.

    Matches on the stored ``content_hash`` metadata when present; for sources
    ingested before hashing existed, falls back to hashing the stored file on
    disk so re-ingesting an old source is still recognized as a duplicate.
    A row whose stored file is missing is returned only when no row with
    the file matches; the caller restores the file.
    """
    unhashed = []
    missing = None
    for src in db.list_sources():
        stored = _source_metadata(src).get("content_hash")
        if stored is None:
            unhashed.append(src)
        elif stored == content_hash:
            # A row whose file is gone (deleted, or never there on a fresh
            # clone) is returned only if no row has the file: ingest then
            # restores it.
            if (root / src["path"]).is_file():
                return src
            missing = missing or src
    # Rows without a hash (older ingests, disk scans, rebuilt databases) are
    # compared by hashing their files, but only those of the same size: on a
    # fresh clone every row lacks a hash, and hashing every stored file on
    # every ingest would be O(files x sources). (The hash is not saved back
    # into the row: a content_hash in the metadata marks a row as made by
    # ingest, see _from_ingest.)
    for src in unhashed:
        src_file = root / src["path"]
        try:
            if size is not None and src_file.stat().st_size != size:
                continue
            stored = file_sha256(src_file)
        except OSError:
            continue
        if stored == content_hash:
            return src
    return missing


def _unique_cite_key(base: str, db: CrucibleDB, root: Path | None = None) -> str:
    """Return ``base``, or a suffixed variant, that no source or bib entry uses.

    Disambiguates colliding generated keys (e.g. two different smith2024
    papers) as smith2024, smith2024a, smith2024b, ... so a new source never
    silently reuses another's cite key (and its bib entry). Keys in
    references.bib count even without a source row: an entry with no
    `file` field has none, and reusing its key would attach the new file
    to that unrelated entry. Keys articles cite count too: a summary of a
    paper whose entry was removed still cites its key, and a new source
    under that key would silently become what the summary describes.
    """
    used = {
        k for src in db.list_sources()
        if (k := _source_metadata(src).get("cite_key"))
    }
    if root is not None:
        used.update(e["cite_key"] for e in parse_bib_file(root / CRUCIBLE_DIR / "references.bib"))
        used.update(_keys_cited_in_wiki(root))
    if base not in used:
        return base
    for suffix in "abcdefghijklmnopqrstuvwxyz":
        if f"{base}{suffix}" not in used:
            return f"{base}{suffix}"
    i = 1
    while f"{base}{i}" in used:
        i += 1
    return f"{base}{i}"


# {article path: ((mtime_ns, size), cite keys)}, so ingesting many files
# parses each article once, not once per file (see _keys_cited_in_wiki).
_cited_keys_cache: dict[Path, tuple[tuple[int, int], frozenset[str]]] = {}


def _keys_cited_in_wiki(root: Path) -> set[str]:
    """Cite keys the wiki's articles name, in :SOURCE_KEYS: or citations.

    The articles are the ones sync indexes (wiki_article_files), symlinked
    directories included. An article is parsed again only when its size or
    mtime changed since the last call. One that cannot be read or decoded
    is skipped; one it cannot list is an error, since a missed article could
    hand its key to a new source.
    """
    wiki = root / CRUCIBLE_DIR / "wiki"
    if not wiki.is_dir():
        return set()
    keys: set[str] = set()
    for path in wiki_article_files(wiki):
        try:
            st = path.stat()
            stamp = (st.st_mtime_ns, st.st_size)
            cached = _cited_keys_cache.get(path)
            if cached is None or cached[0] != stamp:
                meta = parse_org_file(path)
                cached = (stamp, frozenset(
                    meta.properties.get("SOURCE_KEYS", "").split()) | frozenset(meta.cite_keys))
                _cited_keys_cache[path] = cached
        except (OSError, UnicodeDecodeError):
            continue
        keys.update(cached[1])
    return keys


def _sidecar_path(path: Path) -> Path:
    """Where the extracted text of the stored file `path` goes: <stem>.txt,
    or `path` itself for a .txt file, which is its own text.

    The suffix is compared without case: NOTES.TXT and NOTES.txt are one
    file on a case-insensitive filesystem (macOS, Windows), so a sidecar
    beside NOTES.TXT would be the source itself.
    """
    return path if path.suffix.lower() == ".txt" else path.with_suffix(".txt")


# How many numbered names (<stem>_1, <stem>_2, ...) ingest tries before
# giving up on storing a file.
_MAX_NAME_TRIES = 10000


def _dest_taken(dest_path: Path) -> bool:
    """Whether storing a source at dest_path would collide with an existing file.

    A source's text sidecar is <stem>.txt, so the name is taken if either
    the file or its sidecar exists. A .txt source is also refused a name
    that another file's sidecar would use (any <stem>.* sibling).
    """
    if dest_path.exists() or dest_path.with_suffix(".txt").exists():
        return True
    if dest_path.suffix.lower() == ".txt":
        pattern = glob_escape(dest_path.stem) + ".*"
        return any(p.is_file() for p in dest_path.parent.glob(pattern))
    return False


def _claim(path: Path, created: list[Path]) -> bool:
    """Create path exclusively; record it in created and return True if we got it."""
    try:
        with open(path, "xb"):
            pass
    except FileExistsError:
        return False
    created.append(path)
    return True


def _source_result(root: Path, src: dict, **extra) -> dict:
    """What ingest_source returns for an existing source row."""
    return {
        "source_id": src["id"],
        "title": src["title"],
        "cite_key": _source_metadata(src).get("cite_key", ""),
        "source_type": src["source_type"],
        "shareable": bool(src["shareable"]),
        "stored_path": str(root / src["path"]),
        "relative_path": src["path"],
        "text_path": None,
        "text_length": 0,
        "date": src.get("date"),
        **extra,
    }


def _norm_doi(doi: str | None) -> str:
    """A DOI for comparison: lowercase, without a resolver prefix."""
    return re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:)", "",
                  (doi or "").strip().lower())


def _norm_title(title: str | None) -> str:
    """A title for comparison: braces, case, punctuation and spacing ignored."""
    text = (title or "").replace("{", "").replace("}", "").casefold()
    return " ".join(re.findall(r"\w+", text))


def _restore_mismatch(root: Path, src: dict, source_path: Path, source_type: str,
                      title: str | None, doi: str | None, text: str,
                      url: str | None = None) -> str | None:
    """Why source_path may not be the missing file of the row src, or None.

    The row was rebuilt from its bib entry, so it has no hash to compare.
    The file is taken to be that source when its DOI (given, or found in a
    paper's or web page's text) equals the row's, or failing a DOI on both
    sides, when its title (given, or declared by the file) equals the
    row's, or when its text equals the row's committed text sidecar. A
    disagreeing DOI, or no evidence at all, is a problem. When both the
    row and the ingest have a URL, it decides: URL ingest names pages from
    a title with the site cut off, so "Introduction" pages of two sites
    share a name.
    """
    key = _source_metadata(src).get("cite_key") or src["path"]
    advice = ("If it is that source, run again with --restore; if not, "
              "rename the file before ingesting it.")
    row_url, file_url = (u.strip().rstrip("/") if u else None for u in (src.get("url"), url))
    if row_url and file_url:
        if row_url != file_url:
            return (f"{source_path.name} would be stored as {src['path']}, the missing "
                    f"file of source {key}, but it comes from {file_url}, not that "
                    f"source's {row_url}. {advice}")
        return None
    row_doi = _norm_doi(_source_metadata(src).get("doi"))
    file_doi = _norm_doi(doi or (extract_doi_from_text(text)
                                 if may_carry_own_doi(source_type, source_path) else None))
    if row_doi and file_doi:
        if row_doi != file_doi:
            return (f"{source_path.name} would be stored as {src['path']}, the missing "
                    f"file of source {key}, but its DOI {file_doi} differs from that "
                    f"source's DOI {row_doi}. {advice}")
        # A DOI found in the text may be one the file cites: it shows the
        # file is the source only when the source's title is near the start
        # too, as ingest requires before using such a DOI. Otherwise the
        # title or text below must show it.
        if doi or _title_on_first_page(src.get("title") or "", text):
            return None
    row_title = _norm_title(src.get("title"))
    file_title = _norm_title(title or declared_title(source_path, text))
    if row_title and file_title and row_title == file_title:
        return None
    sidecar = _sidecar_path(root / src["path"])
    if text and sidecar != root / src["path"] and sidecar.is_file():
        try:
            if sidecar.read_text(encoding="utf-8", errors="replace") == text:
                return None
        except OSError:
            pass
    return (f"{source_path.name} would be stored as {src['path']}, the missing "
            f"file of source {key}, but nothing shows it is that source (no "
            f"matching DOI, title or text). {advice}")


def _add_or_adopt_source(db: CrucibleDB, cite_key: str, **fields) -> int:
    """Add ingest's row for fields["path"], or take over the row a sync made for it.

    Called under the bib lock, right after the entry `cite_key` was
    appended. A sync that ran after the file was copied but before the
    entry was appended may have indexed the file from the disk scan
    (sources/notebooks/, sources/data/); a row under this entry's own key
    can only come from a sync that read the entry. Either row is this
    source, so it is updated in place rather than the insert failing on
    its path and the ingest being undone. Any other row at the path is
    not ours, and the insert's error stands.
    """
    row = db.get_source_by_path(fields["path"])
    if row is not None and (_from_disk_scan(row)
                            or _source_metadata(row).get("cite_key") == cite_key):
        db.update_source(row["id"], **{k: v for k, v in fields.items() if k != "path"})
        return row["id"]
    return db.add_source(**fields)


def _restore_source(root: Path, db: CrucibleDB, src: dict, source_path: Path,
                    content_hash: str, text: str | None = None) -> dict:
    """Put source_path back as the missing stored file of the existing row src.

    This is the same source: on a fresh clone the gitignored external copy
    is absent and its row was rebuilt from the bib entry, or the stored file
    was deleted. The row keeps its id, cite key and article links and the
    bib entry is left alone; the file (and its text sidecar, unless one is
    there) is copied back and the content hash recorded.
    """
    # The row keeps its cite key and leaves the bibliography alone, so the
    # key must have an entry there: a row whose entry is gone (left by an
    # interrupted ingest, or an entry removed by hand) is dropped by the
    # next sync, and restoring into it would report a source that then
    # disappears. A disk-scan row is keyed by the scan and needs no entry.
    key = _source_metadata(src).get("cite_key")
    if isinstance(key, str) and key and not _from_disk_scan(src):
        entries, malformed = _read_bib_entries(root)
        if not malformed and key not in {entry["cite_key"] for entry in entries}:
            raise ValueError(
                f"{source_path.name} would be restored as {src['path']}, the "
                f"missing file of source {key}, but references.bib has no entry "
                f"{key}, so the next sync drops that source. Run `crucible sync`, "
                "then ingest the file again."
            )
    dest = root / src["path"]
    dest.parent.mkdir(parents=True, exist_ok=True)
    created: list[Path] = []
    try:
        if not _claim(dest, created):
            # Restored meanwhile (another ingest): nothing to do.
            return _source_result(root, src, duplicate=True)
        with open(source_path, "rb") as fin, open(dest, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        shutil.copystat(source_path, dest)

        metadata = _source_metadata(src)
        if text is None:
            text = extract_text(source_path)
        text_path = _sidecar_path(dest)
        wrote_text = False
        if text and text_path != dest and _claim(text_path, created):
            atomic_write_text(text_path, text)
            metadata["text_hash"] = file_sha256(text_path)
            wrote_text = True
        if not _from_disk_scan(src):
            # A disk-scan row is keyed by the scan, not by ingest.
            metadata["content_hash"] = content_hash
        db.update_source(src["id"], metadata=metadata)
    except BaseException:
        for path in reversed(created):
            try:
                path.unlink()
            except OSError:
                pass
        raise

    if text_path == dest:
        text_out = str(dest)
    elif wrote_text or text_path.exists():
        text_out = str(text_path)
    else:
        text_out = None
    key = metadata.get("cite_key", "")
    return _source_result(
        root, src,
        text_path=text_out if text else None,
        text_length=len(text),
        duplicate=False,
        restored=True,
        warnings=[(f"restored the missing stored file of existing source {key} "
                   f"({src['path']}); its bib entry is unchanged")],
    )


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
    restore: bool = False,
) -> dict:
    """Ingest a source file into Crucible.

    A file stored under the name of an existing source whose file is
    missing is restored into that source when its DOI, title or text
    sidecar shows it is that source; otherwise a ValueError is raised,
    unless restore=True says it is.

    Returns a dict with source_id, stored_path, extracted_text_path, cite_key, and metadata.
    """
    source_path = source_path.resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Source not found: {source_path}")
    # A supplied entry is checked before anything is copied or written: an
    # unclosed one would otherwise reach references.bib unchanged.
    # Text before the entry (a "% Encoding: UTF-8" line from a JabRef or
    # Zotero export, say) is allowed; text with no entry at all is an error,
    # not something to ignore silently.
    supplied_entry = None
    if bibtex:
        supplied_entry = bibtex_mod.first_entry(bibtex)
        if supplied_entry is None:
            raise ValueError("The --bibtex value is not a complete BibTeX entry "
                             "(it holds no @type{key, ...} entry, or its braces "
                             "are not balanced)")
        if len(bibtex_mod.entries(bibtex)) > 1:
            raise ValueError("The --bibtex value holds more than one BibTeX entry; "
                             "pass exactly one entry per ingested source")
    date = validate_date(date)

    # If this exact file (same content hash) was already ingested, skip it:
    # don't copy, append bib, or add a duplicate DB row. Return the existing
    # source so callers can report it.
    content_hash = file_sha256(source_path)
    existing = _find_duplicate_source(db, root, content_hash,
                                      size=source_path.stat().st_size)
    if existing is not None:
        if not (root / existing["path"]).is_file():
            return _restore_source(root, db, existing, source_path, content_hash)
        return _source_result(root, existing, duplicate=True)

    # Detect type if not specified
    if source_type is None:
        source_type = detect_source_type(source_path)

    # Determine shareability
    if shareable is None:
        shareable = source_type in ("notebook", "data")

    # A row that already holds the path this file would be stored at, while
    # the file itself is missing, is this source: on a fresh clone the
    # gitignored copy is absent and the row, rebuilt from its bib entry, has
    # no content hash to compare. Restore the file into that row rather than
    # add a second source for the same path. (A row whose recorded hash
    # differs is a different source; the name is then taken, see below.)
    held = {src["path"]: src for src in db.list_sources()}
    first_dest = destination_dir(root, source_type, shareable) / _storage_name(source_path.name)
    holder = held.get(_project_rel(root, first_dest))
    if (holder is not None and not first_dest.exists()
            and "content_hash" not in _source_metadata(holder)):
        # Only the name matches, so the file is checked against the row
        # before it inherits that source's key, title and bib entry: a
        # different paper.pdf must not be filed as the old one.
        # The text is extracted once, for the check and the sidecar.
        text = extract_text(source_path)
        if not restore:
            problem = _restore_mismatch(root, holder, source_path, source_type,
                                        title, doi, text, url)
            if problem:
                raise ValueError(problem)
        return _restore_source(root, db, holder, source_path, content_hash, text)

    # Everything that can fail (text extraction, DOI lookup, building the
    # entry) runs before anything is written, from the original file.
    text = extract_text(source_path)

    # If no DOI was supplied, try to recover one from the document text so we
    # can fetch a complete, correctly-typed entry instead of a bare @misc.
    doi_given = bool(doi)
    if not doi and may_carry_own_doi(source_type, source_path):
        doi = extract_doi_from_text(text)

    # The bib entry. Preference order:
    #   1. user-provided --bibtex string
    #   2. fetched from doi.org content negotiation (real entry type + fields)
    #   3. minimal entry from known metadata
    # Its key is a placeholder until the real one is chosen under the lock.
    placeholder = "crucible-pending-key"
    bib_entry = None
    warnings: list[str] = []
    if supplied_entry is not None:
        # Only the entry itself: any comment or preamble before it is dropped.
        bib_entry = replace_cite_key(supplied_entry.text, placeholder)
    elif doi:
        fetched = fetch_bibtex_from_doi(doi)
        if fetched and bibtex_mod.first_entry(fetched) is not None:
            bib_entry = replace_cite_key(fetched, placeholder)
    known = _parse_bib_fields(bib_entry) if bib_entry else {}

    # A DOI guessed from the text may be one the document cites rather than
    # its own. A web page or note keeps it only when its citation metadata
    # names it: guessing from titles ("Title | Blog", front matter, arXiv
    # labels) let pages about a paper become the paper. A PDF keeps it
    # when its declared title, if any, is the fetched one, or when the
    # fetched title is printed near the start, where a paper prints its own.
    if bib_entry is not None and supplied_entry is None and not doi_given:
        fetched_title = known.get("title", "")
        if source_type != "pdf":
            if not _page_declares_doi(source_path, doi):
                warnings.append(
                    f"Ignored DOI {doi} found in the text: the page's citation "
                    f"metadata does not name it. If it is this source's own, "
                    f"ingest it again with --doi {doi}"
                )
                bib_entry, known, doi = None, {}, None
        elif title and not _declares_title(title, fetched_title):
            warnings.append(
                f"Ignored DOI {doi} found in the text: it belongs to "
                f"\"{fetched_title}\", not \"{title}\""
            )
            bib_entry, known, doi = None, {}, None
        elif not title and not _title_on_first_page(fetched_title, text):
            warnings.append(
                f"Ignored DOI {doi} found in the text: its title "
                f"\"{fetched_title}\" is not near the start of the document"
            )
            bib_entry, known, doi = None, {}, None

    # Title, authors and date for the key: what the user gave, else what
    # the entry says, else what the file says (title) or today (date).
    if title is None:
        title = known.get("title") or extract_title(source_path, text)
    key_authors = authors or _authors_from_bib(known.get("author", "")) or None
    date_given = date is not None
    entry_year = (known.get("date") or known.get("year") or "")[:4]
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")
    # The key's year is the year the entry ends up with: an entry's own year
    # wins over a --date that contradicts it (that date is not written).
    if re.fullmatch(r"\d{4}", entry_year):
        key_date = entry_year
        if date_given and date[:4] != entry_year:
            warnings.append(
                f"--date {date} disagrees with the BibTeX entry's year "
                f"{entry_year}; keeping the entry's year"
            )
    else:
        key_date = date
    base_key = generate_cite_key(title, key_authors, key_date)

    if bib_entry is None:
        bib_entry = generate_bib_entry(
            placeholder, title, authors, date, url, source_type, doi=doi,
        )
        known = _parse_bib_fields(bib_entry)
    # The row is rebuilt from this entry on a fresh clone, so the entry must
    # carry what the row records: the authors and full date the user gave,
    # when a supplied or fetched entry lacks them.
    if title and not known.get("title"):
        bib_entry = set_bib_field(bib_entry, "title", title)
    if authors and not known.get("author"):
        bib_entry = set_bib_field(bib_entry, "author", " and ".join(authors))
    if url and not known.get("url"):
        bib_entry = set_bib_field(bib_entry, "url", url)
    if doi and not known.get("doi"):
        bib_entry = set_bib_field(bib_entry, "doi", doi)
    # A date is written only where it agrees with the entry's year: the
    # entry is the source's description, and a date it contradicts (say the
    # day the paper was read) would replace its publication year.
    if (re.fullmatch(r"\d{4}-\d{2}(-\d{2})?", date) and not known.get("date")
            and (date_given or not known.get("year"))
            and known.get("year", date[:4]) == date[:4]):
        bib_entry = set_bib_field(bib_entry, "date", date)

    # Choose where to store it: a name that is free, and whose text sidecar
    # (<stem>.txt) is free too, so neither overwrites an existing file.
    dest_dir = destination_dir(root, source_type, shareable)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / _storage_name(source_path.name)
    in_place = dest_path.exists() and dest_path.samefile(source_path)

    # Claim the names by exclusive create: a free name that another ingest
    # takes first raises FileExistsError, and we move on to the next one.
    # A file is never replaced. A .txt source is its own text; any other
    # file gets a sidecar. Claiming happens inside the try, so a failure or
    # interrupt before the copy still removes the empty placeholders.
    created: list[Path] = []
    cite_key = None
    undone = False

    def undo() -> None:
        # Leave nothing behind: no orphan copy, sidecar or bib entry.
        nonlocal undone
        undone = True
        if cite_key is not None:
            try:
                _drop_bib_entry(root, cite_key, rel_path)
            except Exception:
                pass
        for path in reversed(created):
            try:
                path.unlink()
            except OSError:
                pass

    try:
        write_sidecar = False
        if in_place:
            text_path = _sidecar_path(dest_path)
            if text and text_path != dest_path:
                write_sidecar = _claim(text_path, created)
        else:
            stem, suffix = dest_path.stem, dest_path.suffix
            counter = 1
            while True:
                if counter > _MAX_NAME_TRIES:
                    raise ValueError(
                        f"Cannot find a free name to store {source_path.name} in "
                        f"{dest_dir}: {stem}{suffix} and {_MAX_NAME_TRIES} numbered "
                        "variants (and their .txt sidecars) are all taken"
                    )
                # A path a row holds is taken even when its file is missing.
                if (not _dest_taken(dest_path)
                        and _project_rel(root, dest_path) not in held
                        and _claim(dest_path, created)):
                    text_path = _sidecar_path(dest_path)
                    if not text or text_path == dest_path:
                        break
                    if _claim(text_path, created):
                        write_sidecar = True
                        break
                    created.pop().unlink(missing_ok=True)
                dest_path = dest_dir / f"{stem}_{counter}{suffix}"
                counter += 1

        rel_path = str(dest_path.relative_to(root))
        bib_entry = set_bib_field(bib_entry, "file", rel_path)

        if not in_place:
            with open(source_path, "rb") as src, open(dest_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
            shutil.copystat(source_path, dest_path)
        if write_sidecar:
            atomic_write_text(text_path, text)

        # The entry and the row are written under one hold of the bib lock.
        # A sync reconciles under that lock, so it never sees the entry
        # without the row: it would insert a row from the entry, and this
        # insert would then fail on the path and undo the ingest, leaving
        # the sync's row with no entry and no file.
        with _bib_lock(root):
            try:
                cite_key = append_bib_entry_unique_key(root, db, base_key, bib_entry)
                bib_entry = replace_cite_key(bib_entry, cite_key)

                # Describe the source exactly as a rebuild from the entry would.
                first = bibtex_mod.first_entry(bib_entry)
                rebuilt = _fields_from_bib(
                    {"entry_type": first.entry_type, "cite_key": cite_key, **first.as_dict()}, rel_path,
                )
                title = rebuilt["title"]
                authors = rebuilt["authors"] or authors
                date = rebuilt["date"] or date
                url = rebuilt["url"] or url

                # Register in database (store cite_key and content hash in metadata)
                metadata = {"cite_key": cite_key, "content_hash": content_hash}
                if write_sidecar:
                    # Lets delete tell this sidecar from a user's own <stem>.txt.
                    metadata["text_hash"] = file_sha256(text_path)
                if doi:
                    metadata["doi"] = doi
                source_id = _add_or_adopt_source(
                    db, cite_key,
                    path=rel_path,
                    title=title,
                    source_type=source_type,
                    shareable=shareable,
                    url=url,
                    authors=authors,
                    date=date,
                    metadata=metadata,
                )
            except BaseException:
                # Undone before the lock is released, so a sync never sees
                # the entry without its row.
                undo()
                raise
    except BaseException:
        if not undone:
            undo()
        raise

    if text_path != dest_path and not text_path.exists():
        text_path = None

    return {
        "source_id": source_id,
        "title": title,
        "cite_key": cite_key,
        "source_type": source_type,
        "shareable": shareable,
        "stored_path": str(dest_path),
        "relative_path": rel_path,
        "text_path": str(text_path) if text and text_path else None,
        "text_length": len(text),
        "date": date,
        "duplicate": False,
        "warnings": warnings,
    }
