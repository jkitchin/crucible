"""Crucible CLI - knowledge base management."""

import errno
import json
import os
import re
import shutil
import stat
import tempfile
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from pathlib import Path
from typing import NoReturn

import click

from crucible import DISTRIBUTION, package_version
from crucible import registry as registry_module
from crucible.database import CrucibleDB
from crucible.embeddings import EmbeddingIndex
from crucible.ingest import _source_metadata, ingest_source
from crucible.orgparse import parse_org_file


CRUCIBLE_DIR = ".crucible"

# Permissions that background agents need to operate on a crucible project
CRUCIBLE_PERMISSIONS = [
    "Bash(crucible *)",
    "Bash(crucible)",
    "Bash(pandoc*)",
    "Bash(pdftotext*)",
    "Bash(curl*)",
    "Read(.crucible/**)",
    "Edit(.crucible/wiki/**)",
    "Edit(.crucible/sources/**)",
]

# Rules written by older versions. Claude Code ignores Write(path) in file
# permission checks (Edit(path) covers all file-editing tools), so drop them.
OBSOLETE_PERMISSIONS = [
    "Write(.crucible/wiki/**)",
    "Write(.crucible/sources/**)",
]


def _ensure_settings(root: Path) -> bool:
    """Create or update .claude/settings.json with crucible permissions.

    Merges crucible permissions into existing settings without clobbering
    other configuration. Returns True if the file was created or modified.
    """
    settings_dir = root / ".claude"
    settings_path = settings_dir / "settings.json"

    if settings_path.exists():
        try:
            settings = json.loads(settings_path.read_text(encoding="utf-8"))
        except (ValueError, OSError) as e:
            # Never replace a file we cannot read: it holds the user's
            # other settings.
            click.echo(
                f"  Warning: could not read {settings_path} ({e}); "
                "not adding crucible permissions to it.", err=True,
            )
            return False
    else:
        settings = {}

    if not isinstance(settings, dict):
        click.echo(
            f"  Warning: {settings_path} is not a JSON object; "
            "not adding crucible permissions to it.", err=True,
        )
        return False
    permissions = settings.setdefault("permissions", {})
    allow = permissions.get("allow", []) if isinstance(permissions, dict) else None
    if not isinstance(allow, list) or not all(isinstance(p, str) for p in allow):
        click.echo(
            f"  Warning: {settings_path} has an unexpected \"permissions\" "
            "shape; not adding crucible permissions to it.", err=True,
        )
        return False
    permissions["allow"] = allow

    # Drop obsolete rules, then add any missing crucible permissions
    added = [perm for perm in allow if perm in OBSOLETE_PERMISSIONS]
    allow[:] = [perm for perm in allow if perm not in OBSOLETE_PERMISSIONS]
    for perm in CRUCIBLE_PERMISSIONS:
        if perm not in allow:
            allow.append(perm)
            added.append(perm)

    if not added:
        return False

    settings_dir.mkdir(parents=True, exist_ok=True)
    settings_path.write_text(
        json.dumps(settings, indent=2) + "\n", encoding="utf-8"
    )
    return True


def _warn(message: str) -> None:
    click.echo(message, err=True)


def _is_registry_dir(candidate: Path) -> bool:
    """True if `candidate` is the global registry directory (~/.crucible/)."""
    try:
        return candidate.resolve() == registry_module.REGISTRY_DIR.resolve()
    except OSError:
        return False


def _is_project_dir(candidate: Path) -> bool:
    """True if `candidate` is a crucible project's .crucible/ dir.

    The global registry at ~/.crucible/ is never a project, even if it
    has a wiki/ subdirectory: otherwise every directory under the home
    directory would resolve to it.
    """
    return (candidate.is_dir() and (candidate / "wiki").is_dir()
            and not _is_registry_dir(candidate))


def get_root() -> Path:
    """Find the project root (walks up looking for .crucible/wiki/).

    If the project is found but its crucible.db is missing (e.g. after a
    fresh git clone — the database is gitignored as regenerable), the DB
    is rebuilt from the committed wiki files and references.bib before
    returning.
    """
    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents]:
        candidate = parent / CRUCIBLE_DIR
        if _is_project_dir(candidate):
            db_path = candidate / "crucible.db"
            if not db_path.exists():
                _auto_rebuild_db(parent)
            return parent
    raise click.ClickException(
        "Not inside a crucible project. Run `crucible init` to create one."
    )


def cdir(root: Path) -> Path:
    """Return the .crucible/ directory for a project root."""
    return root / CRUCIBLE_DIR


_ORG_DATE_RE = re.compile(
    r"^[\[<]?\s*(\d{4}-\d{2}-\d{2})"         # date
    r"(?:\s+[^\d\s\]>]+)?"                    # optional weekday, e.g. "Tue"
    r"(?:[\sT]+(\d{1,2}:\d{2}(?::\d{2})?))?"  # optional time
)


def _org_date(value: str | None) -> str | None:
    """Normalize an org date property to the DB's `YYYY-MM-DD HH:MM:SS`.

    Accepts an ISO date (`2026-01-02`), optionally with a time, and org
    timestamps (`[2026-01-02 Fri]`, `<2026-01-02 Fri 09:30>`). Returns None
    for a missing or unparseable value, so callers fall back to sync time.
    """
    from datetime import datetime

    m = _ORG_DATE_RE.match((value or "").strip())
    if not m:
        return None
    date, time = m.group(1), m.group(2) or "00:00"
    if time.count(":") == 1:
        time += ":00"
    try:
        parsed = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _wiki_article_files(wiki_dir: Path) -> list[Path]:
    """Return the authored .org articles under a wiki directory, sorted.

    The top-level wiki/index.org is generated by `crucible index` and is not
    an article, so it is excluded. An index.org in a subdirectory (e.g. a
    concept article about "index") is an ordinary article and is kept.
    Entries that are not regular files are skipped, such as the dangling
    `.#name.org` lock symlinks Emacs creates while a buffer has unsaved
    changes.

    Symlinked directories are followed (a wiki kept in Dropbox is often
    linked in), each real directory once, so a symlink loop cannot recurse
    forever. Paths are returned under `wiki_dir` as spelled, not resolved.

    Errors are raised rather than suppressed, so an incomplete scan can never
    be mistaken for deleted articles. That is why this walks with scandir:
    rglob and os.walk skip entries whose type cannot be read. The exception
    is an entry that vanishes during the scan, which really is gone.
    """
    index_path = wiki_dir / "index.org"
    org_files = []
    seen_dirs = set()
    pending = [wiki_dir]
    while pending:
        directory = pending.pop()
        try:
            dir_stat = directory.stat()
        except FileNotFoundError:
            continue
        dir_key = (dir_stat.st_dev, dir_stat.st_ino)
        if dir_key in seen_dirs:
            continue
        seen_dirs.add(dir_key)
        try:
            with os.scandir(directory) as it:
                entries = list(it)
        except FileNotFoundError:
            continue
        for entry in entries:
            path = Path(entry.path)
            try:
                mode = entry.stat(follow_symlinks=False).st_mode
                if stat.S_ISLNK(mode):
                    # A dangling link, such as an Emacs `.#name.org` lock,
                    # or a link to itself is skipped; a link to a directory
                    # is followed.
                    try:
                        mode = entry.stat(follow_symlinks=True).st_mode
                    except OSError as exc:
                        if isinstance(exc, FileNotFoundError) or exc.errno == errno.ELOOP:
                            continue
                        raise
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(mode):
                pending.append(path)
            elif (
                entry.name.endswith(".org")
                and path != index_path
                and stat.S_ISREG(mode)
            ):
                org_files.append(path)
    return sorted(org_files)


def get_db(root: Path) -> CrucibleDB:
    return CrucibleDB(cdir(root) / "crucible.db")


def _auto_rebuild_db(root: Path):
    """Rebuild a missing database from wiki files and references.bib.

    Triggered by get_root() when a crucible project is detected but its
    DB file is absent. Creates the schema, populates sources from the
    bib file, runs the sync logic, and regenerates MANIFEST.md and
    wiki/index.org.

    The rebuild is staged in a temporary file next to crucible.db and only
    published once it has completed, so a failed or interrupted rebuild
    never leaves a crucible.db that later commands would trust. Publishing
    uses a hard link, which fails rather than overwrites if a concurrent
    process published its own database first; that database is kept.
    """
    click.echo(
        "crucible: database missing, rebuilding from wiki...",
        err=True,
    )
    db_path = cdir(root) / "crucible.db"
    fd, tmp_name = tempfile.mkstemp(
        prefix=".crucible.db.rebuild-", suffix=".tmp", dir=cdir(root)
    )
    os.close(fd)
    staged = Path(tmp_name)
    try:
        db = CrucibleDB(staged)
        try:
            db.initialize()
            _rebuild_db_from_wiki(root, db)
            # Leave WAL mode so everything is in the main file and no
            # -wal/-shm companions are needed when it is renamed.
            db.conn.execute("PRAGMA journal_mode = DELETE")
        finally:
            db.close()
        try:
            os.link(staged, db_path)
        except FileExistsError:
            pass  # A concurrent process published a database first.
        except OSError:
            # Filesystem without hard links: fall back to a rename, still
            # declining to replace a database that appeared meanwhile.
            if not db_path.exists():
                os.replace(staged, db_path)
    finally:
        for path in (staged, Path(f"{staged}-wal"), Path(f"{staged}-shm"),
                     Path(f"{staged}-journal")):
            try:
                path.unlink()
            except FileNotFoundError:
                pass


def json_option(f):
    """Add --json (aliases --json-output, -j): machine-readable output on stdout."""
    return click.option("--json", "--json-output", "-j", "json_output", is_flag=True,
                        help="Output as JSON (for editors and agents)")(f)


def _emit_json(data) -> None:
    click.echo(json.dumps(data, indent=2, default=str))


@click.group()
@click.version_option(package_name=DISTRIBUTION, prog_name="crucible")
@click.pass_context
def cli(ctx):
    """Crucible - LLM-compiled knowledge base."""
    ctx.ensure_object(dict)


@cli.command()
@json_option
def about(json_output):
    """Show crucible version, project info, and quick overview."""
    from crucible.database import SCHEMA_VERSION
    from crucible.registry import list_instances

    version = package_version()

    if json_output:
        project = None
        try:
            root = get_root()
            db = get_db(root)
            project = {
                "root": str(root),
                "wiki": str(cdir(root) / "wiki"),
                "bibliography": str(cdir(root) / "references.bib"),
                "schema_version": db.schema_version(),
                "stats": db.stats(),
            }
            db.close()
        except click.ClickException:
            pass
        _emit_json({
            "version": version,
            "schema_version": SCHEMA_VERSION,
            "project": project,
            "registry": list_instances(),
        })
        return

    click.echo(f"Crucible v{version} (schema v{SCHEMA_VERSION})")
    click.echo("LLM-compiled knowledge base with org-mode wiki and graph database")
    click.echo()

    # Project info (if inside a crucible project)
    try:
        root = get_root()
        db = get_db(root)
        s = db.stats()
        v = db.schema_version()
        db.close()
        click.echo(f"Project: {root}")
        click.echo(f"  Database: schema v{v}")
        click.echo(f"  Sources:  {s.get('sources', 0)}")
        click.echo(f"  Articles: {s.get('articles', 0)}")
        click.echo(f"  Concepts: {s.get('concepts', 0)}")
        click.echo(f"  Links:    {s.get('article_links', 0)}")
    except click.ClickException:
        click.echo("Project: not inside a crucible project")

    click.echo()

    # Global registry
    instances = list_instances()
    click.echo(f"Global registry: {len(instances)} crucible(s)")
    for inst in instances:
        click.echo(f"  {inst['name']}: {inst['path']}")

    click.echo()
    click.echo("Help topics: overview, workflow, ingest, distill, search,")
    click.echo("  sync, maintain, org-format, registry")
    click.echo("Run `crucible help <topic>` or `crucible help all`")


@cli.command()
@click.option("--root", type=click.Path(path_type=Path), default=None,
              help="Project root directory (default: current directory)")
def init(root):
    """Initialize a new crucible project.

    Creates .crucible/ with the full directory structure, CLAUDE.md,
    .gitignore, and the SQLite database. Everything lives inside
    .crucible/ to keep the project directory clean.

    Safe to re-run on an existing project (creates only what's missing).
    """
    if root is None:
        root = Path.cwd()
    root = root.resolve()
    cdir = root / CRUCIBLE_DIR
    if _is_registry_dir(cdir):
        raise click.ClickException(
            f"{cdir} is the global crucible registry, so {root} cannot be a "
            "crucible project. Run `crucible init` in a project directory."
        )

    # Create directory structure inside .crucible/
    dirs = [
        "sources/external/pdfs",
        "sources/external/web",
        "sources/notebooks",
        "sources/data",
        "wiki/concepts",
        "wiki/summaries",
        "wiki/comparisons",
        "wiki/methods",
    ]
    for d in dirs:
        (cdir / d).mkdir(parents=True, exist_ok=True)

    created = []

    # .gitignore at the project root
    gitignore = root / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text(
            "# Crucible: external sources (copyrighted, not shareable)\n"
            ".crucible/sources/external/\n"
            "\n"
            "# Crucible: database (regenerable from wiki via auto-rebuild)\n"
            ".crucible/crucible.db\n"
            ".crucible/crucible.db-wal\n"
            ".crucible/crucible.db-shm\n"
            ".crucible/crucible.db-journal\n"
            ".crucible/.crucible.db.rebuild-*.tmp\n"
            "\n"
            "# Crucible: lock file for references.bib\n"
            ".crucible/references.bib.lock\n"
            "\n"
            "# Crucible: publish output\n"
            ".crucible/wiki/_build/\n"
            "\n"
            "# Crucible: left behind only if a merge is killed\n"
            ".crucible/.merge-staging-*/\n",
            encoding="utf-8",
        )
        created.append(".gitignore")

    # CLAUDE.md at project root
    claude_md = root / "CLAUDE.md"
    crucible_marker = "<!-- crucible-project -->"
    crucible_section = (
        f"\n{crucible_marker}\n"
        "## Crucible Knowledge Base\n"
        "\n"
        "This project has a Crucible knowledge base in `.crucible/`.\n"
        "Use the `crucible` CLI to ingest sources, search, and maintain the wiki.\n"
        "\n"
        "Layout: `.crucible/sources/` (primary sources), `.crucible/wiki/` (distilled articles),\n"
        "`.crucible/crucible.db` (graph database).\n"
        "\n"
        "Conventions: org-mode with scimax, org-ref citations, narrative prose.\n"
        "Each article's top heading has :ARTICLE_TYPE:, :SOURCE_KEYS: (the cite keys\n"
        "`crucible ingest` prints), :STATUS:, :CREATED: and :MODIFIED: (update it on\n"
        "every edit). Run `crucible sync` after writing or editing articles;\n"
        "it also regenerates wiki/MANIFEST.md and wiki/index.org.\n"
        "The LLM maintains the wiki; manual edits are the exception.\n"
        "Run `crucible help all` for the full CLI reference.\n"
        f"{crucible_marker}\n"
    )
    if claude_md.exists():
        existing = claude_md.read_text(encoding="utf-8")
        if crucible_marker not in existing:
            claude_md.write_text(existing.rstrip() + "\n" + crucible_section,
                                 encoding="utf-8")
            created.append("CLAUDE.md (appended)")
    else:
        claude_md.write_text(crucible_section.lstrip(), encoding="utf-8")
        created.append("CLAUDE.md")

    # Claude Code settings for background agent permissions
    if _ensure_settings(root):
        created.append(".claude/settings.json")

    # Database
    db_path = cdir / "crucible.db"
    db = CrucibleDB(db_path)
    db.initialize()
    db.close()

    if created:
        click.echo(f"Created: {', '.join(created)}")
    click.echo(f"Initialized crucible at {root}")

    # Auto-register in global registry
    actual_name = registry_module.register_or_warn(root.name, str(root), _warn)
    if actual_name:
        click.echo(f"  Registered in global registry as '{actual_name}'")

    # Install the skill and this project's manifest; the global CLAUDE.md
    # directive is left to an explicit `crucible install`.
    _install(root=root, directive=False)


@cli.command()
@click.argument("source", default="-")
@click.option("--title", "-t", default=None, help="Source title (auto-detected if omitted)")
@click.option("--type", "source_type",
              type=click.Choice(["pdf", "web", "notebook", "data", "other"]),
              default=None, help="Source type (auto-detected if omitted)")
@click.option("--shareable/--no-shareable", default=None,
              help="Whether source can be shared (default: notebooks/data yes, external no)")
@click.option("--url", default=None, help="Original URL of the source")
@click.option("--date", default=None, help="Source date (ISO format, default: today)")
@click.option("--author", "-a", multiple=True, help="Author(s), can be repeated")
@click.option("--doi", default=None, help="DOI for the source")
@click.option("--bibtex", default=None,
              help="Full BibTeX entry (overrides DOI lookup and auto-generation). "
                   "Pass a path with --bibtex @file.bib to read from a file.")
@click.option("--filename", "-f", default=None,
              help="Filename for stdin input (default: derived from title or timestamp)")
def ingest(source, title, source_type, shareable, url, date, author, doi, bibtex, filename):
    """Ingest a source into Crucible.

    SOURCE is a file path, URL, glob pattern, or - for stdin.

    Examples:

      crucible ingest paper.pdf

      crucible ingest https://example.com/article

      crucible ingest "papers/*.pdf"

      crucible ingest "notebooks/**/*.org" --type notebook

      cat notes.org | crucible ingest - --type notebook -t "Lab Notes 2026-04-03"
    """
    import glob as globmod
    import sys
    import tempfile

    from crucible.ingest import slugify, validate_date

    try:
        date = validate_date(date)
    except ValueError as e:
        raise click.BadParameter(str(e), param_hint="'--date'")

    # --bibtex @file.bib reads bibtex from a file. A literal entry starts
    # with @type{ or @type( ; anything else after the @ is a path.
    if bibtex and bibtex.startswith("@") and not _BIB_ENTRY_HEAD_RE.match(bibtex):
        bib_path = Path(bibtex[1:])
        if not bib_path.exists():
            raise click.ClickException(f"BibTeX file not found: {bib_path}")
        bibtex = bib_path.read_text(encoding="utf-8")
    if bibtex and bibtex.strip().startswith("@"):
        from crucible import bibtex as bibtex_mod
        if bibtex_mod.first_entry(bibtex.strip()) is None:
            raise click.ClickException(
                "The --bibtex entry is not a complete BibTeX entry "
                "(check that its braces are balanced)"
            )
        if len(bibtex_mod.entries(bibtex)) > 1:
            raise click.ClickException(
                "The --bibtex value holds more than one BibTeX entry; "
                "pass exactly one entry per ingested source"
            )

    root = get_root()
    db = get_db(root)

    def run_ingest(path: Path, **overrides) -> dict:
        kwargs = dict(
            root=root, db=db, source_path=path,
            title=title, source_type=source_type, shareable=shareable,
            url=url, date=date, authors=list(author) if author else None,
            doi=doi, bibtex=bibtex,
        )
        kwargs.update(overrides)
        try:
            return ingest_source(**kwargs)
        except ValueError as e:
            raise click.ClickException(str(e))

    if source == "-":
        # Reading from stdin
        content = sys.stdin.read()
        if not content.strip():
            raise click.ClickException("No input received on stdin.")

        if source_type is None:
            source_type = "web"
        if filename is None:
            slug = slugify(title) if title else ""
            if not slug:
                from datetime import datetime as dt
                slug = dt.now().strftime("stdin-%Y%m%d-%H%M%S")
            ext = ".org" if source_type in ("notebook",) else ".md"
            filename = f"{slug}{ext}"

        # Write to a temp file, then ingest normally
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            tmp = tmp_dir / Path(filename).name
            tmp.write_text(content, encoding="utf-8")
            result = run_ingest(tmp, source_type=source_type)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        _print_ingest_result(result)
    elif source.startswith("http://") or source.startswith("https://"):
        # URL: fetch, then store what was served (HTML page, PDF, ...)
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            tmp_file, fetched_title, fetched_type = _fetch_url_source(source, tmp_dir)
            result = run_ingest(
                tmp_file,
                title=title or fetched_title,
                source_type=source_type or fetched_type,
                shareable=False if shareable is None else shareable,
                url=source,
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        _print_ingest_result(result)

    else:
        # Check for glob pattern
        if "*" in source or "?" in source:
            paths = sorted(globmod.glob(source, recursive=True))
            paths = [Path(p) for p in paths if Path(p).is_file()]
            if not paths:
                raise click.ClickException(f"No files matched: {source}")
            click.echo(f"Ingesting {len(paths)} files...")
            for p in paths:
                try:
                    result = ingest_source(
                        root=root, db=db, source_path=p,
                        title=None, source_type=source_type, shareable=shareable,
                        url=None, date=date, authors=list(author) if author else None,
                        doi=doi,
                    )
                    if result.get("duplicate"):
                        click.echo(f"  DUP  {result['title']} (identical to existing source, skipped)")
                    else:
                        click.echo(f"  [{result['source_id']}] {result['title']} ({result['source_type']})")
                except Exception as e:
                    click.echo(f"  SKIP {p}: {e}", err=True)
            click.echo(f"\nDone. Run `crucible undigested` to see what needs distilling.")
        else:
            source_path = Path(source)
            if not source_path.exists():
                raise click.ClickException(f"Source not found: {source}")
            result = run_ingest(source_path)
            _print_ingest_result(result)

    db.close()


# A literal BibTeX entry passed to --bibtex: @type{ or @type(
_BIB_ENTRY_HEAD_RE = re.compile(r"\s*@\s*[A-Za-z][\w-]*\s*[{(]")

_HTML_TYPES = {"text/html", "application/xhtml+xml"}


def _fetch_url_source(source: str, tmp_dir: Path) -> tuple[Path, str | None, str]:
    """Download `source` into tmp_dir as the file to ingest.

    Returns (path, title, source_type). An HTML page is decoded with the
    charset it declares and saved as .html, with its <title> (entities
    decoded) as the title; anything else (a PDF, say) is saved byte for
    byte under the extension its Content-Type or URL names.
    """
    import html
    import http.client
    import mimetypes
    from urllib.error import URLError
    from urllib.parse import unquote, urlparse
    from urllib.request import Request, urlopen

    from crucible.ingest import detect_source_type, slugify

    click.echo(f"Fetching {source}...")
    try:
        req = Request(source, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urlopen(req, timeout=30) as resp:
            body = resp.read()
            content_type = resp.headers.get_content_type()
            charset = resp.headers.get_content_charset()
    except (URLError, OSError, ValueError, http.client.HTTPException) as e:
        raise click.ClickException(f"Failed to fetch URL: {e}")

    url_name = unquote(Path(urlparse(source).path).name)
    url_ext = Path(url_name).suffix.lower()
    is_html = content_type in _HTML_TYPES or (
        content_type in ("application/octet-stream", "text/plain")
        and url_ext in (".html", ".htm")
    )

    if is_html:
        try:
            raw_html = body.decode(charset or "utf-8", errors="replace")
        except LookupError:   # a charset Python does not know
            raw_html = body.decode("utf-8", errors="replace")
        title = None
        m = re.search(r"<title[^>]*>(.*?)</title>", raw_html, re.IGNORECASE | re.DOTALL)
        if m:
            title = " ".join(html.unescape(m.group(1)).split())
            # Clean up common title cruft
            for sep in [" | ", " - ", " :: "]:
                if sep in title:
                    title = title.split(sep)[0].strip()
        title = title or url_name or "Untitled Web Page"
        stem = slugify(title) or slugify(Path(url_name).stem) or "web-page"
        path = tmp_dir / f"{stem}.html"
        # Stored as UTF-8, the encoding text extraction reads.
        path.write_text(raw_html, encoding="utf-8")
        return path, title, "web"

    ext = url_ext if url_ext and mimetypes.guess_type(f"x{url_ext}")[0] == content_type else ""
    ext = ext or mimetypes.guess_extension(content_type) or url_ext or ".bin"
    stem = slugify(Path(url_name).stem) or "download"
    path = tmp_dir / f"{stem}{ext}"
    path.write_bytes(body)
    source_type = detect_source_type(path)
    if source_type not in ("pdf", "web"):
        source_type = "web"
    return path, None, source_type


def _print_ingest_result(result: dict):
    if result.get("duplicate"):
        click.echo(f"Already ingested (identical content): {result['title']}")
        click.echo(f"  Source ID: {result['source_id']}")
        click.echo(f"  Cite key: {result['cite_key']}")
        click.echo(f"  Stored: {result['relative_path']}")
        click.echo("  Skipped: no changes made.")
        return
    click.echo(f"Ingested: {result['title']}")
    click.echo(f"  Source ID: {result['source_id']}")
    click.echo(f"  Cite key: {result['cite_key']}")
    click.echo(f"  Type: {result['source_type']} ({'shareable' if result['shareable'] else 'external'})")
    click.echo(f"  Stored: {result['relative_path']}")
    if result['text_length'] > 0:
        click.echo(f"  Extracted: {result['text_length']} chars")
    else:
        click.echo("  Warning: no text extracted")
    click.echo()
    click.echo("Ready for distillation. Use citep:{} in articles.".format(result['cite_key']))


def _safe_relative_to(target: Path, root: Path) -> str | None:
    """Return ``target`` relative to ``root``, or None if it lies outside root.

    ``Path.relative_to`` raises ``ValueError`` for paths outside ``root``. A wiki
    article can easily contain a link to a file outside the wiki tree (an
    absolute ``[[file:/...]]`` link, or one with enough ``../``), and that must
    not abort the whole sync.
    """
    try:
        return str(target.relative_to(root))
    except ValueError:
        return None


def _link_path(org_path: Path, link: str) -> Path:
    """The path a file link in `org_path` names, normalised as org does.

    `~` is expanded and `..` is resolved lexically (like Emacs's
    expand-file-name), without following symlinks: resolving would carry a
    path inside a symlinked wiki out of the project root.
    """
    target = Path(os.path.expanduser(link))
    if not target.is_absolute():
        target = org_path.parent / target
    return Path(os.path.normpath(target))


def _on_disk_spelling(root: Path, rel: str) -> str | None:
    """`rel` with each component spelled as it is on disk, or None.

    On a case-insensitive filesystem [[file:B.org]] opens b.org, but the
    database keys articles by their on-disk path, case-sensitively.
    """
    current = root
    parts = []
    for part in Path(rel).parts:
        try:
            names = os.listdir(current)
        except OSError:
            return None
        if part not in names:
            matches = [n for n in names if n.casefold() == part.casefold()]
            if len(matches) != 1:
                return None
            part = matches[0]
        parts.append(part)
        current = current / part
    return str(Path(*parts)) if parts else None


def _link_target_rel(root: Path, org_path: Path, link: str) -> str | None:
    """The project-relative path a wiki file link points at, or None.

    None means the target lies outside the project. An absolute link may
    name the real location of a symlinked wiki or project, so the resolved
    path is also tried against the resolved wiki and root.
    """
    target = _link_path(org_path, link)
    rel = _safe_relative_to(target, root)
    if rel is not None:
        return rel
    try:
        real = target.resolve()
    except OSError:
        return None
    wiki_dir = cdir(root) / "wiki"
    for base, spelled in ((wiki_dir, wiki_dir), (root, root)):
        try:
            within = real.relative_to(base.resolve())
        except (ValueError, OSError):
            continue
        return str((spelled / within).relative_to(root))
    return None


def _link_is_broken(root: Path, org_path: Path, link: str) -> bool:
    """True if a file link in `org_path` names nothing on disk.

    Links into sources/external/ are exempt: those files are gitignored,
    so a fresh clone legitimately lacks them.
    """
    target = _link_path(org_path, link)
    if target.exists():
        return False
    rel = _link_target_rel(root, org_path, link)
    external = (CRUCIBLE_DIR, "sources", "external")
    return rel is None or Path(rel).parts[:3] != external


def _find_linked_article(db: CrucibleDB, root: Path, rel: str | None) -> dict | None:
    """The article stored at project-relative `rel`, matching on-disk case."""
    if rel is None:
        return None
    article = db.get_article_by_path(rel)
    if article is None and (root / rel).exists():
        spelled = _on_disk_spelling(root, rel)
        if spelled is not None and spelled != rel:
            article = db.get_article_by_path(spelled)
    return article


def _source_key_index(
    sources: list[dict],
) -> tuple[dict[str, list[int]], list[dict]]:
    """Split sources into a cite_key -> ids map and the legacy sources without one.

    A cite key normally names one source, but two sources can end up with the
    same key (for example, files with the same stem in sources/notebooks/ and
    sources/data/). Every source carrying the key is kept, so callers can link
    all of them and report the collision instead of silently picking one.

    Build this once per command and pass it to _resolve_source_keys, rather
    than re-parsing every source's metadata for each article.
    """
    key_to_src: dict[str, list[int]] = {}
    legacy_sources = []
    for src in sources:
        md = _source_metadata(src)
        cite_key = md.get("cite_key")
        if isinstance(cite_key, str) and cite_key:
            key_to_src.setdefault(cite_key, []).append(src["id"])
        else:
            legacy_sources.append(src)
    return key_to_src, legacy_sources


def _legacy_key_matches(key: str, src: dict) -> bool:
    """Whether a key names a legacy source by its file stem or title.

    The key must appear as a whole token (bounded by non-alphanumerics), so
    `legacy-path` matches `legacy-path-file.pdf` but `ml` does not match
    `page.html`.
    """
    token = re.compile(
        rf"(?<![0-9A-Za-z]){re.escape(key)}(?![0-9A-Za-z])", re.IGNORECASE
    )
    return bool(
        token.search(Path(src["path"]).stem) or token.search(src.get("title") or "")
    )


def _resolve_source_keys(
    keys: list[str], index: tuple[dict[str, list[int]], list[dict]]
) -> dict[str, list[int]]:
    """Resolve cite keys, falling back to legacy source file stems and titles.

    A canonical cite key takes precedence over legacy matching. A key that
    resolves to more than one source is ambiguous: all candidates are
    returned so none is presented as the single provenance, and lint
    reports the collision.
    """
    key_to_src, legacy_sources = index
    return {
        key: list(key_to_src[key])
        if key in key_to_src
        else [src["id"] for src in legacy_sources if _legacy_key_matches(key, src)]
        for key in keys
        if key
    }


ARTICLE_TYPES = ("concept", "summary", "comparison", "method")


def _normalize_article_type(value: str | None) -> str:
    """An :ARTICLE_TYPE: value as stored: lowercase, defaulting to concept."""
    return (value or "").strip().lower() or "concept"


def _report_unreadable(path: Path, exc: Exception) -> None:
    """Tell the user on stderr that an article could not be read."""
    if isinstance(exc, UnicodeDecodeError):
        reason = "not valid UTF-8"
    elif isinstance(exc, OSError) and exc.strerror:
        reason = exc.strerror
    else:
        reason = str(exc)
    click.echo(f"crucible: skipping unreadable article {path}: {reason}", err=True)


def _sync_wiki_to_db(root: Path, db: CrucibleDB, on_skip=None,
                     on_unreadable=None) -> dict:
    """Core sync logic: scan wiki/*.org and reconcile with the DB.

    Returns a stats dict with counts. `on_skip` is called with the path
    of each file skipped due to concurrent modification (defaults to a
    no-op so callers that don't want CLI output stay quiet).
    `on_unreadable` is called with the path and exception of each file
    that cannot be read or decoded (by default it is reported on stderr);
    such a file is skipped, like a concurrently modified one, so one bad
    file never aborts a sync or an automatic rebuild.
    """
    if on_skip is None:
        on_skip = lambda _p: None  # noqa: E731
    if on_unreadable is None:
        on_unreadable = _report_unreadable

    wiki_dir = cdir(root) / "wiki"
    generated_index = str((wiki_dir / "index.org").relative_to(root))
    stats = {
        "added": 0,
        "updated": 0,
        "unchanged": 0,
        "removed": 0,
        "article_links": 0,
        "concept_links": 0,
        "skipped": 0,
        "unreadable": 0,
        "files": 0,
        "ambiguous_source_keys": 0,
    }

    try:
        wiki_stat = wiki_dir.stat()
    except FileNotFoundError:
        return stats
    if not stat.S_ISDIR(wiki_stat.st_mode):
        return stats

    org_files = _wiki_article_files(wiki_dir)
    stats["files"] = len(org_files)

    # A file deleted after the scan is treated like any other removed article.
    file_mtimes = {}
    for p in org_files:
        try:
            file_mtimes[p] = p.stat().st_mtime
        except FileNotFoundError:
            continue
    org_files = list(file_mtimes)

    with db.conn.transaction():
        parsed = {}
        for org_path in org_files:
            try:
                changed = org_path.stat().st_mtime != file_mtimes[org_path]
            except FileNotFoundError:
                changed = True
            if changed:
                on_skip(org_path)
                stats["skipped"] += 1
                continue

            try:
                meta = parse_org_file(org_path)
            except (UnicodeDecodeError, OSError) as exc:
                on_unreadable(org_path, exc)
                stats["unreadable"] += 1
                continue
            rel_path = str(org_path.relative_to(root))
            article_type = _normalize_article_type(meta.properties.get("ARTICLE_TYPE"))
            status = meta.properties.get("STATUS") or "draft"
            # :CREATED: and :MODIFIED: make an article's dates part of its
            # file, so they survive a rebuild; without them sync time is used.
            # :MODIFIED: falls back to :CREATED:, by the same rule for new and
            # existing rows, so incremental sync and a rebuild agree. Property
            # dates have no zone and count as midnight; the sync-time fallback
            # (datetime('now')) is UTC. Ordering is only meaningful by day.
            # An article with only :MODIFIED: was created no later than that,
            # and using it keeps created_at <= updated_at on a rebuild too.
            created = (_org_date(meta.properties.get("CREATED"))
                       or _org_date(meta.properties.get("MODIFIED")))
            modified = _org_date(meta.properties.get("MODIFIED")) or created

            existing = db.get_article_by_path(rel_path)
            if existing:
                article_id = existing["id"]
                fields = {
                    "title": meta.title,
                    "article_type": article_type,
                    "status": status,
                    "distill_model": meta.properties.get("DISTILL_MODEL"),
                    "abstract": meta.properties.get("ABSTRACT", ""),
                }
                indexed = db.get_article_fts(article_id)
                changed = (
                    any(existing[k] != v for k, v in fields.items())
                    or indexed != {"path": rel_path, "title": meta.title,
                                   "content": meta.content}
                )
                # Only rewrite articles whose file changed, so updated_at
                # records real edits and unchanged articles keep their
                # embeddings. A changed article's embedding is stale, so
                # drop it for `crucible embed` to regenerate.
                if changed:
                    db.update_article(article_id, **fields)
                    db.update_article_fts(article_id, rel_path, meta.title, meta.content)
                    db.delete_embedding(article_id)
                # Dates from properties are applied on their own, so a DB
                # whose dates predate this (every :CREATED: article on
                # upgrade) is corrected without dropping embeddings.
                dates = {}
                if created and existing["created_at"] != created:
                    dates["created_at"] = created
                if modified and (changed or existing["updated_at"] != modified):
                    dates["updated_at"] = modified
                if dates:
                    db.set_article_dates(article_id, **dates)
                if changed or dates:
                    stats["updated"] += 1
                else:
                    stats["unchanged"] += 1
                # Relations are rebuilt even for unchanged files: a link or
                # SOURCE_KEYS entry can start (or stop) resolving when other
                # articles or sources change. Clear them so ones removed from
                # the file don't linger in the DB.
                db.clear_article_relations(article_id)
            else:
                article_id = db.add_article(
                    path=rel_path,
                    title=meta.title,
                    article_type=article_type,
                    distill_model=meta.properties.get("DISTILL_MODEL"),
                    abstract=meta.properties.get("ABSTRACT", ""),
                    content=meta.content,
                    status=status,
                    created_at=created,
                    updated_at=modified,
                )
                stats["added"] += 1

            type_tags = set(ARTICLE_TYPES)
            for tag in meta.filetags:
                if tag.lower() not in type_tags:
                    concept_id = db.add_concept(tag.lower())
                    db.link_article_concept(article_id, concept_id)
                    stats["concept_links"] += 1

            parsed[org_path] = (article_id, meta)

        # Prune articles whose .org file was removed from disk. Build the
        # present set from the full on-disk file list (not just successfully
        # parsed files) so a file skipped for concurrent modification is never
        # mistaken for a deletion.
        present = {str(p.relative_to(root)) for p in org_files}
        for art in db.list_articles():
            if art["path"] not in present:
                db.delete_article(art["id"])
                # Older versions registered the generated index as an
                # article; drop that row quietly rather than report it as
                # a user-visible removal.
                if art["path"] != generated_index:
                    stats["removed"] += 1

        source_index = _source_key_index(db.list_sources())
        for org_path, (article_id, meta) in parsed.items():
            for link_target in meta.file_links:
                target_article = _find_linked_article(
                    db, root, _link_target_rel(root, org_path, link_target)
                )
                if target_article:
                    db.add_article_link(article_id, target_article["id"])
                    stats["article_links"] += 1

            source_keys = meta.properties.get("SOURCE_KEYS", "").split()
            if source_keys:
                for source_ids in _resolve_source_keys(source_keys, source_index).values():
                    if len(source_ids) > 1:
                        stats["ambiguous_source_keys"] += 1
                    for sid in source_ids:
                        db.link_article_source(article_id, sid)

            # DERIVED_FROM paths are relative to the article, or to the wiki
            # directory (optionally written with a leading wiki/).
            wiki_rel = cdir(root) / "wiki"
            for ref in meta.properties.get("DERIVED_FROM", "").split():
                source_article = _find_linked_article(
                    db, root, _link_target_rel(root, org_path, ref)
                )
                if source_article is None and not Path(ref).is_absolute():
                    within = ref[len("wiki/"):] if ref.startswith("wiki/") else ref
                    source_article = _find_linked_article(
                        db, root,
                        _safe_relative_to(
                            Path(os.path.normpath(wiki_rel / within)), root
                        ),
                    )
                if source_article:
                    db.add_derivation(article_id, source_article["id"])

        # Tags dropped from files, or deleted articles, can leave concepts
        # that no article uses any more.
        db.prune_unused_concepts()

    return stats


def _rebuild_db_from_wiki(root: Path, db: CrucibleDB) -> dict:
    """Populate a fresh DB from wiki/*.org and references.bib.

    Used by the auto-rebuild path in get_root(). Idempotent — safe to
    call on a DB that's already populated. Also regenerates MANIFEST.md
    and wiki/index.org so GitHub-rendered landing pages stay current.
    """
    from crucible.ingest import reconcile_sources

    sources_added = reconcile_sources(root, db)
    sync_stats = _sync_wiki_to_db(root, db)
    _write_manifest(root, db)
    _write_index(root, db)
    return {"sources_added": sources_added, **sync_stats}


@cli.command()
def sync():
    """Sync the database from wiki org files.

    Scans wiki/ for org files, parses their metadata (title, properties,
    links, citations), and updates the articles, concepts, and links
    tables. Also upserts sources from references.bib and from the
    sources/notebooks and sources/data directories so new entries get
    picked up automatically, and regenerates wiki/MANIFEST.md and
    wiki/index.org. Run this after Claude writes or updates wiki articles.
    """
    from crucible.ingest import reconcile_sources

    root = get_root()
    db = get_db(root)
    try:
        # Auto-register in global registry (ensures pre-registry crucibles get registered)
        registry_module.register_or_warn(root.name, str(root), _warn)

        sources_added = reconcile_sources(root, db)

        stats = _sync_wiki_to_db(
            root, db,
            on_skip=lambda p: click.echo(
                f"  Skipping {p.name} (modified during sync)"
            ),
        )
        # Keep the generated landing pages in step with the database.
        _write_manifest(root, db)
        _write_index(root, db)
    finally:
        db.close()

    if stats["files"] == 0 and sources_added == 0 and stats["removed"] == 0:
        click.echo("No org files found in wiki/.")
        return

    msg = (
        f"Sync complete: {stats['added']} added, {stats['updated']} updated, "
        f"{stats['unchanged']} unchanged, {stats['removed']} removed, "
        f"{stats['concept_links']} concept links, "
        f"{stats['article_links']} article links"
    )
    if sources_added:
        msg += f", {sources_added} sources added"
    if stats["skipped"]:
        msg += f", {stats['skipped']} skipped (modified during sync)"
    if stats["unreadable"]:
        msg += f", {stats['unreadable']} unreadable (skipped)"
    click.echo(msg)
    if stats["ambiguous_source_keys"]:
        click.echo(
            f"Warning: {stats['ambiguous_source_keys']} SOURCE_KEYS value(s) match"
            " more than one source and were linked to every match;"
            " run `crucible lint` for details."
        )


@cli.command()
@click.option("--model", "-m", default="nomic-embed-text", help="Ollama embedding model")
@click.option("--url", default="http://localhost:11434/api/embed", help="Ollama API URL")
def embed(model, url):
    """Generate embeddings for all wiki articles.

    Uses ollama to create vector embeddings for semantic search.
    Only embeds articles that don't already have embeddings. `crucible sync`
    drops the embedding of any article whose file changed, so edited
    articles are re-embedded here.
    Requires ollama running with an embedding model pulled.
    """
    root = get_root()
    db = get_db(root)
    idx = EmbeddingIndex(db.conn, model=model, url=url)
    idx.initialize()

    # Get all articles with their content from FTS
    articles = []
    for a in db.list_articles():
        org_path = root / a["path"]
        if org_path.exists():
            a["content"] = org_path.read_text(encoding="utf-8")
        articles.append(a)

    click.echo(f"Embedding with {model}...")
    count = idx.embed_missing(articles)
    s = idx.stats()
    db.close()
    click.echo(f"Embedded {count} new articles ({s['embedded']}/{s['total_articles']} total)")


@cli.group()
def registry():
    """Manage the global crucible registry.

    The registry at ~/.crucible/registry.json tracks all crucible
    instances on this machine for automatic cross-discovery.
    """
    pass


@registry.command("list")
@json_option
def registry_list(json_output):
    """List all registered crucible instances."""
    from crucible.registry import list_instances, resolve_db_path
    instances = list_instances()
    if json_output:
        for inst in instances:
            db_path = resolve_db_path(inst)
            inst["db"] = str(db_path) if db_path else None
        _emit_json(instances)
        return
    if not instances:
        click.echo("No crucible instances registered.")
        return
    for inst in instances:
        db_path = resolve_db_path(inst)
        status = "ok" if db_path else "MISSING"
        desc = f" - {inst['description']}" if inst.get("description") else ""
        click.echo(f"  {inst['name']}: {inst['path']} ({status}){desc}")


@registry.command("add")
@click.argument("name")
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option("--description", "-d", default="", help="Optional description")
def registry_add(name, path, description):
    """Manually register a crucible instance."""
    from crucible.registry import register, resolve_db_path
    # Verify it's a crucible
    entry = {"path": str(path.resolve())}
    if resolve_db_path(entry) is None:
        raise click.ClickException(
            f"No crucible database found at {path.resolve()}. "
            f"Is this a crucible project?"
        )
    from crucible.registry import RegistryError, lookup_path
    previous = lookup_path(str(path.resolve()))
    try:
        actual_name = register(name, str(path.resolve()), description,
                               rename=True)
    except RegistryError as e:
        raise click.ClickException(str(e))
    if actual_name != name:
        if previous == actual_name:
            click.echo(f"Name '{name}' is taken by another crucible; "
                       f"this one stays registered as '{actual_name}'")
        else:
            click.echo(f"Name '{name}' is taken by another crucible; "
                       f"registered as '{actual_name}'")
    elif previous and previous != name:
        click.echo(f"Renamed '{previous}' to '{name}'")
    click.echo(f"Registered '{actual_name}' -> {path.resolve()}")


@registry.command("remove")
@click.argument("name")
def registry_remove(name):
    """Remove a crucible from the global registry."""
    from crucible.registry import RegistryError, unregister
    try:
        removed = unregister(name)
    except RegistryError as e:
        raise click.ClickException(str(e))
    if not removed:
        raise click.ClickException(f"'{name}' not found in registry")
    click.echo(f"Removed '{name}' from registry")


@registry.command("clean")
def registry_clean():
    """Remove stale entries (crucibles that no longer exist on disk)."""
    from crucible.registry import RegistryError, clean
    try:
        removed = clean()
    except RegistryError as e:
        raise click.ClickException(str(e))
    if not removed:
        click.echo("No stale entries found.")
        return
    for entry in removed:
        click.echo(f"  Removed '{entry['name']}': {entry['path']}")
    click.echo(f"Cleaned {len(removed)} stale entries.")


@cli.command()
@click.argument("query")
@click.option("--limit", "-n", default=20, help="Max results")
@click.option("--all", "search_all", is_flag=True, help="Search all registered crucibles")
@click.option("--mode", type=click.Choice(["auto", "fts", "semantic", "hybrid"]),
              default="auto", help="Search mode (default: auto)")
@click.option("--model", "-m", default="nomic-embed-text", help="Embedding model")
@click.option("--fts-weight", default=1.0, help="FTS weight for hybrid reranking")
@click.option("--semantic-weight", default=1.0, help="Semantic weight for hybrid reranking")
@click.option("--raw", is_flag=True, help="Skip dedup/quality filtering (return raw ranked results)")
@json_option
def search(query, limit, search_all, mode, model, fts_weight, semantic_weight, raw, json_output):
    """Search wiki articles.

    By default, returns deduplicated, quality-ranked results. Similar
    articles are clustered and the best representative is shown. Use
    --raw to disable this and see all raw hits.

    Modes:
      auto      Hybrid if embeddings exist, FTS otherwise (default)
      fts       Keyword search only (FTS5 with porter stemming)
      semantic  Vector similarity only (requires `crucible embed`)
      hybrid    Combines FTS + semantic with reciprocal rank fusion
    """
    root = get_root()
    db = get_db(root)

    # Determine effective mode
    effective_mode = mode
    if mode == "auto":
        idx = EmbeddingIndex(db.conn, model=model)
        idx.initialize()
        s = idx.stats()
        effective_mode = "hybrid" if s["embedded"] > 0 else "fts"

    # Resolve peers from global registry for --all
    if search_all:
        from crucible.registry import get_peers
        peers = get_peers(exclude_path=str(root.resolve()))
    else:
        peers = []

    if effective_mode == "hybrid":
        idx = EmbeddingIndex(db.conn, model=model)
        idx.initialize()
        if search_all and peers:
            results = idx.hybrid_search_all(
                query, peers=peers, limit=limit,
                fts_weight=fts_weight, semantic_weight=semantic_weight,
            )
        else:
            results = idx.hybrid_search(
                query, limit=limit,
                fts_weight=fts_weight, semantic_weight=semantic_weight,
                raw=raw,
            )
        db.close()
        if json_output:
            _emit_json(results)
            return
        if not results:
            click.echo("No results.")
            return
        for r in results:
            score = r.get("rrf_score", 0)
            methods = r.get("methods", [])
            method_str = " + ".join(m.split(":")[0] for m in methods)
            source = r.get("_crucible", "")
            prefix = f"[{source}] " if source and source not in ("local", "") else ""
            click.echo(f"  {prefix}[{r['article_type']}] {r['title']}  (score: {score:.4f}, via: {method_str})")
            click.echo(f"    {r['path']}")
            absorbed = r.get("dedup_absorbed")
            if absorbed:
                click.echo(f"    (absorbed: {', '.join(absorbed)})")

    elif effective_mode == "semantic":
        idx = EmbeddingIndex(db.conn, model=model)
        idx.initialize()
        s = idx.stats()
        if s["embedded"] == 0:
            raise click.ClickException(
                "No embeddings found. Run `crucible embed` first."
            )
        if search_all and peers:
            results = idx.search_all(query, peers=peers, limit=limit)
        else:
            results = idx.search(query, limit=limit)
        db.close()
        if json_output:
            _emit_json(results)
            return
        if not results:
            click.echo("No results.")
            return
        for r in results:
            sim = r.get("similarity", 0)
            source = r.get("_crucible", "")
            prefix = f"[{source}] " if source and source not in ("local", "") else ""
            click.echo(f"  {prefix}[{r['article_type']}] {r['title']}  (similarity: {sim:.3f})")
            click.echo(f"    {r['path']}")

    else:  # fts
        if search_all:
            results = db.search_all(query, limit=limit, peers=peers)
        else:
            results = db.search(query, limit=limit)

        db.close()
        if json_output:
            _emit_json(results)
            return
        if not results:
            click.echo("No results.")
            return
        for r in results:
            source = r.get("_crucible", "")
            prefix = f"[{source}] " if source and source not in ("local", "") else ""
            status = r.get("status", "")
            click.echo(f"  {prefix}[{r['article_type']}] {r['title']}  ({status})")
            click.echo(f"    {r['path']}")


def _resolve_article_arg(root: Path, db: CrucibleDB, article_path: str) -> str:
    """Return the database path of the article that ARTICLE_PATH names.

    Database paths are relative to the project root
    (``.crucible/wiki/concepts/foo.org``). The argument may also be relative
    to the current directory (``./.crucible/...``, or ``foo.org`` from inside
    the wiki), absolute, or relative to the wiki directory
    (``concepts/foo.org``). Raises a ClickException (exit 1) if no article
    has that path, so a typo is not mistaken for "no results".
    """
    if db.get_article_by_path(article_path):
        return article_path
    candidates = []
    given = Path(os.path.expanduser(article_path))
    absolute = given if given.is_absolute() else Path.cwd() / given
    for target in (Path(os.path.abspath(absolute)), absolute.resolve()):
        for base in (root, root.resolve()):
            rel = _safe_relative_to(target, base)
            if rel is not None:
                candidates.append(Path(rel).as_posix())
    if not given.is_absolute():
        candidates.append((Path(CRUCIBLE_DIR) / "wiki" / given).as_posix())
    for candidate in candidates:
        if db.get_article_by_path(candidate):
            return candidate
    raise click.ClickException(
        f"No article at {article_path} (paths are relative to the current "
        "directory, the project root, or the wiki; run `crucible sync` if "
        "the file is new)"
    )


@cli.command()
@click.argument("article_path")
@json_option
def history(article_path, json_output):
    """Show the derivation history of an article.

    Shows what articles this one was derived from (upstream)
    and what articles have been derived from it (downstream).
    Traces the evolution of knowledge over time.

    ARTICLE_PATH may be relative to the current directory, the project
    root or the wiki, or absolute. Exits 1 if no article has that path.
    """
    root = get_root()
    db = get_db(root)
    try:
        article_path = _resolve_article_arg(root, db, article_path)
    except click.ClickException:
        db.close()
        raise

    upstream = db.derived_from(article_path)
    downstream = db.derivatives(article_path)
    db.close()
    if json_output:
        _emit_json({"derived_from": upstream, "derivatives": downstream})
        return

    if not upstream and not downstream:
        click.echo("No derivation history for this article.")
        return

    if upstream:
        click.echo("Derived from:")
        for r in upstream:
            click.echo(f"  [{r['article_type']}] {r['title']}")
            click.echo(f"    {r['path']}  (at {r['derived_at']})")

    if downstream:
        click.echo("Derivatives:")
        for r in downstream:
            click.echo(f"  [{r['article_type']}] {r['title']}")
            click.echo(f"    {r['path']}  (at {r['derived_at']})")


@cli.command()
@click.argument("article_path")
@json_option
def backlinks(article_path, json_output):
    """Show articles that link to the given article.

    ARTICLE_PATH may be relative to the current directory, the project
    root or the wiki, or absolute. Exits 1 if no article has that path.
    """
    root = get_root()
    db = get_db(root)
    try:
        article_path = _resolve_article_arg(root, db, article_path)
    except click.ClickException:
        db.close()
        raise
    results = db.backlinks(article_path)
    db.close()
    if json_output:
        _emit_json(results)
        return
    if not results:
        click.echo("No backlinks found.")
        return
    for r in results:
        click.echo(f"  {r['title']} ({r['path']})")
        if r.get("context"):
            click.echo(f"    ...{r['context']}...")


@cli.command("source")
@click.argument("source_path")
@json_option
def source_info(source_path, json_output):
    """Show everything known about a source.

    Displays the source metadata and all articles distilled from it.
    Useful for resummarization: see what's already been captured before
    asking Claude to reread the source. Exits 1 if no source matches.
    """
    root = get_root()
    db = get_db(root)

    src = db.get_source_by_path(source_path)
    if not src:
        # Try matching by title or partial path
        for s in db.list_sources():
            if source_path in s["path"] or source_path in s["title"]:
                src = s
                break
    if not src:
        db.close()
        raise click.ClickException(f"Source not found: {source_path}")

    if json_output:
        articles = db.source_articles(src["id"])
        db.close()
        _emit_json({**src, "articles": articles})
        return

    click.echo(f"Source: {src['title']}")
    click.echo(f"  Path: {src['path']}")
    click.echo(f"  Type: {src['source_type']} ({'shareable' if src.get('shareable') else 'external'})")
    click.echo(f"  Date: {src.get('date', 'unknown')}")
    click.echo(f"  Ingested: {src['ingested_at']}")
    if src.get("url"):
        click.echo(f"  URL: {src['url']}")

    articles = db.source_articles(src["id"])
    if articles:
        click.echo(f"\nDistilled into {len(articles)} article(s):")
        for a in articles:
            click.echo(f"  [{a['article_type']}] {a['title']} ({a['status']})")
            click.echo(f"    {a['path']}")
    else:
        click.echo("\n  Not yet distilled.")

    db.close()


@cli.command()
@click.argument("article_path")
@json_option
def sources(article_path, json_output):
    """Show primary sources for an article.

    ARTICLE_PATH may be relative to the current directory, the project
    root or the wiki, or absolute. Exits 1 if no article has that path.
    """
    root = get_root()
    db = get_db(root)
    try:
        article_path = _resolve_article_arg(root, db, article_path)
    except click.ClickException:
        db.close()
        raise
    results = db.article_sources(article_path)
    db.close()
    if json_output:
        _emit_json(results)
        return
    if not results:
        click.echo("No sources linked.")
        return
    for r in results:
        click.echo(f"  [{r['source_type']}] {r['title']}")
        if r.get("url"):
            click.echo(f"    {r['url']}")


@cli.command()
@click.option("--all", "query_all", is_flag=True, help="Include all registered crucibles")
@json_option
def concepts(query_all, json_output):
    """List all concepts with article counts."""
    root = get_root()
    db = get_db(root)
    if query_all:
        from crucible.registry import get_peers
        peers = get_peers(exclude_path=str(root.resolve()))
        results = db.concepts_all(peers=peers)
    else:
        results = db.list_concepts()
    db.close()
    if json_output:
        _emit_json(results)
        return
    if not results:
        click.echo("No concepts yet.")
        return
    for r in results:
        source = r.get("_crucible", "")
        prefix = f"[{source}] " if source and source != "local" else ""
        click.echo(f"  {prefix}{r['name']} ({r['article_count']} articles)")


@cli.command()
@click.argument("name")
@json_option
def concept(name, json_output):
    """Show articles covering a concept.

    NAME is matched case-insensitively (sync lowercases FILETAGS).
    """
    name = name.lower()
    root = get_root()
    db = get_db(root)
    results = db.concept_articles(name)
    db.close()
    if json_output:
        _emit_json(results)
        return
    if not results:
        click.echo(f"No articles found for concept '{name}'.")
        return
    for r in results:
        click.echo(f"  [{r['article_type']}] {r['title']} ({r['path']})")


@cli.command()
@json_option
def orphans(json_output):
    """Show articles with no incoming links."""
    root = get_root()
    db = get_db(root)
    results = db.orphans()
    db.close()
    if json_output:
        _emit_json(results)
        return
    if not results:
        click.echo("No orphan articles.")
        return
    for r in results:
        click.echo(f"  [{r['article_type']}] {r['title']} ({r['path']})")


@cli.command()
@json_option
def undigested(json_output):
    """Show sources with no articles distilled from them."""
    root = get_root()
    db = get_db(root)
    results = db.undigested()
    db.close()
    if json_output:
        _emit_json(results)
        return
    if not results:
        click.echo("All sources have been distilled.")
        return
    for r in results:
        click.echo(f"  [{r['source_type']}] {r['title']} ({r['path']})")


@cli.command()
@click.argument("article_path")
@json_option
def related(article_path, json_output):
    """Show articles related through implied links.

    Computes relationships via shared concepts, shared sources,
    temporal proximity, and explicit links.

    ARTICLE_PATH may be relative to the current directory, the project
    root or the wiki, or absolute. Exits 1 if no article has that path.
    """
    root = get_root()
    db = get_db(root)
    try:
        article_path = _resolve_article_arg(root, db, article_path)
    except click.ClickException:
        db.close()
        raise
    results = db.related(article_path)
    db.close()
    if json_output:
        _emit_json(results)
        return
    if not results:
        click.echo("No related articles found.")
        return
    for r in results:
        reasons = ", ".join(r["reasons"])
        click.echo(f"  [{r['article_type']}] {r['title']}")
        click.echo(f"    {r['path']}")
        click.echo(f"    via: {reasons}")


@cli.command()
@click.option("--output", "-o", type=click.Path(path_type=Path), default=None,
              help="Output file (default: stdout)")
def graph(output):
    """Export the article link graph in DOT format."""
    root = get_root()
    db = get_db(root)
    dot = db.graph_dot()
    db.close()
    if output:
        output.write_text(dot)
        click.echo(f"Graph written to {output}")
    else:
        click.echo(dot)


def _write_viz(root: Path, db: CrucibleDB,
               output: Path | None = None) -> tuple[Path, int, int]:
    """Write the knowledge-graph HTML; return (path, node count, edge count)."""
    # Build graph data
    nodes = []
    node_ids = {}  # path -> index

    # Article nodes
    for a in db.list_articles():
        idx = len(nodes)
        node_ids[a["path"]] = idx
        nodes.append({
            "id": idx,
            "label": a["title"],
            "path": a["path"],
            "type": a["article_type"],
            "status": a.get("status", "draft"),
            "group": a["article_type"],
        })

    # Concept nodes
    for c in db.list_concepts():
        idx = len(nodes)
        node_ids[f"concept:{c['name']}"] = idx
        nodes.append({
            "id": idx,
            "label": c["name"],
            "type": "concept",
            "group": "concept",
        })

    edges = []

    # Article-to-article links
    for link in db.all_article_links():
        src = node_ids.get(link["from_path"])
        tgt = node_ids.get(link["to_path"])
        if src is not None and tgt is not None:
            edges.append({"source": src, "target": tgt, "type": "link"})

    # Article-to-concept links
    by_concept = db.articles_by_concept()
    for concept_name, articles in by_concept.items():
        concept_key = f"concept:{concept_name}"
        if concept_key not in node_ids:
            continue
        concept_idx = node_ids[concept_key]
        for a in articles:
            article_idx = node_ids.get(a["path"])
            if article_idx is not None:
                edges.append({"source": article_idx, "target": concept_idx,
                              "type": "concept"})

    # Derivation links
    for a in db.list_articles():
        for d in db.derivatives(a["path"]):
            src = node_ids.get(a["path"])
            tgt = node_ids.get(d["path"])
            if src is not None and tgt is not None:
                edges.append({"source": src, "target": tgt, "type": "derivation"})

    graph_json = json.dumps({"nodes": nodes, "edges": edges})
    root_path = json.dumps(str(root.resolve()))

    html = _VIZ_TEMPLATE.replace("/*GRAPH_DATA*/", graph_json)
    html = html.replace("/*ROOT_PATH*/", root_path)

    if output is None:
        output = cdir(root) / "wiki" / "viz.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    return output, len(nodes), len(edges)


@cli.command()
@click.option("--output", "-o", type=click.Path(path_type=Path), default=None,
              help="Output file (default: .crucible/wiki/viz.html)")
@click.option("--open", "open_browser", is_flag=True, help="Open in browser after generating")
def viz(output, open_browser):
    """Generate an interactive knowledge graph visualization.

    Creates a self-contained HTML file with a force-directed graph.
    Nodes are articles (colored by type) and concepts. Edges are
    explicit links, shared concepts, and derivations.
    """
    root = get_root()
    db = get_db(root)
    try:
        output, n_nodes, n_edges = _write_viz(root, db, output)
    finally:
        db.close()
    click.echo(f"Visualization: {output} ({n_nodes} nodes, {n_edges} edges)")

    if open_browser:
        import webbrowser
        webbrowser.open(f"file://{output.resolve()}")


_VIZ_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Crucible Knowledge Graph</title>
<style>
  body { margin: 0; background: #1a1a2e; font-family: system-ui, sans-serif; }
  svg { width: 100vw; height: 100vh; }
  .node text { font-size: 11px; pointer-events: none; }
  .link { stroke-opacity: 0.4; }
  .link.link-type-link { stroke: #888; }
  .link.link-type-concept { stroke: #444; stroke-dasharray: 3,3; }
  .link.link-type-derivation { stroke: #e94560; }
  #tooltip {
    position: absolute; padding: 8px 12px; background: #16213e;
    color: #eee; border-radius: 6px; font-size: 13px;
    pointer-events: none; display: none; max-width: 300px;
    border: 1px solid #0f3460;
  }
  #legend {
    position: absolute; top: 12px; left: 12px; background: #16213e;
    color: #eee; padding: 12px; border-radius: 6px; font-size: 12px;
    border: 1px solid #0f3460;
  }
  #legend div { margin: 4px 0; }
  #legend span { display: inline-block; width: 12px; height: 12px;
    border-radius: 50%; margin-right: 6px; vertical-align: middle; }
  #search {
    position: absolute; top: 12px; right: 12px;
    padding: 8px 12px; border-radius: 6px; border: 1px solid #0f3460;
    background: #16213e; color: #eee; font-size: 14px; width: 220px;
  }
  #search::placeholder { color: #666; }
</style>
</head>
<body>
<div id="tooltip"></div>
<div id="legend">
  <strong>Crucible Knowledge Graph</strong>
  <div><span style="background:#e94560"></span>Concept article</div>
  <div><span style="background:#0f3460"></span>Summary</div>
  <div><span style="background:#533483"></span>Comparison</div>
  <div><span style="background:#1a8a5c"></span>Method</div>
  <div><span style="background:#e9a045"></span>Topic tag</div>
  <div style="margin-top:8px; color:#888">Drag to rearrange. Scroll to zoom.</div>
</div>
<input id="search" type="text" placeholder="Search nodes...">
<svg></svg>
<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
const data = /*GRAPH_DATA*/;

const colors = {
  concept: "#e94560", summary: "#0f3460", comparison: "#533483",
  method: "#1a8a5c", "concept": "#e9a045"
};
function nodeColor(d) {
  if (d.type === "concept" && !d.path) return colors["concept"];
  return colors[d.group] || "#888";
}
function nodeRadius(d) {
  return d.type === "concept" && !d.path ? 6 : 10;
}

const svg = d3.select("svg");
const width = window.innerWidth, height = window.innerHeight;
const g = svg.append("g");

svg.call(d3.zoom().scaleExtent([0.1, 8]).on("zoom", (e) => {
  g.attr("transform", e.transform);
}));

const sim = d3.forceSimulation(data.nodes)
  .force("link", d3.forceLink(data.edges).id(d => d.id).distance(80))
  .force("charge", d3.forceManyBody().strength(-200))
  .force("center", d3.forceCenter(width / 2, height / 2))
  .force("collision", d3.forceCollide().radius(20));

const link = g.append("g").selectAll("line")
  .data(data.edges).join("line")
  .attr("class", d => "link link-type-" + d.type)
  .attr("stroke-width", d => d.type === "derivation" ? 2 : 1);

const node = g.append("g").selectAll("g")
  .data(data.nodes).join("g")
  .call(d3.drag()
    .on("start", (e, d) => { if (!e.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
    .on("drag", (e, d) => { d.fx = e.x; d.fy = e.y; })
    .on("end", (e, d) => { if (!e.active) sim.alphaTarget(0); d.fx = null; d.fy = null; }));

node.append("circle")
  .attr("r", nodeRadius)
  .attr("fill", nodeColor)
  .attr("stroke", "#fff")
  .attr("stroke-width", 1.5);

node.append("text")
  .text(d => d.label.length > 25 ? d.label.slice(0, 23) + "..." : d.label)
  .attr("dx", 14).attr("dy", 4)
  .attr("fill", "#ccc");

const tooltip = d3.select("#tooltip");
node.on("mouseover", (e, d) => {
  let html = "<strong>" + d.label + "</strong>";
  if (d.type) html += "<br>Type: " + d.type;
  if (d.status) html += "<br>Status: " + d.status;
  if (d.path) html += "<br>" + d.path;
  tooltip.html(html).style("display", "block");
}).on("mousemove", (e) => {
  tooltip.style("left", (e.pageX + 12) + "px").style("top", (e.pageY - 12) + "px");
}).on("mouseout", () => tooltip.style("display", "none"))
.on("click", (e, d) => {
  if (d.path) {
    const rootPath = /*ROOT_PATH*/;
    window.location.href = "vscode://file/" + rootPath + "/" + d.path;
  }
});
node.style("cursor", d => d.path ? "pointer" : "default");

sim.on("tick", () => {
  link.attr("x1", d => d.source.x).attr("y1", d => d.source.y)
      .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
  node.attr("transform", d => "translate(" + d.x + "," + d.y + ")");
});

d3.select("#search").on("input", function() {
  const q = this.value.toLowerCase();
  node.select("circle").attr("opacity", d =>
    !q || d.label.toLowerCase().includes(q) ? 1 : 0.15);
  node.select("text").attr("opacity", d =>
    !q || d.label.toLowerCase().includes(q) ? 1 : 0.1);
  link.attr("opacity", d => {
    if (!q) return 1;
    const sn = data.nodes[typeof d.source === "object" ? d.source.id : d.source];
    const tn = data.nodes[typeof d.target === "object" ? d.target.id : d.target];
    return (sn.label.toLowerCase().includes(q) || tn.label.toLowerCase().includes(q)) ? 1 : 0.05;
  });
});
</script>
</body>
</html>
"""


@cli.command()
@click.option("--port", "-p", default=8088, type=int, help="Port number (default: 8088)")
@click.option("--no-open", is_flag=True, help="Don't open browser automatically")
def browse(port, no_open):
    """Browse the wiki in your web browser.

    Starts a local HTTP server that renders org-mode articles as HTML
    with working inter-file links, search, and sidebar navigation.
    """
    import subprocess
    import webbrowser

    from crucible.browse import start_server

    root = get_root()
    db = get_db(root)
    wiki_dir = cdir(root) / "wiki"

    if not wiki_dir.exists():
        raise click.ClickException("No wiki directory found. Run 'crucible init' first.")

    # Ensure knowledge graph is up to date
    viz_path = wiki_dir / "viz.html"
    if not viz_path.exists():
        click.echo("Generating knowledge graph...")
        subprocess.run(["crucible", "viz"], cwd=str(root), capture_output=True)

    articles = db.list_articles()
    db_path = cdir(root) / "crucible.db"
    db.close()

    project_name = root.resolve().name

    # Detect GitHub remote URL for linking in the UI
    github_url = ""
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            remote = result.stdout.strip()
            # Normalize git@github.com:user/repo.git to https://github.com/user/repo
            if remote.startswith("git@github.com:"):
                remote = "https://github.com/" + remote[len("git@github.com:"):]
            if remote.endswith(".git"):
                remote = remote[:-4]
            if "github.com" in remote:
                github_url = remote
    except (OSError, subprocess.TimeoutExpired):
        pass

    server = start_server(wiki_dir, db_path, articles, port=port,
                          project_name=project_name,
                          github_url=github_url)
    url = f"http://localhost:{port}"
    click.echo(f"Serving {project_name} wiki at {url}  (Ctrl+C to stop)")

    if not no_open:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        click.echo("\nStopped.")
        server.server_close()


@cli.command()
@click.argument("file", required=False, default=None)
def vscode(file):
    """Open the crucible in VS Code.

    Opens the project root as the workspace, so the scimax extension's
    Crucible panel shows this crucible first, and opens the wiki's index
    file. Optionally open a specific FILE (relative wiki path like
    concepts/foo.org) instead.
    """
    import subprocess

    root = get_root()
    wiki_dir = cdir(root) / "wiki"

    if not wiki_dir.exists():
        raise click.ClickException("No wiki directory found. Run 'crucible init' first.")

    # Ensure knowledge graph is up to date
    viz_path = wiki_dir / "viz.html"
    if not viz_path.exists():
        click.echo("Generating knowledge graph...")
        db = get_db(root)
        try:
            _write_viz(root, db, viz_path)
        finally:
            db.close()

    # Open the project root in VS Code
    cmd = ["code", str(root)]

    # Also open a specific file (or index.org by default)
    if file:
        target = wiki_dir / file
    else:
        target = wiki_dir / "index.org"

    if target.exists():
        cmd.extend(["--goto", str(target)])

    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        raise click.ClickException(
            "'code' command not found. Install the VS Code shell command:\n"
            "  VS Code > Cmd+Shift+P > 'Shell Command: Install code command in PATH'"
        )


@cli.command()
@click.argument("path")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.option("--file", "delete_file", is_flag=True, help="Also delete the file on disk")
def delete(path, yes, delete_file):
    """Delete an article or source from the database.

    PATH can be a database path, partial path, or title fragment.
    Searches articles first, then sources.
    """
    root = get_root()
    db = get_db(root)

    # Try to find as article
    article = db.get_article_by_path(path)
    if not article:
        for a in db.list_articles():
            if path in a["path"] or path in a["title"]:
                article = a
                break

    if article:
        click.echo(f"Article: {article['title']}")
        click.echo(f"  Path: {article['path']}")
        click.echo(f"  Type: {article['article_type']}, Status: {article['status']}")
        if not yes:
            click.confirm("Delete this article?", abort=True)
        if delete_file:
            disk_path = root / article["path"]
            if disk_path.exists():
                disk_path.unlink()
                click.echo(f"  Deleted file: {disk_path}")
        db.delete_article(article["id"])
        click.echo("  Deleted from database.")
        db.close()
        return

    # Try to find as source
    src = db.get_source_by_path(path)
    if not src:
        for s in db.list_sources():
            if path in s["path"] or path in s["title"]:
                src = s
                break

    if src:
        articles = db.source_articles(src["id"])
        click.echo(f"Source: {src['title']}")
        click.echo(f"  Path: {src['path']}")
        click.echo(f"  Type: {src['source_type']}")
        if articles:
            click.echo(f"  Linked to {len(articles)} article(s):")
            for a in articles:
                click.echo(f"    {a['title']}")
        if not yes:
            click.confirm("Delete this source?", abort=True)
        disk_path = root / src["path"]
        if delete_file and disk_path.exists():
            disk_path.unlink()
            click.echo(f"  Deleted file: {disk_path}")
            # Its extracted-text sidecar too, which sync would otherwise
            # index as a new source. A .txt that is itself a source stays.
            sidecar = disk_path.with_suffix(".txt")
            sidecar_rel = _safe_relative_to(sidecar, root)
            if (sidecar != disk_path and sidecar.is_file()
                    and (sidecar_rel is None or db.get_source_by_path(sidecar_rel) is None)):
                sidecar.unlink()
                click.echo(f"  Deleted text sidecar: {sidecar}")
        # The bibliography is what sync rebuilds sources from, so the entry
        # must stop naming the file or the next sync restores the row.
        from crucible.ingest import drop_bib_file_fields
        for key in drop_bib_file_fields(root, src["path"]):
            click.echo(f"  Removed the file field from bib entry '{key}' (the entry is kept).")
        db.delete_source(src["id"])
        click.echo("  Deleted from database.")
        scanned = (f"{CRUCIBLE_DIR}/sources/notebooks/", f"{CRUCIBLE_DIR}/sources/data/")
        if disk_path.exists() and src["path"].startswith(scanned):
            click.echo(
                "  Note: the file is still on disk, so the next `crucible sync` adds it "
                "back. Use --file to delete it too."
            )
        db.close()
        return

    db.close()
    raise click.ClickException(f"Not found: {path}")


@cli.command()
@click.argument("source")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without making changes")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
def merge(source, dry_run, yes):
    """Merge another crucible into this one.

    SOURCE is a path to a crucible project or a name from the
    global registry. Copies all sources, wiki articles, and
    references into the current project, then syncs the database.

    Every source copy is planned and checked before anything is written:
    distinct sources keep distinct paths, extracted-text sidecars are never
    overwritten, and BibTeX ``file`` fields follow renamed sources. If any
    source cannot be merged safely, the merge stops and lists every problem.

    Use --dry-run to preview what would be merged.
    """
    root = get_root()

    # Resolve source crucible
    source_root = _resolve_merge_source(source)
    if source_root.resolve() == root.resolve():
        raise click.ClickException("Cannot merge a crucible into itself.")

    click.echo(f"Source: {source_root}")
    click.echo(f"Target: {root}")

    source_cdir = source_root / CRUCIBLE_DIR
    target_wiki = cdir(root) / "wiki"
    source_wiki = source_cdir / "wiki"

    for stale in sorted(cdir(root).glob(_MERGE_STAGING_PREFIX + "*")):
        click.echo(
            f"Warning: {stale} is left over from an interrupted merge; "
            "nothing in it was merged, and it can be deleted.",
            err=True,
        )

    source_db = CrucibleDB(source_cdir / "crucible.db")
    target_db = get_db(root)
    try:
        src_stats = source_db.stats()
        click.echo(
            f"  {src_stats['sources']} sources, {src_stats['articles']} articles, "
            f"{src_stats['concepts']} concepts"
        )

        # Plan before asking for confirmation, so a merge that cannot be done
        # safely is refused up front and --dry-run shows the real outcome.
        plan = _plan_source_merge(source_root, root, source_db, target_db)
        _plan_article_merge(plan, source_wiki, target_wiki)
        for warning in plan.warnings:
            click.echo(f"Warning: {warning}", err=True)

        if dry_run:
            click.echo("(dry run)")
        elif not yes:
            click.confirm("\nProceed with merge?", abort=True)
        click.echo()

        if dry_run:
            for copy in plan.sources:
                click.echo(f"  source: {copy.src['title']} -> {copy.new_rel}")
            for _, dest_text in plan.sidecars:
                click.echo(f"  sidecar: {os.path.relpath(dest_text, root)}")
            for org_path, dest_path in plan.articles:
                rel_within_wiki = str(org_path.relative_to(source_wiki))
                if rel_within_wiki in plan.wiki_renames:
                    label = plan.wiki_renames[rel_within_wiki]
                    click.echo(f"  article: {rel_within_wiki} -> {label} (renamed)")
                else:
                    click.echo(f"  article: {rel_within_wiki}")
            for cite_key, _ in plan.bib_entries:
                click.echo(f"  bib: {cite_key}")
            for cite_key, path in plan.bib_file_fields.items():
                click.echo(f"  bib: {cite_key} (existing entry gets file = {path})")
        else:
            rewrites = _apply_merge(root, plan, target_db)
            if rewrites:
                click.echo(f"  Rewrote links in {rewrites} file(s)")
    finally:
        source_db.close()
        target_db.close()

    # Sync. Everything above is already in place, so a failure here leaves
    # a complete merge whose database the next sync brings up to date.
    if not dry_run and (plan.articles or plan.sources or plan.bib_entries):
        click.echo()
        ctx = click.get_current_context()
        try:
            ctx.invoke(sync)
        except Exception as exc:
            reason = exc.format_message() if isinstance(exc, click.ClickException) else exc
            raise click.ClickException(
                f"Merged all files, but the sync afterwards failed: {reason}\n"
                "Run `crucible sync` to bring the database up to date."
            ) from exc

    # --- Summary ---
    click.echo()
    verb = "Would merge" if dry_run else "Merged"
    click.echo(
        f"{verb}: {len(plan.sources)} sources, {len(plan.articles)} articles, "
        f"{len(plan.bib_entries)} bib entries"
    )
    if plan.bib_file_fields:
        action = "Would attach" if dry_run else "Attached"
        click.echo(
            f"  {action} {len(plan.bib_file_fields)} source file(s) to existing "
            "bibliography entries"
        )
    if plan.skipped:
        click.echo(f"  Skipped {plan.skipped} existing sources")
    if plan.outside:
        click.echo(
            f"  Skipped {plan.outside} source(s) stored outside {CRUCIBLE_DIR}/"
        )
    if plan.sidecars:
        action = "Would copy" if dry_run else "Copied"
        click.echo(f"  {action} {len(plan.sidecars)} missing text sidecar(s)")
    if plan.wiki_renames:
        click.echo(
            f"  Renamed {len(plan.wiki_renames)} article(s) to avoid conflicts"
        )


# Merge copies files into a directory with this prefix inside .crucible/
# before moving them into place, so a failed merge leaves nothing behind.
_MERGE_STAGING_PREFIX = ".merge-staging-"


def _plan_article_merge(plan: "_MergePlan", source_wiki: Path, target_wiki: Path) -> None:
    """Add the incoming wiki articles to plan, renaming any that collide."""
    if not source_wiki.exists():
        return
    planned: set[Path] = set()
    # The same walk sync uses: only the top-level index.org is generated.
    for org_path in _wiki_article_files(source_wiki):
        rel_within_wiki = org_path.relative_to(source_wiki)
        dest_path = target_wiki / rel_within_wiki
        if dest_path.exists() or dest_path.is_symlink() or dest_path in planned:
            stem, suffix = dest_path.stem, dest_path.suffix
            counter = 1
            while dest_path.exists() or dest_path.is_symlink() or dest_path in planned:
                dest_path = dest_path.parent / f"{stem}_{counter}{suffix}"
                counter += 1
            plan.wiki_renames[str(rel_within_wiki)] = str(
                dest_path.relative_to(target_wiki)
            )
        planned.add(dest_path)
        plan.articles.append((org_path, dest_path))


def _apply_merge(root: Path, plan: "_MergePlan", target_db: CrucibleDB) -> int:
    """Write a merge plan into the target, all or nothing.

    Every file is first copied into a staging directory inside .crucible/
    (so the moves below are renames on one filesystem) and article links
    are rewritten there. Only then are the files moved into place, the
    bibliography rewritten in one atomic write, and the source rows
    inserted in one transaction. If any step fails or is interrupted, what
    was already done is undone and the target is left as it was.

    Returns the number of articles whose links were rewritten.
    """
    import tempfile

    from filelock import FileLock

    from crucible.ingest import atomic_write_text

    target_cdir = cdir(root)
    target_wiki = target_cdir / "wiki"
    bib_path = target_cdir / "references.bib"
    staging = Path(tempfile.mkdtemp(prefix=_MERGE_STAGING_PREFIX, dir=target_cdir))

    copies: list[tuple[Path, Path]] = []
    for copy in plan.sources:
        copies.append((copy.src_file, copy.dest_file))
        if copy.src_text is not None:
            copies.append((copy.src_text, copy.dest_file.with_suffix(".txt")))
    copies.extend(plan.sidecars)
    copies.extend(plan.articles)

    placed: list[tuple[Path, Path]] = []   # (staged, dest) moved into place
    made_dirs: list[Path] = []             # directories created for them
    old_bib: str | None = None
    bib_written = False
    try:
        # 1. Stage every file.
        staged_files = []
        for i, (src, dest) in enumerate(copies):
            staged = staging / f"{i}{dest.suffix}"
            shutil.copy2(src, staged)
            staged_files.append((staged, dest))
        article_files = staged_files[len(copies) - len(plan.articles):]
        rewrites = (
            _rewrite_article_links(article_files, target_wiki, plan.wiki_renames)
            if plan.wiki_renames else 0
        )

        with FileLock(bib_path.with_suffix(".bib.lock"), timeout=10):
            # 2. Move the staged files into place.
            for staged, dest in staged_files:
                missing = []
                parent = dest.parent
                while not parent.exists():
                    missing.append(parent)
                    parent = parent.parent
                for directory in reversed(missing):
                    directory.mkdir()
                    made_dirs.append(directory)
                if dest.exists() or dest.is_symlink():
                    raise click.ClickException(
                        f"{os.path.relpath(dest, root)} appeared while merging."
                    )
                os.rename(staged, dest)
                placed.append((staged, dest))

            # 3. The bibliography, in one atomic write.
            if plan.bib_entries or plan.bib_file_fields or plan.bib_macros:
                old_bib = bib_path.read_text(encoding="utf-8") if bib_path.exists() else None
                new_bib = _merged_bib_text(old_bib or "", plan)
                if new_bib != (old_bib or ""):
                    atomic_write_text(bib_path, new_bib)
                    bib_written = True

            # 4. The source rows, in one transaction.
            with target_db.conn.transaction():
                for copy in plan.sources:
                    authors = copy.src.get("authors") or "[]"
                    if isinstance(authors, str):
                        authors = json.loads(authors)
                    target_db.add_source(
                        path=copy.new_rel, title=copy.src["title"],
                        source_type=copy.src["source_type"],
                        shareable=bool(copy.src["shareable"]),
                        url=copy.src.get("url"), authors=authors,
                        date=copy.src.get("date"), metadata=copy.metadata,
                    )
    except BaseException as exc:
        leftovers = _undo_merge(root, bib_path, old_bib, bib_written, placed, made_dirs)
        if not isinstance(exc, Exception):
            raise   # Ctrl-C: undone, re-raised for click to report.
        reason = exc.format_message() if isinstance(exc, click.ClickException) else exc
        if leftovers:
            listed = "\n".join(f"  - {item}" for item in leftovers)
            raise click.ClickException(
                f"Merge failed: {reason}\nUndoing it also failed, so these "
                f"changes to the target remain:\n{listed}"
            ) from exc
        raise click.ClickException(
            f"Merge failed: {reason}\nNothing was merged; the target is unchanged."
        ) from exc
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return rewrites


def _undo_merge(
    root: Path, bib_path: Path, old_bib: str | None, bib_written: bool,
    placed: list[tuple[Path, Path]], made_dirs: list[Path],
) -> list[str]:
    """Undo the writes _apply_merge made; return what could not be undone."""
    from crucible.ingest import atomic_write_text

    leftovers = []
    if bib_written:
        try:
            if old_bib is None:
                bib_path.unlink()
            else:
                atomic_write_text(bib_path, old_bib)
        except OSError as exc:
            leftovers.append(f"{os.path.relpath(bib_path, root)} (could not restore: {exc})")
    for staged, dest in reversed(placed):
        try:
            os.rename(dest, staged)
        except OSError as exc:
            leftovers.append(f"{os.path.relpath(dest, root)} (could not remove: {exc})")
    for directory in reversed(made_dirs):
        try:
            directory.rmdir()
        except OSError:
            pass   # an empty directory left behind changes nothing
    return leftovers


def _merged_bib_text(text: str, plan: "_MergePlan") -> str:
    """The target bibliography with the plan's file fields and new entries."""
    from crucible import bibtex
    from crucible.ingest import bib_has_key, set_bib_field

    attached: list[str] = []

    def attach(entry: "bibtex.Entry") -> str:
        path = plan.bib_file_fields.get(entry.key)
        if path is None:
            return entry.text
        if entry.get("file") or entry.key in attached:
            raise click.ClickException(
                f"the bibliography entry '{entry.key}' changed while merging."
            )
        attached.append(entry.key)
        return set_bib_field(entry.text, "file", path)

    text = bibtex.map_entries(text, attach)
    missing = set(plan.bib_file_fields) - set(attached)
    if missing:
        raise click.ClickException(
            f"the bibliography entry '{sorted(missing)[0]}' changed while merging."
        )
    # Macros first: BibTeX needs a @string defined before an entry uses it.
    # A name the target already defines keeps the target's definition.
    existing = bibtex.macro_blocks(text)
    defined = {name for kind, name, _ in existing if kind == "string"}
    preambles = {" ".join(block.split()) for kind, _, block in existing if kind == "preamble"}
    additions: list[str] = []
    for kind, name, block in plan.bib_macros:
        if kind == "string":
            if name is None or name in defined:
                continue
            defined.add(name)
        else:
            norm = " ".join(block.split())
            if norm in preambles:
                continue
            preambles.add(norm)
        additions.append(block)
    for cite_key, entry_text in plan.bib_entries:
        if bib_has_key(text, cite_key):
            continue
        additions.append(entry_text)
    for block in additions:
        text = (text.rstrip() + "\n\n" + block + "\n") if text.strip() else block + "\n"
    return text


@dataclass
class _SourceCopy:
    """One incoming source file to copy into the target crucible."""

    src: dict             # source row from the incoming database
    src_file: Path
    dest_file: Path
    new_rel: str          # destination path relative to the target root
    metadata: dict
    src_text: Path | None  # extracted-text sidecar copied alongside, if any


@dataclass
class _MergePlan:
    """Everything a merge will write."""

    sources: list[_SourceCopy] = dataclass_field(default_factory=list)
    # Sidecars missing beside sources the target already has.
    sidecars: list[tuple[Path, Path]] = dataclass_field(default_factory=list)
    bib_entries: list[tuple[str, str]] = dataclass_field(default_factory=list)
    # Incoming @string / @preamble blocks (kind, name, text) the entries may
    # rely on; added unless the target already defines them.
    bib_macros: list[tuple[str, str | None, str]] = dataclass_field(default_factory=list)
    # Target entries with no `file` field that get one naming an incoming
    # source copied for them (cite key -> path relative to the target root).
    bib_file_fields: dict[str, str] = dataclass_field(default_factory=dict)
    # Incoming wiki articles (source path, destination path), and the
    # wiki-relative renames of those that collide with existing articles.
    articles: list[tuple[Path, Path]] = dataclass_field(default_factory=list)
    wiki_renames: dict[str, str] = dataclass_field(default_factory=dict)
    skipped: int = 0   # identical sources the target already has
    outside: int = 0   # indexed sources stored outside .crucible/, not copied
    warnings: list[str] = dataclass_field(default_factory=list)


class _MergeConflict(Exception):
    """A single reason an incoming source cannot be merged safely."""


def _plan_source_merge(
    source_root: Path, root: Path, source_db: CrucibleDB, target_db: CrucibleDB
) -> _MergePlan:
    """Plan the source and bibliography part of a merge without writing.

    Sources are identified by citation key and content, not by path: a key
    already in the target is reused only when the bytes match, and every
    other source gets a destination (with its ``.txt`` sidecar) that collides
    with nothing existing or already planned. Bibliography ``file`` fields
    are rewritten to the planned paths.

    Raises a ClickException listing every problem found, so the user can fix
    them all at once. Nothing has been written when it is raised.
    """
    from crucible import bibtex
    from crucible.ingest import _parse_bib_fields, _resolve_bib_file, file_sha256

    source_cdir = source_root / CRUCIBLE_DIR
    target_cdir = cdir(root)
    source_bib = source_cdir / "references.bib"
    target_bib = target_cdir / "references.bib"

    plan = _MergePlan()
    problems: list[str] = []

    try:
        source_entries = (
            _parse_bib_entries(source_bib.read_text(encoding="utf-8"))
            if source_bib.exists() else {}
        )
    except _MergeConflict as exc:
        _raise_merge_problems([str(exc)])
    if source_bib.exists():
        plan.bib_macros = bibtex.macro_blocks(source_bib.read_text(encoding="utf-8"))

    target_sources = target_db.list_sources()
    # Real paths already taken in the target, plus every path planned below.
    reserved = {os.path.realpath(root / s["path"]) for s in target_sources}
    target_by_key: dict[str, list[dict]] = {}
    for existing in target_sources:
        try:
            _, key = _merge_source_metadata(existing)
        except _MergeConflict as exc:
            problems.append(f"target {exc}")
            continue
        if key:
            target_by_key.setdefault(key, []).append(existing)

    # Where each incoming file lands, keyed by its literal and its real path,
    # so bibliography `file` fields can be rewritten however they spell it.
    planned_by_path: dict[str, list[tuple[str, str | None]]] = {}
    planned_by_realpath: dict[str, list[tuple[str, str | None]]] = {}

    def remember(path: Path, new_rel: str, key: str | None) -> None:
        planned_by_path.setdefault(os.path.abspath(path), []).append((new_rel, key))
        planned_by_realpath.setdefault(os.path.realpath(path), []).append((new_rel, key))

    def taken(path: Path) -> bool:
        return (
            path.exists() or path.is_symlink()
            or os.path.realpath(path) in reserved
        )

    incoming_keys: set[str] = set()
    mapped_keys: set[str] = set()   # keys whose source file is accounted for
    copied_keys: dict[str, str] = {}   # key -> new path, for sources copied as new

    for src in source_db.list_sources():
        try:
            metadata, cite_key = _merge_source_metadata(src)
            if cite_key:
                if cite_key in incoming_keys:
                    raise _MergeConflict(
                        f"Cannot safely merge source with citation key '{cite_key}': "
                        "multiple incoming sources use it. Give each its own key in the "
                        "source crucible (`crucible lint` there reports it as "
                        "duplicate_cite_key), sync it, and merge again."
                    )
                incoming_keys.add(cite_key)

            try:
                rel_within = Path(src["path"]).relative_to(CRUCIBLE_DIR)
            except ValueError:
                plan.outside += 1
                continue

            src_file = source_root / src["path"]
            if not src_file.is_file():
                entry = source_entries.get(cite_key) if cite_key else None
                if (
                    entry is not None
                    and src["path"] == f"{CRUCIBLE_DIR}/bib/{cite_key}"
                    and not _parse_bib_fields(entry).get("file")
                ):
                    # Bibliography-only source: its entry is merged below.
                    continue
                raise _MergeConflict(
                    f"Cannot safely merge '{src['path']}': the source file is "
                    "missing or not a regular file."
                )

            src_text = src_file.with_suffix(".txt")
            if src_text == src_file or not src_text.exists():
                src_text = None
            private = not bool(src["shareable"])

            existing = target_by_key.get(cite_key, []) if cite_key else []
            if existing:
                existing_file = root / existing[0]["path"]
                if (
                    len(existing) != 1
                    or not existing_file.is_file()
                    or file_sha256(src_file) != file_sha256(existing_file)
                ):
                    raise _MergeConflict(
                        f"Cannot safely merge source with citation key '{cite_key}': "
                        "the existing source is ambiguous, differs, or is missing."
                    )
                # Same key, same bytes: reuse the target's copy.
                new_rel = existing[0]["path"]
                if src_text is not None:
                    dest_text = (root / new_rel).with_suffix(".txt")
                    _check_merge_copy(src_text, dest_text, source_root, root,
                                      private=private)
                    if dest_text.is_symlink():
                        raise _MergeConflict(
                            f"Cannot safely merge text sidecar for '{cite_key}': "
                            "the destination is a symlink."
                        )
                    if dest_text.exists():
                        if (
                            not dest_text.is_file()
                            or file_sha256(src_text) != file_sha256(dest_text)
                        ):
                            raise _MergeConflict(
                                f"Cannot safely merge text sidecar for '{cite_key}': "
                                "the existing sidecar differs."
                            )
                    elif os.path.realpath(dest_text) in reserved:
                        raise _MergeConflict(
                            f"Cannot safely merge text sidecar for '{cite_key}': "
                            "the destination is reserved."
                        )
                    else:
                        reserved.add(os.path.realpath(dest_text))
                        plan.sidecars.append((src_text, dest_text))
                plan.skipped += 1
            else:
                # Reserve the source and its sidecar together.
                dest_file = target_cdir / rel_within
                stem, suffix = dest_file.stem, dest_file.suffix
                counter = 1
                while taken(dest_file) or (
                    src_text is not None and taken(dest_file.with_suffix(".txt"))
                ):
                    dest_file = dest_file.parent / f"{stem}_{counter}{suffix}"
                    counter += 1
                _check_merge_copy(src_file, dest_file, source_root, root,
                                  private=private)
                if src_text is not None:
                    _check_merge_copy(src_text, dest_file.with_suffix(".txt"),
                                      source_root, root, private=private)
                    reserved.add(os.path.realpath(dest_file.with_suffix(".txt")))
                reserved.add(os.path.realpath(dest_file))
                new_rel = str(dest_file.relative_to(root))
                plan.sources.append(_SourceCopy(
                    src, src_file, dest_file, new_rel, metadata, src_text,
                ))
                if cite_key:
                    copied_keys[cite_key] = new_rel
        except _MergeConflict as exc:
            problems.append(str(exc))
            continue

        if cite_key:
            mapped_keys.add(cite_key)
        remember(src_file, new_rel, cite_key)
        if src_text is not None:
            remember(src_text, str(Path(new_rel).with_suffix(".txt")), cite_key)

    # Bibliography entries, with `file` fields pointing at the planned paths.
    target_text = target_bib.read_text(encoding="utf-8") if target_bib.exists() else ""
    target_entries: dict[str, list[str]] = {}
    for e in bibtex.entries(target_text):
        target_entries.setdefault(e.key, []).append(e.text)
    source_sources = (source_cdir / "sources").resolve()
    for cite_key, entry_text in source_entries.items():
        if not _parse_bib_fields(entry_text).get("file"):
            if cite_key in mapped_keys:
                plan.warnings.append(
                    f"bibliography entry '{cite_key}' has no durable file mapping."
                )
            else:
                plan.warnings.append(
                    f"bibliography-only source '{cite_key}' has no source file."
                )
        if cite_key in target_entries:
            if cite_key in copied_keys:
                # The target has the reference but no source for it. Attach
                # the incoming file to the target's entry when it is plainly
                # the same reference; otherwise the target's entry would win
                # on the next rebuild and point this key elsewhere.
                problem = _bib_attach_problem(
                    cite_key, target_entries.get(cite_key, []), entry_text
                )
                if problem:
                    problems.append(problem)
                else:
                    plan.bib_file_fields[cite_key] = copied_keys[cite_key]
            continue

        try:
            parsed = bibtex.first_entry(entry_text)
            file_fields = [f for f in parsed.fields if f.name == "file"] if parsed else []
            for fld in reversed(file_fields):
                file_path = _resolve_bib_file(source_root, fld.value.strip())
                direct = planned_by_path.get(os.path.abspath(file_path), [])
                resolved = planned_by_realpath.get(os.path.realpath(file_path), [])
                # Prefer a destination planned for this same citation key.
                candidates = (
                    [m for m in direct if m[1] == cite_key]
                    or [m for m in resolved if m[1] == cite_key]
                    or direct or resolved
                )
                if len(candidates) > 1:
                    raise _MergeConflict(
                        f"Cannot safely map bibliography entry '{cite_key}' "
                        "to a source: several sources match its file field."
                    )
                if candidates:
                    start = parsed.start + fld.value_start
                    end = parsed.start + fld.value_end
                    entry_text = entry_text[:start] + candidates[0][0] + entry_text[end:]
                elif (
                    Path(os.path.abspath(file_path)).is_relative_to(source_cdir / "sources")
                    or Path(os.path.realpath(file_path)).is_relative_to(source_sources)
                ):
                    raise _MergeConflict(
                        f"Cannot safely map bibliography entry '{cite_key}' to an "
                        "indexed source. Sync or correct its file path."
                    )
        except _MergeConflict as exc:
            problems.append(str(exc))
            continue
        plan.bib_entries.append((cite_key, entry_text))

    if problems:
        _raise_merge_problems(problems)
    return plan


def _bib_attach_problem(
    cite_key: str, target_entries: list[str], incoming_entry: str
) -> str | None:
    """Why an incoming source cannot be attached to the target's entry, if it cannot.

    The target's entry must be the only one with the key, must not already
    name a file, and must not disagree with the incoming entry on DOI or
    title, which would mean the two keys name different references.
    """
    from crucible.ingest import _parse_bib_fields

    prefix = (
        f"Cannot safely merge source with citation key '{cite_key}': "
        "the target bibliography already has an entry with this key"
    )
    if len(target_entries) != 1:
        return (
            f"{prefix}, but it cannot be read or is not the only one. Fix the "
            "target's references.bib, or rename one of the keys."
        )
    target_fields = _parse_bib_fields(target_entries[0])
    if target_fields.get("file"):
        return (
            f"{prefix}, and it names the file '{target_fields['file']}', but no "
            "indexed source in the target matches it. Run `crucible sync` in the "
            "target (correcting the file field if the file has moved), or rename "
            "one of the keys."
        )
    incoming_fields = _parse_bib_fields(incoming_entry)
    identifying = [
        ("doi", "DOIs", lambda v: re.sub(
            r"^(?:https?://(?:dx\.)?doi\.org/|doi:)", "", v.strip().lower())),
        # Braces only protect case: {E}vidence is Evidence.
        ("title", "titles", lambda v: re.sub(
            r"[\W_]+", " ", v.replace("{", "").replace("}", "").lower()).strip()),
    ]
    for name, label, norm in identifying:
        ours, theirs = target_fields.get(name), incoming_fields.get(name)
        if ours and theirs and norm(ours) != norm(theirs):
            return (
                f"{prefix}, and the two entries have different {label}, so they "
                "may not be the same reference. Rename one of the keys."
            )
    return None


def _raise_merge_problems(problems: list[str]) -> NoReturn:
    if len(problems) == 1:
        raise click.ClickException(f"{problems[0]} No files were copied.")
    listed = "\n".join(f"  - {p}" for p in problems)
    raise click.ClickException(
        f"Cannot safely merge ({len(problems)} problems); no files were copied:\n{listed}"
    )


def _merge_source_metadata(source: dict) -> tuple[dict, str | None]:
    """Return a source row's metadata dict and citation key, or raise _MergeConflict."""
    raw = source.get("metadata") or "{}"
    try:
        metadata = json.loads(raw) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        metadata = None
    if not isinstance(metadata, dict):
        raise _MergeConflict(
            f"Cannot safely merge source '{source['path']}': invalid metadata."
        )
    cite_key = metadata.get("cite_key")
    if cite_key is not None and not isinstance(cite_key, str):
        raise _MergeConflict(
            f"Cannot safely merge source '{source['path']}': invalid metadata "
            "citation key."
        )
    return metadata, cite_key


def _check_merge_copy(
    source: Path, destination: Path, source_root: Path, target_root: Path, *, private: bool
) -> None:
    """Refuse copies that leave either crucible or publish private files."""
    if not source.is_file():
        raise _MergeConflict(f"Cannot safely merge '{source}': not a regular file.")
    source_root = source_root.resolve()
    target_root = target_root.resolve()
    source_path = source.resolve()
    destination_path = destination.resolve()
    if not source_path.is_relative_to(source_root / CRUCIBLE_DIR):
        raise _MergeConflict(
            f"Cannot safely merge '{source}': it resolves outside the source crucible."
        )
    if not destination_path.is_relative_to(target_root / CRUCIBLE_DIR):
        raise _MergeConflict(
            f"Cannot safely merge '{destination}': it resolves outside the target crucible."
        )
    source_private = source_root / CRUCIBLE_DIR / "sources/external"
    target_private = target_root / CRUCIBLE_DIR / "sources/external"
    if (
        (private or source_path.is_relative_to(source_private)
         or Path(os.path.abspath(source)).is_relative_to(source_private))
        and not destination_path.is_relative_to(target_private)
    ):
        raise _MergeConflict(
            f"Cannot safely merge '{source}': a private file would be copied to "
            "a shareable path. In the source crucible, move it under "
            "sources/external/, or mark the source shareable if it may be shared."
        )


def _resolve_merge_source(source: str) -> Path:
    """Resolve a merge source by path or registry name."""
    from crucible.registry import list_instances

    path = Path(source).resolve()
    if (path / CRUCIBLE_DIR / "crucible.db").exists():
        return path

    for inst in list_instances():
        if inst["name"] == source:
            inst_path = Path(inst["path"])
            if (inst_path / CRUCIBLE_DIR / "crucible.db").exists():
                return inst_path
            raise click.ClickException(
                f"Registry entry '{source}' points to {inst_path} "
                f"but no database found there."
            )

    raise click.ClickException(
        f"Cannot find crucible: {source}\n"
        f"Provide a path or a name from `crucible registry list`."
    )


def _rewrite_article_links(
    files: list[tuple[Path, Path]], target_wiki: Path, wiki_renames: dict[str, str]
) -> int:
    """Rewrite [[file:...]] links in copied articles for renamed files.

    Each item is (file to edit, the path that file will have in the target
    wiki); links are resolved relative to the latter. Returns the number of
    files modified.
    """
    target_wiki_resolved = target_wiki.resolve()
    count = 0

    for org_file, final_path in files:
        content = org_file.read_text(encoding="utf-8")

        def repl(match: re.Match) -> str:
            # An org search option (file:foo.org::*Heading) is not part of
            # the path: resolve without it and keep it on the new link.
            link_path, sep, search = match.group(1).partition("::")
            resolved = (final_path.parent / link_path).resolve()
            try:
                wiki_rel = str(resolved.relative_to(target_wiki_resolved))
            except ValueError:
                return match.group(0)
            if wiki_rel not in wiki_renames:
                return match.group(0)
            new_target = target_wiki / wiki_renames[wiki_rel]
            new_link = os.path.relpath(new_target, final_path.parent)
            return f"[[file:{new_link}{sep}{search}]"

        # One pass, so a link already rewritten is not rewritten again.
        new_content = re.sub(r"\[\[file:(.*?)\]", repl, content)
        if new_content != content:
            org_file.write_text(new_content, encoding="utf-8")
            count += 1

    return count


def _parse_bib_entries(text: str) -> dict[str, str]:
    """Parse BibTeX text into a dict of cite_key -> full entry text."""
    from crucible import bibtex

    entries = {}
    for entry in bibtex.entries(text):
        if entry.key in entries:
            raise _MergeConflict(
                f"Cannot safely merge: duplicate bibliography key '{entry.key}'. "
                "Give each entry its own key (`crucible lint` reports it as "
                "duplicate_bib_key) and merge again."
            )
        entries[entry.key] = entry.text
    return entries


@cli.command()
@json_option
def stats(json_output):
    """Show database statistics."""
    root = get_root()
    db = get_db(root)
    s = db.stats()
    db.close()
    if json_output:
        _emit_json(s)
        return
    click.echo(f"Sources:  {s['sources']}")
    click.echo(f"Articles: {s['articles']}")
    click.echo(f"Concepts: {s['concepts']}")
    click.echo(f"Links:    {s['article_links']}")


def _render_manifest(root: Path, db: CrucibleDB) -> str:
    """Return the MANIFEST.md text for the current DB state."""
    from datetime import datetime

    s = db.stats()
    concept_list = db.list_concepts()
    source_types: dict[str, int] = {}
    for src in db.list_sources():
        t = src["source_type"]
        source_types[t] = source_types.get(t, 0) + 1
    article_types: dict[str, int] = {}
    for art in db.list_articles():
        t = art["article_type"]
        article_types[t] = article_types.get(t, 0) + 1

    concepts_str = ", ".join(c["name"] for c in concept_list[:30])
    if len(concept_list) > 30:
        concepts_str += f", ... ({len(concept_list) - 30} more)"

    source_breakdown = ", ".join(f"{v} {k}" for k, v in sorted(source_types.items()))
    article_breakdown = ", ".join(f"{v} {k}" for k, v in sorted(article_types.items()))

    lines = [
        f"# Crucible Knowledge Base",
        f"",
        f"Location: {root.resolve()}",
        f"Updated: {datetime.now().strftime('%Y-%m-%d')}",
        f"",
        f"## Contents",
        f"",
        f"- {s['articles']} articles ({article_breakdown})",
        f"- {s['concepts']} concepts",
        f"- {s['sources']} sources ({source_breakdown})",
        f"- {s['article_links']} cross-links",
        f"",
        f"## Topics",
        f"",
        f"{concepts_str}",
        f"",
        f"## How to Query",
        f"",
        f"```bash",
        f"cd {root.resolve()}",
        f"crucible search \"your query\"     # full-text search",
        f"crucible concepts                 # browse all topics",
        f"crucible concept <name>           # articles on a topic",
        f"crucible help all                 # full CLI reference",
        f"```",
    ]
    return "\n".join(lines) + "\n"


def _write_manifest(root: Path, db: CrucibleDB) -> Path:
    """Write wiki/MANIFEST.md from the current DB state."""
    text = _render_manifest(root, db)
    manifest_path = cdir(root) / "wiki" / "MANIFEST.md"
    manifest_path.write_text(text, encoding="utf-8")
    return manifest_path


@cli.command()
def manifest():
    """Generate a manifest summarizing the knowledge base.

    Outputs a concise summary that agents can read to decide whether
    this knowledge base is relevant to their query. Also writes
    the manifest to wiki/MANIFEST.md for reference from CLAUDE.md.
    """
    root = get_root()
    db = get_db(root)
    text = _render_manifest(root, db)
    db.close()
    manifest_path = cdir(root) / "wiki" / "MANIFEST.md"
    manifest_path.write_text(text, encoding="utf-8")
    click.echo(text)
    click.echo(f"Written to {manifest_path}")


def _render_index(root: Path, db: CrucibleDB) -> str:
    """Return wiki/index.org text for the current DB state."""
    from datetime import datetime

    s = db.stats()
    by_type = db.articles_by_type()
    by_concept = db.articles_by_concept()
    source_list = db.list_sources()

    type_labels = {
        "concept": "Concepts",
        "summary": "Source Summaries",
        "comparison": "Comparisons",
        "method": "Methods",
    }

    wiki_rel = Path(CRUCIBLE_DIR) / "wiki"

    def _link_path(stored_path: str) -> str:
        """Convert a db-stored path (relative to project root) into a
        path relative to wiki/index.org so org file: links resolve."""
        p = Path(stored_path)
        try:
            return p.relative_to(wiki_rel).as_posix()
        except ValueError:
            return p.as_posix()

    def _index_link(article: dict) -> str:
        """An org link to an article. Brackets in the title would end the
        link description early, so they become parentheses."""
        title = (article["title"] or "").translate(str.maketrans("[]", "()"))
        return f"[[file:{_link_path(article['path'])}][{title}]]"

    lines = [
        f"#+TITLE: Crucible Wiki Index",
        f"#+DATE: [{datetime.now().strftime('%Y-%m-%d %a')}]",
        f"#+STARTUP: overview",
        "",
        f"Auto-generated index of the Crucible knowledge base.",
        f"{s['articles']} articles, {s['concepts']} concepts, "
        f"{s['sources']} sources, {s['article_links']} links.",
        "",
    ]

    # Articles by type
    lines.append("* Articles by Type")
    lines.append("")
    # Articles of an unrecognised :ARTICLE_TYPE: are listed under Other, so
    # every article appears in the index.
    sections = [(type_labels[t], by_type.get(t, [])) for t in ARTICLE_TYPES]
    sections.append(("Other", [
        a for t, arts in sorted(by_type.items()) if t not in type_labels
        for a in arts
    ]))
    for label, articles in sections:
        if not articles:
            continue
        lines.append(f"** {label} ({len(articles)})")
        lines.append("")
        for a in articles:
            status = f" /{a['status']}/" if a.get("status") else ""
            lines.append(f"- {_index_link(a)}{status}")
        lines.append("")

    # Articles by concept
    lines.append("* Articles by Concept")
    lines.append("")
    for concept in sorted(by_concept.keys()):
        articles = by_concept[concept]
        lines.append(f"** {concept.title()} ({len(articles)})")
        lines.append("")
        for a in articles:
            lines.append(f"- {_index_link(a)} [{a['article_type']}]")
        lines.append("")

    # Sources
    if source_list:
        lines.append("* Sources")
        lines.append("")
        for src in source_list:
            shareable = "shareable" if src.get("shareable") else "external"
            lines.append(f"- [{src['source_type']}] {src['title']} ({shareable})")
            if src.get("url"):
                lines.append(f"  {src['url']}")
        lines.append("")

    return "\n".join(lines)


def _write_index(root: Path, db: CrucibleDB) -> Path:
    """Write wiki/index.org from the current DB state."""
    text = _render_index(root, db)
    index_path = cdir(root) / "wiki" / "index.org"
    index_path.write_text(text, encoding="utf-8")
    return index_path


@cli.command()
def index():
    """Generate wiki/index.org from database state.

    Creates an auto-maintained index of all wiki articles, organized
    by type and by concept, with links to each article.
    """
    root = get_root()
    db = get_db(root)
    s = db.stats()
    index_path = _write_index(root, db)
    db.close()
    click.echo(
        f"Generated {index_path} ({s['articles']} articles, {s['concepts']} concepts)"
    )


@cli.command()
@click.option("--strict", is_flag=True,
              help="Also exit nonzero when there are warnings")
@json_option
def lint(strict, json_output):
    """Check wiki health and report issues.

    Finds broken links, orphan articles, missing concept articles,
    stale sources, unresolved or ambiguous source keys, and other
    consistency problems.

    Exits 1 if any issue has severity "error" (or "warning", with
    --strict), so scripts and CI can gate on it; info never fails.
    """
    root = get_root()
    db = get_db(root)
    issues = []

    # 1. Broken file links (link targets that don't exist on disk)
    for article in db.list_articles():
        org_path = root / article["path"]
        if not org_path.exists():
            issues.append({
                "type": "missing_file",
                "severity": "error",
                "message": f"Article registered in db but file missing: {article['path']}",
                "path": article['path'],
            })
            continue
        try:
            meta = parse_org_file(org_path)
        except (UnicodeDecodeError, OSError) as exc:
            issues.append({
                "type": "unreadable_file",
                "severity": "error",
                "message": f"Cannot read {article['path']}: {exc}",
                "path": article['path'],
            })
            continue
        for link in meta.file_links:
            if _link_is_broken(root, org_path, link):
                issues.append({
                    "type": "broken_link",
                    "severity": "warning",
                    "message": f"Broken link in {article['path']}: {link}",
                    "path": article['path'],
                    "link": link,
                })

    # 2. Orphan articles (no incoming links)
    orphan_list = db.orphans()
    for a in orphan_list:
        issues.append({
            "type": "orphan",
            "severity": "info",
            "message": f"No incoming links: {a['path']} ({a['title']})",
            "path": a['path'],
        })

    # 3. Undigested sources
    undigested_list = db.undigested()
    for s in undigested_list:
        issues.append({
            "type": "undigested",
            "severity": "warning",
            "message": f"Source not yet distilled: {s['title']} ({s['path']})",
            "path": s['path'],
        })

    # 3b. Committed sources whose file is gone. External sources are
    # gitignored, so a fresh clone legitimately lacks their files.
    for s in db.list_sources():
        if s["path"].startswith(f"{CRUCIBLE_DIR}/sources/external/"):
            continue
        if not (root / s["path"]).exists():
            issues.append({
                "type": "missing_source_file",
                "severity": "warning",
                "message": f"Source file not found: {s['path']} ({s['title']})",
                "path": s["path"],
            })

    # 4. Concepts without dedicated articles
    missing_concepts = db.concepts_without_articles()
    for c in missing_concepts:
        issues.append({
            "type": "missing_concept_article",
            "severity": "info",
            "message": f"Concept '{c}' has no dedicated concept article",
        })

    # 5. Articles missing required properties or referencing unknown sources
    all_sources = db.list_sources()
    source_paths = {src["id"]: src["path"] for src in all_sources}
    source_index = _source_key_index(all_sources)
    for key, ids in sorted(source_index[0].items()):
        if len(ids) > 1:
            paths = sorted(source_paths[sid] for sid in ids)
            issues.append({
                "type": "duplicate_cite_key",
                "severity": "warning",
                "message": (
                    f"Cite key '{key}' is shared by {len(paths)} sources: "
                    f"{', '.join(paths)}"
                ),
                "key": key,
                "sources": paths,
            })
    # The bibliography itself: a key used by two entries is ambiguous (each
    # citation can only mean one), and an entry with no closing brace is
    # ignored by everything that reads the file.
    from crucible import bibtex
    bib_file = cdir(root) / "references.bib"
    bib_text = (bib_file.read_text(encoding="utf-8", errors="replace")
                if bib_file.exists() else "")
    bib_entries, malformed = bibtex.scan(bib_text)
    if bib_entries or malformed:
        counts: dict[str, int] = {}
        for e in bib_entries:
            counts[e.key] = counts.get(e.key, 0) + 1
        for key, n in sorted(counts.items()):
            if n > 1:
                issues.append({
                    "type": "duplicate_bib_key",
                    "severity": "warning",
                    "message": f"references.bib has {n} entries with the key '{key}'",
                    "key": key,
                })
        for offset in malformed:
            line = bib_text.count("\n", 0, offset) + 1
            issues.append({
                "type": "malformed_bib_entry",
                "severity": "warning",
                "message": f"references.bib line {line}: entry is never closed and is ignored",
                "line": line,
            })
    bib_without_file = {e.key for e in bib_entries if not e.get("file")}
    # Ingested rows whose full date sync withheld from the bib (it may be
    # the day of ingest rather than a publication date): a rebuild keeps
    # only the entry's year, so say so and let the user decide.
    from crucible.ingest import _source_metadata
    dated_keys = {e.key for e in bib_entries if e.get("date")}
    for src in db.list_sources():
        src_meta = _source_metadata(src)
        key, withheld = src_meta.get("cite_key"), src_meta.get("unrecorded_date")
        if withheld and key not in dated_keys:
            issues.append({
                "type": "unrecorded_source_date",
                "severity": "info",
                "message": (
                    f"Source date {withheld} for '{key}' is not in references.bib "
                    "(it may be the day of ingest); add "
                    f"date = {{{withheld}}} to the entry if it is the publication date"
                ),
                "path": src["path"],
                "key": key,
            })
    for article in db.list_articles():
        org_path = root / article["path"]
        if not org_path.exists():
            continue
        meta = parse_org_file(org_path)
        if not meta.properties.get("ARTICLE_TYPE"):
            issues.append({
                "type": "missing_property",
                "severity": "warning",
                "message": f"Missing ARTICLE_TYPE property: {article['path']}",
                "path": article['path'],
            })
        dates = {}
        for prop in ("CREATED", "MODIFIED"):
            raw = meta.properties.get(prop)
            if raw is None:
                continue
            dates[prop] = _org_date(raw)
            if dates[prop] is None:
                issues.append({
                    "type": "invalid_date",
                    "severity": "warning",
                    "message": (
                        f"Unreadable :{prop}: date '{raw}' in {article['path']} "
                        "(use YYYY-MM-DD or an org timestamp)"
                    ),
                    "path": article['path'],
                })
        if not meta.properties.get("CREATED") and not meta.properties.get("MODIFIED"):
            issues.append({
                "type": "missing_property",
                "severity": "info",
                "message": (
                    f"Missing CREATED and MODIFIED properties: {article['path']} "
                    "(its dates are sync time, which a rebuild cannot reproduce)"
                ),
                "path": article['path'],
            })
        elif dates.get("CREATED") and dates.get("MODIFIED") and dates["MODIFIED"] < dates["CREATED"]:
            issues.append({
                "type": "invalid_date",
                "severity": "warning",
                "message": f":MODIFIED: is earlier than :CREATED: in {article['path']}",
                "path": article['path'],
            })
        source_keys = meta.properties.get("SOURCE_KEYS", "").split()
        if not source_keys:
            issues.append({
                "type": "missing_property",
                "severity": "info",
                "message": f"Missing SOURCE_KEYS property: {article['path']}",
                "path": article['path'],
            })
        else:
            for key, source_ids in _resolve_source_keys(source_keys, source_index).items():
                if len(source_ids) > 1:
                    paths = sorted(source_paths[sid] for sid in source_ids)
                    issues.append({
                        "type": "ambiguous_source_key",
                        "severity": "warning",
                        "message": (
                            f"Ambiguous SOURCE_KEYS value '{key}' in {article['path']}"
                            f" matches {len(paths)} sources: {', '.join(paths)}"
                        ),
                        "path": article['path'],
                        "key": key,
                        "sources": paths,
                    })
                if not source_ids:
                    message = f"Unresolved SOURCE_KEYS value '{key}' in {article['path']}"
                    if key in bib_without_file:
                        message += (
                            f" (references.bib has '{key}' but no file field naming"
                            " its stored file; add file = {.crucible/sources/...})"
                        )
                    issues.append({
                        "type": "unresolved_source_key",
                        "severity": "warning",
                        "message": message,
                        "path": article['path'],
                        "key": key,
                    })
        if not meta.filetags:
            issues.append({
                "type": "missing_tags",
                "severity": "warning",
                "message": f"No filetags: {article['path']}",
                "path": article['path'],
            })

    # 6. Wiki files not in database
    wiki_dir = cdir(root) / "wiki"
    wiki_files = _wiki_article_files(wiki_dir) if wiki_dir.is_dir() else []
    for org_path in wiki_files:
        rel = str(org_path.relative_to(root))
        if not db.get_article_by_path(rel):
            issues.append({
                "type": "untracked",
                "severity": "warning",
                "message": f"Wiki file not in database (run crucible sync): {rel}",
                "path": rel,
            })

    # 7. Citation keys not in references.bib
    bib_keys = {e.key for e in bib_entries}

    for article in db.list_articles():
        org_path = root / article["path"]
        if not org_path.exists():
            continue
        meta = parse_org_file(org_path)
        for key in meta.cite_keys:
            if key not in bib_keys:
                issues.append({
                    "type": "missing_citation",
                    "severity": "warning",
                    "message": f"Citation key '{key}' in {article['path']} not found in references.bib",
                    "path": article['path'],
                    "key": key,
                })

    db.close()

    failing = {"error", "warning"} if strict else {"error"}
    failed = any(i["severity"] in failing for i in issues)

    if json_output:
        _emit_json(issues)
        if failed:
            raise SystemExit(1)
        return

    if not issues:
        click.echo("No issues found.")
        return

    # Group by severity
    for severity in ["error", "warning", "info"]:
        group = [i for i in issues if i["severity"] == severity]
        if not group:
            continue
        label = {"error": "ERRORS", "warning": "WARNINGS", "info": "INFO"}[severity]
        click.echo(f"\n{label} ({len(group)}):")
        for i in group:
            click.echo(f"  [{i['type']}] {i['message']}")

    click.echo(f"\nTotal: {len(issues)} issues "
               f"({sum(1 for i in issues if i['severity'] == 'error')} errors, "
               f"{sum(1 for i in issues if i['severity'] == 'warning')} warnings, "
               f"{sum(1 for i in issues if i['severity'] == 'info')} info)")
    if failed:
        raise SystemExit(1)


@cli.command()
@json_option
def suggest(json_output):
    """Suggest improvements to the wiki.

    Analyzes the wiki graph to find opportunities for new articles,
    missing connections, and areas worth investigating.
    """
    root = get_root()
    db = get_db(root)
    suggestions = []

    # 1. Concepts that could use their own article
    missing = db.concepts_without_articles()
    for c in missing:
        articles = db.concept_articles(c)
        if len(articles) >= 2:
            titles = [a["title"] for a in articles[:3]]
            suggestions.append({
                "type": "new_concept_article",
                "priority": "high",
                "message": f"Concept '{c}' appears in {len(articles)} articles but has no "
                           f"dedicated article. Referenced by: {', '.join(titles)}",
            })

    # 2. Articles that share sources but aren't linked
    all_articles = db.list_articles()
    for article in all_articles:
        related = db.related(article["path"])
        for rel in related:
            reasons = rel.get("reasons", [])
            has_shared = any(r.startswith("shared_") for r in reasons)
            has_explicit = any(r.startswith("explicit_") for r in reasons)
            if has_shared and not has_explicit:
                suggestions.append({
                    "type": "missing_link",
                    "priority": "medium",
                    "message": f"'{article['title']}' and '{rel['title']}' are related "
                               f"({', '.join(reasons)}) but have no explicit cross-link",
                })

    # 3. Undigested sources
    undigested = db.undigested()
    for s in undigested:
        suggestions.append({
            "type": "pending_distillation",
            "priority": "high",
            "message": f"Source '{s['title']}' ({s['source_type']}) has not been distilled yet",
        })

    # 4. Comparison article opportunities
    by_concept = db.articles_by_concept()
    for concept, articles in by_concept.items():
        summaries = [a for a in articles if a["article_type"] == "summary"]
        has_comparison = any(a["article_type"] == "comparison" for a in articles)
        if len(summaries) >= 3 and not has_comparison:
            titles = [a["title"] for a in summaries[:4]]
            suggestions.append({
                "type": "comparison_opportunity",
                "priority": "medium",
                "message": f"Concept '{concept}' has {len(summaries)} source summaries "
                           f"but no comparison article. Sources: {', '.join(titles)}",
            })

    # 5. Orphans that could be linked
    orphans_list = db.orphans()
    for o in orphans_list:
        related = db.related(o["path"])
        if related:
            top = related[0]
            suggestions.append({
                "type": "link_orphan",
                "priority": "low",
                "message": f"Orphan '{o['title']}' could link to '{top['title']}' "
                           f"(via {', '.join(top['reasons'])})",
            })

    # 6. Semantically similar articles that aren't linked (if embeddings exist)
    idx = EmbeddingIndex(db.conn)
    idx.initialize()
    es = idx.stats()
    if es["embedded"] > 0:
        # Find similar pairs not already linked
        similar_pairs = idx.find_similar_pairs(threshold=0.7)
        linked_pairs = set()
        for link in db.all_article_links():
            linked_pairs.add((link["from_path"], link["to_path"]))
            linked_pairs.add((link["to_path"], link["from_path"]))

        for pair in similar_pairs:
            key = (pair["article_a"], pair["article_b"])
            if key not in linked_pairs:
                suggestions.append({
                    "type": "semantic_link",
                    "priority": "medium",
                    "message": f"'{pair['title_a']}' and '{pair['title_b']}' are "
                               f"semantically similar ({pair['similarity']:.3f}) "
                               f"but have no cross-link",
                })

        # Find clusters that could become comparison articles
        clusters = idx.find_cluster_candidates(min_articles=3, threshold=0.6)
        for cluster in clusters:
            titles = [a["title"] for a in cluster["articles"][:4]]
            suggestions.append({
                "type": "semantic_cluster",
                "priority": "medium",
                "message": f"Cluster of {len(cluster['articles'])} semantically similar "
                           f"articles around '{cluster['center']}': {', '.join(titles)}. "
                           f"Consider a synthesis or comparison article.",
            })

    db.close()

    # Deduplicate (missing_link can appear twice, once per direction)
    seen = set()
    deduped = []
    for s in suggestions:
        key = s["message"]
        if key not in seen:
            seen.add(key)
            deduped.append(s)
    suggestions = deduped

    if json_output:
        _emit_json(suggestions)
        return

    if not suggestions:
        click.echo("No suggestions. The wiki looks well-connected.")
        return

    for priority in ["high", "medium", "low"]:
        group = [s for s in suggestions if s["priority"] == priority]
        if not group:
            continue
        label = priority.upper()
        click.echo(f"\n{label} PRIORITY ({len(group)}):")
        for s in group:
            click.echo(f"  [{s['type']}] {s['message']}")

    click.echo(f"\nTotal: {len(suggestions)} suggestions")


@cli.command("help")
@click.argument("topic", default="overview")
def help_cmd(topic):
    """Show detailed help for agents and humans.

    TOPIC can be: overview, workflow, ingest, distill, search, sync, maintain, org-format, registry, or all.
    """
    topics = {
        "overview": """
CRUCIBLE - LLM-Compiled Knowledge Base

Crucible ingests primary sources (papers, notebooks, data) and distills them
into a structured, interlinked org-mode wiki. The wiki is maintained by an LLM;
humans rarely edit it directly.

Directory layout (everything lives in .crucible/ at the project root):
  .crucible/sources/external/   Copyrighted material (gitignored)
  .crucible/sources/notebooks/  Your own ELN entries (shareable)
  .crucible/sources/data/       Your own data files (shareable)
  .crucible/wiki/concepts/      Concept articles (one topic per file)
  .crucible/wiki/summaries/     Per-source summaries
  .crucible/wiki/comparisons/   Cross-source comparison articles
  .crucible/wiki/methods/       Methodology articles
  .crucible/wiki/index.org      Auto-maintained master index
  .crucible/references.bib      Bibliography; each entry's `file` field
                                names the stored source file
  .crucible/crucible.db         Wiki graph database (SQLite, regenerable)

Run `crucible help all` for complete documentation.
Run `crucible help <topic>` for a specific topic.
Topics: overview, workflow, ingest, distill, search, sync, maintain, org-format, registry
""",
        "workflow": """
TYPICAL WORKFLOW

1. Ingest a source:
     crucible ingest paper.pdf
     crucible ingest notes.org --type notebook
     curl URL | pandoc -t org | crucible ingest - -t "Title" --type web

2. Distill the source (done by the LLM):
   Read the source from .crucible/sources/ and write org articles into
   .crucible/wiki/. Use the two-step process:
   a) Plan: identify concepts, propose articles, note cross-links
   b) Write: create/update org files in .crucible/wiki/ with proper metadata

3. Sync the database:
     crucible sync

4. Query the knowledge base:
     crucible search "query"
     crucible concepts
     crucible related .crucible/wiki/concepts/some-article.org
     crucible backlinks .crucible/wiki/concepts/some-article.org

5. Maintain the wiki:
     crucible index            # regenerate .crucible/wiki/index.org
     crucible lint             # check for broken links, orphans, issues
     crucible suggest          # get improvement suggestions
     crucible orphans          # find unlinked articles
     crucible undigested       # find sources not yet distilled
     crucible stats            # overview counts

6. Browse the wiki:
     crucible browse           # open in browser (localhost:8088)
     crucible viz --open       # interactive knowledge graph

7. Install skill for Claude Code:
     crucible install          # copies skill to ~/.claude/skills/crucible/
""",
        "ingest": """
INGEST COMMAND

  crucible ingest SOURCE [OPTIONS]

SOURCE is a file path or - for stdin.

Options:
  -t, --title TEXT          Source title (auto-detected if omitted)
  --type TYPE               pdf, web, notebook, data, other (auto-detected)
  --shareable/--no-shareable  Override default shareability
  --url TEXT                Original URL
  --date TEXT               ISO date (default: today)
  -a, --author TEXT         Author(s), repeatable
  --doi TEXT                DOI; the full BibTeX entry is fetched from doi.org
  --bibtex TEXT             A BibTeX entry to use for the source
  -f, --filename TEXT       Filename for stdin input

Examples:
  crucible ingest paper.pdf
  crucible ingest paper.pdf -t "Smith 2024" -a "Smith, J." --date 2024-03-15
  crucible ingest lab-notes.org --type notebook --date 2026-04-03
  crucible ingest data.csv --type data --shareable
  cat article.md | crucible ingest - -t "Article Title" --type web --url "https://..."
  curl -s URL | pandoc -t org | crucible ingest - -t "Title" --type web
  crucible ingest paper.pdf --doi 10.1038/s41586-020-2649-2
  crucible ingest paper.pdf --bibtex "@article{x, title={...}, year={2024}}"

Auto-detection:
  .pdf -> pdf (external)     .org -> notebook (shareable)
  .md/.html -> web (external)  .csv/.json/.xlsx/.hdf5 -> data (shareable)

The source is copied to the appropriate directory, text is extracted,
and an entry is added to .crucible/references.bib. Its BibTeX entry is,
in order of preference: --bibtex, the entry doi.org returns for --doi (or
for a DOI found in the text), or a minimal entry from the options. The
cite key is made unique against existing sources and bib entries
(smith2024, smith2024a, ...), and the entry's `file` field records the
stored copy. The sources-table row is described by that entry, so a fresh
clone rebuilds the same row. Ingest prints the cite key: put it in the
SOURCE_KEYS of the articles you distill from the source.
""",
        "distill": """
DISTILLATION GUIDE (for LLM agents)

After `crucible ingest`, distill the source into wiki articles.
This is a two-step process: plan, then write.

STEP 1 - PLAN:
  Read the source file (check `crucible undigested` for pending sources).
  Review existing wiki articles (`crucible concepts`, `crucible search`).
  Produce a plan:
    - Summary article: title, outline, key findings
    - Concept articles: new topics to create, existing ones to update
    - Cross-links: connections to existing wiki articles
    - Cite keys: references to use in org-ref format

STEP 2 - WRITE:
  After the plan is approved, write org files into wiki/.
  Follow the org-format conventions (run `crucible help org-format`).
  Then run `crucible sync` to update the database.

LINKING TO SOURCES:
  In the :PROPERTIES: drawer, set SOURCE_KEYS to the space-separated
  cite keys of the article's sources: the keys in references.bib that
  `crucible ingest` printed. A file in sources/notebooks/ or sources/data/
  that no bib entry claims is keyed <stem>-<ext> (results.csv ->
  results-csv). Sync uses these keys to link articles to their sources;
  `crucible lint` reports keys that resolve to no source or to several.

UPDATING EXISTING ARTICLES:
  When a new source adds to an existing concept, add a new section
  to the existing article rather than creating a duplicate. Include
  the new source key in SOURCE_KEYS.
""",
        "search": """
SEARCH AND QUERY COMMANDS

Full-text search (FTS5 with porter stemming):
  crucible search "activation energy"
  crucible search "platinum catalysis" -n 5
  crucible search "zeolites" --all     Search every registered crucible

Semantic and hybrid search (after `crucible embed`, needs ollama):
  crucible search "query" --mode semantic
  crucible search "query" --mode hybrid

Structured queries:
  crucible concepts                    List all concepts with article counts
  crucible concept thermodynamics      Articles covering a concept
  crucible backlinks .crucible/wiki/concepts/x.org   What links here
  crucible sources .crucible/wiki/concepts/x.org     Primary sources
  crucible related .crucible/wiki/concepts/x.org     Implied links (shared
                                    concepts, shared sources, temporal proximity)
  crucible source SOURCE_PATH          Everything known about one source
  crucible orphans                     Articles with no incoming links
  crucible undigested                  Sources not yet distilled

  Article paths may be relative to the project root (as shown), to the
  current directory, or to the wiki (concepts/x.org), or absolute. A path
  that names no article, or an unknown source, exits 1 with an error.
  Concept names are case-insensitive.

Graph export:
  crucible graph                       DOT format to stdout
  crucible graph -o wiki.dot           DOT format to file

Statistics:
  crucible stats                       Counts of sources, articles, concepts, links
""",
        "sync": """
SYNC COMMAND

  crucible sync

Scans .crucible/wiki/ for .org files and updates the database:
  - Registers new articles (from path, title, ARTICLE_TYPE property)
  - Rewrites only articles whose file changed (others count as unchanged
    and keep their embeddings)
  - Takes article dates from :CREATED: and :MODIFIED: (MODIFIED falls back
    to CREATED, and an article with only MODIFIED uses it for both; with
    neither, sync time is used, which a rebuild cannot reproduce)
  - Links articles to concepts (from #+FILETAGS)
  - Links articles to each other (from [[file:...]] links)
  - Links articles to sources (from SOURCE_KEYS cite keys)

It also reconciles the sources table with .crucible/references.bib:
  - An entry with a `file` field owns the row for that file
  - An entry without one gets no row (lint reports keys cited this way)
  - A file in sources/notebooks/ or sources/data/ that no entry claims gets
    a row keyed <stem>-<ext> from a disk scan

The database is regenerable: delete .crucible/crucible.db and the next
command rebuilds it from the wiki, references.bib and the source files,
with the same rows an incremental sync gives.

Run sync after writing or updating any wiki articles.
Sync is idempotent and safe to run repeatedly.
""",
        "maintain": """
WIKI MAINTENANCE COMMANDS

Index generation:
  crucible index               Regenerate .crucible/wiki/index.org
                               Organized by article type and by concept.
                               Re-run after any changes.

Lint (health checks):
  crucible lint                Check for issues:
                               - Broken file links
                               - Missing article files
                               - Orphan articles (no incoming links)
                               - Undigested sources
                               - Missing concept articles
                               - Missing properties or filetags
                               - Missing or unreadable :CREATED: and
                                 :MODIFIED: dates
                               - SOURCE_KEYS that resolve to no source
                                 or to several
                               - Cite keys shared by several sources, or
                                 by several references.bib entries
                               - references.bib entries never closed
                               - Committed sources whose file is gone
                               - Citations missing from references.bib
                               - Source dates not copied to
                                 references.bib (maybe the ingest day)
                               - Wiki files not in database
  crucible lint -j             JSON output (for agent consumption)
  crucible lint --strict       Also fail on warnings
                               Exit status: 1 if any error (or, with
                               --strict, any warning); info never fails.

Suggest (improvement recommendations):
  crucible suggest             Analyze the wiki graph and suggest:
                               - New concept articles for well-referenced topics
                               - Missing cross-links between related articles
                               - Sources waiting for distillation
                               - Comparison article opportunities
                               - Ways to link orphan articles
  crucible suggest -j          JSON output (for agent consumption)

Merge another crucible:
  crucible merge SOURCE        Merge sources, articles, and references
                               from another crucible into this one.
                               SOURCE is a path or registry name.
  crucible merge SOURCE -n     Dry run (preview without changes)
  crucible merge SOURCE -y     Skip confirmation prompt
                               A source crucible whose sources or bib
                               entries repeat a cite key is refused:
                               give each its own key there first.

Delete:
  crucible delete PATH         Remove an article or source from the
                               database (--file also deletes the file).
                               Deleting a source removes the `file` field
                               from its references.bib entry so the next
                               sync does not restore it.

Typical maintenance cycle:
  crucible sync                # pick up any changes
  crucible lint                # find problems
  crucible suggest             # find opportunities
  crucible index               # regenerate the index
""",
        "org-format": """
ORG-MODE ARTICLE FORMAT

Every wiki article must follow this structure:

  #+TITLE: Article Title
  #+FILETAGS: :type-tag:concept1:concept2:

  * Article Title
  :PROPERTIES:
  :ARTICLE_TYPE: concept|summary|comparison|method
  :SOURCE_KEYS: key1 key2
  :STATUS: draft|reviewed|verified
  :CREATED: YYYY-MM-DD
  :MODIFIED: YYYY-MM-DD
  :END:

  Body text in narrative prose. Use org-ref citations:
  citep:smith2024, citet:jones2023, cite:doe2025.

  ** Subsection
  Cross-reference other articles with file links:
  [[file:../concepts/other-topic.org][Other Topic]]

FILETAGS: include the article type (concept, summary, etc.) and
all relevant topic tags. Tags become concepts in the database.

SOURCE_KEYS: space-separated cite keys from references.bib (the key
`crucible ingest` printed), or <stem>-<ext> for a file in sources/notebooks/
or sources/data/ with no bib entry. These create article-to-source links
during sync.

CREATED / MODIFIED: the article's dates; update MODIFIED on every edit.
They are what the database records, so they survive a rebuild.

ARTICLE TYPES:
  concept     - A single topic or idea (e.g., activation-energy.org)
  summary     - Summary of one primary source
  comparison  - Cross-source analysis on a theme
  method      - A technique or methodology

FILE NAMING: use-lowercase-kebab-case.org
""",
        "registry": """
GLOBAL REGISTRY

All crucible instances on this machine are tracked in a global registry
at ~/.crucible/registry.json. Instances auto-register during init, sync,
and install. Any crucible can discover all others without manual setup.

Commands:
  crucible registry list            List all registered instances
  crucible registry add NAME PATH   Manually register an instance
  crucible registry remove NAME     Remove an instance from the registry
  crucible registry clean           Remove stale entries (deleted crucibles)

Cross-crucible search:
  crucible search "query" --all     Search across all registered crucibles
  crucible concepts --all           List concepts from all crucibles

The registry is a simple JSON file. Each entry has a name (defaulting to
the directory name), an absolute path, and an optional description.
Auto-registration is idempotent and handles name conflicts by appending
a numeric suffix (e.g., my-project-2).
""",
    }

    if topic == "all":
        for t in ["overview", "workflow", "ingest", "distill", "search", "sync", "maintain", "org-format", "registry"]:
            click.echo(topics[t])
    elif topic in topics:
        click.echo(topics[topic])
    else:
        raise click.ClickException(
            f"Unknown topic: {topic}\n"
            f"Available topics: {', '.join(topics.keys())}, all"
        )


@cli.command()
@click.option("--output", "-o", type=click.Path(path_type=Path), default=None,
              help="Output directory (default: wiki/_build/html)")
@click.option("--init-only", is_flag=True, help="Only write the publish config, don't publish")
def publish(output, init_only):
    """Publish the wiki as a browsable HTML site via scimax.

    Generates an .org-publish.json config in wiki/ and runs
    `scimax publish` to export all org articles as linked HTML pages.
    """
    import subprocess

    root = get_root()
    wiki_dir = cdir(root) / "wiki"

    # Ensure index is up to date
    db = get_db(root)
    try:
        _write_index(root, db)
    finally:
        db.close()

    out_dir = str(output.resolve()) if output else str(wiki_dir / "_build" / "html")

    # Write publish config
    config = {
        "projects": {
            "crucible-wiki": {
                "name": "crucible-wiki",
                "baseDirectory": "./",
                "publishingDirectory": out_dir,
                "recursive": True,
                "autoSitemap": False,
                "useDefaultTheme": True,
                "withToc": True,
                "sectionNumbers": False,
                "baseExtension": "org",
                "publishingFunction": "org-html-publish-to-html",
                "withAuthor": True,
                "withCreator": True,
            }
        },
        "githubPages": False,
    }

    config_path = wiki_dir / ".org-publish.json"
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    click.echo(f"Publish config: {config_path}")

    if init_only:
        click.echo("Config written. Run `scimax publish` from wiki/ to publish.")
        return

    # Run scimax publish
    try:
        result = subprocess.run(
            ["scimax", "publish"],
            cwd=str(wiki_dir),
            capture_output=True, text=True, timeout=300,
        )
    except FileNotFoundError:
        raise click.ClickException(
            "'scimax' command not found. Install scimax, or use --init-only "
            "to write the publish config without publishing."
        )
    if result.returncode != 0:
        click.echo(result.stderr, err=True)
        raise click.ClickException("scimax publish failed")

    click.echo(result.stdout)
    click.echo(f"\nPublished to {out_dir}")


@cli.command()
def install():
    """Install the Crucible skill and register in global CLAUDE.md.

    1. Copies the skill into ~/.claude/skills/crucible/
    2. If inside a crucible project, generates the manifest and adds
       a CLAUDE.md directive pointing agents at it (replacing a directive
       for another crucible)
    """
    _install(root=None, directive=True)


_SKILL_RECORD = ".crucible-installed.json"


def _skill_source() -> Path:
    """The packaged skill directory.

    Wheel installs bundle skills/crucible into the package; editable
    installs read it from the repository root.
    """
    pkg_dir = Path(__file__).parent
    skill_src = pkg_dir / "skills" / "crucible"
    if not skill_src.exists():
        skill_src = pkg_dir.parent.parent / "skills" / "crucible"
    return skill_src


def _skill_dest() -> Path:
    return Path.home() / ".claude" / "skills" / "crucible"


def _file_sha256(path: Path) -> str:
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_skill_record(skill_dest: Path) -> dict[str, str]:
    """Files crucible installed into skill_dest, mapped to their sha256.

    Lets install tell a user-edited file from an older crucible copy, and
    lets uninstall remove only what crucible put there.
    """
    try:
        data = json.loads((skill_dest / _SKILL_RECORD).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    files = data.get("files") if isinstance(data, dict) else None
    if not isinstance(files, dict):
        return {}
    return {str(k): str(v) for k, v in files.items()}


def _write_skill_record(skill_dest: Path, record: dict[str, str]) -> None:
    (skill_dest / _SKILL_RECORD).write_text(
        json.dumps({"files": record}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _install(root: Path | None, directive: bool) -> None:
    """Install the skill; for a project, also its manifest and registry entry.

    `root` is the project (None: the one containing the current directory).
    Only `crucible install` passes directive=True: init and update must not
    repoint the global ~/.claude/CLAUDE.md at the project they act on.
    """
    # 1. Install skill files (works from anywhere)
    skill_src = _skill_source()
    if not skill_src.exists():
        raise click.ClickException(f"Skill source not found at {skill_src}")

    skill_dest = _skill_dest()
    skill_dest.mkdir(parents=True, exist_ok=True)
    record = _read_skill_record(skill_dest)

    for src_file in sorted(skill_src.iterdir()):
        if src_file.is_file():
            dest_file = skill_dest / src_file.name
            new_hash = _file_sha256(src_file)
            if dest_file.is_file():
                current = _file_sha256(dest_file)
                if current != new_hash and current != record.get(src_file.name):
                    # Not what crucible last installed: keep the local edits.
                    backup = dest_file.with_name(dest_file.name + ".bak")
                    shutil.copy2(str(dest_file), str(backup))
                    click.echo(f"  Skill: {dest_file} had local changes; "
                               f"saved a copy to {backup}")
            shutil.copy2(str(src_file), str(dest_file))
            record[src_file.name] = new_hash
            click.echo(f"  Skill: {src_file.name} -> {dest_file}")
    _write_skill_record(skill_dest, record)

    # 2. If inside a project, generate manifest and add CLAUDE.md directive
    if root is None:
        try:
            root = get_root()
        except click.ClickException:
            click.echo("\nSkill installed. No crucible project found in current directory,")
            click.echo("so manifest and CLAUDE.md directive were not generated.")
            click.echo("Run `crucible install` from inside a project to complete setup.")
            return

    db = get_db(root)
    try:
        manifest_path = _write_manifest(root, db)
    finally:
        db.close()
    click.echo(f"  Manifest: {manifest_path}")

    # Auto-register in global registry
    actual_name = registry_module.register_or_warn(root.name, str(root), _warn)
    if actual_name:
        click.echo(f"  Registry: registered as '{actual_name}'")

    if not directive:
        click.echo("  CLAUDE.md: unchanged (run `crucible install` here to point agents at this crucible)")
        return

    # 3. Add directive to ~/.claude/CLAUDE.md
    claude_md = Path.home() / ".claude" / "CLAUDE.md"
    marker = "<!-- crucible-knowledge-base -->"
    directive_text = (
        f"\n{marker}\n"
        f"## Crucible Knowledge Base\n"
        f"\n"
        f"A curated knowledge base is available at `{root.resolve()}`.\n"
        f"Before answering domain-specific questions, check if the knowledge base\n"
        f"covers the topic by reading the manifest:\n"
        f"`{manifest_path.resolve()}`\n"
        f"\n"
        f"If relevant, use `crucible search \"query\"` and `crucible concept <name>`\n"
        f"to find articles. Run `crucible help all` for the full CLI reference.\n"
        f"Always `cd {root.resolve()}` before running crucible commands.\n"
        f"{marker}\n"
    )

    if claude_md.exists():
        existing = claude_md.read_text(encoding="utf-8")
        if marker in existing:
            # Replace existing directive
            import re as _re
            pattern = _re.compile(
                _re.escape(marker) + r".*?" + _re.escape(marker),
                _re.DOTALL,
            )
            # A function, not a string: the directive holds the project
            # path, and a string replacement would read its backslashes
            # (every Windows path) as regex escapes.
            updated = pattern.sub(lambda _m: directive_text.strip(), existing)
            claude_md.write_text(updated, encoding="utf-8")
            click.echo(f"  CLAUDE.md: updated existing directive")
        else:
            claude_md.write_text(existing.rstrip() + "\n" + directive_text, encoding="utf-8")
            click.echo(f"  CLAUDE.md: added directive")
    else:
        claude_md.parent.mkdir(parents=True, exist_ok=True)
        claude_md.write_text(directive_text, encoding="utf-8")
        click.echo(f"  CLAUDE.md: created with directive")

    click.echo(f"\nCrucible installed. Restart Claude Code to pick up the skill.")


@cli.command()
def uninstall():
    """Remove the Crucible skill and CLAUDE.md directive.

    Reverses what `crucible install` did:
    1. Removes the skill files crucible installed in ~/.claude/skills/crucible/
       (files you added or edited there are left in place)
    2. Removes the crucible directive from ~/.claude/CLAUDE.md
    """
    # 1. Remove the skill files crucible installed
    skill_dest = _skill_dest()
    if skill_dest.is_dir():
        record = _read_skill_record(skill_dest)
        if record:
            installed = set(record)
        else:
            # Installed by a version that kept no record: only the files
            # the package ships are crucible's.
            skill_src = _skill_source()
            installed = ({p.name for p in skill_src.iterdir() if p.is_file()}
                         if skill_src.is_dir() else set())
        for name in sorted(installed):
            path = skill_dest / name
            if not path.is_file() or path.is_symlink():
                continue
            if name in record and _file_sha256(path) != record[name]:
                click.echo(f"  Kept {path}: it has local changes")
                continue
            path.unlink()
            click.echo(f"  Removed skill file: {path}")
        (skill_dest / _SKILL_RECORD).unlink(missing_ok=True)
        leftover = sorted(p.name for p in skill_dest.iterdir())
        if leftover:
            click.echo(f"  Left {skill_dest} in place; it still holds: "
                       f"{', '.join(leftover)}")
        else:
            skill_dest.rmdir()
            click.echo(f"  Removed skill: {skill_dest}")
    else:
        click.echo(f"  Skill not found at {skill_dest}")

    # 2. Remove directive from CLAUDE.md
    claude_md = Path.home() / ".claude" / "CLAUDE.md"
    marker = "<!-- crucible-knowledge-base -->"
    if claude_md.exists():
        existing = claude_md.read_text(encoding="utf-8")
        if marker in existing:
            pattern = re.compile(
                r"\n?" + re.escape(marker) + r".*?" + re.escape(marker) + r"\n?",
                re.DOTALL,
            )
            updated = pattern.sub("", existing).rstrip() + "\n"
            claude_md.write_text(updated, encoding="utf-8")
            click.echo(f"  Removed directive from CLAUDE.md")
        else:
            click.echo(f"  No crucible directive found in CLAUDE.md")
    else:
        click.echo(f"  No CLAUDE.md found")

    click.echo("\nCrucible uninstalled. Restart Claude Code to apply.")


@cli.command()
def update():
    """Update crucible to the latest version.

    Runs all pending database migrations, re-registers in the global
    registry, and reinstalls the skill files. Safe to run repeatedly.

    Use this after upgrading the crucible package to apply schema
    changes and pick up new skill definitions.
    """
    root = get_root()
    click.echo("Updating crucible...")

    # 1. Database migrations
    db = get_db(root)
    old_version = db.schema_version()
    db.initialize()  # runs _migrate() internally
    new_version = db.schema_version()
    db.close()
    if new_version > old_version:
        click.echo(f"  Database: migrated v{old_version} -> v{new_version}")
    else:
        click.echo(f"  Database: up to date (v{new_version})")

    # 2. Claude Code permissions
    if _ensure_settings(root):
        click.echo("  Settings: updated .claude/settings.json")
    else:
        click.echo("  Settings: up to date")

    # 3. Global registry
    actual_name = registry_module.register_or_warn(root.name, str(root), _warn)
    if actual_name:
        click.echo(f"  Registry: registered as '{actual_name}'")

    # 4. Reinstall skill
    _install(root=root, directive=False)


if __name__ == "__main__":
    cli()
