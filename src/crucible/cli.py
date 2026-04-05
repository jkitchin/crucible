"""Crucible CLI - knowledge base management."""

import json
import re
import shutil
from pathlib import Path

import click

from crucible.database import CrucibleDB
from crucible.embeddings import EmbeddingIndex
from crucible.ingest import ingest_source
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
    "Write(.crucible/wiki/**)",
    "Write(.crucible/sources/**)",
    "Edit(.crucible/wiki/**)",
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
        except (json.JSONDecodeError, OSError):
            settings = {}
    else:
        settings = {}

    permissions = settings.setdefault("permissions", {})
    allow = permissions.setdefault("allow", [])

    # Add any missing crucible permissions
    added = []
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


def get_root() -> Path:
    """Find the project root (walks up looking for .crucible/).

    A valid crucible project has .crucible/crucible.db (not just a
    .crucible/ directory, which could be the global registry at ~/.crucible/).
    """
    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents]:
        candidate = parent / CRUCIBLE_DIR
        if candidate.is_dir() and (candidate / "crucible.db").exists():
            return parent
    raise click.ClickException(
        "Not inside a crucible project. Run `crucible init` to create one."
    )


def cdir(root: Path) -> Path:
    """Return the .crucible/ directory for a project root."""
    return root / CRUCIBLE_DIR


def get_db(root: Path) -> CrucibleDB:
    return CrucibleDB(cdir(root) / "crucible.db")


@click.group()
@click.pass_context
def cli(ctx):
    """Crucible - LLM-compiled knowledge base."""
    ctx.ensure_object(dict)


@cli.command()
def about():
    """Show crucible version, project info, and quick overview."""
    from crucible.database import SCHEMA_VERSION
    from crucible.registry import list_instances

    # Package version
    try:
        from importlib.metadata import version as pkg_version
        version = pkg_version("crucible")
    except Exception:
        version = "dev"

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

    # .gitignore inside .crucible/
    gitignore = root / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text(
            "# Crucible: external sources (copyrighted, not shareable)\n"
            ".crucible/sources/external/\n"
            "\n"
            "# Crucible: database (regenerable from wiki)\n"
            ".crucible/crucible.db\n"
            "\n"
            "# Crucible: publish output\n"
            ".crucible/wiki/_build/\n",
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
    from crucible.registry import register as registry_register
    actual_name = registry_register(root.name, str(root))
    click.echo(f"  Registered in global registry as '{actual_name}'")

    # Install skill and CLAUDE.md directive automatically
    ctx = click.get_current_context()
    ctx.invoke(install)


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
@click.option("--filename", "-f", default=None,
              help="Filename for stdin input (default: derived from title or timestamp)")
def ingest(source, title, source_type, shareable, url, date, author, doi, filename):
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

    root = get_root()
    db = get_db(root)

    if source == "-":
        # Reading from stdin
        content = sys.stdin.read()
        if not content.strip():
            raise click.ClickException("No input received on stdin.")

        if source_type is None:
            source_type = "web"
        if filename is None:
            if title:
                slug = title.lower().replace(" ", "-")
                slug = re.sub(r"[^a-z0-9-]", "", slug)[:60]
            else:
                from datetime import datetime as dt
                slug = dt.now().strftime("stdin-%Y%m%d-%H%M%S")
            ext = ".org" if source_type in ("notebook",) else ".md"
            filename = f"{slug}{ext}"

        # Write to a temp file, then ingest normally
        tmp = Path(tempfile.mkdtemp()) / filename
        tmp.write_text(content, encoding="utf-8")
        result = ingest_source(
            root=root, db=db, source_path=tmp,
            title=title, source_type=source_type, shareable=shareable,
            url=url, date=date, authors=list(author) if author else None, doi=doi,
        )
        tmp.unlink()
        tmp.parent.rmdir()
        _print_ingest_result(result)
    elif source.startswith("http://") or source.startswith("https://"):
        # URL: fetch, convert, and ingest
        import subprocess
        import tempfile
        from urllib.request import Request, urlopen
        from urllib.error import URLError

        click.echo(f"Fetching {source}...")
        try:
            req = Request(source, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            })
            resp = urlopen(req, timeout=30)
            raw_html = resp.read().decode("utf-8", errors="replace")
        except (URLError, OSError) as e:
            raise click.ClickException(f"Failed to fetch URL: {e}")

        # Extract title from HTML if not provided
        if title is None:
            import re as _re
            m = _re.search(r"<title>(.*?)</title>", raw_html,
                           _re.IGNORECASE | _re.DOTALL)
            if m:
                title = m.group(1).strip()
                # Clean up common title cruft
                for sep in [" | ", " - ", " :: "]:
                    if sep in title:
                        title = title.split(sep)[0].strip()

        if title is None:
            title = source.split("/")[-1] or "Untitled Web Page"

        # Convert to plain text via pandoc if available
        if shutil.which("pandoc"):
            proc = subprocess.run(
                ["pandoc", "-f", "html", "-t", "plain", "--wrap=none"],
                input=raw_html, capture_output=True, text=True, timeout=60,
            )
            content = proc.stdout if proc.returncode == 0 else raw_html
        else:
            content = raw_html

        # Also save the raw HTML
        slug = re.sub(r"[^a-z0-9-]", "", title.lower().replace(" ", "-"))[:60]
        tmp_dir = Path(tempfile.mkdtemp())
        tmp_html = tmp_dir / f"{slug}.html"
        tmp_html.write_text(raw_html, encoding="utf-8")

        # Ingest the HTML file (text extraction will use pandoc)
        result = ingest_source(
            root=root, db=db, source_path=tmp_html,
            title=title, source_type="web", shareable=False,
            url=source, date=date, authors=list(author) if author else None,
        )
        tmp_html.unlink()
        tmp_dir.rmdir()
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
                    )
                    click.echo(f"  [{result['source_id']}] {result['title']} ({result['source_type']})")
                except Exception as e:
                    click.echo(f"  SKIP {p}: {e}", err=True)
            click.echo(f"\nDone. Run `crucible undigested` to see what needs distilling.")
        else:
            source_path = Path(source)
            if not source_path.exists():
                raise click.ClickException(f"Source not found: {source}")
            result = ingest_source(
                root=root, db=db, source_path=source_path,
                title=title, source_type=source_type, shareable=shareable,
                url=url, date=date, authors=list(author) if author else None,
            )
            _print_ingest_result(result)

    db.close()


def _print_ingest_result(result: dict):
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


@cli.command()
def sync():
    """Sync the database from wiki org files.

    Scans wiki/ for org files, parses their metadata (title, properties,
    links, citations), and updates the articles, concepts, and links
    tables. Run this after Claude writes or updates wiki articles.
    """
    root = get_root()
    db = get_db(root)
    wiki_dir = cdir(root) / "wiki"

    # Auto-register in global registry (ensures pre-registry crucibles get registered)
    from crucible.registry import register as registry_register
    registry_register(root.name, str(root))

    org_files = sorted(wiki_dir.rglob("*.org"))
    if not org_files:
        click.echo("No org files found in wiki/.")
        db.close()
        return

    # Capture file mtimes before parsing so we can detect concurrent edits
    file_mtimes = {p: p.stat().st_mtime for p in org_files}

    added = 0
    updated = 0
    links_added = 0
    concepts_added = 0
    skipped = 0

    with db.conn.transaction():
        # Pass 1: register/update all articles and concepts
        parsed = {}  # org_path -> (article_id, meta)
        for org_path in org_files:
            if org_path.stat().st_mtime != file_mtimes[org_path]:
                click.echo(f"  Skipping {org_path.name} (modified during sync)")
                skipped += 1
                continue

            meta = parse_org_file(org_path)
            rel_path = str(org_path.relative_to(root))
            article_type = meta.properties.get("ARTICLE_TYPE", "concept")

            existing = db.get_article_by_path(rel_path)
            if existing:
                db.update_article_fts(existing["id"], rel_path, meta.title, meta.content)
                article_id = existing["id"]
                updated += 1
            else:
                article_id = db.add_article(
                    path=rel_path,
                    title=meta.title,
                    article_type=article_type,
                    distill_model=meta.properties.get("DISTILL_MODEL"),
                    abstract=meta.properties.get("ABSTRACT", ""),
                    content=meta.content,
                )
                added += 1

            type_tags = {"concept", "summary", "comparison", "method"}
            for tag in meta.filetags:
                if tag.lower() not in type_tags:
                    concept_id = db.add_concept(tag.lower())
                    db.link_article_concept(article_id, concept_id)
                    concepts_added += 1

            parsed[org_path] = (article_id, meta)

        # Pass 2: inter-article links, source links, derivations
        for org_path, (article_id, meta) in parsed.items():
            for link_target in meta.file_links:
                target_path = (org_path.parent / link_target).resolve()
                if target_path.exists():
                    target_rel = str(target_path.relative_to(root))
                    target_article = db.get_article_by_path(target_rel)
                    if target_article:
                        db.add_article_link(article_id, target_article["id"])
                        links_added += 1

            source_keys = meta.properties.get("SOURCE_KEYS", "").split()
            for key in source_keys:
                if not key:
                    continue
                for src in db.list_sources():
                    if key in src["path"] or key in src.get("title", ""):
                        db.link_article_source(article_id, src["id"])

            derived_from = meta.properties.get("DERIVED_FROM", "").split()
            for ref in derived_from:
                if not ref:
                    continue
                ref_path = (org_path.parent / ref).resolve()
                if ref_path.exists():
                    ref_rel = str(ref_path.relative_to(root))
                else:
                    ref_rel = f"wiki/{ref}" if not ref.startswith("wiki/") else ref
                source_article = db.get_article_by_path(ref_rel)
                if source_article:
                    db.add_derivation(article_id, source_article["id"])

    db.close()
    msg = f"Sync complete: {added} added, {updated} updated, "
    msg += f"{concepts_added} concept links, {links_added} article links"
    if skipped:
        msg += f", {skipped} skipped (modified during sync)"
    click.echo(msg)


@cli.command()
@click.option("--model", "-m", default="nomic-embed-text", help="Ollama embedding model")
@click.option("--url", default="http://localhost:11434/api/embed", help="Ollama API URL")
def embed(model, url):
    """Generate embeddings for all wiki articles.

    Uses ollama to create vector embeddings for semantic search.
    Only embeds articles that don't already have embeddings.
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
def registry_list():
    """List all registered crucible instances."""
    from crucible.registry import list_instances, resolve_db_path
    instances = list_instances()
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
    actual_name = register(name, str(path.resolve()), description)
    if actual_name != name:
        click.echo(f"Name '{name}' taken, registered as '{actual_name}'")
    click.echo(f"Registered '{actual_name}' -> {path.resolve()}")


@registry.command("remove")
@click.argument("name")
def registry_remove(name):
    """Remove a crucible from the global registry."""
    from crucible.registry import unregister
    if unregister(name):
        click.echo(f"Removed '{name}' from registry")
    else:
        click.echo(f"'{name}' not found in registry")


@registry.command("clean")
def registry_clean():
    """Remove stale entries (crucibles that no longer exist on disk)."""
    from crucible.registry import clean
    removed = clean()
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
def search(query, limit, search_all, mode, model, fts_weight, semantic_weight, raw):
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
        if not results:
            click.echo("No results.")
            return
        for r in results:
            source = r.get("_crucible", "")
            prefix = f"[{source}] " if source and source not in ("local", "") else ""
            status = r.get("status", "")
            click.echo(f"  {prefix}[{r['article_type']}] {r['title']}  ({status})")
            click.echo(f"    {r['path']}")


@cli.command()
@click.argument("article_path")
def history(article_path):
    """Show the derivation history of an article.

    Shows what articles this one was derived from (upstream)
    and what articles have been derived from it (downstream).
    Traces the evolution of knowledge over time.
    """
    root = get_root()
    db = get_db(root)

    upstream = db.derived_from(article_path)
    downstream = db.derivatives(article_path)
    db.close()

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
def backlinks(article_path):
    """Show articles that link to the given article."""
    root = get_root()
    db = get_db(root)
    results = db.backlinks(article_path)
    db.close()
    if not results:
        click.echo("No backlinks found.")
        return
    for r in results:
        click.echo(f"  {r['title']} ({r['path']})")
        if r.get("context"):
            click.echo(f"    ...{r['context']}...")


@cli.command("source")
@click.argument("source_path")
def source_info(source_path):
    """Show everything known about a source.

    Displays the source metadata and all articles distilled from it.
    Useful for resummarization: see what's already been captured before
    asking Claude to reread the source.
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
        click.echo(f"Source not found: {source_path}")
        db.close()
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
def sources(article_path):
    """Show primary sources for an article."""
    root = get_root()
    db = get_db(root)
    results = db.article_sources(article_path)
    db.close()
    if not results:
        click.echo("No sources linked.")
        return
    for r in results:
        click.echo(f"  [{r['source_type']}] {r['title']}")
        if r.get("url"):
            click.echo(f"    {r['url']}")


@cli.command()
@click.option("--all", "query_all", is_flag=True, help="Include all registered crucibles")
def concepts(query_all):
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
    if not results:
        click.echo("No concepts yet.")
        return
    for r in results:
        source = r.get("_crucible", "")
        prefix = f"[{source}] " if source and source != "local" else ""
        click.echo(f"  {prefix}{r['name']} ({r['article_count']} articles)")


@cli.command()
@click.argument("name")
def concept(name):
    """Show articles covering a concept."""
    root = get_root()
    db = get_db(root)
    results = db.concept_articles(name)
    db.close()
    if not results:
        click.echo(f"No articles found for concept '{name}'.")
        return
    for r in results:
        click.echo(f"  [{r['article_type']}] {r['title']} ({r['path']})")


@cli.command()
def orphans():
    """Show articles with no incoming links."""
    root = get_root()
    db = get_db(root)
    results = db.orphans()
    db.close()
    if not results:
        click.echo("No orphan articles.")
        return
    for r in results:
        click.echo(f"  [{r['article_type']}] {r['title']} ({r['path']})")


@cli.command()
def undigested():
    """Show sources with no articles distilled from them."""
    root = get_root()
    db = get_db(root)
    results = db.undigested()
    db.close()
    if not results:
        click.echo("All sources have been distilled.")
        return
    for r in results:
        click.echo(f"  [{r['source_type']}] {r['title']} ({r['path']})")


@cli.command()
@click.argument("article_path")
def related(article_path):
    """Show articles related through implied links.

    Computes relationships via shared concepts, shared sources,
    temporal proximity, and explicit links.
    """
    root = get_root()
    db = get_db(root)
    results = db.related(article_path)
    db.close()
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

    db.close()

    graph_json = json.dumps({"nodes": nodes, "edges": edges})
    root_path = json.dumps(str(root.resolve()))

    html = _VIZ_TEMPLATE.replace("/*GRAPH_DATA*/", graph_json)
    html = html.replace("/*ROOT_PATH*/", root_path)

    if output is None:
        output = cdir(root) / "wiki" / "viz.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    click.echo(f"Visualization: {output} ({len(nodes)} nodes, {len(edges)} edges)")

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
    """Open the crucible wiki in VS Code.

    Opens the wiki directory as a workspace with the index file.
    Optionally open a specific FILE (relative wiki path like concepts/foo.org).
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
        subprocess.run(["crucible", "viz"], cwd=str(root), capture_output=True)

    # Open the wiki folder in VS Code
    cmd = ["code", str(wiki_dir)]

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
        if delete_file:
            disk_path = root / src["path"]
            if disk_path.exists():
                disk_path.unlink()
                click.echo(f"  Deleted file: {disk_path}")
        db.delete_source(src["id"])
        click.echo("  Deleted from database.")
        db.close()
        return

    db.close()
    raise click.ClickException(f"Not found: {path}")


@cli.command()
def stats():
    """Show database statistics."""
    root = get_root()
    db = get_db(root)
    s = db.stats()
    db.close()
    click.echo(f"Sources:  {s['sources']}")
    click.echo(f"Articles: {s['articles']}")
    click.echo(f"Concepts: {s['concepts']}")
    click.echo(f"Links:    {s['article_links']}")


@cli.command()
def manifest():
    """Generate a manifest summarizing the knowledge base.

    Outputs a concise summary that agents can read to decide whether
    this knowledge base is relevant to their query. Also writes
    the manifest to wiki/MANIFEST.md for reference from CLAUDE.md.
    """
    from datetime import datetime

    root = get_root()
    db = get_db(root)
    s = db.stats()
    concept_list = db.list_concepts()
    source_types = {}
    for src in db.list_sources():
        t = src["source_type"]
        source_types[t] = source_types.get(t, 0) + 1
    article_types = {}
    for art in db.list_articles():
        t = art["article_type"]
        article_types[t] = article_types.get(t, 0) + 1
    db.close()

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

    text = "\n".join(lines) + "\n"

    manifest_path = cdir(root) / "wiki" / "MANIFEST.md"
    manifest_path.write_text(text, encoding="utf-8")
    click.echo(text)
    click.echo(f"Written to {manifest_path}")


@cli.command()
def index():
    """Generate wiki/index.org from database state.

    Creates an auto-maintained index of all wiki articles, organized
    by type and by concept, with links to each article.
    """
    from datetime import datetime

    root = get_root()
    db = get_db(root)
    s = db.stats()
    by_type = db.articles_by_type()
    by_concept = db.articles_by_concept()
    source_list = db.list_sources()
    db.close()

    type_labels = {
        "concept": "Concepts",
        "summary": "Source Summaries",
        "comparison": "Comparisons",
        "method": "Methods",
    }

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
    for atype in ["concept", "summary", "comparison", "method"]:
        articles = by_type.get(atype, [])
        if not articles:
            continue
        label = type_labels.get(atype, atype.title())
        lines.append(f"** {label} ({len(articles)})")
        lines.append("")
        for a in articles:
            status = f" /{a['status']}/" if a.get("status") else ""
            lines.append(f"- [[file:{a['path']}][{a['title']}]]{status}")
        lines.append("")

    # Articles by concept
    lines.append("* Articles by Concept")
    lines.append("")
    for concept in sorted(by_concept.keys()):
        articles = by_concept[concept]
        lines.append(f"** {concept.title()} ({len(articles)})")
        lines.append("")
        for a in articles:
            lines.append(f"- [[file:{a['path']}][{a['title']}]] [{a['article_type']}]")
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

    index_path = cdir(root) / "wiki" / "index.org"
    index_path.write_text("\n".join(lines), encoding="utf-8")
    click.echo(f"Generated {index_path} ({s['articles']} articles, {s['concepts']} concepts)")


@cli.command()
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON for agents")
def lint(json_output):
    """Check wiki health and report issues.

    Finds broken links, orphan articles, missing concept articles,
    stale sources, and other consistency problems.
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
            })
            continue
        meta = parse_org_file(org_path)
        for link in meta.file_links:
            target = (org_path.parent / link).resolve()
            if not target.exists():
                issues.append({
                    "type": "broken_link",
                    "severity": "warning",
                    "message": f"Broken link in {article['path']}: {link}",
                })

    # 2. Orphan articles (no incoming links)
    orphan_list = db.orphans()
    for a in orphan_list:
        issues.append({
            "type": "orphan",
            "severity": "info",
            "message": f"No incoming links: {a['path']} ({a['title']})",
        })

    # 3. Undigested sources
    undigested_list = db.undigested()
    for s in undigested_list:
        issues.append({
            "type": "undigested",
            "severity": "warning",
            "message": f"Source not yet distilled: {s['title']} ({s['path']})",
        })

    # 4. Concepts without dedicated articles
    missing_concepts = db.concepts_without_articles()
    for c in missing_concepts:
        issues.append({
            "type": "missing_concept_article",
            "severity": "info",
            "message": f"Concept '{c}' has no dedicated concept article",
        })

    # 5. Articles missing required properties
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
            })
        if not meta.properties.get("SOURCE_KEYS"):
            issues.append({
                "type": "missing_property",
                "severity": "info",
                "message": f"Missing SOURCE_KEYS property: {article['path']}",
            })
        if not meta.filetags:
            issues.append({
                "type": "missing_tags",
                "severity": "warning",
                "message": f"No filetags: {article['path']}",
            })

    # 6. Wiki files not in database
    wiki_dir = cdir(root) / "wiki"
    for org_path in wiki_dir.rglob("*.org"):
        rel = str(org_path.relative_to(root))
        if rel.endswith("index.org"):
            continue
        if not db.get_article_by_path(rel):
            issues.append({
                "type": "untracked",
                "severity": "warning",
                "message": f"Wiki file not in database (run crucible sync): {rel}",
            })

    # 7. Citation keys not in references.bib
    bib_path = cdir(root) / "references.bib"
    bib_keys = set()
    if bib_path.exists():
        import re as _re
        for m in _re.finditer(r"@\w+\{(\w+),", bib_path.read_text(encoding="utf-8")):
            bib_keys.add(m.group(1))

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
                })

    db.close()

    if json_output:
        click.echo(json.dumps(issues, indent=2))
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


@cli.command()
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON for agents")
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
        click.echo(json.dumps(suggestions, indent=2))
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

Directory layout:
  sources/external/     Copyrighted material (gitignored)
  sources/notebooks/    Your own ELN entries (shareable)
  sources/data/         Your own data files (shareable)
  wiki/concepts/        Concept articles (one topic per file)
  wiki/summaries/       Per-source summaries
  wiki/comparisons/     Cross-source comparison articles
  wiki/methods/         Methodology articles
  wiki/index.org        Auto-maintained master index
  db/crucible.db        Wiki graph database (SQLite)

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
   Read the source from sources/ and write org articles into wiki/.
   Use the two-step process:
   a) Plan: identify concepts, propose articles, note cross-links
   b) Write: create/update org files in wiki/ with proper metadata

3. Sync the database:
     crucible sync

4. Query the knowledge base:
     crucible search "query"
     crucible concepts
     crucible related wiki/concepts/some-article.org
     crucible backlinks wiki/concepts/some-article.org

5. Maintain the wiki:
     crucible index            # regenerate wiki/index.org
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
  -f, --filename TEXT       Filename for stdin input

Examples:
  crucible ingest paper.pdf
  crucible ingest paper.pdf -t "Smith 2024" -a "Smith, J." --date 2024-03-15
  crucible ingest lab-notes.org --type notebook --date 2026-04-03
  crucible ingest data.csv --type data --shareable
  cat article.md | crucible ingest - -t "Article Title" --type web --url "https://..."
  curl -s URL | pandoc -t org | crucible ingest - -t "Title" --type web

Auto-detection:
  .pdf -> pdf (external)     .org -> notebook (shareable)
  .md/.html -> web (external)  .csv/.json/.xlsx/.hdf5 -> data (shareable)

The source is copied to the appropriate directory, text is extracted,
and a row is added to the sources table. The source is then ready
for distillation by an LLM agent.
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
  In the :PROPERTIES: drawer, set SOURCE_KEYS to space-separated
  identifiers that match the source filename or title in the database.
  The sync command uses these to link articles to their sources.

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

Structured queries:
  crucible concepts                    List all concepts with article counts
  crucible concept thermodynamics      Articles covering a concept
  crucible backlinks wiki/concepts/x.org   What links to this article
  crucible sources wiki/concepts/x.org     Primary sources for an article
  crucible related wiki/concepts/x.org     Implied links (shared concepts,
                                           shared sources, temporal proximity)
  crucible orphans                     Articles with no incoming links
  crucible undigested                  Sources not yet distilled

Graph export:
  crucible graph                       DOT format to stdout
  crucible graph -o wiki.dot           DOT format to file

Statistics:
  crucible stats                       Counts of sources, articles, concepts, links
""",
        "sync": """
SYNC COMMAND

  crucible sync

Scans wiki/ for .org files and updates the database:
  - Registers new articles (from path, title, ARTICLE_TYPE property)
  - Updates FTS index for existing articles
  - Links articles to concepts (from #+FILETAGS)
  - Links articles to each other (from [[file:...]] links)
  - Links articles to sources (from SOURCE_KEYS property)

Run sync after writing or updating any wiki articles.
Sync is idempotent and safe to run repeatedly.
""",
        "maintain": """
WIKI MAINTENANCE COMMANDS

Index generation:
  crucible index               Regenerate wiki/index.org from database
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
                               - Wiki files not in database
  crucible lint -j             JSON output (for agent consumption)

Suggest (improvement recommendations):
  crucible suggest             Analyze the wiki graph and suggest:
                               - New concept articles for well-referenced topics
                               - Missing cross-links between related articles
                               - Sources waiting for distillation
                               - Comparison article opportunities
                               - Ways to link orphan articles
  crucible suggest -j          JSON output (for agent consumption)

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
  :END:

  Body text in narrative prose. Use org-ref citations:
  citep:smith2024, citet:jones2023, cite:doe2025.

  ** Subsection
  Cross-reference other articles with file links:
  [[file:../concepts/other-topic.org][Other Topic]]

FILETAGS: include the article type (concept, summary, etc.) and
all relevant topic tags. Tags become concepts in the database.

SOURCE_KEYS: space-separated identifiers matching source filenames
or titles. These create article-to-source links during sync.

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
        click.echo(f"Unknown topic: {topic}")
        click.echo(f"Available topics: {', '.join(topics.keys())}, all")


@cli.command()
@click.option("--output", "-o", type=click.Path(path_type=Path), default=None,
              help="Output directory (default: wiki/_build/html)")
@click.option("--init-only", is_flag=True, help="Only write the publish config, don't publish")
def publish(output, init_only):
    """Publish the wiki as a browsable HTML site via scimax.

    Generates an .org-publish.json config in wiki/ and runs
    `scimax publish` to export all org articles as linked HTML pages.
    """
    import shutil
    import subprocess

    root = get_root()
    wiki_dir = cdir(root) / "wiki"

    # Ensure index is up to date
    subprocess.run(["crucible", "index"], cwd=str(root), capture_output=True)

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
    result = subprocess.run(
        ["scimax", "publish"],
        cwd=str(wiki_dir),
        capture_output=True, text=True, timeout=300,
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
       a CLAUDE.md directive pointing agents at it
    """
    import shutil
    import subprocess

    # 1. Install skill files (works from anywhere)
    pkg_dir = Path(__file__).parent
    skill_src = pkg_dir / "skill"
    if not skill_src.exists():
        raise click.ClickException(f"Skill source not found at {skill_src}")

    skill_dest = Path.home() / ".claude" / "skills" / "crucible"
    skill_dest.mkdir(parents=True, exist_ok=True)

    for src_file in skill_src.iterdir():
        if src_file.is_file():
            dest_file = skill_dest / src_file.name
            shutil.copy2(str(src_file), str(dest_file))
            click.echo(f"  Skill: {src_file.name} -> {dest_file}")

    # 2. If inside a project, generate manifest and add CLAUDE.md directive
    try:
        root = get_root()
    except click.ClickException:
        click.echo("\nSkill installed. No crucible project found in current directory,")
        click.echo("so manifest and CLAUDE.md directive were not generated.")
        click.echo("Run `crucible install` from inside a project to complete setup.")
        return

    subprocess.run(["crucible", "manifest"], cwd=str(root),
                   capture_output=True, text=True)
    manifest_path = cdir(root) / "wiki" / "MANIFEST.md"
    click.echo(f"  Manifest: {manifest_path}")

    # Auto-register in global registry
    from crucible.registry import register as registry_register
    actual_name = registry_register(root.name, str(root))
    click.echo(f"  Registry: registered as '{actual_name}'")

    # 3. Add directive to ~/.claude/CLAUDE.md
    claude_md = Path.home() / ".claude" / "CLAUDE.md"
    marker = "<!-- crucible-knowledge-base -->"
    directive = (
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
            updated = pattern.sub(directive.strip(), existing)
            claude_md.write_text(updated, encoding="utf-8")
            click.echo(f"  CLAUDE.md: updated existing directive")
        else:
            claude_md.write_text(existing.rstrip() + "\n" + directive, encoding="utf-8")
            click.echo(f"  CLAUDE.md: added directive")
    else:
        claude_md.parent.mkdir(parents=True, exist_ok=True)
        claude_md.write_text(directive, encoding="utf-8")
        click.echo(f"  CLAUDE.md: created with directive")

    click.echo(f"\nCrucible installed. Restart Claude Code to pick up the skill.")


@cli.command()
def uninstall():
    """Remove the Crucible skill and CLAUDE.md directive.

    Reverses what `crucible install` did:
    1. Removes ~/.claude/skills/crucible/
    2. Removes the crucible directive from ~/.claude/CLAUDE.md
    """
    import shutil

    # 1. Remove skill directory
    skill_dest = Path.home() / ".claude" / "skills" / "crucible"
    if skill_dest.exists():
        shutil.rmtree(str(skill_dest))
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
    import subprocess

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
    from crucible.registry import register as registry_register
    actual_name = registry_register(root.name, str(root))
    click.echo(f"  Registry: registered as '{actual_name}'")

    # 4. Reinstall skill
    ctx = click.get_current_context()
    ctx.invoke(install)


if __name__ == "__main__":
    cli()
