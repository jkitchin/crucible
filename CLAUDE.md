# Crucible: LLM-Compiled Knowledge Base

## Overview

Crucible is a personal knowledge base where primary sources are ingested and
distilled by an LLM into a structured, interlinked org-mode wiki. The wiki is
the LLM's domain; humans rarely edit it directly.

## Directory Layout

- `sources/` - Primary source documents, organized by shareability:
  - `external/` - Copyrighted material (PDFs, web articles). Gitignored.
    - `pdfs/` - Academic papers, reports
    - `web/` - Web clippings
  - `notebooks/` - Your own ELN entries. NOT gitignored.
  - `data/` - Your own experimental data. NOT gitignored.
- `wiki/` - Distilled org-mode articles. Shareable. Organized by type:
  - `concepts/` - Concept articles (one topic per file)
  - `summaries/` - Per-source summaries
  - `comparisons/` - Cross-source comparison articles
  - `methods/` - Methodology articles
  - `index.org` - Auto-maintained master index
- `db/crucible.db` - Wiki graph database (SQLite). Tracks sources, articles,
  concepts, and links between them. Regenerable from wiki content.
- `bin/crucible` - CLI entry point
- `src/crucible/` - Python source code

## Conventions

### Org-Mode Articles

All wiki articles use org-mode format with scimax conventions:

- Use `#+TITLE:` for the article title
- Use `#+FILETAGS:` for categorization (include article type as a tag)
- Use a properties drawer on the top heading with:
  - `:ARTICLE_TYPE:` - concept, summary, comparison, or method
  - `:SOURCE_KEYS:` - space-separated cite keys of primary sources
  - `:STATUS:` - draft, reviewed, or verified
  - `:CREATED:` - ISO date
- Use org-ref citations: `cite:key`, `citep:key`, `citet:key`
- Use `[[file:path][description]]` links for cross-references between articles
- Write in narrative prose, not bulleted lists (unless content demands enumeration)

### Ingest Workflow

Ingestion is a two-step process:
1. **Plan**: Read the source, propose articles to create/update, identify concepts
   and cross-links. Present plan for user approval.
2. **Write**: Execute the approved plan, producing org-mode articles.

### Sync After Writing

The database and MANIFEST.md are not updated automatically when wiki files are
created or modified. You must run `crucible sync` after writing or editing any
wiki articles. Without this, the database will be stale and MANIFEST.md will
show incorrect counts (e.g. 0 articles even when article files exist on disk).
Always sync before running `crucible search`, `crucible stats`, `crucible index`,
or `crucible manifest`.

### Search

The wiki graph database supports:
- Full-text keyword search (FTS5)
- Structured queries (backlinks, concept lookup, source provenance)
- Future: semantic/vector search

### Principles

- Never commit contents of `sources/external/` to version control
- Notebook entries and data in `sources/notebooks/` and `sources/data/` are shareable
- The database is always regenerable from the wiki files
- Articles should cite their sources and link to related concepts
- The LLM maintains the wiki; manual edits are the exception

### Concurrency Limitations

Crucible includes basic protections for concurrent access (SQLite WAL mode with
busy_timeout, file locking on references.bib, atomic file writes, transactional
sync), but it is **not designed for heavy concurrent writing**. Be aware of:

- **Same-article edits**: Two agents editing the same `.org` file simultaneously
  will clobber each other. Assign each agent to distinct articles to avoid this.
- **Sync during edits**: `crucible sync` skips files whose mtime changes during
  the scan, but rapid concurrent edits may still cause stale metadata. Re-run
  sync after edits settle.
- **Dropbox and network filesystems**: SQLite over Dropbox can produce
  "conflicted copy" files. Avoid running crucible from multiple machines
  against the same Dropbox-synced database simultaneously.
- **Embedding API**: Multiple concurrent `crucible embed` calls will avoid
  duplicate DB writes but may make redundant API calls to the embedding service.
