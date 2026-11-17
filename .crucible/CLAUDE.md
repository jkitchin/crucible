# Crucible: LLM-Compiled Knowledge Base

Everything lives inside `.crucible/`. Use the `crucible` CLI
to interact with the knowledge base.

## Layout

- `sources/external/` - Copyrighted material. Gitignored.
- `sources/notebooks/` - Your own ELN entries.
- `sources/data/` - Your own experimental data.
- `wiki/concepts/` - Concept articles (one topic per file)
- `wiki/summaries/` - Per-source summaries
- `wiki/comparisons/` - Cross-source comparison articles
- `wiki/methods/` - Methodology articles
- `wiki/index.org` - Auto-maintained master index
- `references.bib` - Bibliography; each entry's `file` field names its stored source
- `crucible.db` - Wiki graph database (SQLite)

## Conventions

- All wiki articles use org-mode with scimax conventions
- Each article's top heading has a properties drawer with `:ARTICLE_TYPE:`,
  `:SOURCE_KEYS:` (cite keys from references.bib), `:STATUS:`, `:CREATED:` and
  `:MODIFIED:` (update it on every edit)
- Use org-ref citations: `cite:key`, `citep:key`, `citet:key`
- Use `[[file:path][description]]` links for cross-references
- Write in narrative prose, not bulleted lists
- The database is regenerable from the wiki, references.bib and the source files
- Run `crucible sync` after writing or editing articles
- The LLM maintains the wiki; manual edits are the exception

Run `crucible help all` for the full CLI reference.
