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
- `crucible.db` - Wiki graph database (SQLite)

## Conventions

- All wiki articles use org-mode with scimax conventions
- Use org-ref citations: `cite:key`, `citep:key`, `citet:key`
- Use `[[file:path][description]]` links for cross-references
- Write in narrative prose, not bulleted lists
- The database is regenerable from the wiki files
- The LLM maintains the wiki; manual edits are the exception

Run `crucible help all` for the full CLI reference.
