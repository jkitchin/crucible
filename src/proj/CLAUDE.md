<!-- crucible-project -->
## Crucible Knowledge Base

This project has a Crucible knowledge base in `.crucible/`.
Use the `crucible` CLI to ingest sources, search, and maintain the wiki.

Layout: `.crucible/sources/` (primary sources), `.crucible/wiki/` (distilled articles),
`.crucible/crucible.db` (graph database).

Conventions: org-mode with scimax, org-ref citations, narrative prose.
Each article's top heading has :ARTICLE_TYPE:, :SOURCE_KEYS: (the cite keys
`crucible ingest` prints), :STATUS:, :CREATED: and :MODIFIED: (update it on
every edit). Run `crucible sync` after writing or editing articles;
it also regenerates wiki/MANIFEST.md and wiki/index.org.
The LLM maintains the wiki; manual edits are the exception.
Run `crucible help all` for the full CLI reference.
<!-- crucible-project -->
