"""Crucible wiki graph database.

Manages the libSQL database that tracks sources, articles, concepts,
and the links between them. Uses libsql-python for native vector
search support (F32_BLOB, vector_distance_cos, libsql_vector_idx).
"""

import json
from contextlib import contextmanager
from pathlib import Path

import libsql


class DictCursor:
    """Wrapper around a libsql cursor that yields dicts instead of tuples."""

    def __init__(self, cursor):
        self._cursor = cursor

    @property
    def description(self):
        return self._cursor.description

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None or self._cursor.description is None:
            return row
        return {col[0]: val for col, val in zip(self._cursor.description, row)}

    def fetchall(self):
        rows = self._cursor.fetchall()
        if not rows or self._cursor.description is None:
            return rows
        cols = [col[0] for col in self._cursor.description]
        return [{c: v for c, v in zip(cols, row)} for row in rows]

    def __iter__(self):
        rows = self._cursor.fetchall()
        if not rows or self._cursor.description is None:
            return
        cols = [col[0] for col in self._cursor.description]
        for row in rows:
            yield {c: v for c, v in zip(cols, row)}


class DictConnection:
    """Wrapper around a libsql connection that returns dicts from queries."""

    def __init__(self, conn):
        self._conn = conn
        self._in_transaction = False

    def execute(self, sql, params=()):
        return DictCursor(self._conn.execute(sql, params))

    def executemany(self, sql, params):
        return self._conn.executemany(sql, params)

    def commit(self):
        if not self._in_transaction:
            self._conn.commit()

    @contextmanager
    def transaction(self):
        """Explicit write transaction with automatic rollback on failure.

        Uses BEGIN IMMEDIATE to acquire a write lock upfront, preventing
        deadlocks when two connections each hold read locks. Individual
        commit() calls become no-ops inside the transaction.
        """
        self._conn.execute("BEGIN IMMEDIATE")
        self._in_transaction = True
        try:
            yield
            self._conn.commit()
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        finally:
            self._in_transaction = False

    def close(self):
        self._conn.close()

SCHEMA_VERSION = 2

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Primary sources (the raw inputs)
CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    source_type TEXT NOT NULL,
    shareable INTEGER NOT NULL DEFAULT 0,
    url TEXT,
    authors TEXT DEFAULT '[]',
    date TEXT,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    metadata TEXT DEFAULT '{}'
);

-- Distilled wiki articles
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    article_type TEXT NOT NULL,
    status TEXT DEFAULT 'draft',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    distill_model TEXT,
    abstract TEXT
);

-- Which sources an article was distilled from
CREATE TABLE IF NOT EXISTS article_sources (
    article_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
    source_id INTEGER REFERENCES sources(id) ON DELETE CASCADE,
    PRIMARY KEY (article_id, source_id)
);

-- Named concepts/topics
CREATE TABLE IF NOT EXISTS concepts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    aliases TEXT DEFAULT '[]'
);

-- Which concepts an article covers
CREATE TABLE IF NOT EXISTS article_concepts (
    article_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
    concept_id INTEGER REFERENCES concepts(id) ON DELETE CASCADE,
    PRIMARY KEY (article_id, concept_id)
);

-- Directed links between articles (the wiki graph)
CREATE TABLE IF NOT EXISTS article_links (
    from_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
    to_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
    context TEXT,
    PRIMARY KEY (from_id, to_id)
);

-- Full-text search over article content
CREATE VIRTUAL TABLE IF NOT EXISTS article_fts USING fts5(
    path, title, content, tokenize='porter unicode61'
);

-- Derivation chain (article Y was synthesized from articles A, B, C)
CREATE TABLE IF NOT EXISTS article_derivations (
    derived_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
    source_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
    derived_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (derived_id, source_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_articles_type ON articles(article_type);
CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status);
CREATE INDEX IF NOT EXISTS idx_sources_type ON sources(source_type);
CREATE INDEX IF NOT EXISTS idx_sources_date ON sources(date);
CREATE INDEX IF NOT EXISTS idx_article_links_to ON article_links(to_id);
"""


class CrucibleDB:
    """Interface to the Crucible wiki graph database (libSQL)."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        raw_conn = libsql.connect(str(db_path))
        raw_conn.execute("PRAGMA foreign_keys = ON")
        raw_conn.execute("PRAGMA journal_mode = WAL")
        raw_conn.execute("PRAGMA busy_timeout = 5000")
        self.conn = DictConnection(raw_conn)
        self._attached: dict[str, str] = {}

    def initialize(self):
        """Create all tables and set schema version."""
        for statement in SCHEMA_SQL.split(";"):
            statement = statement.strip()
            if statement:
                self.conn.execute(statement)
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_version (version) VALUES (?)",
            (SCHEMA_VERSION,),
        )
        self.conn.commit()
        self._migrate()

    def _migrate(self):
        """Run any pending migrations based on schema_version."""
        row = self.conn.execute(
            "SELECT MAX(version) as v FROM schema_version"
        ).fetchone()
        current = row["v"] if row else 0

        if current < 2:
            self._migrate_v2()
        # Future migrations:
        # if current < 3: self._migrate_v3()

    def _migrate_v2(self):
        """v2: Drop peers table (replaced by global registry)."""
        self.conn.execute("DROP TABLE IF EXISTS peers")
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_version (version) VALUES (2)"
        )
        self.conn.commit()

    def schema_version(self) -> int:
        """Return the current schema version."""
        try:
            row = self.conn.execute(
                "SELECT MAX(version) as v FROM schema_version"
            ).fetchone()
            return row["v"] if row else 0
        except (ValueError, Exception):
            # Table doesn't exist yet (pre-init database)
            return 0

    def close(self):
        self.conn.close()

    # -- Cross-crucible helpers --

    @staticmethod
    def _peer_db_path(peer_path: str) -> str | None:
        """Resolve the DB file for a peer, checking both layouts."""
        for subpath in (".crucible/crucible.db", "db/crucible.db"):
            candidate = Path(peer_path) / subpath
            if candidate.exists():
                return str(candidate)
        return None

    def attach_peer(self, name: str, db_path: str):
        """ATTACH another crucible's database for cross-querying."""
        if name in self._attached:
            return
        self.conn.execute(f"ATTACH DATABASE ? AS [{name}]", (db_path,))
        self._attached[name] = db_path

    # -- Cross-crucible queries --

    def _query_peer(self, peer_db_path: str, query_fn):
        """Open a separate connection to a peer and run a query function."""
        try:
            conn = DictConnection(libsql.connect(peer_db_path))
            results = query_fn(conn)
            conn.close()
            return results
        except Exception:
            return []

    def search_all(self, query: str, limit: int = 20,
                   peers: list[dict] | None = None) -> list[dict]:
        """FTS search across this crucible and peers.

        peers: list of dicts with 'name' and 'path' keys (from registry).
        """
        if peers is None:
            peers = []
        results = []
        # Local
        for r in self.search(query, limit=limit):
            r["_crucible"] = "local"
            results.append(r)
        # Peers (separate connections, since FTS5 MATCH doesn't work via ATTACH)
        for peer in peers:
            peer_db = self._peer_db_path(peer["path"])
            if not peer_db:
                continue
            name = peer["name"]

            def do_search(conn, _name=name):
                rows = []
                for row in conn.execute("""
                    SELECT a.*, rank
                    FROM article_fts fts
                    JOIN articles a ON a.id = fts.rowid
                    WHERE article_fts MATCH ?
                    ORDER BY rank
                    LIMIT ?
                """, (query, limit)):
                    r = dict(row)
                    r["_crucible"] = _name
                    rows.append(r)
                return rows

            results.extend(self._query_peer(peer_db, do_search))
        results.sort(key=lambda x: x.get("rank", 0))
        return results[:limit]

    def concepts_all(self, peers: list[dict] | None = None) -> list[dict]:
        """List concepts across this crucible and peers.

        peers: list of dicts with 'name' and 'path' keys (from registry).
        """
        if peers is None:
            peers = []
        results = []
        for r in self.list_concepts():
            r["_crucible"] = "local"
            results.append(r)
        for peer in peers:
            peer_db = self._peer_db_path(peer["path"])
            if not peer_db:
                continue
            name = peer["name"]

            def do_concepts(conn, _name=name):
                rows = []
                for row in conn.execute("""
                    SELECT c.*, COUNT(ac.article_id) as article_count
                    FROM concepts c
                    LEFT JOIN article_concepts ac ON c.id = ac.concept_id
                    GROUP BY c.id
                    ORDER BY article_count DESC
                """):
                    r = dict(row)
                    r["_crucible"] = _name
                    rows.append(r)
                return rows

            results.extend(self._query_peer(peer_db, do_concepts))
        return results

    # -- Derivations --

    def add_derivation(self, derived_id: int, source_id: int):
        """Record that derived_id was synthesized from source_id."""
        self.conn.execute(
            "INSERT OR IGNORE INTO article_derivations (derived_id, source_id) VALUES (?, ?)",
            (derived_id, source_id),
        )
        self.conn.commit()

    def derived_from(self, article_path: str) -> list[dict]:
        """What articles was this one derived from?"""
        return [dict(r) for r in self.conn.execute("""
            SELECT a.*, ad.derived_at
            FROM article_derivations ad
            JOIN articles a ON a.id = ad.source_id
            JOIN articles target ON target.id = ad.derived_id
            WHERE target.path = ?
            ORDER BY ad.derived_at
        """, (article_path,))]

    def derivatives(self, article_path: str) -> list[dict]:
        """What articles were derived from this one?"""
        return [dict(r) for r in self.conn.execute("""
            SELECT a.*, ad.derived_at
            FROM article_derivations ad
            JOIN articles a ON a.id = ad.derived_id
            JOIN articles source ON source.id = ad.source_id
            WHERE source.path = ?
            ORDER BY ad.derived_at
        """, (article_path,))]

    # -- Sources --

    def add_source(self, path: str, title: str, source_type: str,
                   shareable: bool = False,
                   url: str | None = None, authors: list[str] | None = None,
                   date: str | None = None, metadata: dict | None = None) -> int:
        cur = self.conn.execute(
            """INSERT INTO sources (path, title, source_type, shareable, url, authors, date, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (path, title, source_type, int(shareable), url,
             json.dumps(authors or []), date, json.dumps(metadata or {})),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_source(self, source_id: int) -> dict | None:
        row = self.conn.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
        return dict(row) if row else None

    def list_sources(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute("SELECT * FROM sources ORDER BY ingested_at DESC")]

    def delete_source(self, source_id: int):
        self.conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
        self.conn.commit()

    # -- Articles --

    def add_article(self, path: str, title: str, article_type: str,
                    distill_model: str | None = None, abstract: str | None = None,
                    content: str = "") -> int:
        cur = self.conn.execute(
            """INSERT INTO articles (path, title, article_type, distill_model, abstract)
               VALUES (?, ?, ?, ?, ?)""",
            (path, title, article_type, distill_model, abstract),
        )
        article_id = cur.lastrowid
        # Index content for FTS
        if content:
            self.conn.execute(
                "INSERT INTO article_fts (rowid, path, title, content) VALUES (?, ?, ?, ?)",
                (article_id, path, title, content),
            )
        self.conn.commit()
        return article_id

    def get_article(self, article_id: int) -> dict | None:
        row = self.conn.execute("SELECT * FROM articles WHERE id = ?", (article_id,)).fetchone()
        return dict(row) if row else None

    def get_article_by_path(self, path: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM articles WHERE path = ?", (path,)).fetchone()
        return dict(row) if row else None

    def list_articles(self, article_type: str | None = None) -> list[dict]:
        if article_type:
            return [dict(r) for r in self.conn.execute(
                "SELECT * FROM articles WHERE article_type = ? ORDER BY updated_at DESC",
                (article_type,))]
        return [dict(r) for r in self.conn.execute("SELECT * FROM articles ORDER BY updated_at DESC")]

    def update_article_fts(self, article_id: int, path: str, title: str, content: str):
        """Update FTS index for an article (delete old, insert new)."""
        self.conn.execute("DELETE FROM article_fts WHERE rowid = ?", (article_id,))
        self.conn.execute(
            "INSERT INTO article_fts (rowid, path, title, content) VALUES (?, ?, ?, ?)",
            (article_id, path, title, content),
        )
        self.conn.commit()

    def delete_article(self, article_id: int):
        """Delete an article and all its relationships."""
        self.conn.execute("DELETE FROM article_fts WHERE rowid = ?", (article_id,))
        self.conn.execute("DELETE FROM embeddings WHERE article_id = ?", (article_id,))
        self.conn.execute("DELETE FROM articles WHERE id = ?", (article_id,))
        self.conn.commit()

    # -- Concepts --

    def add_concept(self, name: str, aliases: list[str] | None = None) -> int:
        # Atomic upsert: insert-if-missing, then select. Safe under concurrency.
        self.conn.execute(
            "INSERT OR IGNORE INTO concepts (name, aliases) VALUES (?, ?)",
            (name, json.dumps(aliases or [])),
        )
        row = self.conn.execute(
            "SELECT id FROM concepts WHERE name = ?", (name,)
        ).fetchone()
        self.conn.commit()
        return row["id"]

    def list_concepts(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute("""
            SELECT c.*, COUNT(ac.article_id) as article_count
            FROM concepts c
            LEFT JOIN article_concepts ac ON c.id = ac.concept_id
            GROUP BY c.id
            ORDER BY article_count DESC
        """)]

    # -- Relationships --

    def link_article_source(self, article_id: int, source_id: int):
        self.conn.execute(
            "INSERT OR IGNORE INTO article_sources (article_id, source_id) VALUES (?, ?)",
            (article_id, source_id),
        )
        self.conn.commit()

    def link_article_concept(self, article_id: int, concept_id: int):
        self.conn.execute(
            "INSERT OR IGNORE INTO article_concepts (article_id, concept_id) VALUES (?, ?)",
            (article_id, concept_id),
        )
        self.conn.commit()

    def add_article_link(self, from_id: int, to_id: int, context: str | None = None):
        self.conn.execute(
            "INSERT OR IGNORE INTO article_links (from_id, to_id, context) VALUES (?, ?, ?)",
            (from_id, to_id, context),
        )
        self.conn.commit()

    # -- Queries --

    def search(self, query: str, limit: int = 20) -> list[dict]:
        """Full-text search over article content."""
        return [dict(r) for r in self.conn.execute("""
            SELECT a.*, rank
            FROM article_fts fts
            JOIN articles a ON a.id = fts.rowid
            WHERE article_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """, (query, limit))]

    def backlinks(self, article_path: str) -> list[dict]:
        """Articles that link TO the given article."""
        return [dict(r) for r in self.conn.execute("""
            SELECT a.*, al.context
            FROM article_links al
            JOIN articles a ON a.id = al.from_id
            JOIN articles target ON target.id = al.to_id
            WHERE target.path = ?
            ORDER BY a.title
        """, (article_path,))]

    def source_articles(self, source_id: int) -> list[dict]:
        """Articles that were distilled from a given source."""
        return [dict(r) for r in self.conn.execute("""
            SELECT a.*
            FROM article_sources asrc
            JOIN articles a ON a.id = asrc.article_id
            WHERE asrc.source_id = ?
            ORDER BY a.title
        """, (source_id,))]

    def get_source_by_path(self, path: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM sources WHERE path = ?", (path,)).fetchone()
        return dict(row) if row else None

    def article_sources(self, article_path: str) -> list[dict]:
        """Sources that a given article was distilled from."""
        return [dict(r) for r in self.conn.execute("""
            SELECT s.*
            FROM article_sources asrc
            JOIN sources s ON s.id = asrc.source_id
            JOIN articles a ON a.id = asrc.article_id
            WHERE a.path = ?
        """, (article_path,))]

    def concept_articles(self, concept_name: str) -> list[dict]:
        """Articles covering a given concept."""
        return [dict(r) for r in self.conn.execute("""
            SELECT a.*
            FROM article_concepts ac
            JOIN articles a ON a.id = ac.article_id
            JOIN concepts c ON c.id = ac.concept_id
            WHERE c.name = ?
            ORDER BY a.title
        """, (concept_name,))]

    def orphans(self) -> list[dict]:
        """Articles with no incoming links."""
        return [dict(r) for r in self.conn.execute("""
            SELECT a.*
            FROM articles a
            LEFT JOIN article_links al ON a.id = al.to_id
            WHERE al.from_id IS NULL
            ORDER BY a.title
        """)]

    def undigested(self) -> list[dict]:
        """Sources with no articles distilled from them."""
        return [dict(r) for r in self.conn.execute("""
            SELECT s.*
            FROM sources s
            LEFT JOIN article_sources asrc ON s.id = asrc.source_id
            WHERE asrc.article_id IS NULL
            ORDER BY s.ingested_at DESC
        """)]

    def graph_dot(self) -> str:
        """Export the article link graph in DOT format."""
        lines = ["digraph crucible {", "  rankdir=LR;", "  node [shape=box];"]
        for row in self.conn.execute("""
            SELECT a1.path as from_path, a1.title as from_title,
                   a2.path as to_path, a2.title as to_title
            FROM article_links al
            JOIN articles a1 ON a1.id = al.from_id
            JOIN articles a2 ON a2.id = al.to_id
        """):
            from_label = row["from_title"].replace('"', '\\"')
            to_label = row["to_title"].replace('"', '\\"')
            lines.append(f'  "{from_label}" -> "{to_label}";')
        lines.append("}")
        return "\n".join(lines)

    # -- Implied / computed relationships --

    def related(self, article_path: str) -> list[dict]:
        """Compute implied relationships for an article.

        Finds articles related through:
        - shared concepts (both cover the same topic)
        - shared sources (both distilled from the same source)
        - temporal proximity (sources dated within 7 days of each other)
        - explicit links (direct references, included for completeness)

        Returns a list of related articles with relationship type and strength.
        """
        article = self.get_article_by_path(article_path)
        if not article:
            return []
        aid = article["id"]
        related = {}

        # Explicit links (outgoing and incoming)
        for row in self.conn.execute("""
            SELECT a.*, 'explicit_outgoing' as rel_type
            FROM article_links al JOIN articles a ON a.id = al.to_id
            WHERE al.from_id = ?
            UNION
            SELECT a.*, 'explicit_incoming' as rel_type
            FROM article_links al JOIN articles a ON a.id = al.from_id
            WHERE al.to_id = ?
        """, (aid, aid)):
            r = dict(row)
            key = r["path"]
            if key not in related:
                related[key] = {**r, "reasons": []}
            related[key]["reasons"].append(r["rel_type"])

        # Shared concepts
        for row in self.conn.execute("""
            SELECT DISTINCT a.*, c.name as shared_concept
            FROM article_concepts ac1
            JOIN article_concepts ac2 ON ac1.concept_id = ac2.concept_id
                AND ac2.article_id != ac1.article_id
            JOIN articles a ON a.id = ac2.article_id
            JOIN concepts c ON c.id = ac1.concept_id
            WHERE ac1.article_id = ?
        """, (aid,)):
            r = dict(row)
            key = r["path"]
            if key not in related:
                related[key] = {**r, "reasons": []}
            related[key]["reasons"].append(f"shared_concept:{r['shared_concept']}")

        # Shared sources
        for row in self.conn.execute("""
            SELECT DISTINCT a.*, s.title as shared_source
            FROM article_sources as1
            JOIN article_sources as2 ON as1.source_id = as2.source_id
                AND as2.article_id != as1.article_id
            JOIN articles a ON a.id = as2.article_id
            JOIN sources s ON s.id = as1.source_id
            WHERE as1.article_id = ?
        """, (aid,)):
            r = dict(row)
            key = r["path"]
            if key not in related:
                related[key] = {**r, "reasons": []}
            related[key]["reasons"].append(f"shared_source:{r['shared_source']}")

        # Temporal proximity (articles whose sources are dated within 7 days)
        for row in self.conn.execute("""
            SELECT DISTINCT a2.*, s1.date as my_date, s2.date as their_date
            FROM article_sources as1
            JOIN sources s1 ON s1.id = as1.source_id
            JOIN sources s2 ON s2.date IS NOT NULL AND s1.date IS NOT NULL
                AND ABS(julianday(s1.date) - julianday(s2.date)) <= 7
                AND s2.id != s1.id
            JOIN article_sources as2 ON as2.source_id = s2.id
                AND as2.article_id != as1.article_id
            JOIN articles a2 ON a2.id = as2.article_id
            WHERE as1.article_id = ?
        """, (aid,)):
            r = dict(row)
            key = r["path"]
            if key not in related:
                related[key] = {**r, "reasons": []}
            related[key]["reasons"].append(
                f"temporal:{r['my_date']}<->{r['their_date']}"
            )

        # Sort by number of reasons (more connections = more related)
        result = sorted(related.values(), key=lambda x: len(x["reasons"]), reverse=True)
        return result

    # -- Index / lint helpers --

    def articles_by_concept(self) -> dict[str, list[dict]]:
        """Return articles grouped by concept name."""
        result: dict[str, list[dict]] = {}
        for row in self.conn.execute("""
            SELECT c.name as concept, a.*
            FROM article_concepts ac
            JOIN concepts c ON c.id = ac.concept_id
            JOIN articles a ON a.id = ac.article_id
            ORDER BY c.name, a.title
        """):
            r = dict(row)
            concept = r.pop("concept")
            result.setdefault(concept, []).append(r)
        return result

    def articles_by_type(self) -> dict[str, list[dict]]:
        """Return articles grouped by article_type."""
        result: dict[str, list[dict]] = {}
        for row in self.conn.execute(
            "SELECT * FROM articles ORDER BY article_type, title"
        ):
            r = dict(row)
            result.setdefault(r["article_type"], []).append(r)
        return result

    def all_article_links(self) -> list[dict]:
        """Return all article links with paths."""
        return [dict(r) for r in self.conn.execute("""
            SELECT a1.path as from_path, a2.path as to_path,
                   a1.title as from_title, a2.title as to_title
            FROM article_links al
            JOIN articles a1 ON a1.id = al.from_id
            JOIN articles a2 ON a2.id = al.to_id
        """)]

    def concepts_without_articles(self) -> list[str]:
        """Concepts that are referenced in tags but have no dedicated concept article."""
        # Get all concept names
        all_concepts = {r["name"] for r in self.conn.execute("SELECT name FROM concepts")}
        # Get concepts that have a dedicated concept-type article
        covered = set()
        for row in self.conn.execute("""
            SELECT DISTINCT c.name
            FROM article_concepts ac
            JOIN concepts c ON c.id = ac.concept_id
            JOIN articles a ON a.id = ac.article_id
            WHERE a.article_type = 'concept'
        """):
            covered.add(row["name"])
        return sorted(all_concepts - covered)

    def single_concept_articles(self) -> list[dict]:
        """Articles that cover only one concept (potential merge candidates)."""
        return [dict(r) for r in self.conn.execute("""
            SELECT a.*, COUNT(ac.concept_id) as concept_count
            FROM articles a
            JOIN article_concepts ac ON a.id = ac.article_id
            GROUP BY a.id
            HAVING concept_count = 1
            ORDER BY a.title
        """)]

    # -- Stats --

    def stats(self) -> dict:
        """Return summary statistics."""
        result = {}
        for table in ("sources", "articles", "concepts", "article_links"):
            row = self.conn.execute(f"SELECT COUNT(*) as n FROM {table}").fetchone()
            result[table] = row["n"]
        return result
