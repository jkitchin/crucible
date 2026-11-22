"""Crucible wiki graph database.

Manages the libSQL database that tracks sources, articles, concepts,
and the links between them. Uses libsql-python for native vector
search support (F32_BLOB, vector_distance_cos, libsql_vector_idx).
"""

import json
import re
import unicodedata
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

import libsql


def _concept_key(name: str) -> str:
    """Normalize a concept name, slug or title for comparison.

    Case, whitespace, underscores, dashes (any kind) and apostrophes are
    ignored, so the tag ``activation_energy``, the slug
    ``activation-energy`` and the title "Activation Energy" agree. Other
    punctuation is kept, so ``c``, ``c++`` and ``c#`` stay distinct. Letters
    of any script are kept, so a non-ASCII concept such as ``触媒`` has a
    key of its own rather than an empty one.
    """
    return "".join(
        ch for ch in name.casefold()
        if not (ch.isspace() or ch in "_'\u2019"
                or unicodedata.category(ch) == "Pd")
    )


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
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        finally:
            self._in_transaction = False

    def close(self):
        self._conn.close()

SCHEMA_VERSION = 3

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

-- The articles with chemical subscripts, once more with them joined
-- (see fts_joined), so CO2 finds CO_2
CREATE VIRTUAL TABLE IF NOT EXISTS article_fts_joined USING fts5(
    title, content, joined, tokenize='porter unicode61'
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


def _fts_literal(token: str) -> str:
    """An FTS5 string literal for token (double quotes doubled)."""
    return '"' + token.replace('"', '""') + '"'


# A chemistry-style subscript: org's X_2 / X_{2} after a letter, digit or
# ")". Only all-digit subscripts count, and the bare form only when what
# follows cannot continue the subscript (org reads X_2b as subscript "2b"),
# except an uppercase letter, the next element in H_2O. So CO_2, TiO_{2},
# (NH_4)_2SO_4 and Fe_2O_3 are written CO2, TiO2, (NH4)2SO4 and Fe2O3, while
# snake_case names (file_2024_report, foo_bar) are left alone.
_SUBSCRIPT_RE = re.compile(r"(?<=[^\W_]|\))_(?:\{(\d+)\}|(\d+)(?![a-z0-9_]))")
_UNICODE_SUBSCRIPTS = str.maketrans("\u2080\u2081\u2082\u2083\u2084\u2085\u2086\u2087\u2088\u2089",
                                    "0123456789")


def join_subscripts(text: str) -> str:
    """TEXT with chemical subscripts joined: CO_2, CO_{2} and CO\u2082 become CO2."""
    if not text:
        return text
    text = text.translate(_UNICODE_SUBSCRIPTS)
    return _SUBSCRIPT_RE.sub(lambda m: m.group(1) or m.group(2), text)


def fts_joined(title: str, content: str) -> str:
    """The paragraphs of TITLE and CONTENT that have a chemical subscript,
    with it joined.

    The unicode61 tokenizer splits words at "_", so CO_2 is indexed as "co"
    and "2" and a search for CO2 would miss it. article_fts holds the title
    and content as written, as it always has; an article with a subscript
    also has a row in article_fts_joined, which holds its title, content
    and these paragraphs (in the "joined" column). CO2 and the phrase "CO2
    reduction" then find CO_2, CO_{2} and CO\u2082, while step_1 is still
    found by step. Since article_fts is unchanged, so are the rows it finds
    and their ranking: the rows only article_fts_joined finds come after.
    """
    joined = []
    for text in (title, content):
        for para in re.split(r"\n\s*\n", text or ""):
            j = join_subscripts(para)
            if j != para:
                joined.append(j)
    return "\n\n".join(joined)


def fts_has_joined(conn) -> bool:
    """Whether conn's database has article_fts_joined (schema v3).

    A database made by an older version has none until `crucible update`
    migrates it, and a peer's may never be migrated.
    """
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name = 'article_fts_joined'"
    ).fetchone() is not None


# A query term: a "quoted phrase" (straight or curly quotes) starting a
# word, or else a run of anything up to whitespace. A quote with no partner,
# or inside a word (a"b), is literal text, which the tokenizer drops.
_QUERY_TERM_RE = re.compile(
    r'(?<!\S)(?:"([^"]*)"|\u201c([^\u201c\u201d]*)\u201d)|(\S+)')


def _query_terms(raw: str) -> list[tuple[bool, str, str]]:
    """Split a raw query into (is_phrase, text, unnormalized text) terms.

    Control characters are dropped first: a NUL terminates the string at
    the SQLite C layer ("unterminated string"), and the others contribute
    nothing to a text search. Terms with no letter or digit are dropped.
    """
    cleaned = "".join(ch for ch in raw if ch >= " " or ch in "\t\n\r")
    terms = []
    for m in _QUERY_TERM_RE.finditer(cleaned):
        phrase = m.group(1) if m.group(1) is not None else m.group(2)
        orig = phrase if phrase is not None else m.group(3)
        if phrase is not None:
            orig = " ".join(orig.split())
        if not any(ch.isalnum() for ch in orig.rstrip("*")):
            continue  # "*", "&", "--", "": nothing the tokenizer would index
        terms.append((phrase is not None, join_subscripts(orig), orig))
    return terms


def _fts_query(raw: str, broad: bool = False, joined: bool = False,
               joined_column: bool | None = None) -> str:
    """Turn a raw user query into a safe FTS5 MATCH expression.

    A "quoted phrase" is searched as a phrase: its words in that order,
    next to each other. A quote with no partner is ignored.
    Every other whitespace-separated token is wrapped as a quoted string
    literal so FTS5 query operators in user input (-, :, parentheses, the
    bare keywords AND/OR/NOT/NEAR) are treated as ordinary text rather than
    crashing the parser. A token ending in * is a prefix query (catal*
    matches catalysis); a * inside a phrase is not.
    The porter tokenizer stems indexed words and the prefix alike, and a
    stem can be shorter than the prefix ("oxidation" is indexed as "oxid",
    so oxidat* finds nothing) or differ from it ("catalys" stems to
    "catali", not a prefix of the indexed "catalysi"). Stemming only
    rewrites the end of a word, and leaves words of two letters alone, so
    with `broad` the query also asks for the prefix's first two characters,
    which every matching word's stem starts with (of the last word, in a
    token the tokenizer splits, such as a-b*). That over-matches, often
    most of the wiki, so fts_rows asks for it only when the plain prefix
    finds too few rows, and keeps only rows that really contain a word
    starting with each prefix.
    A token with no letter or digit in it ("&", "-", "/", an en dash) is
    dropped: the tokenizer reads it as an empty phrase, which, ANDed with
    the rest, would make every search return nothing.
    Without `joined` the terms are asked for as typed, for article_fts,
    which then finds and ranks rows exactly as it always has. With
    `joined` (for article_fts_joined, see fts_joined) a term with a
    chemical subscript is asked for both as typed and joined, so CO_2,
    CO_{2}, CO\u2082 and CO2 each find all four. The match covers
    article_fts_joined's joined column when `joined_column` (by default
    `joined`) is set.

    The match is limited to the title and content columns: the path column
    holds ".crucible/wiki/concepts/x.org", whose words would otherwise match
    every article. A column filter rather than an UNINDEXED schema keeps
    this working against peers whose index predates the change.

    Returns "" when the query has no usable tokens, in which case callers
    should skip the search and return no results.
    """
    terms = []
    for is_phrase, token, orig in _query_terms(raw):
        if is_phrase:
            # As typed, and joined, which matches article_fts_joined
            # (fts_joined): "CO2 reduction" finds each way of writing it.
            alts = list(dict.fromkeys((orig, token) if joined else (orig,)))
            terms.append(_fts_literal(alts[0]) if len(alts) == 1 else
                         "(" + " OR ".join(_fts_literal(a) for a in alts) + ")")
            continue
        alts = []
        for tok in dict.fromkeys((token, orig) if joined else (orig,)):
            stem = tok.rstrip("*")
            if stem == tok:
                alts.append(_fts_literal(tok))
                continue
            # Only the last word of the token is the prefix ("a-b*" is the
            # phrase a, then a word starting with b); cut it to two characters.
            # The full prefix is asked for too, so rows that match it rank
            # as they would without the short one.
            last = re.search(r"[^\W_]+$", stem)
            if broad and last and len(last.group(0)) > 2:
                short = stem[:last.start() + 2]
                alts.append(f"{_fts_literal(stem)}* OR {_fts_literal(short)}*")
            else:
                alts.append(f"{_fts_literal(stem)}*")
        terms.append(alts[0] if len(alts) == 1 and " OR " not in alts[0]
                     else "(" + " OR ".join(alts) + ")")
    if not terms:
        return ""
    # Explicit AND: FTS5 allows implicit AND only between plain phrases.
    if joined_column is None:
        joined_column = joined
    columns = "{title content joined}" if joined_column else "{title content}"
    return columns + " : (" + " AND ".join(terms) + ")"


def _fold(text: str) -> str:
    """Casefolded text without diacritics, as unicode61 compares words."""
    decomposed = unicodedata.normalize("NFKD", text.casefold())
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def _fts_prefix_res(raw: str, joined: bool = False) -> list[list[re.Pattern]]:
    """For each prefix token (catal*) in a query, patterns for a word starting with it.

    A row matches the token if any of its patterns matches: the token as
    typed (file_2024*, which fts_joined leaves alone), or, with `joined`,
    also joined as in fts_joined (CO2*), as _fts_query asks.
    """
    patterns = []
    for is_phrase, token, orig in _query_terms(raw):
        if is_phrase or token.rstrip("*") == token:
            continue
        alts = []
        for tok in dict.fromkeys((token, orig) if joined else (orig,)):
            # A word starts after anything that is not a letter or digit
            # (unicode61 splits words there, underscores included).
            # A quote left open ("catal*) is not part of the word.
            stem = re.sub(r"^[\W_]+", "", tok.rstrip("*"))
            alts.append(re.compile(r"(?<![^\W_])" + re.escape(_fold(stem))))
        patterns.append(alts)
    return patterns


def fts_rows(conn, query: str, limit: int, tiers: bool = False) -> list[dict]:
    """Rows of articles matching a raw search query, best first, at most limit.

    Each row is an article with its FTS rank. A prefix token is matched
    exactly: the FTS query for it can be broader or narrower than the
    prefix (see _fts_query), so rows are kept only when their title or
    content has a word that starts with it. The broad query, which can
    reach most of the wiki, is run only when the plain one finds fewer
    than `limit` rows; the rows only it finds come after the others.
    With `tiers`, each row has "_fts_tier": 0 for a row article_fts finds,
    as it always has, and 1 for one only the joined subscripts find, so
    results from several crucibles can be merged with the tier-1 rows last.
    """
    match = _fts_query(query)
    if not match:
        return []
    prefixes = _fts_prefix_res(query)
    joined_prefixes = _fts_prefix_res(query, joined=True)
    sql = """
        SELECT a.*, rank, fts.title AS _fts_title, fts.content AS _fts_content{extra}
        FROM {table} fts
        JOIN articles a ON a.id = fts.rowid
        WHERE {table} MATCH ?
        ORDER BY rank
    """
    # (table, match, tier): article_fts first, which finds and ranks rows
    # as it always has (tier 0); then the rows only the joined subscripts
    # find (tier 1, see fts_joined): CO2 written in an article, for CO_2
    # typed, from article_fts; CO_2 written, for CO2 typed, from
    # article_fts_joined.
    searches = [("article_fts", match, 0)]
    if prefixes:
        searches.append(("article_fts", _fts_query(query, broad=True), 0))
    # Joining only glues subscript digits to the word before them, so a
    # query with no digit finds nothing new in the joined text: it gets
    # tier 0 alone, as on older versions.
    if any(ch.isdigit() for ch in query):
        searches.append(("article_fts", _fts_query(query, joined=True,
                                                   joined_column=False), 1))
        if prefixes:
            searches.append(("article_fts", _fts_query(
                query, broad=True, joined=True, joined_column=False), 1))
        if fts_has_joined(conn) and conn.execute(
                "SELECT 1 FROM article_fts_joined LIMIT 1").fetchone():
            searches.append(("article_fts_joined", _fts_query(query, joined=True), 1))
            if prefixes:
                searches.append(("article_fts_joined",
                                 _fts_query(query, broad=True, joined=True), 1))
    rows: list[dict] = []
    seen: set = set()
    # With prefixes, each query's rows are kept: a tier-1 search that asks
    # the same (a query with no subscript) re-checks the rows tier 0
    # rejected against the joined text, without running the query again.
    # Folded text is kept per article, as several searches can reach it.
    fetched: dict = {}
    folded: dict = {}
    # article_fts_joined stores the joined text, so it is not made again.
    extra = {"article_fts": "", "article_fts_joined": ", fts.joined AS _fts_joined"}
    for table, query_match, tier in searches:
        if (table, query_match) in fetched:
            if not prefixes:
                continue  # the query has no subscript: tier 1 asks the same
            found = fetched[(table, query_match)]
        elif prefixes:
            found = fetched[(table, query_match)] = [dict(row) for row in conn.execute(
                sql.format(table=table, extra=extra[table]), (query_match,))]
        else:
            fetched[(table, query_match)] = None
            # The FTS match is exact, so SQL can stop at limit, past the
            # rows already found.
            found = conn.execute(sql.format(table=table, extra=extra[table]) + " LIMIT ?",
                                 (query_match, limit + len(seen)))
        for row in found:
            r = dict(row)
            title, content = r.pop("_fts_title") or "", r.pop("_fts_content") or ""
            stored_joined = r.pop("_fts_joined", None)
            if r["id"] in seen:
                continue
            if prefixes:
                key = r["id"]  # both tables hold the same title and content
                if key not in folded:
                    folded[key] = [_fold(title + "\n" + content), None]
                text = folded[key][0]
                wanted = prefixes
                if tier:
                    if folded[key][1] is None:
                        folded[key][1] = _fold(stored_joined if stored_joined is not None
                                               else fts_joined(title, content))
                    text = text + "\n" + folded[key][1]
                    wanted = joined_prefixes
                if not all(any(p.search(text) for p in alts) for alts in wanted):
                    continue
            if tiers:
                r["_fts_tier"] = tier
            seen.add(r["id"])
            rows.append(r)
            if len(rows) >= limit:
                return rows
    return rows


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
        # Why peers were left out of the last cross-crucible query.
        self.peer_warnings: list[str] = []
        # (stamp, {concept key: [concept ids]}); see _concept_ids_by_key.
        self._concept_key_cache: tuple | None = None

    def initialize(self):
        """Create all tables, run pending migrations and set schema version.

        The version on disk is read before the new one is recorded, so an
        older database is migrated; a brand-new one has nothing to migrate
        and is simply stamped with SCHEMA_VERSION.
        """
        current = self._stored_version()
        for statement in SCHEMA_SQL.split(";"):
            statement = statement.strip()
            if statement:
                self.conn.execute(statement)
        self.conn.commit()
        if current:
            self._migrate(current)
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_version (version) VALUES (?)",
            (SCHEMA_VERSION,),
        )
        self.conn.commit()

    def _stored_version(self) -> int:
        """The schema version of the database as found on disk.

        0 for a brand-new (empty) database. One with crucible tables but no
        recorded version predates versioning and counts as v1.
        """
        version = self.schema_version()
        if version:
            return version
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table'"
            " AND name IN ('articles', 'sources', 'peers')"
        ).fetchone()
        return 1 if row and row["n"] else 0

    def _migrate(self, current: int):
        """Run the migrations from version `current` up to SCHEMA_VERSION."""
        if current < 2:
            self._migrate_v2()
        if current < 3:
            self._migrate_v3()
        # Future migrations:
        # if current < 4: self._migrate_v4()

    def _migrate_v2(self):
        """v2: Drop peers table (replaced by global registry)."""
        self.conn.execute("DROP TABLE IF EXISTS peers")
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_version (version) VALUES (2)"
        )
        self.conn.commit()

    def _migrate_v3(self):
        """v3: Fill article_fts_joined (see fts_joined); article_fts is unchanged."""
        with self.conn.transaction():
            self.conn.execute("DELETE FROM article_fts_joined")
            for row in self.conn.execute(
                    "SELECT rowid, title, content FROM article_fts").fetchall():
                self._insert_joined(row["rowid"], row["title"], row["content"])
            self.conn.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (3)"
            )

    def schema_version(self) -> int:
        """Return the current schema version."""
        try:
            row = self.conn.execute(
                "SELECT MAX(version) as v FROM schema_version"
            ).fetchone()
            return (row["v"] or 0) if row else 0
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

    def _query_peer(self, peer_db_path: str, query_fn, name: str = "?"):
        """Open a separate connection to a peer and run a query function.

        A peer that cannot be queried contributes nothing; why is recorded
        in ``peer_warnings``.
        """
        try:
            conn = DictConnection(libsql.connect(peer_db_path))
            try:
                return query_fn(conn)
            finally:
                conn.close()
        except Exception as e:
            self.peer_warnings.append(f"Peer '{name}' skipped: {e}")
            return []

    def _peer_db_or_warn(self, peer: dict) -> str | None:
        db = self._peer_db_path(peer["path"])
        if not db:
            self.peer_warnings.append(
                f"Peer '{peer['name']}' skipped: no crucible database at {peer['path']}."
            )
        return db

    def search_all(self, query: str, limit: int = 20,
                   peers: list[dict] | None = None) -> list[dict]:
        """FTS search across this crucible and peers.

        peers: list of dicts with 'name' and 'path' keys (from registry).
        """
        if peers is None:
            peers = []
        self.peer_warnings = []
        match = _fts_query(query)
        if not match:
            return []
        results = []
        # Local
        for r in fts_rows(self.conn, query, limit, tiers=True):
            r["_crucible"] = "local"
            results.append(r)
        # Peers (separate connections, since FTS5 MATCH doesn't work via ATTACH)
        for peer in peers:
            peer_db = self._peer_db_or_warn(peer)
            if not peer_db:
                continue
            name = peer["name"]

            def do_search(conn, _name=name):
                rows = fts_rows(conn, query, limit, tiers=True)
                for r in rows:
                    r["_crucible"] = _name
                return rows

            results.extend(self._query_peer(peer_db, do_search, name))
        # Rows article_fts finds, ranked as they always were, then the rows
        # only the joined subscripts find: their ranks come from another
        # table and do not compare with the others.
        results.sort(key=lambda x: (x.pop("_fts_tier", 0), x.get("rank", 0)))
        return results[:limit]

    def concepts_all(self, peers: list[dict] | None = None) -> list[dict]:
        """List concepts across this crucible and peers.

        peers: list of dicts with 'name' and 'path' keys (from registry).
        """
        if peers is None:
            peers = []
        self.peer_warnings = []
        results = []
        for r in self.list_concepts():
            r["_crucible"] = "local"
            results.append(r)
        for peer in peers:
            peer_db = self._peer_db_or_warn(peer)
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

            results.extend(self._query_peer(peer_db, do_concepts, name))
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

    _SOURCE_COLUMNS = ("path", "title", "source_type", "shareable", "url",
                       "authors", "date", "metadata")

    def update_source(self, source_id: int, **fields):
        """Update a source row's columns; authors and metadata are JSON-encoded."""
        unknown = set(fields) - set(self._SOURCE_COLUMNS)
        if unknown:
            raise ValueError(f"Unknown source columns: {sorted(unknown)}")
        if not fields:
            return
        values = []
        for name, value in fields.items():
            if name in ("authors", "metadata"):
                value = json.dumps(value or ([] if name == "authors" else {}))
            elif name == "shareable":
                value = int(value)
            values.append(value)
        assignments = ", ".join(f"{name} = ?" for name in fields)
        self.conn.execute(
            f"UPDATE sources SET {assignments} WHERE id = ?", (*values, source_id)
        )
        self.conn.commit()

    def merge_source_into(self, duplicate_id: int, keep_id: int):
        """Move a duplicate source's article links onto another source, then delete it."""
        self.conn.execute(
            """INSERT OR IGNORE INTO article_sources (article_id, source_id)
               SELECT article_id, ? FROM article_sources WHERE source_id = ?""",
            (keep_id, duplicate_id),
        )
        self.conn.execute("DELETE FROM article_sources WHERE source_id = ?", (duplicate_id,))
        self.conn.execute("DELETE FROM sources WHERE id = ?", (duplicate_id,))
        self.conn.commit()

    def delete_source(self, source_id: int):
        self.conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
        self.conn.commit()

    # -- Articles --

    def add_article(self, path: str, title: str, article_type: str,
                    distill_model: str | None = None, abstract: str | None = None,
                    content: str = "", status: str = "draft",
                    created_at: str | None = None, updated_at: str | None = None) -> int:
        """Insert an article. Timestamps default to now when not given."""
        cur = self.conn.execute(
            """INSERT INTO articles (path, title, article_type, distill_model, abstract, status,
                                     created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?,
                       COALESCE(?, datetime('now')), COALESCE(?, datetime('now')))""",
            (path, title, article_type, distill_model, abstract, status,
             created_at, updated_at),
        )
        article_id = cur.lastrowid
        # Index for FTS even when content is empty: sync compares the indexed
        # row with the file, and a missing row reads as a change.
        self._insert_fts(article_id, path, title, content)
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

    def update_article(self, article_id: int, title: str, article_type: str, status: str,
                       distill_model: str | None = None, abstract: str | None = None):
        """Update an article's metadata from its file's front matter and properties."""
        self.conn.execute(
            """UPDATE articles SET title = ?, article_type = ?, status = ?,
                      distill_model = ?, abstract = ?, updated_at = datetime('now')
               WHERE id = ?""",
            (title, article_type, status, distill_model, abstract, article_id),
        )
        self.conn.commit()

    def set_article_dates(self, article_id: int, created_at: str | None = None,
                          updated_at: str | None = None):
        """Set an article's timestamps; a None argument leaves that one as is."""
        self.conn.execute(
            """UPDATE articles SET created_at = COALESCE(?, created_at),
                                   updated_at = COALESCE(?, updated_at)
               WHERE id = ?""",
            (created_at, updated_at, article_id),
        )
        self.conn.commit()

    def update_article_fts(self, article_id: int, path: str, title: str, content: str):
        """Update FTS index for an article (delete old, insert new)."""
        self._delete_fts(article_id)
        self._insert_fts(article_id, path, title, content)
        self.conn.commit()

    def _insert_fts(self, article_id: int, path: str, title: str, content: str):
        """Index an article in article_fts, and in article_fts_joined if
        the database has it and the article has a subscript."""
        self.conn.execute(
            "INSERT INTO article_fts (rowid, path, title, content) VALUES (?, ?, ?, ?)",
            (article_id, path, title, content),
        )
        if fts_has_joined(self.conn):
            self._insert_joined(article_id, title, content)

    def _insert_joined(self, article_id: int, title: str, content: str):
        joined = fts_joined(title, content)
        if joined:
            self.conn.execute(
                "INSERT INTO article_fts_joined (rowid, title, content, joined)"
                " VALUES (?, ?, ?, ?)", (article_id, title, content, joined))

    def _delete_fts(self, article_id: int):
        self.conn.execute("DELETE FROM article_fts WHERE rowid = ?", (article_id,))
        if fts_has_joined(self.conn):
            self.conn.execute("DELETE FROM article_fts_joined WHERE rowid = ?",
                              (article_id,))

    def get_article_fts(self, article_id: int) -> dict | None:
        """Return the indexed path, title and content of an article.

        With schema v3 the row also has "joined": the article's row in
        article_fts_joined as (title, content, joined), or None if it has
        none.
        """
        row = self.conn.execute(
            "SELECT path, title, content FROM article_fts WHERE rowid = ?",
            (article_id,),
        ).fetchone()
        if not row:
            return None
        row = dict(row)
        if fts_has_joined(self.conn):
            j = self.conn.execute(
                "SELECT title, content, joined FROM article_fts_joined WHERE rowid = ?",
                (article_id,),
            ).fetchone()
            row["joined"] = (j["title"], j["content"], j["joined"]) if j else None
        return row

    def delete_embedding(self, article_id: int):
        """Drop an article's embedding so `crucible embed` regenerates it."""
        # The embeddings table is created lazily by the first `crucible embed`,
        # so it may not exist yet on a knowledge base that was never embedded.
        if self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='embeddings'"
        ).fetchone():
            self.conn.execute("DELETE FROM embeddings WHERE article_id = ?", (article_id,))
            self.conn.commit()

    def delete_article(self, article_id: int):
        """Delete an article and all its relationships."""
        self._delete_fts(article_id)
        self.delete_embedding(article_id)
        self.conn.execute("DELETE FROM articles WHERE id = ?", (article_id,))
        self.conn.commit()

    # -- Concepts --

    def add_concept(self, name: str, aliases: list[str] | None = None) -> int:
        # Atomic upsert: insert-if-missing, then select. Safe under concurrency.
        self._concept_key_cache = None
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

    def add_article_link(self, from_id: int, to_id: int, context: str | None = None) -> bool:
        """Record a link; returns whether it was new (not a duplicate)."""
        self.conn.execute(
            "INSERT OR IGNORE INTO article_links (from_id, to_id, context) VALUES (?, ?, ?)",
            (from_id, to_id, context),
        )
        inserted = self.conn.execute("SELECT changes() AS n").fetchone()["n"] > 0
        self.conn.commit()
        return inserted

    def set_derivations(self, derived_id: int, source_ids):
        """Make derived_id's derivations exactly source_ids.

        Rows that are still wanted are kept, so derived_at stays the time
        each derivation first appeared; new ones are added and the rest
        removed.
        """
        wanted = set(source_ids)
        current = {r["source_id"] for r in self.conn.execute(
            "SELECT source_id FROM article_derivations WHERE derived_id = ?",
            (derived_id,),
        )}
        for sid in current - wanted:
            self.conn.execute(
                "DELETE FROM article_derivations WHERE derived_id = ? AND source_id = ?",
                (derived_id, sid),
            )
        for sid in sorted(wanted - current):
            self.conn.execute(
                "INSERT OR IGNORE INTO article_derivations (derived_id, source_id) VALUES (?, ?)",
                (derived_id, sid),
            )
        self.conn.commit()

    def clear_article_relations(self, article_id: int):
        """Drop the relations sync derives from an article's own file.

        Removes its concept tags, outgoing links and source links so sync
        can re-add them from the file's current contents. Incoming links
        belong to the articles they come from and are left alone.
        Derivations are not cleared: sync updates them with
        set_derivations, which keeps each one's derived_at.
        """
        for sql in (
            "DELETE FROM article_concepts WHERE article_id = ?",
            "DELETE FROM article_links WHERE from_id = ?",
            "DELETE FROM article_sources WHERE article_id = ?",
        ):
            self.conn.execute(sql, (article_id,))
        self.conn.commit()

    def prune_unused_concepts(self):
        """Delete concepts that no article is tagged with.

        Concepts are treated as disposable: they hold nothing but a name
        derived from FILETAGS. If curated data (e.g. aliases) is ever stored
        on a concept, this would delete it and must be revisited.
        """
        # NOT EXISTS, not NOT IN: a single NULL concept_id would make
        # NOT IN match nothing and silently skip the prune.
        self._concept_key_cache = None
        self.conn.execute(
            "DELETE FROM concepts WHERE NOT EXISTS ("
            "SELECT 1 FROM article_concepts ac WHERE ac.concept_id = concepts.id)"
        )
        self.conn.commit()

    # -- Queries --

    def search(self, query: str, limit: int = 20) -> list[dict]:
        """Full-text search over article content."""
        return fts_rows(self.conn, query, limit)

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

    def _concept_ids_by_key(self) -> dict[str, list[int]]:
        """Concept ids grouped by _concept_key of their names.

        Cached per connection: building it scans every concept, and callers
        such as suggest look up each concept in turn. The cache is dropped
        whenever this connection writes (total_changes) or another one
        commits (data_version).
        """
        stamp = (
            self.conn.execute("PRAGMA data_version").fetchone()["data_version"],
            self.conn.execute("SELECT total_changes() AS n").fetchone()["n"],
        )
        cached = self._concept_key_cache
        if cached is not None and cached[0] == stamp:
            return cached[1]
        by_key: dict[str, list[int]] = {}
        for r in self.conn.execute("SELECT id, name FROM concepts ORDER BY id"):
            by_key.setdefault(_concept_key(r["name"]), []).append(r["id"])
        self._concept_key_cache = (stamp, by_key)
        return by_key

    def concept_articles(self, concept_name: str) -> list[dict]:
        """Articles covering a given concept.

        The name is matched by _concept_key, ignoring case and punctuation,
        so "activation energy" finds the tag ``activation_energy``. An
        article tagged with several spellings of the concept appears once.
        """
        key = _concept_key(concept_name)
        if not key:
            return []
        ids = self._concept_ids_by_key().get(key)
        if not ids:
            return []
        marks = ",".join("?" * len(ids))
        return [dict(r) for r in self.conn.execute(f"""
            SELECT DISTINCT a.*
            FROM article_concepts ac
            JOIN articles a ON a.id = ac.article_id
            WHERE ac.concept_id IN ({marks})
            ORDER BY a.title
        """, tuple(ids))]

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
        def quoted(text: str | None) -> str:
            # A DOT string escapes only \ and "; an unescaped trailing
            # backslash would escape the closing quote.
            text = (text or "").replace("\\", "\\\\").replace('"', '\\"')
            return '"' + text.replace("\r", " ").replace("\n", " ") + '"'

        lines = ["digraph crucible {", "  rankdir=LR;", "  node [shape=box];"]
        links = [dict(r) for r in self.conn.execute("""
            SELECT al.from_id, al.to_id
            FROM article_links al
            JOIN articles a1 ON a1.id = al.from_id
            JOIN articles a2 ON a2.id = al.to_id
            ORDER BY a1.path, a2.path
        """)]
        # Nodes are named by article id and labelled with the title, so
        # two articles that share a title stay two nodes.
        # A subquery rather than one placeholder per id, which a large wiki
        # could push past SQLite's variable limit.
        for row in self.conn.execute("""
            SELECT id, title, path FROM articles
            WHERE id IN (SELECT from_id FROM article_links
                         UNION SELECT to_id FROM article_links)
            ORDER BY path
        """):
            lines.append(f'  a{row["id"]} [label={quoted(row["title"])},'
                         f' tooltip={quoted(row["path"])}];')
        for r in links:
            lines.append(f'  a{r["from_id"]} -> a{r["to_id"]};')
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
        """Concepts that are referenced in tags but have no dedicated concept article.

        A concept's dedicated article is a concept-type article whose file
        name (slug) or title names it: ``concepts/activation-energy.org`` or
        "Activation Energy" covers the tag ``activation_energy``. Merely
        carrying the tag is not enough, or one concept article tagged
        ``:concept:a:b:c:`` would hide the missing articles for a, b and c.
        """
        all_concepts = {r["name"] for r in self.conn.execute("SELECT name FROM concepts")}
        names = set()
        for row in self.conn.execute(
            "SELECT path, title FROM articles WHERE article_type = 'concept'"
        ):
            names.add(_concept_key(PurePosixPath(row["path"]).stem))
            names.add(_concept_key(row["title"] or ""))
        names.discard("")  # an untitled article names no concept
        return sorted(c for c in all_concepts
                      if not _concept_key(c) or _concept_key(c) not in names)

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
