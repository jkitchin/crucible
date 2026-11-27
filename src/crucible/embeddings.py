"""Embedding generation and vector search for Crucible.

Uses ollama's embedding API to generate vectors for wiki articles, stored
in libSQL as packed float32 blobs and compared with libSQL's native
vector_distance_cos. Each embedding records the model that produced it and
its dimension, so models of any dimension can be used and vectors from
different models are never compared with each other.
"""

import json
import os
import re
import struct
import urllib.error
import urllib.request
from pathlib import Path

import libsql

from crucible.database import FILTER_SQL, DictConnection, filter_ids, fts_rows
from crucible.orgparse import DRAWER_RE

DEFAULT_MODEL = "nomic-embed-text"
DEFAULT_URL = "http://localhost:11434/api/embed"
# Honoured by `crucible embed` and `crucible search` when --url is not given.
URL_ENV_VAR = "CRUCIBLE_OLLAMA_URL"

# Characters of article text (after the org header is stripped) sent to
# the embedding model.
MAX_EMBED_CHARS = 8000
# Articles sent to the embedding service per request.
EMBED_BATCH_SIZE = 32

# The vector column is an untyped BLOB rather than F32_BLOB(n): a typed
# column fixes the dimension for every row, so a model with another
# dimension could not be stored. Searches are brute-force scans with
# vector_distance_cos, filtered to one model and dimension.
EMBEDDINGS_SCHEMA_TABLE = """
CREATE TABLE IF NOT EXISTS embeddings (
    article_id INTEGER PRIMARY KEY REFERENCES articles(id) ON DELETE CASCADE,
    model TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    vector BLOB,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
)
"""


class EmbeddingError(RuntimeError):
    """The embedding service failed or returned unusable vectors."""


def default_url() -> str:
    """The ollama embed URL: $CRUCIBLE_OLLAMA_URL, else localhost."""
    return os.environ.get(URL_ENV_VAR) or DEFAULT_URL


def embed_texts(texts: list[str], model: str = DEFAULT_MODEL,
                url: str = DEFAULT_URL) -> list[list[float]]:
    """Generate embeddings for a list of texts via ollama.

    Raises EmbeddingError when the service is unreachable, the model is
    missing, or the response is malformed.
    """
    data = json.dumps({"model": model, "input": texts}).encode()
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            resp = json.loads(r.read())
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = json.loads(e.read()).get("error", "")
        except Exception:
            pass
        hint = f" (is the model pulled? try `ollama pull {model}`)" if e.code == 404 else ""
        raise EmbeddingError(
            f"Embedding service at {url} returned HTTP {e.code}"
            f"{': ' + detail if detail else ''}{hint}"
        ) from e
    except (urllib.error.URLError, OSError) as e:
        reason = getattr(e, "reason", e)
        raise EmbeddingError(
            f"Cannot reach the embedding service at {url} ({reason}). "
            "Is ollama running? Set --url or $" + URL_ENV_VAR + " for another server."
        ) from e
    except ValueError as e:
        raise EmbeddingError(f"Embedding service at {url} returned invalid JSON") from e
    vecs = resp.get("embeddings") if isinstance(resp, dict) else None
    if not isinstance(vecs, list) or len(vecs) != len(texts) or not all(vecs):
        raise EmbeddingError(
            f"Embedding service at {url} returned no embeddings for model {model}"
        )
    return vecs


def embed_text(text: str, model: str = DEFAULT_MODEL,
               url: str = DEFAULT_URL) -> list[float]:
    """Generate embedding for a single text."""
    return embed_texts([text], model=model, url=url)[0]


def vector_to_blob(vec: list[float]) -> bytes:
    """Pack a float vector into a binary blob."""
    return struct.pack(f"{len(vec)}f", *vec)


def blob_to_vector(blob: bytes) -> list[float]:
    """Unpack a binary blob into a float vector."""
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


_KEYWORD_LINE_RE = re.compile(r"^[ \t]*#\+(\w+):[ \t]*(.*)$")
# Drawers (:PROPERTIES:, :LOGBOOK:, ...), bounded by headings as in org; the
# browser renders them with the same pattern (here with the line break after
# :END:, so no blank line is left behind).
_DRAWER_RE = re.compile(DRAWER_RE.pattern + r"\n?", DRAWER_RE.flags)


def embedding_text(content: str, limit: int = MAX_EMBED_CHARS) -> str:
    """The text of an article that is embedded.

    The org header (#+KEYWORD: lines; the title is kept as the first line)
    and drawers such as :PROPERTIES: carry no meaning for semantic search,
    so they are stripped before the text is cut to `limit` characters.
    #+BEGIN_/#+END_ block lines have no colon and are kept.
    """
    title = ""
    body = []
    for line in _DRAWER_RE.sub("", content).splitlines():
        m = _KEYWORD_LINE_RE.match(line)
        if m:
            if m.group(1).upper() == "TITLE" and not title:
                title = m.group(2).strip()
            continue
        body.append(line)
    text = "\n".join(body).strip()
    if title:
        text = f"{title}\n\n{text}"
    return text[:limit]


def _peer_db_path(peer: dict) -> str | None:
    for rel in (Path(".crucible") / "crucible.db", Path("db") / "crucible.db"):
        p = Path(peer["path"]) / rel
        if p.exists():
            return str(p)
    return None


def _has_embeddings_table(conn) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='embeddings'"
    ).fetchone() is not None


_SEMANTIC_SQL = """
    SELECT a.*, vector_distance_cos(e.vector, ?) AS distance
    FROM embeddings e JOIN articles a ON a.id = e.article_id
    WHERE e.model = ? AND e.dimensions = ? AND e.vector IS NOT NULL{where}
    ORDER BY distance ASC LIMIT ?
"""


def _semantic_rows(conn, query_blob: bytes, model: str, dim: int,
                   limit: int, filters=None) -> list[dict]:
    """Nearest articles to a query vector among one model's embeddings,
    among those a SearchFilter keeps (see filter_ids)."""
    ids = filter_ids(conn, filters)
    if ids is None:
        sql, params = _SEMANTIC_SQL.format(where=""), (query_blob, model, dim, limit)
    elif not ids:
        return []
    else:
        sql = _SEMANTIC_SQL.format(where=FILTER_SQL)
        params = (query_blob, model, dim, json.dumps(ids), limit)
    results = []
    for row in conn.execute(sql, params):
        r = dict(row)
        r["similarity"] = 1.0 - r.pop("distance")
        results.append(r)
    return results


def _unit(vec: list[float]) -> list[float]:
    norm = sum(x * x for x in vec) ** 0.5
    return [x / norm for x in vec] if norm else vec


def _vectors_for(conn, ids, model: str, dim: int) -> dict[int, list[float]]:
    """Unit vectors of the given article ids (one model and dimension)."""
    ids = sorted({i for i in ids if i is not None})
    out: dict[int, list[float]] = {}
    if not ids:
        return out
    marks = ",".join("?" * len(ids))
    for row in conn.execute(
        f"SELECT article_id, vector FROM embeddings WHERE article_id IN ({marks})"
        " AND model = ? AND dimensions = ? AND vector IS NOT NULL",
        (*ids, model, dim),
    ):
        out[row["article_id"]] = _unit(blob_to_vector(row["vector"]))
    return out


def _rrf(scores: dict, fts_rows: list[dict], sem_rows: list[dict],
         crucible: str | None, fts_weight: float, semantic_weight: float,
         k: int, scope=None):
    """Add Reciprocal Rank Fusion contributions of two ranked lists.

    Keys are article ids, or (scope, id) when crucible is given; scope
    defaults to the crucible name. Results get crucible as their _crucible
    label. A search across crucibles passes a scope that tells the local
    crucible from a peer of the same name, so their rows never merge.
    """
    if scope is None:
        scope = crucible

    def entry(r):
        key = r["id"] if crucible is None else (scope, r["id"])
        if key not in scores:
            scores[key] = {**r, "rrf_score": 0.0, "methods": []}
            if crucible is not None:
                scores[key]["_crucible"] = crucible
        elif "similarity" in r and "similarity" not in scores[key]:
            scores[key]["similarity"] = r["similarity"]
        return scores[key]

    for rank, r in enumerate(fts_rows, start=1):
        e = entry(r)
        e["rrf_score"] += fts_weight / (k + rank)
        e["methods"].append(f"fts:rank={rank}")
    for rank, r in enumerate(sem_rows, start=1):
        e = entry(r)
        e["rrf_score"] += semantic_weight / (k + rank)
        e["methods"].append(f"semantic:rank={rank},sim={r['similarity']:.3f}")


class EmbeddingIndex:
    """Manages article embeddings in the crucible database.

    `model` is the embedding model. When it is None, the model most stored
    embeddings were made with is used (DEFAULT_MODEL if there are none).
    Embeddings made with another model count as missing for embed_missing
    and are ignored by searches. `warnings` collects non-fatal problems
    (such as a peer skipped by a search across crucibles) for the caller
    to report.
    """

    def __init__(self, conn: DictConnection, model: str | None = None,
                 url: str | None = None):
        self.conn = conn
        self._model = model
        self.url = url or default_url()
        self.warnings: list[str] = []

    @property
    def model(self) -> str:
        if self._model is None:
            self._model = self.stored_model() or DEFAULT_MODEL
        return self._model

    def initialize(self):
        """Create the embeddings table, migrating an old fixed-size one."""
        self._migrate()
        self.conn.execute(EMBEDDINGS_SCHEMA_TABLE)
        self.conn.commit()

    def _migrate(self):
        """Replace an F32_BLOB(768) vector column (and its index) by a BLOB.

        The typed column and its libSQL vector index fixed every vector at
        768 dimensions, so no model of another size could be stored. The
        stored rows are kept.
        """
        row = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='embeddings'"
        ).fetchone()
        if not row or "F32_BLOB" not in (row["sql"] or "").upper():
            return
        self.conn.execute("DROP INDEX IF EXISTS idx_embeddings_vector")
        self.conn.execute("DROP TABLE IF EXISTS embeddings_migrate")
        self.conn.execute(EMBEDDINGS_SCHEMA_TABLE.replace(
            "IF NOT EXISTS embeddings", "embeddings_migrate"))
        self.conn.execute(
            "INSERT INTO embeddings_migrate"
            " (article_id, model, dimensions, vector, updated_at)"
            " SELECT article_id, model, dimensions, vector, updated_at FROM embeddings"
        )
        self.conn.execute("DROP TABLE embeddings")
        self.conn.execute("ALTER TABLE embeddings_migrate RENAME TO embeddings")
        self.conn.commit()

    def stored_models(self) -> dict[str, int]:
        """{model: count} of the stored embeddings, most common first."""
        if not _has_embeddings_table(self.conn):
            return {}
        return {r["model"]: r["n"] for r in self.conn.execute(
            "SELECT model, COUNT(*) AS n FROM embeddings"
            " GROUP BY model ORDER BY n DESC, model")}

    def stored_model(self) -> str | None:
        """The model most stored embeddings were made with, if any."""
        return next(iter(self.stored_models()), None)

    def _embed_query(self, query: str) -> tuple[bytes, int]:
        """Embed a query, checking it can be compared with stored vectors."""
        vec = embed_text(query, model=self.model, url=self.url)
        dim = len(vec)
        dims = {r["dimensions"] for r in self.conn.execute(
            "SELECT DISTINCT dimensions FROM embeddings WHERE model = ?",
            (self.model,))}
        if dims and dim not in dims:
            raise EmbeddingError(
                f"Stored embeddings for model {self.model} have "
                f"{', '.join(map(str, sorted(dims)))} dimensions but the service "
                f"now returns {dim}. Re-embed with "
                f"`crucible embed --force -m {self.model}`."
            )
        return vector_to_blob(vec), dim

    def _store(self, article_id: int, vec: list[float]):
        self.conn.execute(
            """INSERT OR REPLACE INTO embeddings
               (article_id, model, dimensions, vector, updated_at)
               VALUES (?, ?, ?, ?, datetime('now'))""",
            (article_id, self.model, len(vec), vector_to_blob(vec)),
        )

    def embed_article(self, article_id: int, content: str):
        """Generate and store the embedding for one article."""
        if self.has_embedding(article_id):
            return
        vec = embed_text(embedding_text(content), model=self.model, url=self.url)
        self._store(article_id, vec)
        self.conn.commit()

    def has_embedding(self, article_id: int) -> bool:
        """Whether the article has an embedding from this index's model."""
        row = self.conn.execute(
            "SELECT 1 FROM embeddings WHERE article_id = ? AND model = ?",
            (article_id, self.model),
        ).fetchone()
        return row is not None

    def embed_missing(self, articles: list[dict], force: bool = False,
                      batch_size: int = EMBED_BATCH_SIZE) -> int:
        """Embed articles with no embedding from this model. Returns count.

        An embedding from another model counts as missing and is replaced;
        with force every article is re-embedded. Articles go to the service
        in batches of batch_size. Each batch is re-checked first, so
        concurrent runs do not redo each other's work.
        """
        existing = set()
        if not force:
            for row in self.conn.execute(
                    "SELECT article_id FROM embeddings WHERE model = ?",
                    (self.model,)):
                existing.add(row["article_id"])

        missing = [a for a in articles if a["id"] not in existing]
        count = 0
        for start in range(0, len(missing), batch_size):
            batch = missing[start:start + batch_size]
            if not force:
                batch = [a for a in batch if not self.has_embedding(a["id"])]
            if not batch:
                continue
            texts = [
                embedding_text(a.get("content", "") or a.get("abstract", "")
                               or a["title"])
                for a in batch
            ]
            vecs = embed_texts(texts, model=self.model, url=self.url)
            for a, vec in zip(batch, vecs):
                self._store(a["id"], vec)
            self.conn.commit()
            count += len(batch)
        return count

    def search(self, query: str, limit: int = 10,
               filters=None) -> list[dict]:
        """Semantic search: a brute-force scan with vector_distance_cos."""
        query_blob, dim = self._embed_query(query)
        return _semantic_rows(self.conn, query_blob, self.model, dim, limit,
                              filters)

    @staticmethod
    def _fts_rows(conn, query: str, limit: int, filters=None) -> list[dict]:
        return fts_rows(conn, query, limit, filters=filters)

    def hybrid_search(self, query: str, limit: int = 10,
                      fts_weight: float = 1.0,
                      semantic_weight: float = 1.0,
                      k: int = 60, raw: bool = False,
                      filters=None) -> list[dict]:
        """Hybrid search combining FTS5 keyword and vector semantic results.

        Uses Reciprocal Rank Fusion (RRF) to merge two ranked lists,
        then deduplicates by clustering semantically similar results
        and picking the best representative from each cluster.

        Args:
            query: search query
            limit: max results to return
            fts_weight: weight for FTS results (default 1.0)
            semantic_weight: weight for semantic results (default 1.0)
            k: RRF constant (default 60, standard value)
            raw: if True, skip dedup/quality filtering
            filters: a SearchFilter of the articles to keep (see filter_ids)
        """
        query_blob, dim = self._embed_query(query)
        scores: dict = {}
        fts_results = self._fts_rows(self.conn, query, limit * 3, filters)
        semantic_results = _semantic_rows(self.conn, query_blob, self.model,
                                          dim, limit * 3, filters)
        _rrf(scores, fts_results, semantic_results, None,
             fts_weight, semantic_weight, k)
        ranked = sorted(scores.values(), key=lambda x: x["rrf_score"], reverse=True)
        if raw or not semantic_results:
            return ranked[:limit]
        vectors = _vectors_for(self.conn, [r["id"] for r in ranked], self.model, dim)
        return self._dedup_results(ranked, limit, vectors,
                                   key=lambda a: a.get("id"))

    def _dedup_results(self, ranked: list[dict], limit: int,
                       vectors: dict, key,
                       similarity_threshold: float = 0.85,
                       is_local=None) -> list[dict]:
        """Cluster ranked results and pick the best from each cluster.

        vectors maps key(article) to its unit vector; results without one
        are never clustered. Within a cluster, prefers: higher status, more
        derivatives (local articles), more recent, higher RRF score.
        is_local(article) tells local results from peers' (by default, by
        their _crucible label).
        """
        if is_local is None:
            def is_local(article):
                return article.get("_crucible", "local") == "local"
        status_rank = {"verified": 3, "reviewed": 2, "draft": 1}

        derivative_counts = {}
        try:
            for row in self.conn.execute("""
                SELECT source_id, COUNT(*) as cnt
                FROM article_derivations GROUP BY source_id
            """):
                derivative_counts[row["source_id"]] = row["cnt"]
        except Exception:
            pass

        def quality_key(article):
            local = is_local(article)
            return (
                status_rank.get(article.get("status", "draft"), 0),
                derivative_counts.get(article.get("id", 0), 0) if local else 0,
                article.get("updated_at", "") or "",
                article.get("rrf_score", 0),
            )

        def similar(a, b):
            va, vb = vectors.get(key(a)), vectors.get(key(b))
            if va is None or vb is None or len(va) != len(vb):
                return False
            return sum(x * y for x, y in zip(va, vb)) >= similarity_threshold

        selected = []
        used = set()
        for article in ranked:
            ak = key(article)
            if ak in used:
                continue
            cluster = [article]
            for other in ranked:
                ok = key(other)
                if ok == ak or ok in used:
                    continue
                if similar(article, other):
                    cluster.append(other)

            best = max(cluster, key=quality_key)
            if len(cluster) > 1:
                best["dedup_absorbed"] = [a["title"] for a in cluster
                                          if a is not best]
            selected.append(best)
            for a in cluster:
                used.add(key(a))
            if len(selected) >= limit:
                break
        return selected

    def _peer_semantic(self, conn, peer_name: str, query_blob: bytes,
                       dim: int, limit: int, filters=None) -> list[dict]:
        """A peer's semantic hits; [] with a warning when its embeddings
        are from another model or dimension than the query's."""
        if not _has_embeddings_table(conn):
            return []
        rows = _semantic_rows(conn, query_blob, self.model, dim, limit, filters)
        if not rows:
            other = conn.execute(
                "SELECT model, dimensions FROM embeddings"
                " WHERE NOT (model = ? AND dimensions = ?) LIMIT 1",
                (self.model, dim)).fetchone()
            if other:
                self.warnings.append(
                    f"Peer '{peer_name}' has embeddings from {other['model']} "
                    f"({other['dimensions']} dims), not {self.model} ({dim} dims);"
                    " its semantic results were skipped."
                )
        return rows

    def search_all(self, query: str, peers: list[dict],
                   limit: int = 10, filters=None) -> list[dict]:
        """Semantic search across local and all peers."""
        query_blob, dim = self._embed_query(query)
        results = []
        for r in _semantic_rows(self.conn, query_blob, self.model, dim, limit,
                                filters):
            r["_crucible"] = "local"
            results.append(r)

        for peer in peers:
            peer_db = _peer_db_path(peer)
            if not peer_db:
                self.warnings.append(
                    f"Peer '{peer['name']}' skipped: no crucible database at "
                    f"{peer['path']}.")
                continue
            try:
                conn = DictConnection(libsql.connect(peer_db))
                try:
                    for r in self._peer_semantic(conn, peer["name"], query_blob,
                                                 dim, limit, filters):
                        r["_crucible"] = peer["name"]
                        results.append(r)
                finally:
                    conn.close()
            except Exception as e:
                self.warnings.append(f"Peer '{peer['name']}' skipped: {e}")

        results.sort(key=lambda x: x["similarity"], reverse=True)
        return results[:limit]

    def hybrid_search_all(self, query: str, peers: list[dict],
                          limit: int = 10,
                          fts_weight: float = 1.0,
                          semantic_weight: float = 1.0,
                          k: int = 60, raw: bool = False,
                          filters=None) -> list[dict]:
        """Hybrid search across local and all peers with RRF reranking.

        Results are deduplicated as in hybrid_search unless raw is set.
        """
        query_blob, dim = self._embed_query(query)
        scores: dict = {}
        vectors: dict = {}

        # Rows are keyed by (scope, article id). The scope (is_peer, name)
        # keeps the local crucible apart from a peer that happens to be
        # registered under the name "local"; the label shown is unchanged.
        def collect(conn, scope, fts_rows, sem_rows):
            _rrf(scores, fts_rows, sem_rows, scope[1], fts_weight,
                 semantic_weight, k, scope=scope)
            if not raw and _has_embeddings_table(conn):
                ids = [r["id"] for r in fts_rows + sem_rows]
                for aid, v in _vectors_for(conn, ids, self.model, dim).items():
                    vectors[(scope, aid)] = v

        collect(self.conn, (False, "local"),
                self._fts_rows(self.conn, query, limit * 3, filters),
                _semantic_rows(self.conn, query_blob, self.model, dim, limit * 3,
                               filters))

        for peer in peers:
            peer_db = _peer_db_path(peer)
            if not peer_db:
                self.warnings.append(
                    f"Peer '{peer['name']}' skipped: no crucible database at "
                    f"{peer['path']}.")
                continue
            name = peer["name"]
            try:
                conn = DictConnection(libsql.connect(peer_db))
                try:
                    collect(conn, (True, name),
                            self._fts_rows(conn, query, limit * 3, filters),
                            self._peer_semantic(conn, name, query_blob, dim,
                                                limit * 3, filters))
                finally:
                    conn.close()
            except Exception as e:
                self.warnings.append(f"Peer '{name}' skipped: {e}")

        ranked = sorted(scores.values(), key=lambda x: x["rrf_score"], reverse=True)
        if raw or not vectors:
            return ranked[:limit]
        # key_of[id(row)] is ((is_peer, name), article id).
        key_of = {id(entry): key for key, entry in scores.items()}
        return self._dedup_results(ranked, limit, vectors,
                                   key=lambda a: key_of[id(a)],
                                   is_local=lambda a: not key_of[id(a)][0][0])

    # -- Semantic analysis for suggest --

    def find_similar_pairs(self, threshold: float = 0.7) -> list[dict]:
        """Find pairs of articles with high semantic similarity.

        Uses native vector_distance_cos for pairwise comparison.
        """
        articles = [dict(r) for r in self.conn.execute("""
            SELECT a.id, a.path, a.title, a.article_type
            FROM embeddings e JOIN articles a ON a.id = e.article_id
            WHERE e.model = ? AND e.vector IS NOT NULL
        """, (self.model,))]

        pairs = []
        for a in articles:
            for row in self.conn.execute("""
                SELECT a2.id, a2.path, a2.title,
                       vector_distance_cos(e1.vector, e2.vector) as dist
                FROM embeddings e1, embeddings e2
                JOIN articles a2 ON a2.id = e2.article_id
                WHERE e1.article_id = ? AND e2.article_id > ?
                  AND e2.model = e1.model AND e2.dimensions = e1.dimensions
                  AND e2.vector IS NOT NULL
            """, (a["id"], a["id"])):
                r = dict(row)
                sim = 1.0 - r["dist"]
                if sim >= threshold:
                    pairs.append({
                        "article_a": a["path"],
                        "title_a": a["title"],
                        "article_b": r["path"],
                        "title_b": r["title"],
                        "similarity": sim,
                    })
        pairs.sort(key=lambda x: x["similarity"], reverse=True)
        return pairs

    def find_cluster_candidates(self, min_articles: int = 3,
                                threshold: float = 0.6) -> list[dict]:
        """Find groups of semantically similar articles that might
        warrant a comparison or synthesis article.

        Uses native vector_distance_cos for pairwise comparison.
        """
        articles = [dict(r) for r in self.conn.execute("""
            SELECT a.id, a.path, a.title, a.article_type
            FROM embeddings e JOIN articles a ON a.id = e.article_id
            WHERE e.model = ? AND e.vector IS NOT NULL
        """, (self.model,))]

        # Build adjacency via SQL
        neighbors = {a["id"]: [] for a in articles}
        for a in articles:
            for row in self.conn.execute("""
                SELECT e2.article_id as other_id,
                       vector_distance_cos(e1.vector, e2.vector) as dist
                FROM embeddings e1, embeddings e2
                WHERE e1.article_id = ? AND e2.article_id != ?
                  AND e2.model = e1.model AND e2.dimensions = e1.dimensions
                  AND e2.vector IS NOT NULL
            """, (a["id"], a["id"])):
                r = dict(row)
                sim = 1.0 - r["dist"]
                if sim >= threshold:
                    neighbors[a["id"]].append((r["other_id"], sim))

        id_to_article = {a["id"]: a for a in articles}
        seen = set()
        clusters = []
        for a in articles:
            if a["id"] in seen:
                continue
            group = [{"path": a["path"], "title": a["title"], "similarity": 1.0}]
            for oid, sim in neighbors[a["id"]]:
                if oid not in seen:
                    other = id_to_article[oid]
                    group.append({"path": other["path"], "title": other["title"],
                                  "similarity": sim})
            if len(group) >= min_articles:
                for g in group:
                    aid = next(x["id"] for x in articles if x["path"] == g["path"])
                    seen.add(aid)
                clusters.append({
                    "center": a["title"],
                    "articles": group,
                })
        return clusters

    def stats(self) -> dict:
        """Counts of stored embeddings: all, and those from this model."""
        row = self.conn.execute("SELECT COUNT(*) as n FROM embeddings").fetchone()
        mine = self.conn.execute(
            "SELECT COUNT(*) as n FROM embeddings WHERE model = ?", (self.model,)
        ).fetchone()
        total = self.conn.execute("SELECT COUNT(*) as n FROM articles").fetchone()
        return {
            "embedded": row["n"],
            "embedded_with_model": mine["n"],
            "total_articles": total["n"],
            "model": self.model,
        }
