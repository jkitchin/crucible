"""Embedding generation and vector search for Crucible.

Uses ollama's embedding API to generate vectors for wiki articles,
stored in libSQL with native vector indexing for cosine similarity search.
"""

import json
import struct
import urllib.request
from pathlib import Path

import libsql

from crucible.database import DictConnection

DEFAULT_MODEL = "nomic-embed-text"
DEFAULT_URL = "http://localhost:11434/api/embed"

EMBEDDING_DIM = 768  # nomic-embed-text default

EMBEDDINGS_SCHEMA_TABLE = """
CREATE TABLE IF NOT EXISTS embeddings (
    article_id INTEGER PRIMARY KEY REFERENCES articles(id) ON DELETE CASCADE,
    model TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    vector F32_BLOB({dim}),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
)
""".format(dim=EMBEDDING_DIM)

EMBEDDINGS_SCHEMA_INDEX = """
CREATE INDEX IF NOT EXISTS idx_embeddings_vector
    ON embeddings(libsql_vector_idx(vector))
"""


def embed_texts(texts: list[str], model: str = DEFAULT_MODEL,
                url: str = DEFAULT_URL) -> list[list[float]]:
    """Generate embeddings for a list of texts via ollama."""
    data = json.dumps({"model": model, "input": texts}).encode()
    req = urllib.request.Request(url, data=data,
                                headers={"Content-Type": "application/json"})
    resp = json.loads(urllib.request.urlopen(req, timeout=120).read())
    return resp["embeddings"]


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


class EmbeddingIndex:
    """Manages article embeddings in the crucible database."""

    def __init__(self, conn: DictConnection, model: str = DEFAULT_MODEL,
                 url: str = DEFAULT_URL):
        self.conn = conn
        self.model = model
        self.url = url

    def initialize(self):
        """Create the embeddings table and vector index."""
        self.conn.execute(EMBEDDINGS_SCHEMA_TABLE)
        self.conn.commit()
        try:
            self.conn.execute(EMBEDDINGS_SCHEMA_INDEX)
            self.conn.commit()
        except Exception:
            pass  # Index may already exist

    def embed_article(self, article_id: int, content: str):
        """Generate and store embedding for an article."""
        # Optimistic re-check: another process may have embedded this
        # while we were working through the list
        if self.has_embedding(article_id):
            return
        text = content[:8000]
        vec = embed_text(text, model=self.model, url=self.url)
        blob = vector_to_blob(vec)
        self.conn.execute(
            """INSERT OR REPLACE INTO embeddings
               (article_id, model, dimensions, vector)
               VALUES (?, ?, ?, ?)""",
            (article_id, self.model, len(vec), blob),
        )
        self.conn.commit()

    def has_embedding(self, article_id: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM embeddings WHERE article_id = ?", (article_id,)
        ).fetchone()
        return row is not None

    def embed_missing(self, articles: list[dict]) -> int:
        """Embed articles that don't have embeddings yet. Returns count.

        Batches the missing-check into a single query to reduce N+1 overhead.
        Each article is re-checked before the API call in embed_article()
        to avoid redundant work when multiple processes run concurrently.
        """
        existing = set()
        for row in self.conn.execute("SELECT article_id FROM embeddings"):
            existing.add(row["article_id"])

        missing = [a for a in articles if a["id"] not in existing]
        count = 0
        for a in missing:
            content = a.get("content", "") or a.get("abstract", "") or a["title"]
            self.embed_article(a["id"], content)
            count += 1
        return count

    def search(self, query: str, limit: int = 10) -> list[dict]:
        """Semantic search using native libSQL vector_distance_cos."""
        query_vec = embed_text(query, model=self.model, url=self.url)
        query_blob = vector_to_blob(query_vec)

        results = []
        for row in self.conn.execute("""
            SELECT a.*, vector_distance_cos(e.vector, ?) as distance
            FROM embeddings e
            JOIN articles a ON a.id = e.article_id
            ORDER BY distance ASC
            LIMIT ?
        """, (query_blob, limit)):
            r = dict(row)
            r["similarity"] = 1.0 - r.pop("distance")
            results.append(r)

        results.sort(key=lambda x: x["similarity"], reverse=True)
        return results[:limit]

    def hybrid_search(self, query: str, limit: int = 10,
                      fts_weight: float = 1.0,
                      semantic_weight: float = 1.0,
                      k: int = 60, raw: bool = False) -> list[dict]:
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
        """
        scores: dict[int, dict] = {}  # article_id -> {article_data, score}

        # FTS ranked results
        fts_results = []
        for row in self.conn.execute("""
            SELECT a.*, rank
            FROM article_fts fts
            JOIN articles a ON a.id = fts.rowid
            WHERE article_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """, (query, limit * 3)):
            fts_results.append(dict(row))

        for rank, r in enumerate(fts_results, start=1):
            aid = r["id"]
            if aid not in scores:
                scores[aid] = {**r, "rrf_score": 0.0, "methods": []}
            scores[aid]["rrf_score"] += fts_weight / (k + rank)
            scores[aid]["methods"].append(f"fts:rank={rank}")

        # Semantic ranked results (native vector search)
        query_vec = embed_text(query, model=self.model, url=self.url)
        query_blob = vector_to_blob(query_vec)
        semantic_results = []
        for row in self.conn.execute("""
            SELECT a.*, vector_distance_cos(e.vector, ?) as distance
            FROM embeddings e
            JOIN articles a ON a.id = e.article_id
            ORDER BY distance ASC
            LIMIT ?
        """, (query_blob, limit * 3)):
            r = dict(row)
            r["similarity"] = 1.0 - r.pop("distance")
            semantic_results.append(r)

        has_embeddings = len(semantic_results) > 0
        for rank, r in enumerate(semantic_results, start=1):
            aid = r["id"]
            sim = r["similarity"]
            if aid not in scores:
                scores[aid] = {**r, "rrf_score": 0.0, "methods": []}
            scores[aid]["rrf_score"] += semantic_weight / (k + rank)
            scores[aid]["methods"].append(f"semantic:rank={rank},sim={sim:.3f}")

        ranked = sorted(scores.values(), key=lambda x: x["rrf_score"], reverse=True)

        if raw or not has_embeddings:
            return ranked[:limit]

        # Dedup: cluster similar results, pick best per cluster
        return self._dedup_results(ranked, limit)

    def _dedup_results(self, ranked: list[dict], limit: int,
                       similarity_threshold: float = 0.85) -> list[dict]:
        """Cluster ranked results and pick the best from each cluster.

        Uses native vector_distance_cos for pairwise similarity.
        Within a cluster, prefers: higher status, more derivatives,
        more recent, higher RRF score.
        """
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
            return (
                status_rank.get(article.get("status", "draft"), 0),
                derivative_counts.get(article.get("id", 0), 0),
                article.get("updated_at", ""),
                article.get("rrf_score", 0),
            )

        # Build pairwise distance cache using SQL
        ranked_ids = [a.get("id") for a in ranked if a.get("id") is not None]
        pairwise = {}  # (id_a, id_b) -> distance
        if len(ranked_ids) > 1:
            for aid in ranked_ids:
                try:
                    for row in self.conn.execute("""
                        SELECT e2.article_id as other_id,
                               vector_distance_cos(e1.vector, e2.vector) as dist
                        FROM embeddings e1, embeddings e2
                        WHERE e1.article_id = ? AND e2.article_id != ?
                    """, (aid, aid)):
                        oid = row["other_id"]
                        if oid in ranked_ids:
                            pairwise[(aid, oid)] = row["dist"]
                except Exception:
                    break

        # Greedy clustering
        selected = []
        used = set()

        for article in ranked:
            aid = article.get("id")
            if aid in used:
                continue

            cluster = [article]
            for other in ranked:
                oid = other.get("id")
                if oid == aid or oid in used:
                    continue
                dist = pairwise.get((aid, oid))
                if dist is not None and (1.0 - dist) >= similarity_threshold:
                    cluster.append(other)

            best = max(cluster, key=quality_key)
            if len(cluster) > 1:
                others = [a["title"] for a in cluster if a.get("id") != best.get("id")]
                best["dedup_absorbed"] = others
            selected.append(best)

            for a in cluster:
                used.add(a.get("id"))

            if len(selected) >= limit:
                break

        return selected

    def _search_peer_db(self, peer_db_path: str, peer_name: str,
                        query_blob: bytes, limit: int) -> list[dict]:
        """Semantic search against a peer's embeddings via separate connection."""
        try:
            conn = DictConnection(libsql.connect(peer_db_path))
            results = []
            for row in conn.execute("""
                SELECT a.*, vector_distance_cos(e.vector, ?) as distance
                FROM embeddings e
                JOIN articles a ON a.id = e.article_id
                ORDER BY distance ASC
                LIMIT ?
            """, (query_blob, limit)):
                r = dict(row)
                r["similarity"] = 1.0 - r.pop("distance")
                r["_crucible"] = peer_name
                results.append(r)
            conn.close()
            return results
        except Exception:
            return []

    def search_all(self, query: str, peers: list[dict],
                   limit: int = 10) -> list[dict]:
        """Semantic search across local and all peers."""
        query_vec = embed_text(query, model=self.model, url=self.url)
        query_blob = vector_to_blob(query_vec)

        # Local results
        results = []
        for row in self.conn.execute("""
            SELECT a.*, vector_distance_cos(e.vector, ?) as distance
            FROM embeddings e
            JOIN articles a ON a.id = e.article_id
            ORDER BY distance ASC
            LIMIT ?
        """, (query_blob, limit)):
            r = dict(row)
            r["similarity"] = 1.0 - r.pop("distance")
            r["_crucible"] = "local"
            results.append(r)

        # Peer results
        for peer in peers:
            peer_db = str(Path(peer["path"]) / ".crucible" / "crucible.db")
            if not Path(peer_db).exists():
                # Try old layout
                peer_db = str(Path(peer["path"]) / "db" / "crucible.db")
            if Path(peer_db).exists():
                results.extend(
                    self._search_peer_db(peer_db, peer["name"], query_blob, limit)
                )

        results.sort(key=lambda x: x["similarity"], reverse=True)
        return results[:limit]

    def hybrid_search_all(self, query: str, peers: list[dict],
                          limit: int = 10,
                          fts_weight: float = 1.0,
                          semantic_weight: float = 1.0,
                          k: int = 60) -> list[dict]:
        """Hybrid search across local and all peers with RRF reranking."""
        query_vec = embed_text(query, model=self.model, url=self.url)
        query_blob = vector_to_blob(query_vec)

        scores: dict[str, dict] = {}

        def _add_fts(rows, crucible_name):
            for rank, r in enumerate(rows, start=1):
                key = f"{crucible_name}:{r['id']}"
                if key not in scores:
                    scores[key] = {**r, "_crucible": crucible_name,
                                   "rrf_score": 0.0, "methods": []}
                scores[key]["rrf_score"] += fts_weight / (k + rank)
                scores[key]["methods"].append(f"fts:rank={rank}")

        def _add_semantic(rows, crucible_name):
            for rank, r in enumerate(rows[:limit * 3], start=1):
                key = f"{crucible_name}:{r['id']}"
                sim = r.get("similarity", 0)
                if key not in scores:
                    scores[key] = {**r, "_crucible": crucible_name,
                                   "rrf_score": 0.0, "methods": []}
                scores[key]["rrf_score"] += semantic_weight / (k + rank)
                scores[key]["methods"].append(f"semantic:rank={rank},sim={sim:.3f}")

        # Local FTS
        local_fts = [dict(row) for row in self.conn.execute("""
            SELECT a.*, rank FROM article_fts fts
            JOIN articles a ON a.id = fts.rowid
            WHERE article_fts MATCH ? ORDER BY rank LIMIT ?
        """, (query, limit * 3))]
        _add_fts(local_fts, "local")

        # Local semantic (native vector search)
        local_sem = []
        for row in self.conn.execute("""
            SELECT a.*, vector_distance_cos(e.vector, ?) as distance
            FROM embeddings e JOIN articles a ON a.id = e.article_id
            ORDER BY distance ASC LIMIT ?
        """, (query_blob, limit * 3)):
            r = dict(row)
            r["similarity"] = 1.0 - r.pop("distance")
            local_sem.append(r)
        _add_semantic(local_sem, "local")

        # Peers
        for peer in peers:
            peer_db = str(Path(peer["path"]) / ".crucible" / "crucible.db")
            if not Path(peer_db).exists():
                peer_db = str(Path(peer["path"]) / "db" / "crucible.db")
            if not Path(peer_db).exists():
                continue
            name = peer["name"]
            try:
                conn = DictConnection(libsql.connect(peer_db))

                # Peer FTS
                peer_fts = [dict(row) for row in conn.execute("""
                    SELECT a.*, rank FROM article_fts fts
                    JOIN articles a ON a.id = fts.rowid
                    WHERE article_fts MATCH ? ORDER BY rank LIMIT ?
                """, (query, limit * 3))]
                _add_fts(peer_fts, name)

                # Peer semantic
                peer_sem = []
                for row in conn.execute("""
                    SELECT a.*, vector_distance_cos(e.vector, ?) as distance
                    FROM embeddings e JOIN articles a ON a.id = e.article_id
                    ORDER BY distance ASC LIMIT ?
                """, (query_blob, limit * 3)):
                    r = dict(row)
                    r["similarity"] = 1.0 - r.pop("distance")
                    peer_sem.append(r)
                _add_semantic(peer_sem, name)

                conn.close()
            except Exception:
                pass

        results = sorted(scores.values(), key=lambda x: x["rrf_score"], reverse=True)
        return results[:limit]

    # -- Semantic analysis for suggest --

    def find_similar_pairs(self, threshold: float = 0.7) -> list[dict]:
        """Find pairs of articles with high semantic similarity.

        Uses native vector_distance_cos for pairwise comparison.
        """
        articles = [dict(r) for r in self.conn.execute("""
            SELECT a.id, a.path, a.title, a.article_type
            FROM embeddings e JOIN articles a ON a.id = e.article_id
        """)]

        pairs = []
        for a in articles:
            for row in self.conn.execute("""
                SELECT a2.id, a2.path, a2.title,
                       vector_distance_cos(e1.vector, e2.vector) as dist
                FROM embeddings e1, embeddings e2
                JOIN articles a2 ON a2.id = e2.article_id
                WHERE e1.article_id = ? AND e2.article_id > ?
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
        """)]

        # Build adjacency via SQL
        neighbors = {a["id"]: [] for a in articles}
        for a in articles:
            for row in self.conn.execute("""
                SELECT e2.article_id as other_id,
                       vector_distance_cos(e1.vector, e2.vector) as dist
                FROM embeddings e1, embeddings e2
                WHERE e1.article_id = ? AND e2.article_id != ?
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
        row = self.conn.execute("SELECT COUNT(*) as n FROM embeddings").fetchone()
        total = self.conn.execute("SELECT COUNT(*) as n FROM articles").fetchone()
        return {
            "embedded": row["n"],
            "total_articles": total["n"],
            "model": self.model,
        }
