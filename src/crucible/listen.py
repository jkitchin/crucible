"""Live listening: find wiki articles related to what is being said.

The browser does the speech recognition (the Web Speech API on the
/_listen page that `crucible listen` opens) and posts the last stretch
of transcript to /_api/related every few seconds. related() answers with
the articles that best match it. It keeps no state between calls: the
page decides which results stay on screen as the conversation moves on.

Speech makes a poor search query. Keyword search asks for every word of
its query, so forty words of conversation would match nothing; here the
transcript is reduced to its most salient words, each searched on its
own, and an article scores by how many of them it matches and how well.
When the wiki has embeddings, a semantic search over the whole window is
fused with that by reciprocal rank fusion.
"""

import re
from collections import Counter

from crucible.database import CrucibleDB, fts_rows

# Common English words and spoken fillers that say nothing about a topic.
STOPWORDS = frozenset("""
a about above actually after again against all almost also although always am
among an and another any anyone anything are around as ask asked at away back
basically be became because become been before being below between both but by
came can cannot could did different do does doing done down during each either
else enough even ever every everyone everything example few find first for
from further get gets getting give given go goes going gone good got gotta
great had has have having he her here hers herself him himself his how however
i if in into is isn it its itself just keep kind kinda know known last least
less let like likely little look looking lot lots made make makes making many
may maybe me mean means might mine more most mostly much must my myself need
needs never new next no nobody none nor not nothing now of off often oh ok okay
on once one only onto or other others otherwise our ours ourselves out over own
part people perhaps pretty probably put quite rather really right said same
saw say saying says see seem seems seen several shall she should show since so
some somebody someone something sometimes sort still such sure take talk
talking tell than thank thanks that the their theirs them themselves then
there therefore these they thing things think thinking this those though
thought through thus to today together too took toward try trying two um uh
under unless until up upon us use used uses using very want wanted wants was
way we well went were what whatever when where whether which while who whole
whom whose why will with within without wonder would yeah yes yet you your
yours yourself yourselves
""".split())

WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9]*(?:[-'][A-Za-z0-9]+)*")

MAX_TEXT_CHARS = 4000
SNIPPET_CHARS = 240
RRF_K = 60


def keywords(text: str, max_terms: int = 8) -> list[str]:
    """The most salient words of a stretch of speech, best first.

    Stopwords and words under four letters are dropped (a short word in
    capitals, such as DFT or NEB, is kept). Words are ranked by how often
    they occur, each occurrence weighted by how late in the text it comes,
    so the newest speech counts most.
    """
    words = WORD_RE.findall(text)
    n = len(words)
    weights: Counter = Counter()
    spelling: dict[str, str] = {}
    for i, w in enumerate(words):
        low = w.lower().strip("'")
        if low.endswith("'s"):
            low = low[:-2]
        acronym = w.isupper() and 2 <= len(w) <= 5
        # A contraction (don't, we'll) is never a topic.
        if low in STOPWORDS or "'" in low or (len(low) < 4 and not acronym):
            continue
        weights[low] += 0.5 + (i + 1) / n
        spelling.setdefault(low, w if acronym else low)
    ranked = sorted(weights, key=lambda k: (-weights[k], k))
    return [spelling[k] for k in ranked[:max_terms]]


def _clean_snippet(text: str) -> str:
    """Article text without org keywords, drawers and markup noise."""
    lines = []
    in_drawer = False
    for line in text.splitlines():
        s = line.strip()
        if in_drawer:
            in_drawer = s.upper() != ":END:"
            continue
        if re.fullmatch(r":[A-Za-z_]+:", s):
            in_drawer = True
            continue
        if not s or s.startswith("#+") or s.startswith("*"):
            continue
        lines.append(s)
    out = " ".join(" ".join(lines).split())
    out = re.sub(r"\[\[[^\]]*\]\[([^\]]*)\]\]", r"\1", out)
    out = re.sub(r"\[\[([^\]]*)\]\]", r"\1", out)
    if len(out) > SNIPPET_CHARS:
        out = out[:SNIPPET_CHARS].rsplit(" ", 1)[0] + "..."
    return out


def _snippet(conn, row: dict) -> str:
    text = (row.get("abstract") or "").strip()
    if not text:
        text = row.get("_fts_content") or ""
        if not text:
            hit = conn.execute("SELECT content FROM article_fts WHERE rowid = ?",
                               (row["id"],)).fetchone()
            text = hit["content"] if hit else ""
    return _clean_snippet(text)


def _keyword_ranking(conn, terms: list[str], per_term: int) -> list[dict]:
    """Articles matching any of terms, those matching more and better first."""
    scores: dict[int, float] = {}
    rows: dict[int, dict] = {}
    hits: dict[int, list[str]] = {}
    for t, term in enumerate(terms):
        # Earlier terms are the more salient ones.
        term_weight = 1.0 / (1 + 0.15 * t)
        for rank, r in enumerate(fts_rows(conn, term, per_term)):
            scores[r["id"]] = scores.get(r["id"], 0.0) + term_weight / (1 + rank)
            rows.setdefault(r["id"], r)
            hits.setdefault(r["id"], []).append(term)
    ranked = sorted(scores, key=lambda i: -scores[i])
    return [dict(rows[i], _terms=hits[i]) for i in ranked]


def _semantic_ranking(conn, text: str, limit: int, ollama_url: str | None,
                      notes: list[str]) -> list[dict] | None:
    """Nearest articles to text by embedding, or None when unavailable."""
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='embeddings'"
    ).fetchone() is not None
    if not has_table:
        return None
    from crucible.embeddings import EmbeddingError, EmbeddingIndex
    idx = EmbeddingIndex(conn, url=ollama_url)
    if idx.stats()["embedded_with_model"] == 0:
        return None
    try:
        return idx.search(text, limit=limit)
    except EmbeddingError as e:
        notes.append(f"{e}; used keyword search only")
        return None


def related(conn, text: str, limit: int = 8,
            ollama_url: str | None = None) -> dict:
    """The articles most related to a stretch of transcript.

    Returns {"results": [...], "keywords": [...], "mode": "fts"|"hybrid",
    "notes": [...]}. Each result has path, title, article_type, status,
    score (1.0 for the best, the others relative to it), via (the methods
    that found it), terms (the keywords it matched) and snippet.
    """
    text = (text or "")[-MAX_TEXT_CHARS:]
    terms = keywords(text)
    notes: list[str] = []
    if not terms and not text.strip():
        return {"results": [], "keywords": [], "mode": "fts", "notes": notes}

    keyword_rows = _keyword_ranking(conn, terms, per_term=limit * 2) if terms else []
    semantic_rows = _semantic_ranking(conn, text, limit * 3, ollama_url, notes)

    fused: dict[int, dict] = {}

    def add(rows, method):
        for rank, r in enumerate(rows):
            entry = fused.setdefault(r["id"], {"row": r, "score": 0.0,
                                               "via": [], "terms": []})
            entry["score"] += 1.0 / (RRF_K + rank + 1)
            entry["via"].append(method)
            entry["terms"] = entry["terms"] or r.get("_terms", [])
            if method == "keyword":
                entry["row"] = r  # carries _fts_content for the snippet

    add(keyword_rows, "keyword")
    if semantic_rows:
        add(semantic_rows, "semantic")

    ranked = sorted(fused.values(), key=lambda e: -e["score"])[:limit]
    top = ranked[0]["score"] if ranked else 1.0
    results = []
    for e in ranked:
        r = e["row"]
        results.append({
            "path": r["path"],
            "title": r.get("title") or r["path"],
            "article_type": r.get("article_type", ""),
            "status": r.get("status", ""),
            "score": round(e["score"] / top, 4),
            "via": e["via"],
            "terms": e["terms"],
            "snippet": _snippet(conn, r),
        })
    return {"results": results, "keywords": terms,
            "mode": "hybrid" if semantic_rows else "fts", "notes": notes}


def related_from_db(db_path, text: str, limit: int = 8,
                    ollama_url: str | None = None) -> dict:
    """related() against the database at db_path, opened for this call."""
    db = CrucibleDB(db_path)
    try:
        return related(db.conn, text, limit=limit, ollama_url=ollama_url)
    finally:
        db.close()


# The /_listen page body, served inside the browse template. The browser
# transcribes; every few seconds the page posts the recent transcript to
# /_api/related and folds the answer into a decaying score per article,
# so an article appears once it has stayed relevant for a moment and
# fades when the talk moves on. Settings come from the query string:
# interval (seconds between updates), window (seconds of transcript
# searched) and lang (a BCP 47 tag such as en-US).
LISTEN_HTML = """\
<style>
#listen-bar { display: flex; gap: 0.75em; align-items: center; flex-wrap: wrap; }
#listen-bar button { font-size: 1em; padding: 0.4em 1em; cursor: pointer; }
#listen-status { color: #666; }
#listen-status.on::before { content: "\\25CF "; color: #c0392b; }
#listen-cols { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.3fr);
  gap: 1.5em; margin-top: 1em; }
@media (max-width: 900px) { #listen-cols { grid-template-columns: 1fr; } }
#transcript { max-height: 70vh; overflow-y: auto; font-size: 0.95em;
  line-height: 1.5; border-left: 3px solid #ddd; padding-left: 0.75em; }
#transcript .seg { margin: 0 0 0.4em; }
#transcript .seg time { color: #999; font-size: 0.8em; margin-right: 0.4em; }
#transcript .interim { color: #999; font-style: italic; }
#keywords { color: #666; font-size: 0.85em; min-height: 1.2em; }
#keywords span { background: #eef3f8; border-radius: 3px; padding: 0 0.35em;
  margin-right: 0.3em; }
.live-card { border: 1px solid #ddd; border-radius: 6px; padding: 0.6em 0.8em;
  margin-bottom: 0.6em; transition: opacity 0.6s; }
.live-card h3 { margin: 0 0 0.2em; font-size: 1.05em; }
.live-card .meta { color: #888; font-size: 0.8em; }
.live-card .snippet { margin: 0.3em 0 0; font-size: 0.9em; color: #333; }
.live-card .bar { height: 3px; background: #3498db; border-radius: 2px;
  margin-top: 0.4em; transition: width 0.6s; }
.live-card.new { border-color: #3498db; }
#manual { width: 100%; box-sizing: border-box; margin-top: 0.75em; font: inherit; }
#history li { margin-bottom: 0.2em; }
#history time { color: #999; font-size: 0.8em; margin-right: 0.4em; }
</style>
<h1>Listen</h1>
<div id="listen-bar">
  <button id="listen-toggle" type="button">Start listening</button>
  <button id="listen-save" type="button" disabled>Save session (.org)</button>
  <span id="listen-status">Not listening</span>
</div>
<p id="listen-support" class="meta"></p>
<div id="listen-cols">
  <section>
    <h2>Transcript</h2>
    <div id="transcript"></div>
    <textarea id="manual" rows="2"
      placeholder="Or type here: text entered here is searched as if spoken"></textarea>
  </section>
  <section>
    <h2>Related</h2>
    <div id="keywords"></div>
    <div id="related"><p class="meta">Related articles appear here as you talk.</p></div>
    <h2>Shown this session</h2>
    <ol id="history"></ol>
  </section>
</div>
<script>
(function () {
  var params = new URLSearchParams(location.search);
  function num(name, dflt, lo, hi) {
    var v = parseFloat(params.get(name));
    return isFinite(v) ? Math.min(hi, Math.max(lo, v)) : dflt;
  }
  var INTERVAL = num('interval', 5, 1, 120) * 1000;
  var WINDOW = num('window', 60, 10, 600) * 1000;
  var LANG = params.get('lang') || navigator.language || 'en-US';
  var DECAY = 0.6, SHOW = 0.9, DROP = 0.15, MAX_SHOWN = 8;

  var segments = [];      // {t: Date, text: String}, final results only
  var interim = '';
  var manual = '';
  var scores = {};        // path -> {score, item}
  var history = [];       // {t, path, title, url, terms}
  var shownEver = {};
  var listening = false, rec = null, lastQuery = '', busy = false;

  var el = function (id) { return document.getElementById(id); };
  var Rec = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!Rec) {
    el('listen-support').textContent = 'This browser has no speech recognition ' +
      '(try Chrome, Edge or Safari). You can still type in the box below.';
    el('listen-toggle').disabled = true;
  } else {
    el('listen-support').textContent = 'Language ' + LANG + '; updates every ' +
      INTERVAL / 1000 + ' s over the last ' + WINDOW / 1000 + ' s of speech. ' +
      'Articles open in a new tab so listening continues.';
  }

  function stamp(d) {
    return d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', second: '2-digit'});
  }

  function renderTranscript() {
    var box = el('transcript');
    var atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
    box.textContent = '';
    segments.forEach(function (s) {
      var p = document.createElement('p'); p.className = 'seg';
      var t = document.createElement('time'); t.textContent = stamp(s.t);
      p.appendChild(t); p.appendChild(document.createTextNode(s.text));
      box.appendChild(p);
    });
    if (interim) {
      var p = document.createElement('p'); p.className = 'seg interim';
      p.textContent = interim; box.appendChild(p);
    }
    if (atBottom) box.scrollTop = box.scrollHeight;
  }

  function windowText() {
    var cutoff = Date.now() - WINDOW;
    var parts = segments.filter(function (s) { return s.t.getTime() >= cutoff; })
      .map(function (s) { return s.text; });
    if (interim) parts.push(interim);
    if (manual) parts.push(manual);
    return parts.join(' ').trim();
  }

  function start() {
    rec = new Rec();
    rec.continuous = true;
    rec.interimResults = true;
    rec.lang = LANG;
    rec.onresult = function (ev) {
      interim = '';
      for (var i = ev.resultIndex; i < ev.results.length; i++) {
        var r = ev.results[i], text = r[0].transcript.trim();
        if (!text) continue;
        if (r.isFinal) segments.push({t: new Date(), text: text});
        else interim += (interim ? ' ' : '') + text;
      }
      el('listen-save').disabled = false;
      renderTranscript();
    };
    rec.onerror = function (ev) {
      if (ev.error === 'not-allowed' || ev.error === 'service-not-allowed') {
        stop('Microphone access was refused. Allow it for this page and try again.');
      } else if (ev.error !== 'no-speech' && ev.error !== 'aborted') {
        el('listen-status').textContent = 'Recognition error: ' + ev.error;
      }
    };
    // Recognition ends by itself after a pause or a time limit; restart it.
    rec.onend = function () {
      if (listening) setTimeout(function () { try { rec.start(); } catch (e) {} }, 250);
    };
    rec.start();
    listening = true;
    el('listen-toggle').textContent = 'Stop listening';
    el('listen-status').textContent = 'Listening';
    el('listen-status').className = 'on';
  }

  function stop(message) {
    listening = false;
    if (rec) { try { rec.stop(); } catch (e) {} }
    if (interim) { segments.push({t: new Date(), text: interim}); interim = ''; }
    renderTranscript();
    el('listen-toggle').textContent = 'Start listening';
    el('listen-status').textContent = message || 'Not listening';
    el('listen-status').className = '';
  }

  el('listen-toggle').addEventListener('click', function () {
    if (listening) stop(); else start();
  });
  el('manual').addEventListener('input', function () {
    manual = this.value.trim();
  });

  function update() {
    var text = windowText();
    if (busy || !text || text === lastQuery) return;
    busy = true;
    lastQuery = text;
    fetch('/_api/related', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({text: text})
    }).then(function (r) { return r.json(); }).then(function (data) {
      if (data.error) throw new Error(data.error);
      fold(data);
    }).catch(function (e) {
      el('listen-status').textContent = 'Search failed: ' + e.message;
    }).then(function () { busy = false; });
  }

  function fold(data) {
    Object.keys(scores).forEach(function (p) { scores[p].score *= DECAY; });
    data.results.forEach(function (item) {
      var s = scores[item.path] || (scores[item.path] = {score: 0});
      s.score += item.score;
      s.item = item;
    });
    Object.keys(scores).forEach(function (p) {
      if (scores[p].score < DROP) delete scores[p];
    });
    var kw = el('keywords'); kw.textContent = '';
    (data.keywords || []).forEach(function (k) {
      var s = document.createElement('span'); s.textContent = k; kw.appendChild(s);
    });
    if (data.notes && data.notes.length) {
      kw.appendChild(document.createTextNode(' (' + data.notes.join('; ') + ')'));
    }
    renderRelated();
  }

  function renderRelated() {
    var shown = Object.keys(scores).map(function (p) { return scores[p]; })
      .filter(function (s) { return s.score >= SHOW; })
      .sort(function (a, b) { return b.score - a.score; })
      .slice(0, MAX_SHOWN);
    var box = el('related');
    box.textContent = '';
    if (!shown.length) {
      var p = document.createElement('p'); p.className = 'meta';
      p.textContent = 'Nothing stands out yet.'; box.appendChild(p);
      return;
    }
    var top = shown[0].score;
    shown.forEach(function (s) {
      var it = s.item, card = document.createElement('div');
      card.className = 'live-card';
      if (!shownEver[it.path]) {
        shownEver[it.path] = true;
        card.className += ' new';
        history.push({t: new Date(), path: it.path, title: it.title,
                      url: it.url, file: it.file, terms: it.terms.slice()});
        renderHistory();
      }
      var h = document.createElement('h3'), a = document.createElement('a');
      a.href = it.url; a.target = '_blank'; a.rel = 'noopener';
      a.textContent = it.title; h.appendChild(a); card.appendChild(h);
      var meta = document.createElement('div'); meta.className = 'meta';
      meta.textContent = [it.article_type, it.status, 'via ' + it.via.join(' + ')]
        .concat(it.terms.length ? ['matched: ' + it.terms.join(', ')] : [])
        .filter(Boolean).join(' \\u00b7 ');
      card.appendChild(meta);
      if (it.snippet) {
        var sn = document.createElement('p'); sn.className = 'snippet';
        sn.textContent = it.snippet; card.appendChild(sn);
      }
      var bar = document.createElement('div'); bar.className = 'bar';
      bar.style.width = Math.round(100 * s.score / top) + '%';
      card.appendChild(bar);
      box.appendChild(card);
    });
  }

  function renderHistory() {
    var ol = el('history'); ol.textContent = '';
    history.forEach(function (h) {
      var li = document.createElement('li');
      var t = document.createElement('time'); t.textContent = stamp(h.t);
      var a = document.createElement('a');
      a.href = h.url; a.target = '_blank'; a.rel = 'noopener'; a.textContent = h.title;
      li.appendChild(t); li.appendChild(a);
      ol.appendChild(li);
    });
    el('listen-save').disabled = false;
  }

  function pad(n) { return (n < 10 ? '0' : '') + n; }
  function orgStamp(d) {
    return '[' + d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) +
      ' ' + ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][d.getDay()] + ' ' +
      pad(d.getHours()) + ':' + pad(d.getMinutes()) + ']';
  }

  // A record of the session: the articles shown, with absolute file
  // links (the download lands outside the project), then the transcript.
  el('listen-save').addEventListener('click', function () {
    var now = new Date();
    var lines = ['#+TITLE: Listening session ' + orgStamp(now), '',
                 '* Articles shown', ''];
    history.forEach(function (h) {
      lines.push('- ' + orgStamp(h.t) + ' [[file:' + h.file + '][' + h.title + ']]' +
                 (h.terms.length ? ' (' + h.terms.join(', ') + ')' : ''));
    });
    lines.push('', '* Transcript', '');
    segments.forEach(function (s) { lines.push(orgStamp(s.t) + ' ' + s.text, ''); });
    var blob = new Blob([lines.join('\\n')], {type: 'text/plain'});
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'listen-' + now.toISOString().slice(0, 16).replace(/[:T]/g, '-') + '.org';
    document.body.appendChild(a); a.click(); a.remove();
  });

  setInterval(update, INTERVAL);
})();
</script>
"""
