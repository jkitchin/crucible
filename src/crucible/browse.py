"""Local HTTP server for browsing the Crucible wiki in a browser.

Converts org-mode files to HTML on the fly with working inter-file links,
citation rendering, and a navigable sidebar.
"""

import html
import http.server
import json
import re
import urllib.parse
from pathlib import Path, PurePosixPath


# ---------------------------------------------------------------------------
# BibTeX parser (lightweight, no external deps)
# ---------------------------------------------------------------------------

_BIB_ENTRY_RE = re.compile(
    r"@(\w+)\s*\{\s*([^,\s]+)\s*,\s*(.*?)\n\s*\}",
    re.DOTALL,
)
_BIB_FIELD_RE = re.compile(
    r"(\w+)\s*=\s*\{(.*?)\}",
    re.DOTALL,
)


def parse_bibtex(text: str) -> dict[str, dict[str, str]]:
    """Parse bibtex into {key: {field: value, ...}, ...}.

    Lightweight parser that handles the subset of bibtex that crucible generates.
    """
    entries: dict[str, dict[str, str]] = {}
    for m in _BIB_ENTRY_RE.finditer(text):
        entry_type = m.group(1).lower()
        key = m.group(2).strip()
        body = m.group(3)
        fields: dict[str, str] = {"_type": entry_type}
        for fm in _BIB_FIELD_RE.finditer(body):
            fields[fm.group(1).lower()] = fm.group(2).strip()
        entries[key] = fields
    return entries


def format_citation(entry: dict[str, str]) -> str:
    """Format a bib entry as a readable string for tooltips."""
    author = entry.get("author", "")
    title = entry.get("title", "")
    year = entry.get("year", "")
    journal = entry.get("journal", "")
    url = entry.get("url", "")
    doi = entry.get("doi", "")

    parts = []
    if author:
        parts.append(author)
    if title:
        parts.append(f'"{title}"')
    if journal:
        parts.append(journal)
    if year:
        parts.append(f"({year})")
    if doi:
        parts.append(f"doi:{doi}")
    elif url:
        parts.append(url)

    return ", ".join(parts) if parts else ""


# ---------------------------------------------------------------------------
# Org-to-HTML converter
# ---------------------------------------------------------------------------

# Block patterns
_TITLE_RE = re.compile(r"^#\+TITLE:\s*(.+)$", re.MULTILINE)
_FILETAGS_RE = re.compile(r"^#\+FILETAGS:\s*(.+)$", re.MULTILINE)
_KEYWORD_RE = re.compile(r"^#\+\w+:.*$", re.MULTILINE)
_DRAWER_RE = re.compile(
    r"^\s*:PROPERTIES:\s*\n(.*?)\n\s*:END:\s*$", re.MULTILINE | re.DOTALL
)
_PROP_LINE_RE = re.compile(r"^\s*:([A-Z_]+):\s+(.+?)\s*$", re.MULTILINE)
_SRC_BLOCK_RE = re.compile(
    r"^#\+BEGIN_SRC\s*(.*?)\s*\n(.*?)^#\+END_SRC\s*$",
    re.MULTILINE | re.DOTALL,
)
_EXAMPLE_BLOCK_RE = re.compile(
    r"^#\+BEGIN_EXAMPLE\s*\n(.*?)^#\+END_EXAMPLE\s*$",
    re.MULTILINE | re.DOTALL,
)
_QUOTE_BLOCK_RE = re.compile(
    r"^#\+BEGIN_QUOTE\s*\n(.*?)^#\+END_QUOTE\s*$",
    re.MULTILINE | re.DOTALL,
)
_HEADING_RE = re.compile(r"^(\*{1,4})\s+(.+)$", re.MULTILINE)
_TABLE_BLOCK_RE = re.compile(r"((?:^\|.*$\n?)+)", re.MULTILINE)

# Inline patterns
_FILE_LINK_RE = re.compile(
    r"\[\[file:([^\]]+?)(?:::.*?)?\](?:\[([^\]]*)\])?\]"
)
_PLAIN_LINK_RE = re.compile(
    r"\[\[([^\]]+)\]\[([^\]]*)\]\]"
)
_CITE_RE = re.compile(r"(cite[pt]?):([a-zA-Z0-9_:,-]+)")
_BOLD_RE = re.compile(r"(?<![a-zA-Z0-9])\*([^\s*](?:.*?[^\s*])?)\*(?![a-zA-Z0-9])")
_ITALIC_RE = re.compile(r"(?<![a-zA-Z0-9])/([^\s/](?:.*?[^\s/])?)/(?![a-zA-Z0-9])")
_CODE_RE = re.compile(r"(?<![a-zA-Z0-9])~([^\s~](?:.*?[^\s~])?)~(?![a-zA-Z0-9])")
_VERBATIM_RE = re.compile(r"(?<![a-zA-Z0-9])=([^\s=](?:.*?[^\s=])?)=(?![a-zA-Z0-9])")
_LATEX_DISPLAY_RE = re.compile(r"\$\$(.+?)\$\$", re.DOTALL)
_LATEX_INLINE_RE = re.compile(r"(?<![\\$])\$([^\s$][^$\n]*?[^\s$])\$(?!\$)|(?<![\\$])\$([^\s$])\$(?!\$)")


def _rewrite_file_link(match: re.Match, current_path: str) -> str:
    """Rewrite [[file:...][text]] to a working HTML link."""
    target = match.group(1).strip()
    display = match.group(2) or target.rsplit("/", 1)[-1].replace(".org", "")
    display = html.escape(display)
    # Resolve relative to current file's directory
    current_dir = PurePosixPath(current_path).parent
    resolved = (current_dir / target).as_posix()
    # Normalize (collapse ..)
    parts = []
    for part in resolved.split("/"):
        if part == "..":
            if parts:
                parts.pop()
        elif part and part != ".":
            parts.append(part)
    url = "/" + "/".join(parts)
    return f'<a href="{html.escape(url)}">{display}</a>'


def _rewrite_plain_link(match: re.Match) -> str:
    """Rewrite [[target][text]] non-file links."""
    target = match.group(1).strip()
    display = html.escape(match.group(2) or target)
    if target.startswith(("http://", "https://")):
        return f'<a href="{html.escape(target)}" target="_blank">{display}</a>'
    return f'<span class="link-unknown" title="{html.escape(target)}">{display}</span>'


def _inline_markup(text: str, current_path: str) -> str:
    """Apply inline markup transformations to a text chunk."""
    # Escape HTML first, but preserve our placeholders
    text = html.escape(text)

    # We need to un-escape the org syntax characters that html.escape touched
    # since our regexes need to match the original patterns. Work on raw text
    # instead: re-do this properly by processing raw text.
    # Actually, let's process raw text and escape only the non-markup parts.
    # Simpler approach: process before escaping, use safe HTML generation.
    return text  # placeholder, replaced below


def _process_inline(text: str, current_path: str,
                    bib: dict[str, dict[str, str]] | None = None) -> str:
    """Process inline org markup, returning safe HTML."""
    # Use placeholders to protect generated HTML (which may contain /
    # or * characters) from the bold/italic regexes that run last.
    _ph: dict[str, str] = {}
    _counter = [0]

    def _hold(fragment: str) -> str:
        key = f"\x01I{_counter[0]}\x01"
        _counter[0] += 1
        _ph[key] = fragment
        return key

    # File links
    text = _FILE_LINK_RE.sub(lambda m: _hold(_rewrite_file_link(m, current_path)), text)

    # Plain links
    text = _PLAIN_LINK_RE.sub(lambda m: _hold(_rewrite_plain_link(m)), text)

    # Citations (with bib tooltips, linked to references page)
    def _cite_repl(m: re.Match) -> str:
        cite_type = m.group(1)
        keys = [k.strip() for k in m.group(2).split(",") if k.strip()]
        spans = []
        for key in keys:
            entry = bib.get(key) if bib else None
            ekey = html.escape(key)
            if entry:
                tooltip = html.escape(format_citation(entry))
                spans.append(
                    f'<a class="citation" href="/_references#{ekey}" title="{tooltip}">[{ekey}]</a>'
                )
            else:
                spans.append(
                    f'<a class="citation" href="/_references#{ekey}" title="{cite_type}:{ekey}">[{ekey}]</a>'
                )
        return _hold("".join(spans))

    text = _CITE_RE.sub(_cite_repl, text)

    # LaTeX (before other inline markup to avoid conflicts)
    text = _LATEX_DISPLAY_RE.sub(
        lambda m: _hold(f'<div class="math-display">\\[{m.group(1)}\\]</div>'), text)
    text = _LATEX_INLINE_RE.sub(
        lambda m: _hold(f'<span class="math-inline">\\({m.group(1) or m.group(2)}\\)</span>'),
        text,
    )

    # Code/verbatim (before bold/italic so * inside code is safe)
    text = _CODE_RE.sub(lambda m: _hold(f"<code>{html.escape(m.group(1))}</code>"), text)
    text = _VERBATIM_RE.sub(lambda m: _hold(f"<code>{html.escape(m.group(1))}</code>"), text)

    # Bold and italic (safe now, all HTML-containing elements are placeholders)
    text = _BOLD_RE.sub(r"<strong>\1</strong>", text)
    text = _ITALIC_RE.sub(r"<em>\1</em>", text)

    # Restore placeholders
    for key, value in _ph.items():
        text = text.replace(key, value)

    return text


def _convert_table(table_text: str) -> str:
    """Convert org table lines to HTML table."""
    lines = [l.strip() for l in table_text.strip().splitlines() if l.strip()]
    rows = []
    for line in lines:
        if line.startswith("|") and set(line.replace("|", "").strip()) <= {"-", "+"}:
            rows.append(None)  # separator
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        rows.append(cells)

    out = ['<table>']
    in_head = False
    # If there's a separator after the first row, treat first row as header
    if len(rows) > 1 and rows[1] is None:
        in_head = True

    for i, row in enumerate(rows):
        if row is None:
            continue
        tag = "th" if in_head and i == 0 else "td"
        out.append("<tr>")
        for cell in row:
            out.append(f"  <{tag}>{html.escape(cell)}</{tag}>")
        out.append("</tr>")
    out.append("</table>")
    return "\n".join(out)


def org_to_html(text: str, current_path: str = "",
                bib: dict[str, dict[str, str]] | None = None) -> tuple[str, str, list[str]]:
    """Convert org-mode text to an HTML fragment.

    Returns (title, html_content, filetags).
    current_path is the wiki-relative path (e.g. "concepts/foo.org") for
    resolving relative file links.
    bib is an optional dict of parsed bibtex entries for citation tooltips.
    """
    title = ""
    filetags = []

    # Extract title
    m = _TITLE_RE.search(text)
    if m:
        title = m.group(1).strip()

    # Extract filetags
    m = _FILETAGS_RE.search(text)
    if m:
        raw = m.group(1).strip()
        filetags = [t for t in raw.strip(":").split(":") if t]

    # Remove keyword lines (#+TITLE, #+FILETAGS, #+SOURCE, etc.)
    text = _KEYWORD_RE.sub("", text)

    # Replace blocks with placeholders to protect them from inline processing
    placeholders = {}
    counter = [0]

    def _placeholder(content: str) -> str:
        key = f"\x00BLOCK{counter[0]}\x00"
        counter[0] += 1
        placeholders[key] = content
        return key

    # Source blocks
    def _src_repl(m: re.Match) -> str:
        lang = html.escape(m.group(1).strip()) if m.group(1).strip() else ""
        code = html.escape(m.group(2))
        cls = f' class="language-{lang}"' if lang else ""
        return _placeholder(f"<pre><code{cls}>{code}</code></pre>")

    text = _SRC_BLOCK_RE.sub(_src_repl, text)

    # Example blocks
    text = _EXAMPLE_BLOCK_RE.sub(
        lambda m: _placeholder(f"<pre>{html.escape(m.group(1))}</pre>"), text
    )

    # Quote blocks
    text = _QUOTE_BLOCK_RE.sub(
        lambda m: _placeholder(
            f"<blockquote>{_process_inline(m.group(1), current_path, bib)}</blockquote>"
        ),
        text,
    )

    # Property drawers
    def _drawer_repl(m: re.Match) -> str:
        props = _PROP_LINE_RE.findall(m.group(1))
        if not props:
            return ""
        rows = "".join(
            f"<tr><td>{html.escape(k)}</td><td>{html.escape(v)}</td></tr>"
            for k, v in props
        )
        return _placeholder(
            '<details class="properties"><summary>Properties</summary>'
            f"<table>{rows}</table></details>"
        )

    text = _DRAWER_RE.sub(_drawer_repl, text)

    # Tables
    text = _TABLE_BLOCK_RE.sub(
        lambda m: _placeholder(_convert_table(m.group(1))), text
    )

    # Headings
    def _heading_repl(m: re.Match) -> str:
        level = len(m.group(1)) + 1  # * -> h2, ** -> h3, etc.
        content = _process_inline(m.group(2), current_path, bib)
        slug = re.sub(r"[^a-z0-9]+", "-", m.group(2).lower()).strip("-")
        return _placeholder(f'<h{level} id="{slug}">{content}</h{level}>')

    text = _HEADING_RE.sub(_heading_repl, text)

    # Process remaining text as paragraphs with inline markup
    paragraphs = re.split(r"\n\s*\n", text)
    parts = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        # Check if it's just a placeholder
        if para in placeholders:
            parts.append(para)
            continue
        # It might contain mixed placeholders and text
        processed = _process_inline(para, current_path, bib)
        # Don't wrap placeholders in <p>
        if "\x00BLOCK" in processed:
            parts.append(processed)
        else:
            parts.append(f"<p>{processed}</p>")

    content = "\n".join(parts)

    # Restore placeholders
    for key, value in placeholders.items():
        content = content.replace(key, value)

    return title, content, filetags


# ---------------------------------------------------------------------------
# HTML template and CSS
# ---------------------------------------------------------------------------

_BROWSE_CSS = """\
:root {
  --bg: #1a1a2e;
  --panel: #16213e;
  --border: #0f3460;
  --text: #e0e0e0;
  --text-dim: #8892a0;
  --accent: #e94560;
  --link: #5dade2;
  --code-bg: #0d1117;
  --tag-bg: #0f3460;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: 'Inter', system-ui, -apple-system, sans-serif;
  font-size: 15px; line-height: 1.7;
  display: flex; min-height: 100vh;
}
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }

/* Sidebar */
#sidebar {
  width: 260px; min-width: 260px; background: var(--panel);
  border-right: 1px solid var(--border);
  padding: 16px; overflow-y: auto; position: fixed;
  top: 0; bottom: 0; left: 0;
}
#sidebar h1 {
  font-size: 16px; color: var(--accent); margin-bottom: 12px;
  letter-spacing: 0.5px;
}
#sidebar h1 a { color: var(--accent); }
.project-name {
  font-size: 13px; color: var(--text-dim); margin: -8px 0 8px;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.github-link {
  display: inline-block; font-size: 12px; color: var(--text-dim);
  margin-bottom: 8px; padding: 2px 8px; border-radius: 4px;
  border: 1px solid var(--border); text-decoration: none;
}
.github-link:hover { color: var(--link); border-color: var(--link); text-decoration: none; }
#sidebar h2 {
  font-size: 12px; text-transform: uppercase; letter-spacing: 1px;
  color: var(--text-dim); margin-top: 16px; margin-bottom: 6px;
}
#sidebar ul { list-style: none; }
#sidebar li { margin: 2px 0; }
#sidebar li a {
  display: block; padding: 3px 8px; border-radius: 4px;
  font-size: 13px; color: var(--text); white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis;
}
#sidebar li a:hover { background: var(--border); text-decoration: none; }
#sidebar li a.active { background: var(--accent); color: #fff; }

/* Search */
#search-form { margin: 12px 0; }
#search-input {
  width: 100%; padding: 6px 10px; border-radius: 4px;
  border: 1px solid var(--border); background: var(--bg);
  color: var(--text); font-size: 13px;
}
#search-input:focus { outline: none; border-color: var(--accent); }

/* Main content */
#content {
  margin-left: 260px; padding: 32px 48px; max-width: 860px;
  flex: 1;
}
#content h1 { font-size: 28px; margin-bottom: 8px; color: #fff; }
#content h2 { font-size: 22px; margin-top: 32px; margin-bottom: 12px; color: #fff; }
#content h3 { font-size: 18px; margin-top: 24px; margin-bottom: 8px; color: #fff; }
#content h4 { font-size: 16px; margin-top: 20px; margin-bottom: 6px; color: #fff; }
#content p { margin-bottom: 12px; }

.tags { margin-bottom: 16px; }
.tag {
  display: inline-block; background: var(--tag-bg); color: var(--text-dim);
  padding: 2px 8px; border-radius: 10px; font-size: 12px; margin-right: 4px;
}

/* Code */
pre {
  background: var(--code-bg); border: 1px solid var(--border);
  border-radius: 6px; padding: 14px 16px; overflow-x: auto;
  margin: 12px 0; font-size: 13px; line-height: 1.5;
}
code {
  font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace;
  font-size: 0.9em;
}
p code, li code {
  background: var(--code-bg); padding: 1px 5px; border-radius: 3px;
}

/* Tables */
table {
  border-collapse: collapse; margin: 12px 0; width: 100%;
}
th, td {
  border: 1px solid var(--border); padding: 6px 12px; text-align: left;
}
th { background: var(--panel); color: #fff; font-weight: 600; }
tr:nth-child(even) td { background: rgba(15, 52, 96, 0.3); }

/* Blockquotes */
blockquote {
  border-left: 3px solid var(--accent); padding: 8px 16px;
  margin: 12px 0; color: var(--text-dim); background: var(--panel);
  border-radius: 0 4px 4px 0;
}

/* Properties drawer */
details.properties {
  margin: 8px 0 16px; font-size: 13px; color: var(--text-dim);
}
details.properties summary {
  cursor: pointer; color: var(--text-dim); font-size: 12px;
  text-transform: uppercase; letter-spacing: 0.5px;
}
details.properties table { width: auto; font-size: 13px; }

/* Citations */
a.citation {
  color: var(--accent); font-size: 0.9em;
  text-decoration: none; border-bottom: 1px dotted var(--accent);
}
a.citation:hover { border-bottom-style: solid; text-decoration: none; }

/* Math (KaTeX) */
.math-display { margin: 12px 0; text-align: center; }

/* Search results */
.search-result { margin-bottom: 20px; }
.search-result h3 { margin-top: 0; margin-bottom: 4px; }
.search-result .path { font-size: 12px; color: var(--text-dim); }

/* Index page article list */
.article-list { margin-bottom: 24px; }
.article-list h2 { border-bottom: 1px solid var(--border); padding-bottom: 6px; }
.article-list ul { list-style: none; margin-top: 8px; }
.article-list li { padding: 4px 0; }
.article-list li a { font-size: 15px; }
.article-list .meta { font-size: 12px; color: var(--text-dim); margin-left: 8px; }

/* Nav links (Topics, References, Graph) */
.nav-links {
  display: flex; gap: 8px; margin: 8px 0 4px; flex-wrap: wrap;
}
.nav-links a {
  display: inline-block; padding: 3px 10px; border-radius: 4px;
  font-size: 12px; background: var(--border); color: var(--text);
  text-decoration: none;
}
.nav-links a:hover { background: var(--accent); color: #fff; text-decoration: none; }

/* References page */
.ref-entry {
  margin-bottom: 16px; padding: 12px 16px;
  background: var(--panel); border-radius: 6px;
  border: 1px solid var(--border);
}
.ref-entry:target { border-color: var(--accent); }
.ref-key { font-size: 15px; color: var(--accent); margin-bottom: 4px; }
.ref-link { font-size: 13px; margin: 2px 0; }
.ref-link a { word-break: break-all; }

/* Topics page */
.topic-entry { margin-bottom: 16px; }
.topic-entry h3 { font-size: 16px; color: #fff; margin-bottom: 4px; }
.topic-entry ul { list-style: none; margin-left: 8px; }
.topic-entry li { padding: 2px 0; }

/* VS Code edit link */
.vscode-link {
  display: inline-block; font-size: 12px; color: var(--text-dim);
  margin-left: 12px; padding: 2px 8px; border-radius: 4px;
  border: 1px solid var(--border); text-decoration: none;
  vertical-align: middle;
}
.vscode-link:hover { color: var(--link); border-color: var(--link); text-decoration: none; }
"""

_BROWSE_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} - Crucible</title>
<style>{css}</style>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.css">
<script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.js"></script>
<script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/contrib/auto-render.min.js"
  onload="renderMathInElement(document.getElementById('content'), {{
    delimiters: [
      {{left: '\\\\[', right: '\\\\]', display: true}},
      {{left: '\\\\(', right: '\\\\)', display: false}}
    ]
  }});"></script>
</head>
<body>
<nav id="sidebar">
  <h1><a href="/">Crucible</a></h1>
  <div class="project-name">{project_name}</div>
  {github_link}
  <form id="search-form" action="/_search" method="get">
    <input id="search-input" type="text" name="q" placeholder="Search..." autocomplete="off">
  </form>
  {nav}
</nav>
<main id="content">
  {content}
</main>
</body>
</html>
"""

# Fallback template without KaTeX CDN (for offline use)
_BROWSE_TEMPLATE_OFFLINE = _BROWSE_TEMPLATE.replace(
    '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.css">\n'
    '<script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.js"></script>\n'
    '<script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/contrib/auto-render.min.js"\n'
    '  onload="renderMathInElement(document.getElementById(\'content\'), {{\n'
    '    delimiters: [\n'
    '      {{left: \'\\\\[\', right: \'\\\\]\', display: true}},\n'
    '      {{left: \'\\\\(\', right: \'\\\\)\', display: false}}\n'
    '    ]\n'
    '  }});"></script>',
    "",
)


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

_WIKI_PREFIX = ".crucible/wiki/"


def _url_path(db_path: str) -> str:
    """Convert a DB article path to a URL path (strip .crucible/wiki/ prefix)."""
    if db_path.startswith(_WIKI_PREFIX):
        return db_path[len(_WIKI_PREFIX):]
    return db_path


def _build_nav_html(articles: list[dict]) -> str:
    """Build the sidebar navigation HTML from article list."""
    groups: dict[str, list[dict]] = {}
    for a in articles:
        atype = a.get("article_type", "other")
        groups.setdefault(atype, []).append(a)

    type_labels = {
        "concept": "Concepts",
        "summary": "Summaries",
        "comparison": "Comparisons",
        "method": "Methods",
    }

    # Navigation links to special pages
    parts = [
        '<div class="nav-links">',
        '<a href="/_topics">Topics</a>',
        '<a href="/_references">References</a>',
        '<a href="/_graph">Graph</a>',
        '<a href="/_about">About</a>',
        "</div>",
    ]

    for atype in ("concept", "summary", "comparison", "method"):
        items = groups.get(atype, [])
        if not items:
            continue
        items.sort(key=lambda a: a["title"].lower())
        label = type_labels.get(atype, atype.title())
        parts.append(f"<h2>{label} ({len(items)})</h2>")
        parts.append("<ul>")
        for a in items:
            path = _url_path(a["path"])
            title = html.escape(a["title"])
            parts.append(f'<li><a href="/{path}">{title}</a></li>')
        parts.append("</ul>")

    # Catch any other types
    for atype, items in sorted(groups.items()):
        if atype in type_labels:
            continue
        items.sort(key=lambda a: a["title"].lower())
        parts.append(f"<h2>{atype.title()} ({len(items)})</h2>")
        parts.append("<ul>")
        for a in items:
            path = _url_path(a["path"])
            title = html.escape(a["title"])
            parts.append(f'<li><a href="/{path}">{title}</a></li>')
        parts.append("</ul>")

    return "\n".join(parts)


def _build_index_html(articles: list[dict]) -> str:
    """Build the index/home page content."""
    groups: dict[str, list[dict]] = {}
    for a in articles:
        atype = a.get("article_type", "other")
        groups.setdefault(atype, []).append(a)

    type_labels = {
        "concept": "Concepts",
        "summary": "Summaries",
        "comparison": "Comparisons",
        "method": "Methods",
    }

    parts = ["<h1>Crucible Wiki</h1>"]
    total = len(articles)
    parts.append(f'<p class="meta">{total} articles</p>')

    for atype in ("concept", "summary", "comparison", "method"):
        items = groups.get(atype, [])
        if not items:
            continue
        items.sort(key=lambda a: a["title"].lower())
        label = type_labels.get(atype, atype.title())
        parts.append(f'<div class="article-list"><h2>{label}</h2><ul>')
        for a in items:
            path = _url_path(a["path"])
            title = html.escape(a["title"])
            status = a.get("status", "")
            meta = f'<span class="meta">{status}</span>' if status else ""
            parts.append(f'<li><a href="/{path}">{title}</a>{meta}</li>')
        parts.append("</ul></div>")

    # Other types
    for atype, items in sorted(groups.items()):
        if atype in type_labels:
            continue
        items.sort(key=lambda a: a["title"].lower())
        parts.append(f'<div class="article-list"><h2>{atype.title()}</h2><ul>')
        for a in items:
            path = _url_path(a["path"])
            title = html.escape(a["title"])
            parts.append(f'<li><a href="/{path}">{title}</a></li>')
        parts.append("</ul></div>")

    return "\n".join(parts)


def _build_search_html(query: str, results: list[dict]) -> str:
    """Build search results page content."""
    parts = [f"<h1>Search: {html.escape(query)}</h1>"]
    if not results:
        parts.append("<p>No results found.</p>")
    else:
        parts.append(f"<p>{len(results)} result(s)</p>")
        for r in results:
            title = html.escape(r.get("title", r.get("path", "Untitled")))
            path = _url_path(r.get("path", ""))
            parts.append(
                f'<div class="search-result">'
                f'<h3><a href="/{path}">{title}</a></h3>'
                f'<span class="path">{html.escape(path)}</span>'
                f"</div>"
            )
    return "\n".join(parts)


def _build_references_html(bib: dict[str, dict[str, str]]) -> str:
    """Build the references page with all bib entries."""
    parts = [f"<h1>References</h1>", f'<p class="meta">{len(bib)} entries</p>']
    for key in sorted(bib.keys(), key=str.lower):
        entry = bib[key]
        ekey = html.escape(key)
        parts.append(f'<div class="ref-entry" id="{ekey}">')
        parts.append(f'<h3 class="ref-key">{ekey}</h3>')

        author = entry.get("author", "")
        title = entry.get("title", "")
        year = entry.get("year", "")
        journal = entry.get("journal", "")
        url = entry.get("url", "")
        doi = entry.get("doi", "")

        lines = []
        if author:
            lines.append(html.escape(author))
        if title:
            lines.append(f'<em>"{html.escape(title)}"</em>')
        if journal:
            lines.append(html.escape(journal))
        if year:
            lines.append(f"({html.escape(year)})")
        parts.append(f'<p>{", ".join(lines)}</p>' if lines else "")

        if doi:
            doi_url = f"https://doi.org/{doi}" if not doi.startswith("http") else doi
            parts.append(f'<p class="ref-link">DOI: <a href="{html.escape(doi_url)}" target="_blank">{html.escape(doi)}</a></p>')
        if url:
            parts.append(f'<p class="ref-link">URL: <a href="{html.escape(url)}" target="_blank">{html.escape(url)}</a></p>')

        parts.append("</div>")
    return "\n".join(parts)


def _build_topics_html(concepts: list[dict],
                       concept_articles: dict[str, list[dict]]) -> str:
    """Build the topics index page."""
    parts = [f"<h1>Topics</h1>", f'<p class="meta">{len(concepts)} topics</p>']
    for c in sorted(concepts, key=lambda c: c["name"].lower()):
        name = c["name"]
        count = c.get("article_count", 0)
        ename = html.escape(name)
        parts.append(f'<div class="topic-entry" id="{html.escape(name)}">')
        parts.append(f"<h3>{ename}</h3>")
        articles = concept_articles.get(name, [])
        if articles:
            parts.append("<ul>")
            for a in articles:
                path = _url_path(a["path"])
                title = html.escape(a["title"])
                atype = a.get("article_type", "")
                meta = f' <span class="meta">{atype}</span>' if atype else ""
                parts.append(f'<li><a href="/{path}">{title}</a>{meta}</li>')
            parts.append("</ul>")
        else:
            parts.append(f'<p class="meta">No articles yet</p>')
        parts.append("</div>")
    return "\n".join(parts)


def _build_about_html(wiki_dir: Path, db_path: Path, articles: list[dict],
                      bib: dict[str, dict[str, str]]) -> str:
    """Build the about page with paths, version, and statistics."""
    from crucible.database import CrucibleDB, SCHEMA_VERSION
    from crucible.registry import list_instances

    # Package version
    try:
        from importlib.metadata import version as pkg_version
        version = pkg_version("crucible")
    except Exception:
        version = "dev"

    # Stats from database
    db = CrucibleDB(db_path)
    stats = db.stats()
    db_schema = db.schema_version()
    db.close()

    # Project root (parent of .crucible dir or db dir)
    project_root = db_path.parent.parent if db_path.parent.name == ".crucible" else db_path.parent.parent

    parts = [
        f"<h1>About Crucible</h1>",
        f'<p>Crucible v{html.escape(version)} (schema v{SCHEMA_VERSION})</p>',
        "<p>LLM-compiled knowledge base with org-mode wiki and graph database</p>",
        "<h2>Project</h2>",
        '<table>',
        f'<tr><td>Root</td><td><code>{html.escape(str(project_root))}</code></td></tr>',
        f'<tr><td>Wiki</td><td><code>{html.escape(str(wiki_dir))}</code></td></tr>',
        f'<tr><td>Database</td><td><code>{html.escape(str(db_path))}</code> (schema v{db_schema})</td></tr>',
        '</table>',
        "<h2>Statistics</h2>",
        '<table>',
        f'<tr><td>Sources</td><td>{stats.get("sources", 0)}</td></tr>',
        f'<tr><td>Articles</td><td>{stats.get("articles", 0)}</td></tr>',
        f'<tr><td>Concepts</td><td>{stats.get("concepts", 0)}</td></tr>',
        f'<tr><td>Links</td><td>{stats.get("article_links", 0)}</td></tr>',
        f'<tr><td>References</td><td>{len(bib)}</td></tr>',
        '</table>',
    ]

    # Global registry
    instances = list_instances()
    parts.append("<h2>Global Registry</h2>")
    parts.append(f'<p>{len(instances)} crucible instance(s)</p>')
    if instances:
        parts.append('<table>')
        parts.append('<tr><th>Name</th><th>Path</th></tr>')
        for inst in instances:
            parts.append(
                f'<tr><td>{html.escape(inst["name"])}</td>'
                f'<td><code>{html.escape(inst["path"])}</code></td></tr>'
            )
        parts.append('</table>')

    return "\n".join(parts)


class CrucibleHandler(http.server.BaseHTTPRequestHandler):
    """Request handler for the Crucible wiki browser."""

    # Set by start_server before creating the HTTPServer
    wiki_dir: Path
    nav_html: str
    articles: list[dict]
    db_path: Path  # we reopen per-request to avoid threading issues
    bib: dict[str, dict[str, str]]
    project_name: str
    github_url: str

    def do_GET(self):  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/":
            self._serve_page("Crucible Wiki", _build_index_html(self.articles))
        elif path == "/_search":
            q = query.get("q", [""])[0].strip()
            if q:
                from crucible.database import CrucibleDB
                db = CrucibleDB(self.db_path)
                results = db.search(q, limit=30)
                db.close()
            else:
                results = []
            self._serve_page(
                f"Search: {q}" if q else "Search",
                _build_search_html(q, results),
            )
        elif path == "/_references":
            self._serve_page("References", _build_references_html(self.bib))
        elif path == "/_topics":
            from crucible.database import CrucibleDB
            db = CrucibleDB(self.db_path)
            concepts = db.list_concepts()
            concept_articles = db.articles_by_concept()
            db.close()
            self._serve_page("Topics", _build_topics_html(concepts, concept_articles))
        elif path == "/_about":
            self._serve_page(
                "About",
                _build_about_html(self.wiki_dir, self.db_path,
                                  self.articles, self.bib),
            )
        elif path == "/_graph":
            viz_path = self.wiki_dir / "viz.html"
            if viz_path.exists():
                body = viz_path.read_bytes()
                self._send_response(200, "text/html; charset=utf-8", body)
            else:
                self._serve_page(
                    "Graph",
                    "<h1>Knowledge Graph</h1>"
                    "<p>No graph generated yet. Run <code>crucible viz</code> first.</p>",
                )
        elif path == "/_static/style.css":
            self._send_response(200, "text/css", _BROWSE_CSS.encode("utf-8"))
        else:
            # Serve an org file
            self._serve_org(path)

    def _serve_org(self, url_path: str):
        """Convert and serve an org file."""
        # Strip leading slash
        rel = url_path.lstrip("/")
        # Security: prevent path traversal
        try:
            file_path = (self.wiki_dir / rel).resolve()
            if not str(file_path).startswith(str(self.wiki_dir.resolve())):
                self._send_error(403, "Forbidden")
                return
        except (ValueError, OSError):
            self._send_error(400, "Bad request")
            return

        if not file_path.exists() or not file_path.is_file():
            self._send_error(404, f"Not found: {rel}")
            return

        text = file_path.read_text(encoding="utf-8")
        title, content_html, filetags = org_to_html(text, rel, bib=self.bib)

        # Build page content with title, tags, and VS Code link
        vscode_uri = f"vscode://file/{urllib.parse.quote(str(file_path))}"
        parts = []
        if title:
            parts.append(
                f"<h1>{html.escape(title)}"
                f'<a class="vscode-link" href="{vscode_uri}">Open in VS Code</a>'
                f"</h1>"
            )
        if filetags:
            tags = "".join(f'<span class="tag">{html.escape(t)}</span>' for t in filetags)
            parts.append(f'<div class="tags">{tags}</div>')
        parts.append(content_html)

        self._serve_page(title or rel, "\n".join(parts))

    def _serve_page(self, title: str, content: str):
        """Wrap content in the page template and serve it."""
        github_link = ""
        if self.github_url:
            github_link = (
                f'<a class="github-link" href="{html.escape(self.github_url)}" '
                f'target="_blank">GitHub</a>'
            )
        page = _BROWSE_TEMPLATE.format(
            title=html.escape(title),
            css=_BROWSE_CSS,
            nav=self.nav_html,
            content=content,
            project_name=html.escape(self.project_name),
            github_link=github_link,
        )
        self._send_response(200, "text/html; charset=utf-8", page.encode("utf-8"))

    def _send_error(self, code: int, message: str):
        self._serve_page(f"Error {code}", f"<h1>{code}</h1><p>{html.escape(message)}</p>")

    def _send_response(self, code: int, content_type: str, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        """Suppress default logging; too noisy for a local tool."""
        pass


def start_server(wiki_dir: Path, db_path: Path, articles: list[dict],
                 port: int = 8088,
                 project_name: str = "",
                 github_url: str = "") -> http.server.HTTPServer:
    """Create and return the HTTP server (does not start serving)."""
    CrucibleHandler.wiki_dir = wiki_dir
    CrucibleHandler.nav_html = _build_nav_html(articles)
    CrucibleHandler.articles = articles
    CrucibleHandler.db_path = db_path
    CrucibleHandler.project_name = project_name
    CrucibleHandler.github_url = github_url

    # Load references.bib for citation tooltips
    bib_path = db_path.parent / "references.bib"
    if bib_path.exists():
        CrucibleHandler.bib = parse_bibtex(bib_path.read_text(encoding="utf-8"))
    else:
        CrucibleHandler.bib = {}

    server = http.server.HTTPServer(("127.0.0.1", port), CrucibleHandler)
    return server
