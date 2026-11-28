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

from crucible import bibtex
from crucible.orgparse import CITATION_RE, FILE_LINK_RE, citation_keys, citation_type
from crucible.orgparse import DRAWER_RE as _DRAWER_RE
from crucible.wikiwalk import wiki_alias_of


# ---------------------------------------------------------------------------
# BibTeX
# ---------------------------------------------------------------------------

def parse_bibtex(text: str) -> dict[str, dict[str, str]]:
    """Parse bibtex into {key: {field: value, ...}, ...} with the shared reader."""
    entries: dict[str, dict[str, str]] = {}
    for e in bibtex.entries(text):
        entries[e.key] = {"_type": e.entry_type, **e.as_dict()}
    return entries


def reconstruct_bibtex(key: str, entry: dict[str, str]) -> str:
    """Reconstruct a bibtex entry string from parsed fields."""
    entry_type = entry.get("_type", "misc")
    fields = []
    for field, value in entry.items():
        if field.startswith("_"):
            continue
        fields.append(f"  {field} = {{{value}}}")
    body = ",\n".join(fields)
    return f"@{entry_type}{{{key},\n{body}\n}}"


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
_TITLE_RE = re.compile(r"^#\+TITLE:[ \t]*(.+)$", re.MULTILINE | re.IGNORECASE)
_FILETAGS_RE = re.compile(r"^#\+FILETAGS:[ \t]*(.+)$", re.MULTILINE | re.IGNORECASE)
_KEYWORD_RE = re.compile(r"^#\+\w+:.*$", re.MULTILINE)
DRAWER_RE = _DRAWER_RE  # imported from orgparse; kept for callers of browse
_PROP_LINE_RE = re.compile(r"^\s*:([A-Za-z_]+):\s+(.+?)\s*$", re.MULTILINE)
# Org block names are case-insensitive and blocks may be indented. The
# language is matched with [ \t] so a src block with none does not take its
# first code line as the language.
_SRC_BLOCK_RE = re.compile(
    r"^[ \t]*#\+BEGIN_SRC[ \t]*([^\n]*?)[ \t]*\n(.*?)^[ \t]*#\+END_SRC[ \t]*$",
    re.MULTILINE | re.DOTALL | re.IGNORECASE,
)
_EXAMPLE_BLOCK_RE = re.compile(
    r"^[ \t]*#\+BEGIN_EXAMPLE[^\n]*\n(.*?)^[ \t]*#\+END_EXAMPLE[ \t]*$",
    re.MULTILINE | re.DOTALL | re.IGNORECASE,
)
_QUOTE_BLOCK_RE = re.compile(
    r"^[ \t]*#\+BEGIN_QUOTE[^\n]*\n(.*?)^[ \t]*#\+END_QUOTE[ \t]*$",
    re.MULTILINE | re.DOTALL | re.IGNORECASE,
)
# A heading: stars at column 0, its title and any trailing :tag1:tag2: list.
_HEADING_RE = re.compile(
    r"^(\*+)[ \t]+(.+?)(?:[ \t]+(:[\w@#%:]+:))?[ \t]*$", re.MULTILINE)
# "# text" (or a lone "#") is an org comment line; #+KEYWORD lines are not.
_COMMENT_LINE_RE = re.compile(r"^[ \t]*#(?:[ \t].*)?$", re.MULTILINE)
_COMMENT_BLOCK_RE = re.compile(
    r"^[ \t]*#\+BEGIN_COMMENT\b.*?^[ \t]*#\+END_COMMENT[ \t]*$\n?",
    re.MULTILINE | re.DOTALL | re.IGNORECASE,
)
_TABLE_BLOCK_RE = re.compile(r"((?:^[ \t]*\|.*$\n?)+)", re.MULTILINE)

# Inline patterns
# Wiki links are what sync counts as links: [[file:x.org]] and the
# prefix-less forms org also reads as file links ([[./x.org]],
# [[../m/y.org][Y]], [[x.org]]). Other schemes (https:, javascript:, ...)
# never match, so they fall to the plain-link rules below.
_FILE_LINK_RE = FILE_LINK_RE
_PLAIN_LINK_RE = re.compile(
    r"\[\[([^\]]+)\]\[([^\]]*)\]\]"
)
# An external link with no description: [[https://example.com]].
_BARE_LINK_RE = re.compile(r"\[\[(https?://[^\]]+)\]\]")
# A bare URL in prose. Trailing sentence punctuation is not part of it; a
# trailing ')' is, only when it closes a '(' in the URL (see _bare_url).
# A URL also ends at \x00/\x01, the delimiters of the placeholders that
# hold links and citations already rendered (see _process_inline).
_URL_RE = re.compile(
    r"(?<![\w\"'=/])https?://[^\s<>\[\]\"'\x00\x01]*[^\s<>\[\]\"'.,;:!?\x00\x01]"
)
# Org emphasis boundaries: a marker opens after whitespace or one of -('"{
# and closes before whitespace or one of -.,:!?;'")}[ (the \x00/\x01
# placeholder delimiters count as boundaries too). Without these, the
# slashes in "</tag> ... /word/" were read as italics.
_EMPH_PRE = r"(?<![^\s\-('\"{\x00\x01])"
_EMPH_POST = r"(?![^\s\-.,:!?;'\")}\[\x00\x01])"
_BOLD_RE = re.compile(_EMPH_PRE + r"\*([^\s*](?:.*?[^\s*])?)\*" + _EMPH_POST)
_ITALIC_RE = re.compile(_EMPH_PRE + r"/([^\s/](?:.*?[^\s/])?)/" + _EMPH_POST)
# Code and verbatim follow the same boundary rules as emphasis, so the ~ in
# a URL such as https://example.org/~user does not open a code span.
_CODE_RE = re.compile(_EMPH_PRE + r"~([^\s~](?:.*?[^\s~])?)~" + _EMPH_POST)
_VERBATIM_RE = re.compile(_EMPH_PRE + r"=([^\s=](?:.*?[^\s=])?)=" + _EMPH_POST)
_LATEX_DISPLAY_RE = re.compile(r"\$\$(.+?)\$\$", re.DOTALL)
_LATEX_INLINE_RE = re.compile(r"(?<![\\$])\$([^\s$][^$\n]*?[^\s$])\$(?!\$)|(?<![\\$])\$([^\s$])\$(?!\$)")

# Placeholders: \x01I<n>\x01 holds an inline HTML fragment, \x00BLOCK<n>\x00
# a block. A held fragment may itself contain earlier placeholders (a block
# inside a quote, a link inside display math), so they are restored
# recursively rather than in creation order.
_INLINE_PH_RE = re.compile(r"\x01I\d+\x01")
_BLOCK_PH_RE = re.compile(r"\x00BLOCK\d+\x00")


def _restore(text: str, table: dict[str, str], pattern: re.Pattern) -> str:
    """Replace the placeholders of `pattern` in text by their values in
    table, including placeholders nested in those values. Unknown keys are
    dropped, so no placeholder reaches the output."""
    def repl(m: re.Match) -> str:
        value = table.get(m.group(0))
        return "" if value is None else pattern.sub(repl, value)
    return pattern.sub(repl, text)


# Link targets the server serves as images and the renderer shows inline.
IMAGE_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".svg": "image/svg+xml",
    ".webp": "image/webp",
}


def _rewrite_file_link(match: re.Match, current_path: str) -> str:
    """Rewrite [[file:...][text]] to a working HTML link."""
    target = match.group(1).strip()
    description = match.group(2)
    display = description or target.rsplit("/", 1)[-1].replace(".org", "")
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
    href = _href("/".join(parts))
    # [[file:fig.png]] with no description is an inline image, as in org.
    if not description and PurePosixPath(target).suffix.lower() in IMAGE_TYPES:
        return f'<img src="{href}" alt="{display}">'
    return f'<a href="{href}">{display}</a>'


def _external_link(url: str, display: str | None = None) -> str:
    """An HTML link to an external URL; display is already-escaped HTML."""
    if display is None:
        display = html.escape(url)
    return f'<a href="{html.escape(url)}" target="_blank">{display}</a>'


def _bare_url(url: str, hold) -> str:
    """Link a bare URL, leaving unbalanced closing parentheses outside it."""
    trailing = ""
    while url.endswith(")") and url.count(")") > url.count("("):
        url, trailing = url[:-1], ")" + trailing
    return hold(_external_link(url)) + trailing


def _rewrite_plain_link(match: re.Match) -> str:
    """Rewrite [[target][text]] non-file links."""
    target = match.group(1).strip()
    display = html.escape(match.group(2) or target)
    if target.startswith(("http://", "https://")):
        return _external_link(target, display)
    return f'<span class="link-unknown" title="{html.escape(target)}">{display}</span>'


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

    # Code and verbatim first: their content is literal, so no link, cite or
    # math inside them is processed.
    text = _CODE_RE.sub(lambda m: _hold(f"<code>{html.escape(m.group(1))}</code>"), text)
    text = _VERBATIM_RE.sub(lambda m: _hold(f"<code>{html.escape(m.group(1))}</code>"), text)

    # File links
    text = _FILE_LINK_RE.sub(lambda m: _hold(_rewrite_file_link(m, current_path)), text)

    # Citations (with bib tooltips, linked to references page).
    def _cite_repl(m: re.Match) -> str:
        cite_type = citation_type(m)
        keys = citation_keys(m)
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
                    f'<a class="citation" href="/_references#{ekey}" '
                    f'title="{html.escape(cite_type)}:{ekey}">[{ekey}]</a>'
                )
        return _hold("".join(spans))

    # Bracketed citation links ([[cite:&key][description]]) first, so the
    # plain-link rule below does not take them for unknown links.
    text = CITATION_RE.sub(
        lambda m: _cite_repl(m) if m.group("btype") else m.group(0), text)

    # Plain links, then external links without a description. Before bare
    # citations, so a link target is taken literally: the "cite:foo" in
    # [[https://example.com/ cite:foo][desc]] is part of the URL, not a
    # citation to restore inside href="...". Held as placeholders so the
    # italic pattern cannot match the slashes in them.
    text = _PLAIN_LINK_RE.sub(lambda m: _hold(_rewrite_plain_link(m)), text)
    text = _BARE_LINK_RE.sub(lambda m: _hold(_external_link(m.group(1))), text)

    # Bare citations (cite:key in prose), then bare URLs.
    text = CITATION_RE.sub(_cite_repl, text)
    text = _URL_RE.sub(lambda m: _bare_url(m.group(0), _hold), text)

    # LaTeX (before other inline markup to avoid conflicts)
    text = _LATEX_DISPLAY_RE.sub(
        lambda m: _hold(
            f'<div class="math-display">\\[{html.escape(m.group(1), quote=False)}\\]</div>'
        ),
        text,
    )
    text = _LATEX_INLINE_RE.sub(
        lambda m: _hold(
            f'<span class="math-inline">\\('
            f'{html.escape(m.group(1) or m.group(2), quote=False)}\\)</span>'
        ),
        text,
    )

    # Everything turned into HTML above is now a placeholder, so what remains
    # is article text: escape it before adding the bold and italic tags.
    text = html.escape(text, quote=False)

    # Bold and italic (safe now, all HTML-containing elements are placeholders)
    text = _BOLD_RE.sub(r"<strong>\1</strong>", text)
    text = _ITALIC_RE.sub(r"<em>\1</em>", text)

    # Restore placeholders (block placeholders are left for org_to_html)
    return _restore(text, _ph, _INLINE_PH_RE)


def _convert_table(table_text: str, current_path: str = "",
                   bib: dict[str, dict[str, str]] | None = None) -> str:
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
            out.append(f"  <{tag}>{_process_inline(cell, current_path, bib)}</{tag}>")
        out.append("</tr>")
    out.append("</table>")
    return "\n".join(out)


# "* item" is a list item only when indented; at column 0 it is a heading.
_LIST_ITEM_RE = re.compile(
    r"^([ \t]*)([-+]|\d+[.)]|(?<=[ \t])\*)(?:[ \t]+(.*))?$")
_CHECKBOX_RE = re.compile(r"^\[([ xX-])\][ \t]+")


def _list_item_html(text: str, current_path: str,
                    bib: dict[str, dict[str, str]] | None) -> str:
    """Inline HTML of one list item, with checkboxes and `term :: desc`."""
    box = ""
    m = _CHECKBOX_RE.match(text)
    if m:
        box = "&#9745; " if m.group(1) in "xX" else "&#9744; "
        text = text[m.end():]
    if " :: " in text:
        term, desc = text.split(" :: ", 1)
        return (f"{box}<strong>{_process_inline(term, current_path, bib)}</strong>: "
                f"{_process_inline(desc, current_path, bib)}")
    return box + _process_inline(text, current_path, bib)


def _list_html(items: list[list], current_path: str,
               bib: dict[str, dict[str, str]] | None) -> str:
    """Nested <ul>/<ol> from [indent, ordered, text] items."""
    out = []
    stack: list[tuple[int, str]] = []  # (indent, tag) of the open lists
    for indent, ordered, text in items:
        tag = "ol" if ordered else "ul"
        while stack and indent < stack[-1][0]:
            out.append(f"</li></{stack.pop()[1]}>")
        if stack and indent == stack[-1][0]:
            out.append("</li>")
        else:
            out.append(f"<{tag}>")
            stack.append((indent, tag))
        out.append("<li>" + _list_item_html(text, current_path, bib))
    while stack:
        out.append(f"</li></{stack.pop()[1]}>")
    return "".join(out)


def _render_chunk(chunk: str, placeholders: dict[str, str], current_path: str,
                  bib: dict[str, dict[str, str]] | None) -> list[str]:
    """HTML parts of a blank-line separated chunk: paragraphs, plain lists
    and block placeholders (which always stand on lines of their own)."""
    lines = chunk.split("\n")
    parts: list[str] = []
    para: list[str] = []

    def flush():
        text = "\n".join(para).strip()
        if text:
            parts.append(f"<p>{_process_inline(text, current_path, bib)}</p>")
        para.clear()

    i = 0
    while i < len(lines):
        line = lines[i]
        if line.strip() in placeholders:
            flush()
            parts.append(line.strip())
            i += 1
            continue
        m = _LIST_ITEM_RE.match(line)
        if not m:
            para.append(line)
            i += 1
            continue
        flush()
        base = len(m.group(1).expandtabs())
        items: list[list] = []
        while i < len(lines):
            line = lines[i]
            if line.strip() in placeholders:
                break
            indent = len(line) - len(line.lstrip(" \t"))
            m = _LIST_ITEM_RE.match(line)
            if m and len(m.group(1).expandtabs()) >= base:
                items.append([len(m.group(1).expandtabs()), m.group(2)[0].isdigit(),
                              m.group(3) or ""])
            elif items and indent > base and line.strip():
                items[-1][2] += " " + line.strip()  # continuation line
            else:
                break
            i += 1
        parts.append(_list_html(items, current_path, bib))
    flush()
    return parts


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

    # Placeholder delimiters in the source would be taken for placeholders.
    text = text.replace("\x00", "").replace("\x01", "")

    # Replace blocks with placeholders to protect them from inline processing
    placeholders: dict[str, str] = {}
    originals: dict[str, str] = {}  # placeholder -> the org source it replaced
    counter = [0]

    def _placeholder(content: str, original: str | None = None) -> str:
        key = f"\x00BLOCK{counter[0]}\x00"
        counter[0] += 1
        placeholders[key] = content
        if original is not None:
            originals[key] = original
        return key

    # Blocks come out first, so #+ lines inside them (a src block of org
    # code) are neither read as the title nor removed as keywords.
    def _src_repl(m: re.Match) -> str:
        lang = html.escape(m.group(1).strip()) if m.group(1).strip() else ""
        code = html.escape(m.group(2))
        cls = f' class="language-{lang}"' if lang else ""
        return _placeholder(f"<pre><code{cls}>{code}</code></pre>", m.group(0))

    text = _SRC_BLOCK_RE.sub(_src_repl, text)

    # Example blocks
    text = _EXAMPLE_BLOCK_RE.sub(
        lambda m: _placeholder(f"<pre>{html.escape(m.group(1))}</pre>", m.group(0)),
        text,
    )

    # Comments: #+BEGIN_COMMENT blocks and "# text" lines are not rendered.
    text = _COMMENT_BLOCK_RE.sub("", text)
    text = _COMMENT_LINE_RE.sub("", text)

    # Quote blocks: their body is rendered as paragraphs and lists, with any
    # src or example block placeholder inside it restored at the end.
    def _quote_repl(m: re.Match) -> str:
        parts = []
        for chunk in re.split(r"\n\s*\n", m.group(1)):
            parts.extend(_render_chunk(chunk, placeholders, current_path, bib))
        return _placeholder(f"<blockquote>{chr(10).join(parts)}</blockquote>",
                            m.group(0))

    text = _QUOTE_BLOCK_RE.sub(_quote_repl, text)

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

    # Drawers: PROPERTIES as a table, others (LOGBOOK, ...) collapsed.
    def _drawer_repl(m: re.Match) -> str:
        name = m.group(1)
        body = m.group(2)
        if name.upper() == "PROPERTIES":
            props = _PROP_LINE_RE.findall(body)
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
        if not body.strip():
            return ""
        # A drawer shows its raw text: blocks inside it (an example block
        # in a :RESULTS: drawer) go back to their org source.
        body = _restore(body, originals, _BLOCK_PH_RE)
        return _placeholder(
            f'<details class="properties"><summary>{html.escape(name.title())}</summary>'
            f"<pre>{html.escape(body.rstrip())}</pre></details>"
        )

    text = _DRAWER_RE.sub(_drawer_repl, text)

    # Tables (org allows them to be indented)
    text = _TABLE_BLOCK_RE.sub(
        lambda m: _placeholder(_convert_table(m.group(1), current_path, bib)) + "\n",
        text
    )

    # Headings
    def _heading_repl(m: re.Match) -> str:
        level = min(len(m.group(1)) + 1, 6)  # * -> h2, ** -> h3, ... h6
        heading = m.group(2)
        content = _process_inline(heading, current_path, bib)
        tags = [t for t in (m.group(3) or "").split(":") if t]
        if tags:
            content += " " + "".join(
                f'<span class="tag">{html.escape(t)}</span>' for t in tags)
        slug = re.sub(r"[^a-z0-9]+", "-", heading.lower()).strip("-")
        return _placeholder(f'<h{level} id="{slug}">{content}</h{level}>')

    text = _HEADING_RE.sub(_heading_repl, text)

    # Remaining text: paragraphs and plain lists with inline markup
    parts = []
    for chunk in re.split(r"\n\s*\n", text):
        parts.extend(_render_chunk(chunk, placeholders, current_path, bib))

    content = "\n".join(parts)

    # Restore placeholders, including those nested inside other blocks.
    content = _restore(content, placeholders, _BLOCK_PH_RE)
    # Belt and braces: no placeholder delimiter may reach the page.
    content = content.replace("\x00", "").replace("\x01", "")

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

/* Images */
#content img { max-width: 100%; height: auto; }
h2 .tag, h3 .tag, h4 .tag, h5 .tag, h6 .tag { vertical-align: middle; }

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
.ref-bibtex-btn {
  display: inline-block; font-size: 12px; color: var(--text-dim);
  margin-top: 6px; padding: 2px 8px; border-radius: 4px;
  border: 1px solid var(--border); background: transparent;
  cursor: pointer; font-family: inherit;
}
.ref-bibtex-btn:hover { color: var(--link); border-color: var(--link); }
.ref-bibtex-btn.copied { color: var(--accent); border-color: var(--accent); }
.ref-bibtex-pre {
  display: none; margin-top: 8px; padding: 8px 12px;
  background: var(--bg); border-radius: 4px; font-size: 12px;
  white-space: pre-wrap; word-break: break-all;
  border: 1px solid var(--border);
}

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
<script>
function copyBibtex(btn) {{
  var pre = btn.nextElementSibling;
  navigator.clipboard.writeText(pre.textContent).then(function() {{
    btn.textContent = 'Copied!';
    btn.classList.add('copied');
    setTimeout(function() {{
      btn.textContent = 'Copy BibTeX';
      btn.classList.remove('copied');
    }}, 2000);
  }});
}}
</script>
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


def _href(url_path: str) -> str:
    """An href attribute value for a wiki-relative path.

    Percent-encodes characters such as # and ? that would otherwise end the
    path, then HTML-escapes the result for the attribute.
    """
    return html.escape("/" + urllib.parse.quote(url_path.lstrip("/")))


def _safe_url(url: str) -> bool:
    """Whether url is an http(s) URL that may be used as a link."""
    return urllib.parse.urlparse(url.strip()).scheme.lower() in ("http", "https")


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
        '<a href="/_listen">Listen</a>',
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
            parts.append(f'<li><a href="{_href(path)}">{title}</a></li>')
        parts.append("</ul>")

    # Catch any other types
    for atype, items in sorted(groups.items()):
        if atype in type_labels:
            continue
        items.sort(key=lambda a: a["title"].lower())
        parts.append(f"<h2>{html.escape(str(atype).title())} ({len(items)})</h2>")
        parts.append("<ul>")
        for a in items:
            path = _url_path(a["path"])
            title = html.escape(a["title"])
            parts.append(f'<li><a href="{_href(path)}">{title}</a></li>')
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
            status = a.get("status") or ""
            meta = f'<span class="meta">{html.escape(str(status))}</span>' if status else ""
            parts.append(f'<li><a href="{_href(path)}">{title}</a>{meta}</li>')
        parts.append("</ul></div>")

    # Other types
    for atype, items in sorted(groups.items()):
        if atype in type_labels:
            continue
        items.sort(key=lambda a: a["title"].lower())
        parts.append(
            f'<div class="article-list"><h2>{html.escape(str(atype).title())}</h2><ul>')
        for a in items:
            path = _url_path(a["path"])
            title = html.escape(a["title"])
            parts.append(f'<li><a href="{_href(path)}">{title}</a></li>')
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
                f'<h3><a href="{_href(path)}">{title}</a></h3>'
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
            doi_url = doi if _safe_url(doi) else f"https://doi.org/{doi}"
            parts.append(f'<p class="ref-link">DOI: <a href="{html.escape(doi_url)}" target="_blank">{html.escape(doi)}</a></p>')
        if url:
            # Only http(s) URLs become links: a javascript: URL would run
            # in the page when clicked.
            if _safe_url(url):
                parts.append(f'<p class="ref-link">URL: <a href="{html.escape(url)}" target="_blank">{html.escape(url)}</a></p>')
            else:
                parts.append(f'<p class="ref-link">URL: {html.escape(url)}</p>')

        bibtex_str = html.escape(reconstruct_bibtex(key, entry))
        parts.append(
            f'<button class="ref-bibtex-btn" onclick="copyBibtex(this)">Copy BibTeX</button>'
            f'<pre class="ref-bibtex-pre">{bibtex_str}</pre>'
        )

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
                atype = a.get("article_type") or ""
                meta = (f' <span class="meta">{html.escape(str(atype))}</span>'
                        if atype else "")
                parts.append(f'<li><a href="{_href(path)}">{title}</a>{meta}</li>')
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
    from crucible import package_version
    version = package_version()

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
        parts.append('<tr><th>Name</th><th>Path</th><th>Origin</th></tr>')
        for inst in instances:
            origin = inst.get("origin") or ""
            parts.append(
                f'<tr><td>{html.escape(inst["name"])}</td>'
                f'<td><code>{html.escape(inst["path"])}</code></td>'
                f'<td><code>{html.escape(origin)}</code></td></tr>'
            )
        parts.append('</table>')

    return "\n".join(parts)


_LOCAL_HOSTS = ("localhost", "127.0.0.1", "[::1]")
_MAX_POST_BYTES = 64 * 1024


def _host_allowed(host: str | None) -> bool:
    """Whether a Host header names this machine (guards DNS rebinding)."""
    if not host:
        return False
    host = host.strip().lower()
    for name in _LOCAL_HOSTS:
        if host == name:
            return True
        if host.startswith(name + ":") and host[len(name) + 1:].isdigit():
            return True
    return False


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
    ollama_url: str | None = None  # for the embeddings /_api/related uses
    # mtimes of the database files and references.bib when articles, nav
    # and bib were last loaded; see _refresh.
    _state_stamp: tuple = ()

    @classmethod
    def _stamp(cls) -> tuple:
        stamp = []
        for p in (cls.db_path, Path(str(cls.db_path) + "-wal"),
                  cls.db_path.parent / "references.bib"):
            try:
                st = p.stat()
                stamp.append((st.st_mtime_ns, st.st_size))
            except OSError:
                stamp.append(None)
        return tuple(stamp)

    @classmethod
    def _refresh(cls, force: bool = False):
        """Reload articles, sidebar and bibliography if they changed on disk.

        `crucible sync` may run while the server is up; the database and
        references.bib mtimes tell when to reload.
        """
        stamp = cls._stamp()
        if not force and stamp == cls._state_stamp:
            return
        if cls.db_path.exists():
            from crucible.database import CrucibleDB
            try:
                db = CrucibleDB(cls.db_path)
                try:
                    cls.articles = db.list_articles()
                finally:
                    db.close()
            except Exception:
                pass  # keep the last good list if the DB is mid-write
        cls.nav_html = _build_nav_html(cls.articles)
        cls._load_bib()
        cls._state_stamp = stamp

    @classmethod
    def _load_bib(cls):
        bib_path = cls.db_path.parent / "references.bib"
        if bib_path.exists():
            cls.bib = parse_bibtex(
                bib_path.read_text(encoding="utf-8", errors="replace"))
        else:
            cls.bib = {}

    def do_GET(self):  # noqa: N802
        if not _host_allowed(self.headers.get("Host")):
            self._send_response(403, "text/plain; charset=utf-8",
                                b"Forbidden: unexpected Host header\n")
            return
        try:
            self._refresh()
        except Exception:
            pass
        try:
            self._route()
        except Exception as e:  # always answer, never drop the connection
            self._send_error(500, f"Internal error: {type(e).__name__}: {e}")

    def do_POST(self):  # noqa: N802
        if not _host_allowed(self.headers.get("Host")):
            self._send_json(403, {"error": "unexpected Host header"})
            return
        # A page on another site may post here (a form needs no CORS
        # preflight) though it cannot read the answer; refuse it anyway.
        origin = self.headers.get("Origin")
        if origin and not _host_allowed(urllib.parse.urlparse(origin).netloc):
            self._send_json(403, {"error": "cross-origin request"})
            return
        path = urllib.parse.urlparse(self.path).path
        if path != "/_api/related":
            self._send_json(404, {"error": f"no such endpoint: {path}"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = -1
        if not 0 <= length <= _MAX_POST_BYTES:
            self._send_json(413, {"error": "request body missing or too large"})
            return
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
            text = body.get("text", "") if isinstance(body, dict) else ""
            if not isinstance(text, str):
                raise ValueError("text must be a string")
        except ValueError as e:
            self._send_json(400, {"error": f"bad request: {e}"})
            return
        try:
            self._send_json(200, self._related(text))
        except Exception as e:  # a locked database, say
            self._send_json(500, {"error": f"{type(e).__name__}: {e}"})

    def _related(self, text: str) -> dict:
        from crucible.listen import related_from_db
        data = related_from_db(self.db_path, text, ollama_url=self.ollama_url)
        root = self.wiki_dir.parent.parent
        for r in data["results"]:
            url_path = _url_path(r["path"])
            r["url"] = "/" + urllib.parse.quote(url_path.lstrip("/"))
            r["file"] = str(root / r["path"])
        return data

    def _send_json(self, code: int, data: dict):
        self._send_response(code, "application/json; charset=utf-8",
                            json.dumps(data).encode("utf-8"),
                            {"Cache-Control": "no-store"})

    def _route(self):
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
            # Built from the database on every request, so it is never stale,
            # with node clicks opening browse pages rather than VS Code.
            from crucible.database import CrucibleDB
            from crucible.viz import viz_html
            try:
                db = CrucibleDB(self.db_path)
                try:
                    body, _, _ = viz_html(self.wiki_dir.parent.parent, db,
                                          link_mode="browse")
                finally:
                    db.close()
            except Exception as exc:  # a locked or broken database, say
                self._send_response(
                    500, "text/plain; charset=utf-8",
                    f"Could not build the graph: {exc}\n".encode("utf-8"))
                return
            self._send_response(200, "text/html; charset=utf-8",
                                body.encode("utf-8"))
        elif path == "/_listen":
            from crucible.listen import LISTEN_HTML
            self._serve_page("Listen", LISTEN_HTML)
        elif path == "/_static/style.css":
            self._send_response(200, "text/css", _BROWSE_CSS.encode("utf-8"))
        else:
            # Serve an org file
            self._serve_org(path)

    def _serve_org(self, url_path: str):
        """Convert and serve an org file (or an image) from the wiki."""
        # Strip leading slash
        rel = url_path.lstrip("/")
        # Security: only .org files and images inside the wiki directory.
        # Compare resolved paths (symlinks followed), not string prefixes,
        # which would admit a sibling such as wiki-old/.
        try:
            wiki = self.wiki_dir.resolve()
            file_path = (wiki / rel).resolve()
            suffix = file_path.suffix.lower()
            if not file_path.is_relative_to(wiki) or (
                    file_path.suffix != ".org" and suffix not in IMAGE_TYPES):
                self._send_error(403, "Forbidden")
                return
        except (ValueError, OSError):
            self._send_error(400, "Bad request")
            return

        if not file_path.is_file():
            self._send_error(404, f"Not found: {rel}")
            return

        if file_path.suffix != ".org":
            self._serve_image(file_path, IMAGE_TYPES[suffix])
            return

        # Through a symlink alias (dft.org -> concepts/dft-long.org), the
        # article's relative links are relative to its own location.
        alias = wiki_alias_of(self.wiki_dir, self.wiki_dir / rel)
        if alias is not None:
            rel = alias.relative_to(self.wiki_dir).as_posix()
        text = file_path.read_text(encoding="utf-8", errors="replace")
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

    def _serve_image(self, file_path: Path, content_type: str):
        """Serve an image file. An SVG can carry script, so it is sandboxed
        by a Content-Security-Policy that allows no script or fetch."""
        headers = {"X-Content-Type-Options": "nosniff"}
        if content_type == "image/svg+xml":
            headers["Content-Security-Policy"] = (
                "default-src 'none'; style-src 'unsafe-inline'; sandbox")
        self._send_response(200, content_type, file_path.read_bytes(), headers)

    def _serve_page(self, title: str, content: str, code: int = 200):
        """Wrap content in the page template and serve it."""
        github_link = ""
        if self.github_url and _safe_url(self.github_url):
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
        self._send_response(code, "text/html; charset=utf-8", page.encode("utf-8"))

    def _send_error(self, code: int, message: str):
        self._serve_page(f"Error {code}", f"<h1>{code}</h1><p>{html.escape(message)}</p>",
                         code=code)

    def _send_response(self, code: int, content_type: str, body: bytes,
                       headers: dict[str, str] | None = None):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        """Suppress default logging; too noisy for a local tool."""
        pass


def start_server(wiki_dir: Path, db_path: Path, articles: list[dict],
                 port: int = 8088,
                 project_name: str = "",
                 github_url: str = "",
                 ollama_url: str | None = None) -> http.server.HTTPServer:
    """Create and return the HTTP server (does not start serving).

    articles is the initial article list; it, the sidebar and the
    bibliography are reloaded whenever the database or references.bib
    changes. Raises OSError if the port cannot be bound.
    """
    CrucibleHandler.wiki_dir = wiki_dir
    CrucibleHandler.articles = articles
    CrucibleHandler.db_path = db_path
    CrucibleHandler.project_name = project_name
    CrucibleHandler.github_url = github_url
    CrucibleHandler.ollama_url = ollama_url
    CrucibleHandler.nav_html = _build_nav_html(articles)
    # Load references.bib now; later requests reload it, the articles and
    # the sidebar when the database or the bib file changes.
    CrucibleHandler._load_bib()
    CrucibleHandler._state_stamp = CrucibleHandler._stamp()

    server = http.server.HTTPServer(("127.0.0.1", port), CrucibleHandler)
    return server
