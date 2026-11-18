"""Lightweight org-mode parser for extracting wiki metadata.

Extracts titles, filetags, properties drawers, file links,
and citation references from org files. Not a full parser,
just enough for Crucible's needs.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class OrgMetadata:
    """Metadata extracted from an org-mode file."""
    title: str = ""
    filetags: list[str] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)
    file_links: list[str] = field(default_factory=list)
    cite_keys: list[str] = field(default_factory=list)
    content: str = ""


# Patterns
TITLE_RE = re.compile(r"^#\+TITLE:[ \t]*(.+)$", re.MULTILINE | re.IGNORECASE)
FILETAGS_RE = re.compile(r"^#\+FILETAGS:[ \t]*(.+)$", re.MULTILINE | re.IGNORECASE)
# Org keywords, drawer names and property names are case-insensitive;
# property names are normalized to uppercase in OrgMetadata.properties.
# The body may be empty: an empty drawer must end at its own :END:, not run
# on to the :END: of a later drawer.
PROP_DRAWER_RE = re.compile(
    r"^[ \t]*:PROPERTIES:[ \t]*\n(.*?)^[ \t]*:END:[ \t]*$",
    re.DOTALL | re.IGNORECASE | re.MULTILINE,
)
# [ \t], not \s: \s matches a newline, so an empty property would take
# the next property line as its value. A property name is any run of
# characters other than whitespace and ':' (so ID2 counts), and a trailing
# '+' (:SOURCE_KEYS+: b) appends to the value, as in org. Group 1 is the
# name, group 2 the '+' if present, group 3 the value.
PROP_LINE_RE = re.compile(
    r"^[ \t]*:([^\s:]+?)(\+)?:(?:[ \t]+(.*?))?[ \t]*$", re.MULTILINE
)
# File-wide properties: #+PROPERTY: NAME value (NAME+ appends). A drawer
# value overrides them, as in org. Names with ':' (header-args:python) are
# not article properties and do not match.
PROPERTY_KEYWORD_RE = re.compile(
    r"^[ \t]*#\+PROPERTY:[ \t]+([^\s:+]+)(\+)?(?:[ \t]+(.*?))?[ \t]*$",
    re.MULTILINE | re.IGNORECASE,
)
# File links: [[file:path]], plus the prefix-less forms org also reads as
# file links: paths starting with ./ ../ / or ~/, and bare names ending in
# .org (which contain no ':', so other link types such as id:, https: and
# cite: are not taken). Group 1 is the path, group 2 the description.
FILE_LINK_RE = re.compile(
    r"\[\[(?:file:)?"
    r"((?<=file:)[^\]]+?"
    r"|(?<!file:)(?:\.{1,2}/|/|~/)[^\]]+?"
    r"|(?<!file:)[^\]\s:*#<>\[][^\]:]*?\.org)"
    r"(?:::.*?)?\](?:\[([^\]]*)\])?\]"
)

# org-ref citations. Both syntaxes are recognized:
#   v2: cite:key1,key2          (also [[cite:key1,key2]])
#   v3: cite:&key1;&key2        (also [[cite:prefix;&key1 note;&key2]])
# The link types are org-ref's natbib and biblatex commands, listed
# explicitly so prose such as "cites:many" is not read as a citation.
_CITE_TYPE = (
    r"(?:[Cc]ite(?:[pt]|al[tp]|author|year(?:par)?|num|text|title|url|date)?\*?"
    r"|nocite|[Aa]utocite|[Pp]arencite|[Tt]extcite|[Ff]ootcite(?:text)?"
    r"|[Ss]martcite|[Ss]upercite|fullcite)"
)
# BibTeX keys may contain ':', '.', '/', '+' and '-', but a key never ends in
# '.' or ':', so sentence punctuation after a bare citation is not captured.
_CITE_KEY = r"[\w+/-](?:[\w:.+/-]*[\w+/-])?"
# A bare citation must not follow a word character (so "excite:" is not one),
# '[' (bracketed form), or URL, verbatim and code delimiters (so
# https://example.com/cite:foo, =cite:x= and ~cite:x~ are not citations).
CITATION_RE = re.compile(
    rf"\[\[(?P<btype>{_CITE_TYPE}):(?P<bbody>[^\]]+)\](?:\[[^\]]*\])?\]"
    rf"|(?<![\w\[/=~?&#])(?P<type>{_CITE_TYPE}):"
    rf"(?P<body>&?{_CITE_KEY}(?:[;,]\s*&{_CITE_KEY}|[;,]{_CITE_KEY})*)"
)
_V3_KEY_RE = re.compile(rf"&({_CITE_KEY})")

# Blocks whose contents are literal text, not prose: citations and links
# written inside them are examples, not references. Org block names are
# case-insensitive and blocks may be indented.
_LITERAL_BLOCK_RE = re.compile(
    r"^[ \t]*#\+begin_(src|example|export)\b.*?^[ \t]*#\+end_\1[ \t]*$",
    re.MULTILINE | re.DOTALL | re.IGNORECASE,
)

# Text org does not treat as live content, so links and citations in it
# are not references: comment blocks, comment lines ('# ...'), fixed-width
# lines (': ...'), COMMENT subtrees, and inline verbatim (=...=) and code
# (~...~). Keywords (#+TITLE:) and drawer lines (:END:) are neither
# comments nor fixed-width: those need a space or end of line after the
# '#' or ':'.
_COMMENT_BLOCK_RE = re.compile(
    r"^[ \t]*#\+begin_comment\b.*?^[ \t]*#\+end_comment[ \t]*$",
    re.MULTILINE | re.DOTALL | re.IGNORECASE,
)
_COMMENT_OR_FIXED_LINE_RE = re.compile(r"^[ \t]*[#:](?:[ \t].*)?$", re.MULTILINE)
_COMMENT_HEADING_RE = re.compile(
    r"^(\*+)[ \t]+(?:(?:TODO|DONE)[ \t]+)?(?:\[#\w\][ \t]+)?COMMENT(?:[ \t].*)?$",
    re.MULTILINE,
)
# Org's emphasis rules: the marker follows the start of a line, whitespace
# or one of -('"{, the contents neither start nor end with whitespace, and
# the closing marker is followed by whitespace, punctuation or end of line.
_VERBATIM_RE = re.compile(
    r"(?:^|(?<=[\s\-('\"{]))([=~])(?=\S)[^\n]*?(?<=\S)\1"
    r"(?=[\s\-.,;:!?')}\"\\\[]|$)",
    re.MULTILINE,
)


# Any drawer (:PROPERTIES:, :LOGBOOK:, ...) up to its :END: line. Like org,
# a drawer cannot span a heading, so a stray ":note:" line followed later by
# some other drawer's :END: does not swallow the prose between them. Shared
# by browse and embeddings.embedding_text.
DRAWER_RE = re.compile(
    r"^[ \t]*:(?!END:)([A-Za-z][\w-]*):[ \t]*\n"
    r"((?:(?![ \t]*:END:[ \t]*$)(?!\*+[ \t]).*\n)*?)"
    r"[ \t]*:END:[ \t]*$",
    re.MULTILINE | re.IGNORECASE,
)


def citation_type(m: re.Match) -> str:
    """The link type (cite, citep, ...) of a CITATION_RE match."""
    return m.group("btype") or m.group("type")


def citation_keys(m: re.Match) -> list[str]:
    """The cite keys of a CITATION_RE match, in order."""
    body = m.group("bbody") or m.group("body")
    if "&" in body:
        return _V3_KEY_RE.findall(body)
    return [k.strip() for k in re.split(r"[,;]", body) if k.strip()]


_HEADING_RE = re.compile(r"^\*+[ \t]", re.MULTILINE)


def _article_property_sections(prose: str) -> list[str]:
    """The text before the first heading, and the first heading's section."""
    starts = [m.start() for m in _HEADING_RE.finditer(prose)]
    if not starts:
        return [prose]
    sections = [prose[:starts[0]]]
    end = starts[1] if len(starts) > 1 else len(prose)
    sections.append(prose[starts[0]:end])
    return sections


def _strip_comment_subtrees(text: str) -> str:
    """Remove each COMMENT heading and its subtree."""
    while True:
        m = _COMMENT_HEADING_RE.search(text)
        if not m:
            return text
        level = len(m.group(1))
        end = len(text)
        for h in _HEADING_RE.finditer(text, m.end()):
            if h.end() - h.start() - 1 <= level:
                end = h.start()
                break
        text = text[:m.start()] + text[end:]


def live_text(text: str) -> str:
    """The parts of org text whose links and citations are references.

    Drops literal blocks, comment blocks, COMMENT subtrees, comment and
    fixed-width lines, and inline verbatim and code.
    """
    text = _LITERAL_BLOCK_RE.sub("", text)
    text = _COMMENT_BLOCK_RE.sub("", text)
    text = _strip_comment_subtrees(text)
    text = _COMMENT_OR_FIXED_LINE_RE.sub("", text)
    return _VERBATIM_RE.sub(" ", text)


def parse_org(text: str) -> OrgMetadata:
    """Parse org-mode text and extract metadata."""
    meta = OrgMetadata(content=text)
    # A UTF-8 byte order mark would hide a keyword on the first line.
    text = text.removeprefix("﻿")

    # Literal blocks hold examples, not this article's metadata, links or
    # citations, so everything is read from the prose around them.
    prose = _LITERAL_BLOCK_RE.sub("", text)

    # Title
    m = TITLE_RE.search(prose)
    if m:
        meta.title = m.group(1).strip()

    # Filetags: org accumulates every #+FILETAGS: line.
    for m in FILETAGS_RE.finditer(prose):
        # Org separates tags with colons or whitespace: ":a: :b:" and
        # "a b" are both the tags a and b.
        for tag in re.split(r"[\s:]+", m.group(1)):
            if tag and tag not in meta.filetags:
                meta.filetags.append(tag)

    # #+PROPERTY: keywords set file-wide values; a later one wins, and
    # NAME+ appends, as in org.
    # Commented-out text (comment blocks, COMMENT subtrees) sets nothing.
    keyword_text = _strip_comment_subtrees(_COMMENT_BLOCK_RE.sub("", prose))
    keyword_props: dict[str, str] = {}
    for m in PROPERTY_KEYWORD_RE.finditer(keyword_text):
        key = m.group(1).upper()
        value = (m.group(3) or "").strip()
        if m.group(2):
            keyword_props[key] = f"{keyword_props.get(key, '')} {value}".strip()
        else:
            keyword_props[key] = value

    # Properties come from the file-level drawer (before the first heading,
    # where org-id and org-roam put :ID:) and the first heading's drawer,
    # first value winning; :KEY+: appends to the value, as in org. Drawers
    # on later headings describe those sections, not the article. A drawer
    # value overrides a #+PROPERTY: one, and :KEY+: appends to it.
    for section in _article_property_sections(prose):
        drawer = PROP_DRAWER_RE.search(section)
        if not drawer:
            continue
        for m in PROP_LINE_RE.finditer(drawer.group(1)):
            key = m.group(1).upper()
            value = (m.group(3) or "").strip()
            if m.group(2):
                old = meta.properties.get(key, keyword_props.get(key, ""))
                meta.properties[key] = f"{old} {value}".strip()
            elif key not in meta.properties:
                meta.properties[key] = value
    for key, value in keyword_props.items():
        meta.properties.setdefault(key, value)

    # Links and citations count only in live text: not in comments,
    # fixed-width lines or inline verbatim and code.
    live = live_text(text)

    # File links
    for m in FILE_LINK_RE.finditer(live):
        target = m.group(1).strip()
        meta.file_links.append(target)

    # Citation keys
    for m in CITATION_RE.finditer(live):
        meta.cite_keys.extend(citation_keys(m))

    return meta


def source_keys(value: str) -> list[str]:
    """The space-separated entries of a SOURCE_KEYS value.

    A comma next to a space (``a, b``) is dropped. A comma inside an entry
    (``a,b``) is kept: a key sync makes from a file name may contain one
    (``run,1`` for run,1.csv), so whether ``a,b`` is one key or two is for
    the caller to decide, with comma_parts.
    """
    return [k for k in (t.strip(",") for t in value.split()) if k]


def comma_parts(key: str) -> list[str]:
    """The comma-separated keys in a SOURCE_KEYS entry such as ``a,b``."""
    return [k for k in key.split(",") if k]


def parse_org_file(path: Path) -> OrgMetadata:
    """Parse an org file from disk."""
    text = path.read_text(encoding="utf-8")
    meta = parse_org(text)
    if not meta.title:
        meta.title = path.stem.replace("-", " ").replace("_", " ").title()
    return meta
