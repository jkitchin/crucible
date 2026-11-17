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
PROP_DRAWER_RE = re.compile(
    r":PROPERTIES:\s*\n(.*?)\n\s*:END:", re.DOTALL | re.IGNORECASE
)
# [ \t], not \s: \s matches a newline, so an empty property would take
# the next property line as its value.
PROP_LINE_RE = re.compile(r"^[ \t]*:([A-Za-z_]+):[ \t]*(.*?)[ \t]*$", re.MULTILINE)
FILE_LINK_RE = re.compile(r"\[\[file:([^\]]+?)(?:::.*?)?\](?:\[([^\]]*)\])?\]")

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
# '[' (bracketed form), or URL and verbatim delimiters (so
# https://example.com/cite:foo and =cite:x= are not citations).
CITATION_RE = re.compile(
    rf"\[\[(?P<btype>{_CITE_TYPE}):(?P<bbody>[^\]]+)\](?:\[[^\]]*\])?\]"
    rf"|(?<![\w\[/=?&#])(?P<type>{_CITE_TYPE}):"
    rf"(?P<body>&?{_CITE_KEY}(?:[;,]\s*&{_CITE_KEY}|[;,]{_CITE_KEY})*)"
)
_V3_KEY_RE = re.compile(rf"&({_CITE_KEY})")


def citation_type(m: re.Match) -> str:
    """The link type (cite, citep, ...) of a CITATION_RE match."""
    return m.group("btype") or m.group("type")


def citation_keys(m: re.Match) -> list[str]:
    """The cite keys of a CITATION_RE match, in order."""
    body = m.group("bbody") or m.group("body")
    if "&" in body:
        return _V3_KEY_RE.findall(body)
    return [k.strip() for k in re.split(r"[,;]", body) if k.strip()]


def parse_org(text: str) -> OrgMetadata:
    """Parse org-mode text and extract metadata."""
    meta = OrgMetadata(content=text)

    # Title
    m = TITLE_RE.search(text)
    if m:
        meta.title = m.group(1).strip()

    # Filetags
    m = FILETAGS_RE.search(text)
    if m:
        raw = m.group(1).strip()
        meta.filetags = [t for t in raw.strip(":").split(":") if t]

    # Properties from :PROPERTIES: drawers
    for drawer in PROP_DRAWER_RE.finditer(text):
        drawer_text = drawer.group(1)
        for m in PROP_LINE_RE.finditer(drawer_text):
            key = m.group(1).upper()
            val = m.group(2).strip()
            # First occurrence wins (top-level heading properties)
            if key not in meta.properties:
                meta.properties[key] = val

    # File links
    for m in FILE_LINK_RE.finditer(text):
        target = m.group(1).strip()
        meta.file_links.append(target)

    # Citation keys
    for m in CITATION_RE.finditer(text):
        meta.cite_keys.extend(citation_keys(m))

    return meta


def parse_org_file(path: Path) -> OrgMetadata:
    """Parse an org file from disk."""
    text = path.read_text(encoding="utf-8")
    meta = parse_org(text)
    if not meta.title:
        meta.title = path.stem.replace("-", " ").replace("_", " ").title()
    return meta
