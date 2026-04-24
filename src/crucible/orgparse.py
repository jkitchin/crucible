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
TITLE_RE = re.compile(r"^#\+TITLE:\s*(.+)$", re.MULTILINE)
FILETAGS_RE = re.compile(r"^#\+FILETAGS:\s*(.+)$", re.MULTILINE)
PROP_DRAWER_RE = re.compile(
    r":PROPERTIES:\s*\n(.*?)\n\s*:END:", re.DOTALL
)
PROP_LINE_RE = re.compile(r"^\s*:([A-Z_]+):\s+(.+?)\s*$", re.MULTILINE)
FILE_LINK_RE = re.compile(r"\[\[file:([^\]]+?)(?:::.*?)?\](?:\[([^\]]*)\])?\]")
CITE_RE = re.compile(r"(?:cite[pt]?):([a-zA-Z0-9_,-]+)")


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
            key = m.group(1)
            val = m.group(2).strip()
            # First occurrence wins (top-level heading properties)
            if key not in meta.properties:
                meta.properties[key] = val

    # File links
    for m in FILE_LINK_RE.finditer(text):
        target = m.group(1).strip()
        meta.file_links.append(target)

    # Citation keys
    for m in CITE_RE.finditer(text):
        keys = m.group(1).split(",")
        meta.cite_keys.extend(k.strip() for k in keys if k.strip())

    return meta


def parse_org_file(path: Path) -> OrgMetadata:
    """Parse an org file from disk."""
    text = path.read_text(encoding="utf-8")
    meta = parse_org(text)
    if not meta.title:
        meta.title = path.stem.replace("-", " ").replace("_", " ").title()
    return meta
