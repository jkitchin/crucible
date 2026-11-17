"""The one BibTeX reader every part of crucible uses.

Entries are found by balancing braces, not by line layout, so a compact
entry that closes on the same line (``@article{key, title={T}, year={2024} }``,
as doi.org returns them) ends where it should instead of swallowing the
entries after it. Both delimiters BibTeX allows are accepted:
``@type{key, ...}`` and ``@type(key, ...)``.

``@comment``, ``@string`` and ``@preamble`` blocks are skipped, and so is any
``@`` on a line that starts with ``%`` (a commented-out entry). An entry with
no closing delimiter is reported as malformed and does not swallow the rest
of the file: scanning resumes right after its head.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable

_HEAD_RE = re.compile(r"@\s*(?P<type>[A-Za-z][\w-]*)\s*(?P<open>[{(])")
_KEY_RE = re.compile(r"\s*(?P<key>[^,\s{}()]+)\s*")
_NAME_RE = re.compile(r"\s*,?\s*(?P<name>[A-Za-z][\w:.+-]*)\s*=\s*")
_BARE_RE = re.compile(r"[^\s,#{}()\"]+")
_SKIPPED_TYPES = {"comment", "string", "preamble"}


@dataclass
class Field:
    name: str          # lower-cased
    value: str         # without the outer delimiters
    start: int         # span of the whole `name = value` in the entry text
    end: int
    value_start: int   # span of the value inside its delimiters (one part only)
    value_end: int


@dataclass
class Entry:
    entry_type: str    # lower-cased
    key: str
    text: str          # the full entry, from @ to its closing delimiter
    start: int         # span in the scanned text
    end: int
    body_start: int    # offset in `text` where the fields start (after "key,")
    fields: list[Field] = field(default_factory=list)

    def get(self, name: str, default: str | None = None) -> str | None:
        """The value of a field (the last one, if it is repeated)."""
        name = name.lower()
        for f in reversed(self.fields):
            if f.name == name:
                return f.value
        return default

    def as_dict(self) -> dict[str, str]:
        """Field values keyed by name, with whitespace collapsed (but `file` kept)."""
        out: dict[str, str] = {}
        for f in self.fields:
            out[f.name] = f.value.strip() if f.name == "file" else " ".join(f.value.split())
        return out


def _commented(text: str, pos: int) -> bool:
    """Whether position `pos` is on a line whose first non-blank character is %."""
    line_start = text.rfind("\n", 0, pos) + 1
    return text[line_start:pos].lstrip().startswith("%")


def _match_close(text: str, pos: int, opener: str) -> int | None:
    """Index just past the delimiter closing an entry opened at text[pos-1].

    Braces nest. For a parenthesised entry, a ``)`` closes it only outside
    braces and outside a quoted value.
    """
    depth = 0
    in_quote = False
    closer = "}" if opener == "{" else ")"
    i, n = pos, len(text)
    while i < n:
        c = text[i]
        if c == "{":
            depth += 1
        elif c == "}":
            if depth == 0:
                return i + 1 if closer == "}" else None
            depth -= 1
        elif c == '"' and depth == 0:
            in_quote = not in_quote
        elif c == ")" and closer == ")" and depth == 0 and not in_quote:
            return i + 1
        elif c == "@" and depth == 0 and not in_quote and _starts_entry(text, i):
            # An entry head at the top level means this entry was never closed.
            return None
        i += 1
    return None


def _starts_entry(text: str, i: int) -> bool:
    """Whether an @ at text[i] begins a new entry on its own line."""
    line_start = text.rfind("\n", 0, i) + 1
    return not text[line_start:i].strip() and _HEAD_RE.match(text, i) is not None


def _parse_value(text: str, i: int) -> tuple[str, int, int, int] | None:
    """Parse a field value at text[i]: braced, quoted, bare, joined with #.

    Returns (value, end, value_start, value_end), where the value span covers
    the first part's content inside its delimiters.
    """
    parts: list[str] = []
    span: tuple[int, int] | None = None
    n = len(text)
    while True:
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            return None
        c = text[i]
        if c == "{":
            depth, j = 0, i
            while j < n:
                if text[j] == "{":
                    depth += 1
                elif text[j] == "}":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            if j >= n:
                return None
            parts.append(text[i + 1:j])
            span = span or (i + 1, j)
            i = j + 1
        elif c == '"':
            depth, j = 0, i + 1
            while j < n and not (text[j] == '"' and depth == 0):
                if text[j] == "{":
                    depth += 1
                elif text[j] == "}":
                    depth -= 1
                j += 1
            if j >= n:
                return None
            parts.append(text[i + 1:j])
            span = span or (i + 1, j)
            i = j + 1
        else:
            m = _BARE_RE.match(text, i)
            if not m:
                return None
            parts.append(m.group(0))
            span = span or m.span()
            i = m.end()
        j = i
        while j < n and text[j].isspace():
            j += 1
        if j < n and text[j] == "#":
            i = j + 1
            continue
        return "".join(parts), i, span[0], span[1]


def _parse_fields(text: str, start: int, end: int) -> list[Field]:
    """Parse `name = value` pairs in text[start:end]."""
    fields: list[Field] = []
    i = start
    while i < end:
        m = _NAME_RE.match(text, i, end)
        if not m:
            # Skip to the next comma; anything unparseable is ignored.
            nxt = text.find(",", i + 1, end)
            if nxt == -1:
                break
            i = nxt
            continue
        parsed = _parse_value(text[:end], m.end())
        if parsed is None:
            break
        value, vend, vs, ve = parsed
        fstart = m.start("name")
        fields.append(Field(m["name"].lower(), value, fstart, vend, vs, ve))
        i = vend
    return fields


def scan(text: str) -> tuple[list[Entry], list[int]]:
    """Every entry in BibTeX text, plus the offsets of malformed entry heads."""
    entries: list[Entry] = []
    malformed: list[int] = []
    i = 0
    while True:
        at = text.find("@", i)
        if at == -1:
            break
        head = _HEAD_RE.match(text, at)
        if head is None or _commented(text, at):
            i = at + 1
            continue
        etype = head["type"].lower()
        close = _match_close(text, head.end(), head["open"])
        if close is None:
            malformed.append(at)
            i = head.end()
            continue
        if etype in _SKIPPED_TYPES:
            i = close
            continue
        inner_end = close - 1   # the closing delimiter
        km = _KEY_RE.match(text, head.end(), inner_end)
        if km is None:
            malformed.append(at)
            i = close
            continue
        body = km.end()
        if body < inner_end and text[body] == ",":
            body += 1
        entry_text = text[at:close]
        fields = _parse_fields(entry_text, body - at, inner_end - at)
        entries.append(Entry(etype, km["key"], entry_text, at, close, body - at, fields))
        i = close
    return entries, malformed


_STRING_NAME_RE = re.compile(r"\s*(?P<name>[A-Za-z][\w:.+-]*)\s*=")


def macro_blocks(text: str) -> list[tuple[str, str | None, str]]:
    """The ``@string`` and ``@preamble`` blocks scan() skips, in order.

    Each is (kind, name, block text): kind is "string" or "preamble", and
    name is the macro a @string defines (None for a @preamble).
    """
    blocks: list[tuple[str, str | None, str]] = []
    i = 0
    while True:
        at = text.find("@", i)
        if at == -1:
            break
        head = _HEAD_RE.match(text, at)
        if head is None or _commented(text, at):
            i = at + 1
            continue
        close = _match_close(text, head.end(), head["open"])
        if close is None:
            i = head.end()
            continue
        etype = head["type"].lower()
        if etype == "string":
            m = _STRING_NAME_RE.match(text, head.end(), close)
            blocks.append(("string", m["name"].lower() if m else None, text[at:close]))
        elif etype == "preamble":
            blocks.append(("preamble", None, text[at:close]))
        i = close
    return blocks


def entries(text: str) -> list[Entry]:
    """Every entry in BibTeX text, in order."""
    return scan(text)[0]


def has_key(text: str, key: str) -> bool:
    return any(e.key == key for e in entries(text))


def keys(text: str) -> list[str]:
    """Every entry's key, in order (repeated keys appear more than once)."""
    return [e.key for e in entries(text)]


def map_entries(text: str, fn: Callable[[Entry], str]) -> str:
    """Replace each entry with fn(entry), leaving the text between entries alone."""
    out: list[str] = []
    pos = 0
    for e in entries(text):
        out.append(text[pos:e.start])
        out.append(fn(e))
        pos = e.end
    out.append(text[pos:])
    return "".join(out)


def first_entry(text: str) -> Entry | None:
    found = entries(text)
    return found[0] if found else None


def replace_key(text: str, new_key: str) -> str:
    """Replace the key of the first entry in BibTeX text."""
    e = first_entry(text)
    if e is None:
        return text
    km = _KEY_RE.match(text, _HEAD_RE.match(text, e.start).end())
    return text[:km.start("key")] + new_key + text[km.end("key"):]


def remove_field(entry_text: str, name: str) -> str:
    """Remove every `name` field from the first entry in entry_text."""
    e = first_entry(entry_text)
    if e is None:
        return entry_text
    return _without_field(entry_text, e, name) + entry_text[e.end:]


def _without_field(entry_text: str, e: Entry, name: str) -> str:
    """entry_text up to e.end with every `name` field of e removed."""
    out = entry_text[:e.end]
    # Remove existing fields with this name, last first so offsets hold.
    for f in sorted((f for f in e.fields if f.name == name.lower()),
                    key=lambda f: f.start, reverse=True):
        a, b = e.start + f.start, e.start + f.end
        while a > e.start and out[a - 1] in " \t":
            a -= 1
        if a > e.start and out[a - 1] == "\n":
            a -= 1
        rest = out[b:]
        stripped = rest.lstrip(" \t")
        if stripped.startswith(","):
            b += len(rest) - len(stripped) + 1
        out = out[:a] + out[b:]
    return out


def set_field(entry_text: str, name: str, value: str) -> str:
    """Set a field on the first entry in entry_text, replacing every existing one.

    The new field goes first, on its own line after the key.
    """
    e = first_entry(entry_text)
    if e is None:
        return entry_text
    out = _without_field(entry_text, e, name)
    insert_at = e.start + e.body_start
    if entry_text[insert_at - 1:insert_at] != ",":
        # @misc{key} with no fields: add the comma the field needs.
        out = out[:insert_at] + "," + out[insert_at:]
        insert_at += 1
    line = f"\n  {name} = {{{value}}},"
    return out[:insert_at] + line + out[insert_at:] + entry_text[e.end:]
