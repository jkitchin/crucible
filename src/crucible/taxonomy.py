"""The tag hierarchy of a crucible, from org-mode group tags.

A crucible's taxonomy lives in ``.crucible/taxonomy.org`` and uses org's
own tag-hierarchy syntax, so Emacs reads it the same way::

    #+TAGS: [ methods : electronic_structure molecular_dynamics ]
    #+TAGS: [ electronic_structure : dft coupled_cluster ]

Each ``[ parent : child ... ]`` group (or ``{ parent : child ... }``, org's
mutually exclusive form) makes every child a narrower tag of the parent.
A child that is a group of its own nests the hierarchy, and a tag may sit
under several parents. Selection keys (``dft(d)``) are dropped. Regular
expression members (``{P@.+}``) and plain tags outside a group carry no
hierarchy and are ignored.
"""

import re

TAXONOMY_FILE = "taxonomy.org"

_TAGS_LINE = re.compile(r"^[ \t]*#\+TAGS:(.*)$", re.IGNORECASE | re.MULTILINE)
# [ parent : members ] or { parent : members }; a member may be a {regex}.
_GROUP = re.compile(
    r"[\[{]\s*([^\s:\[\]{}]+)\s*:\s*((?:[^\[\]{}]|\{[^{}]*\})*?)\s*[\]}]")
_SELECTION_KEY = re.compile(r"\([^)]*\)$")


def _tag(token: str) -> str:
    return _SELECTION_KEY.sub("", token)


def parse_taxonomy(text: str) -> tuple[list[tuple[str, str]], list[str]]:
    """The (parent, child) pairs of the group tags in TEXT, in order.

    Also returns warnings: a regex member that was skipped, a tag listed
    under itself, and each cycle (a tag under its own descendant).
    """
    pairs: list[tuple[str, str]] = []
    warnings: list[str] = []
    for line in _TAGS_LINE.findall(text):
        for parent, members in _GROUP.findall(line):
            parent = _tag(parent)
            for token in members.split():
                if token.startswith("{"):
                    warnings.append(f"Regular expression tag {token} under "
                                    f"'{parent}' is not supported; skipped.")
                    continue
                child = _tag(token)
                if not child or child == "\\n":
                    continue
                if child.casefold() == parent.casefold():
                    warnings.append(f"Tag '{parent}' is listed under itself; skipped.")
                    continue
                if (parent, child) not in pairs:
                    pairs.append((parent, child))
    warnings.extend(f"Tag cycle: {' > '.join(c)}" for c in _cycles(pairs))
    return pairs, warnings


def _cycles(pairs: list[tuple[str, str]]) -> list[list[str]]:
    """One path for each cycle in the hierarchy, e.g. [a, b, a]."""
    children: dict[str, list[str]] = {}
    for parent, child in pairs:
        children.setdefault(parent.casefold(), []).append(child.casefold())
    cycles: list[list[str]] = []
    seen_cycles: set[frozenset] = set()
    done: set[str] = set()

    def walk(node: str, path: list[str]):
        if node in path:
            cycle = path[path.index(node):] + [node]
            key = frozenset(cycle)
            if key not in seen_cycles:
                seen_cycles.add(key)
                cycles.append(cycle)
            return
        if node in done:
            return
        for child in children.get(node, ()):
            walk(child, path + [node])
        done.add(node)

    for node in list(children):
        walk(node, [])
    return cycles
