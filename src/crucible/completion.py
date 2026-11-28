"""Shell tab completion for the crucible CLI.

Click completes commands and options by itself, once the shell has the
script `crucible completion` prints. The functions here complete the
values arguments take: source cite keys, article paths, concept names,
registered crucibles and help topics.

Completion runs on every Tab press, so these read the database directly,
read-only, with the standard sqlite3 module (no migrations, no rebuild of
a missing database, nothing printed), and return no candidates on any
error rather than show a traceback in the user's prompt.
"""

import json
import os
import sqlite3
from pathlib import Path

from click.shell_completion import CompletionItem

SHELLS = ("bash", "zsh", "fish")
ENV_VAR = "_CRUCIBLE_COMPLETE"
PROG = "crucible"
WIKI_PREFIX = ".crucible/wiki/"
HELP_TOPICS = ("overview", "workflow", "ingest", "distill", "search", "sync",
               "maintain", "org-format", "registry", "commands", "all")
MAX_ITEMS = 500


def _db_path() -> Path | None:
    """The database of the crucible the current directory is in, if any."""
    cwd = Path.cwd()
    for parent in (cwd, *cwd.parents):
        db = parent / ".crucible" / "crucible.db"
        if (parent / ".crucible" / "wiki").is_dir() and db.is_file():
            return db
    return None


def _query(sql: str) -> list[tuple]:
    db = _db_path()
    if db is None:
        return []
    try:
        conn = sqlite3.connect(f"{db.as_uri()}?mode=ro", uri=True, timeout=0.5)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()
    except (sqlite3.Error, OSError, ValueError):
        return []


def _matching(candidates, incomplete: str, help_of=None) -> list[CompletionItem]:
    out = []
    for value in candidates:
        if value and value.startswith(incomplete):
            out.append(CompletionItem(value, help=help_of(value) if help_of else None))
            if len(out) >= MAX_ITEMS:
                break
    return out


def source_keys(ctx, param, incomplete: str) -> list[CompletionItem]:
    """Cite keys of the current crucible's sources, with their titles."""
    titles: dict[str, str] = {}
    for metadata, title in _query("SELECT metadata, title FROM sources"):
        try:
            key = json.loads(metadata or "{}").get("cite_key")
        except (ValueError, AttributeError):
            key = None
        if key:
            titles[str(key)] = title or ""
    return _matching(sorted(titles), incomplete, titles.get)


def article_paths(ctx, param, incomplete: str) -> list[CompletionItem]:
    """Article paths relative to the wiki (concepts/x.org), with titles.

    Every article argument accepts this form. A path typed from the
    project root (.crucible/wiki/...) completes in that form instead.
    """
    rows = _query("SELECT path, title FROM articles ORDER BY path")
    full = incomplete.startswith(".")
    titles = {}
    for path, title in rows:
        if not full and path.startswith(WIKI_PREFIX):
            path = path[len(WIKI_PREFIX):]
        titles[path] = title or ""
    return _matching(titles, incomplete, titles.get)


def concept_names(ctx, param, incomplete: str) -> list[CompletionItem]:
    rows = _query("SELECT name FROM concepts ORDER BY name")
    return _matching([r[0] for r in rows], incomplete)


def registered_names(ctx, param, incomplete: str) -> list[CompletionItem]:
    """Names in the global registry, read without the registry module's
    locking or repair of a corrupt file."""
    from crucible import registry
    try:
        data = json.loads(Path(registry.REGISTRY_PATH).read_text())
        names = sorted(data.get("instances", {}))
    except (OSError, ValueError, AttributeError):
        return []
    return _matching(names, incomplete)


def help_topics(ctx, param, incomplete: str) -> list[CompletionItem]:
    return _matching(HELP_TOPICS, incomplete)


# -- scripts --------------------------------------------------------------

def detect_shell() -> str | None:
    """The user's shell from $SHELL, if it is one click can complete."""
    name = Path(os.environ.get("SHELL", "")).name
    return name if name in SHELLS else None


# Click's own bash script needs bash 4.4 (compopt, complete -o nosort),
# and macOS ships bash 3.2 as /bin/bash. This one speaks the same
# protocol to `crucible` and runs on 3.2: for a file or directory
# argument it returns nothing and -o default completes filenames.
BASH_SCRIPT = """\
_crucible_completion() {
    local IFS=$'\\n' response completion type value
    response=$(env COMP_WORDS="${COMP_WORDS[*]}" COMP_CWORD=$COMP_CWORD \\
        _CRUCIBLE_COMPLETE=bash_complete crucible 2>/dev/null)
    COMPREPLY=()
    for completion in $response; do
        type=${completion%%,*}
        value=${completion#*,}
        if [ "$type" = plain ]; then
            COMPREPLY+=("$value")
        fi
    done
    return 0
}

complete -o default -F _crucible_completion crucible
"""


def script(cli, shell: str) -> str:
    """The completion script for shell."""
    if shell == "bash":
        return BASH_SCRIPT
    from click.shell_completion import get_completion_class
    cls = get_completion_class(shell)
    if cls is None:
        raise ValueError(f"unsupported shell: {shell}")
    return cls(cli, {}, PROG, ENV_VAR).source()


def install_targets(shell: str, home: Path) -> tuple[Path, Path | None]:
    """(where the script goes, the rc file that must source it, or None).

    Fish loads ~/.config/fish/completions/ by itself; bash and zsh source
    a file under ~/.crucible/ from their rc file. Saving the script, not
    `eval "$(crucible ...)"` in the rc file, keeps Python out of every
    new shell's startup.
    """
    if shell == "fish":
        return home / ".config" / "fish" / "completions" / "crucible.fish", None
    if shell == "zsh":
        rc = home / ".zshrc"
    else:
        # A login shell (every new Terminal window on macOS) reads
        # ~/.bash_profile and not ~/.bashrc, unless the one sources the other.
        rc = home / ".bashrc"
        profile = home / ".bash_profile"
        try:
            if profile.is_file() and "bashrc" not in profile.read_text(errors="replace"):
                rc = profile
        except OSError:
            pass
    return home / ".crucible" / f"completion.{shell}", rc


RC_MARKER = "# crucible shell completion"


def rc_line(script_path: Path, home: Path) -> str:
    try:
        shown = "~/" + script_path.relative_to(home).as_posix()
    except ValueError:
        shown = str(script_path)
    return f'{RC_MARKER}\n[ -f {shown} ] && . {shown}\n'
