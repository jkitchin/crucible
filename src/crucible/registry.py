"""Global crucible registry.

Manages ~/.crucible/registry.json, a machine-wide index of all crucible
instances. Any crucible can discover all others without manual configuration.
"""

import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from filelock import FileLock


REGISTRY_DIR = Path.home() / ".crucible"
REGISTRY_PATH = REGISTRY_DIR / "registry.json"


class RegistryError(Exception):
    """The registry file exists but cannot be used safely."""


def _lock_path() -> Path:
    return REGISTRY_DIR / "registry.json.lock"


@contextmanager
def _locked():
    """Hold the registry lock across a read-modify-write."""
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    with FileLock(str(_lock_path()), timeout=10):
        yield


def _valid_entry(entry) -> bool:
    return isinstance(entry, dict) and isinstance(entry.get("path"), str)


def _backup_corrupt(reason: str) -> Path:
    """Copy an unusable registry aside so nothing in it is lost.

    One copy per distinct content: repeated attempts reuse it.
    """
    content = REGISTRY_PATH.read_bytes()
    for prior in sorted(REGISTRY_DIR.glob("registry.json.corrupt-*")):
        try:
            if prior.read_bytes() == content:
                return prior
        except OSError:
            continue
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = REGISTRY_PATH.with_name(f"registry.json.corrupt-{stamp}")
    n = 1
    while backup.exists():
        backup = REGISTRY_PATH.with_name(f"registry.json.corrupt-{stamp}-{n}")
        n += 1
    shutil.copy2(REGISTRY_PATH, backup)
    return backup


def _read_registry(strict: bool = False) -> dict:
    """Read the registry.

    A missing file is an empty registry. A file that cannot be parsed, or
    is not shaped like a registry, is never treated as empty when
    ``strict`` (as writers are): it is copied aside and RegistryError is
    raised, so the next write cannot erase its entries. Readers
    (``strict=False``) get the entries that are usable.
    """
    data, problem = _load()
    if problem is None:
        return data
    if strict:
        try:
            backup = _backup_corrupt(problem)
        except OSError:
            # A file that cannot even be read (mode 000, a directory in its
            # place) cannot be copied aside either; it is left as it is.
            raise RegistryError(
                f"The registry {REGISTRY_PATH} {problem}. It was left unchanged; "
                "fix or remove it, then retry."
            ) from None
        raise RegistryError(
            f"The registry {REGISTRY_PATH} {problem}. It was left unchanged "
            f"(a copy is at {backup}); fix or remove it, then retry."
        )
    if isinstance(data, dict) and isinstance(data.get("instances"), dict):
        data["instances"] = {
            k: v for k, v in data["instances"].items() if _valid_entry(v)
        }
        return data
    return {"version": 1, "instances": {}}


def problem() -> str | None:
    """Why the registry file cannot be used as it is, or None if it can.

    Lets readers such as ``registry list`` report a damaged or unreadable
    registry instead of presenting it as empty.
    """
    _, found = _load()
    return f"The registry {REGISTRY_PATH} {found}." if found else None


def _load() -> tuple[dict | None, str | None]:
    """Read and check the registry: (data, None), or (data or None, problem)."""
    empty = {"version": 1, "instances": {}}
    try:
        if not REGISTRY_PATH.exists():
            return empty, None
    except OSError as e:
        return None, f"cannot be read ({e})"
    problem = None
    try:
        data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    except ValueError as e:
        problem, data = f"cannot be parsed ({e})", None
    except OSError as e:
        problem, data = f"cannot be read ({e})", None
    if problem is None:
        if not isinstance(data, dict):
            problem = "is not a JSON object"
        elif not isinstance(data.setdefault("instances", {}), dict):
            problem = "has an 'instances' value that is not an object"
        else:
            bad = [k for k, v in data["instances"].items() if not _valid_entry(v)]
            if bad:
                problem = f"has entries without a path: {', '.join(sorted(bad))}"
    return data, problem


def _write_registry(data: dict) -> None:
    """Atomically write the registry (write to temp, then rename)."""
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(REGISTRY_DIR), suffix=".tmp", prefix="registry-"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
        os.replace(tmp_path, str(REGISTRY_PATH))
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def register(name: str, path: str, description: str = "",
             rename: bool = False) -> str:
    """Add or update a crucible instance in the global registry.

    If `path` is already registered, its entry is updated in place and
    keeps its name, unless ``rename`` is true, in which case it moves to
    `name` (when `name` is free). If `name` already exists with a
    different path, appends -2, -3, etc. Returns the actual name used.
    """
    path = str(Path(path).resolve())
    with _locked():
        data = _read_registry(strict=True)
        instances = data["instances"]
        now = datetime.now().isoformat(timespec="seconds")

        # Check if this path is already registered under any name
        for existing_name, entry in list(instances.items()):
            if entry["path"] == path:
                entry["description"] = description or entry.get("description", "")
                entry["registered_at"] = now
                if rename and name != existing_name and name not in instances:
                    del instances[existing_name]
                    instances[name] = entry
                    existing_name = name
                _write_registry(data)
                return existing_name

        # Name not taken, or taken by a different path
        actual_name = name
        if actual_name in instances:
            suffix = 2
            while f"{name}-{suffix}" in instances:
                suffix += 1
            actual_name = f"{name}-{suffix}"

        instances[actual_name] = {
            "path": path,
            "description": description,
            "registered_at": now,
        }
        _write_registry(data)
        return actual_name


def lookup_path(path: str) -> str | None:
    """The name `path` is registered under, or None."""
    path = str(Path(path).resolve())
    for name, entry in _read_registry()["instances"].items():
        if entry["path"] == path:
            return name
    return None


def register_or_warn(name: str, path: str, warn) -> str | None:
    """register(), reporting an unusable registry through ``warn`` instead of raising.

    For the automatic registration done by init, sync and upgrade, which
    must not fail because the registry is damaged.
    """
    try:
        return register(name, path)
    except RegistryError as e:
        warn(f"Warning: not registered globally. {e}")
        return None
    except OSError as e:
        # The registry directory or its lock cannot be created or written.
        warn(f"Warning: not registered globally. The registry at {REGISTRY_PATH} "
             f"cannot be written ({e}).")
        return None


def unregister(name: str) -> bool:
    """Remove an instance by name. Returns True if it existed."""
    with _locked():
        data = _read_registry(strict=True)
        if name in data["instances"]:
            del data["instances"][name]
            _write_registry(data)
            return True
        return False


def list_instances() -> list[dict]:
    """Return all registered instances."""
    data = _read_registry()
    result = []
    for name, entry in sorted(data["instances"].items()):
        result.append({
            "name": name,
            "path": entry["path"],
            "description": entry.get("description", ""),
            "registered_at": entry.get("registered_at", ""),
        })
    return result


def resolve_db_path(instance: dict) -> Path | None:
    """Find the crucible.db for an instance, checking both layouts.

    Returns None if no database file exists (stale entry).
    """
    root = Path(instance["path"])
    for subpath in (".crucible/crucible.db", "db/crucible.db"):
        candidate = root / subpath
        if candidate.exists():
            return candidate
    return None


def get_peers(exclude_path: str | None = None) -> list[dict]:
    """Return registry entries as peer dicts, excluding the caller.

    Each dict has 'name' and 'path' keys, matching the format used by
    search_all/concepts_all. Stale entries (no DB file) are silently skipped.
    """
    if exclude_path:
        exclude_path = str(Path(exclude_path).resolve())
    peers = []
    for instance in list_instances():
        if exclude_path and instance["path"] == exclude_path:
            continue
        if resolve_db_path(instance) is not None:
            peers.append({"name": instance["name"], "path": instance["path"]})
    return peers


def clean() -> list[dict]:
    """Remove stale entries. Returns list of removed entries."""
    with _locked():
        data = _read_registry(strict=True)
        removed = []
        for name, entry in list(data["instances"].items()):
            if resolve_db_path({"path": entry["path"]}) is None:
                del data["instances"][name]
                removed.append({"name": name, "path": entry["path"]})
        if removed:
            _write_registry(data)
        return removed
