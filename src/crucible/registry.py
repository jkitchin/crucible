"""Global crucible registry.

Manages ~/.crucible/registry.json, a machine-wide index of all crucible
instances. Any crucible can discover all others without manual configuration.
"""

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path


REGISTRY_DIR = Path.home() / ".crucible"
REGISTRY_PATH = REGISTRY_DIR / "registry.json"


def _read_registry() -> dict:
    """Read the registry. Returns empty structure if missing or corrupt."""
    if not REGISTRY_PATH.exists():
        return {"version": 1, "instances": {}}
    try:
        data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        if "instances" not in data:
            data["instances"] = {}
        return data
    except (json.JSONDecodeError, OSError):
        return {"version": 1, "instances": {}}


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
        os.rename(tmp_path, str(REGISTRY_PATH))
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def register(name: str, path: str, description: str = "") -> str:
    """Add or update a crucible instance in the global registry.

    If `name` already exists with a different path, appends -2, -3, etc.
    Returns the actual name used (may differ from input if disambiguated).
    """
    path = str(Path(path).resolve())
    data = _read_registry()
    instances = data["instances"]

    # Check if this path is already registered under any name
    for existing_name, entry in instances.items():
        if entry["path"] == path:
            # Update in place (idempotent)
            entry["description"] = description or entry.get("description", "")
            entry["registered_at"] = datetime.now().isoformat(timespec="seconds")
            _write_registry(data)
            return existing_name

    # Name not taken, or taken by a different path
    actual_name = name
    if actual_name in instances:
        # Disambiguate
        suffix = 2
        while f"{name}-{suffix}" in instances:
            suffix += 1
        actual_name = f"{name}-{suffix}"

    instances[actual_name] = {
        "path": path,
        "description": description,
        "registered_at": datetime.now().isoformat(timespec="seconds"),
    }
    _write_registry(data)
    return actual_name


def unregister(name: str) -> bool:
    """Remove an instance by name. Returns True if it existed."""
    data = _read_registry()
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
    data = _read_registry()
    removed = []
    to_remove = []
    for name, entry in data["instances"].items():
        if resolve_db_path({"path": entry["path"]}) is None:
            to_remove.append(name)
            removed.append({"name": name, "path": entry["path"]})
    for name in to_remove:
        del data["instances"][name]
    if removed:
        _write_registry(data)
    return removed
