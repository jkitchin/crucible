"""Shared test fixtures."""

import pytest

from crucible import registry as registry_module


@pytest.fixture(autouse=True)
def registry(tmp_path, monkeypatch):
    """A temporary home directory and global registry, for every test.

    `crucible init` installs the skill into ~/.claude/, so HOME must be
    temporary too. Autouse, because REGISTRY_PATH is computed at import:
    a test that only sets HOME would still write to the real
    ~/.crucible/registry.json.
    """
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    monkeypatch.setenv("HOME", str(home))
    reg = home / ".crucible"
    monkeypatch.setattr(registry_module, "REGISTRY_DIR", reg)
    monkeypatch.setattr(registry_module, "REGISTRY_PATH", reg / "registry.json")
    return reg
