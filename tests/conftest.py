"""Shared test fixtures."""

import pytest

from crucible import registry as registry_module


@pytest.fixture
def registry(tmp_path, monkeypatch):
    """A temporary home directory and global registry.

    `crucible init` installs the skill into ~/.claude/, so HOME must be
    temporary too.
    """
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    reg = home / ".crucible"
    monkeypatch.setattr(registry_module, "REGISTRY_DIR", reg)
    monkeypatch.setattr(registry_module, "REGISTRY_PATH", reg / "registry.json")
    return reg
