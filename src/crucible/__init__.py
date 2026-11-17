"""Crucible - LLM-compiled knowledge base."""

#: The distribution name on PyPI. The import package is ``crucible``, but
#: the distribution is ``crucible-llm``, so version lookups must use this.
DISTRIBUTION = "crucible-llm"


def package_version() -> str:
    """Return the installed crucible version, or "dev" if not installed."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(DISTRIBUTION)
    except PackageNotFoundError:
        return "dev"
