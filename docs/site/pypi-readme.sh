#!/usr/bin/env bash
# Write the PyPI project description (Markdown) from readme.org.
# PyPI cannot render org, so the release workflow converts the readme and
# points pyproject.toml's `readme` at the result for that build only.
# Usage: docs/site/pypi-readme.sh <output.md>
set -euo pipefail

repo="$(cd "$(dirname "$0")/../.." && pwd)"
title="$(sed -n 's/^#+TITLE: *//p' "$repo/readme.org" | head -1)"
{
  echo "# $title"
  echo
  pandoc "$repo/readme.org" --from org --to gfm --wrap=none \
    --lua-filter "$repo/docs/site/links.lua" \
    --metadata srcdir="" \
    --metadata site="https://kitchingroup.cheme.cmu.edu/crucible/"
} > "$1"
