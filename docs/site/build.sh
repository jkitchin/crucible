#!/usr/bin/env bash
# Build the documentation site from readme.org and docs/*.org with pandoc.
# Usage: docs/site/build.sh [output-dir]   (default: _site)
set -euo pipefail

repo="$(cd "$(dirname "$0")/../.." && pwd)"
out="${1:-$repo/_site}"
site="$repo/docs/site"

rm -rf "$out"
mkdir -p "$out/docs"
cp "$site/style.css" "$out/"
cp "$repo/crucible.png" "$out/"

page() {  # page <org file> <output html> <srcdir> <root>
  pandoc "$1" --from org --to html5 --standalone --toc --toc-depth=2 \
    --lua-filter "$site/links.lua" \
    --metadata srcdir="$3" --variable root="$4" \
    --css "${4}style.css" \
    --include-before-body <(sed "s|\\\$root\\\$|$4|g" "$site/nav.html") \
    --output "$2"
}

page "$repo/readme.org" "$out/index.html" "" ""
for f in "$repo"/docs/*.org; do
  page "$f" "$out/docs/$(basename "${f%.org}").html" "docs" "../"
done

echo "Built $(find "$out" -name '*.html' | wc -l | tr -d ' ') pages in $out"
