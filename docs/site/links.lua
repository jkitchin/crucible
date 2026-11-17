-- Rewrite links for the published site.
--   *.org links to other pages   -> *.html
--   other relative links (LICENSE, source files) -> the file on GitHub,
--   since only the pages and images are published.
-- The page's directory relative to the repo root comes from the
-- `srcdir` metadata ("" for readme.org, "docs" for docs/*.org).

local repo = "https://github.com/jkitchin/crucible/blob/main/"
local srcdir = ""

local function is_relative(url)
  return not url:match("^%a[%w+.-]*:") and not url:match("^#") and not url:match("^/")
end

local function normalize(path)
  local parts = {}
  for part in path:gmatch("[^/]+") do
    if part == ".." then table.remove(parts)
    elseif part ~= "." then table.insert(parts, part) end
  end
  return table.concat(parts, "/")
end

function Meta(meta)
  if meta.srcdir then srcdir = pandoc.utils.stringify(meta.srcdir) end
end

function Link(el)
  local url = el.target
  if not is_relative(url) then return el end
  local path, anchor = url:match("^([^#]*)(#?.*)$")
  if path:match("%.org$") then
    el.target = path:gsub("%.org$", ".html") .. anchor
  elseif path ~= "" then
    local prefix = srcdir ~= "" and (srcdir .. "/") or ""
    el.target = repo .. normalize(prefix .. path) .. anchor
  end
  return el
end

-- Run Meta first so srcdir is known when links are rewritten.
return { { Meta = Meta }, { Link = Link } }
