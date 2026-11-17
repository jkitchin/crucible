-- Rewrite links for the published site.
--   *.org links to other pages   -> *.html
--   other relative links (LICENSE, source files) -> the file on GitHub,
--   since only the pages and images are published.
-- The page's directory relative to the repo root comes from the
-- `srcdir` metadata ("" for readme.org, "docs" for docs/*.org).
--
-- With `site` metadata (the published site's URL), page links become
-- absolute links into the site and relative images point at the raw file
-- on GitHub. The PyPI description uses this, since it has no relative
-- files of its own.

local repo = "https://github.com/jkitchin/crucible/blob/main/"
local raw = "https://raw.githubusercontent.com/jkitchin/crucible/main/"
local srcdir = ""
local site = nil

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
  if meta.site then site = pandoc.utils.stringify(meta.site) end
end

local function from_root(path)
  return normalize((srcdir ~= "" and (srcdir .. "/") or "") .. path)
end

function Link(el)
  local url = el.target
  if not is_relative(url) then return el end
  local path, anchor = url:match("^([^#]*)(#?.*)$")
  if path:match("%.org$") then
    local page = path:gsub("%.org$", ".html")
    if site then
      local target = from_root(page)
      if target == "readme.html" then target = "" end
      page = site .. target
    end
    el.target = page .. anchor
  elseif path ~= "" then
    el.target = repo .. from_root(path) .. anchor
  end
  return el
end

function Image(el)
  if site and is_relative(el.src) then
    el.src = raw .. from_root(el.src)
  end
  return el
end

-- Run Meta first so srcdir and site are known when links are rewritten.
return { { Meta = Meta }, { Link = Link, Image = Image } }
