"""The knowledge-graph page written by `crucible viz` and served by `crucible browse`.

It lives apart from the CLI so that browse can build the page without
importing crucible.cli (which itself imports browse).
"""

import json
import re
from pathlib import Path

from crucible.database import CrucibleDB


def _script_json(value) -> str:
    """JSON for embedding in an HTML <script> element.

    "</script>" in a title would end the script element, and "<!--" can
    change how it is parsed, so <, > and & are written as \\u escapes (still
    the same JSON string), as are U+2028/U+2029.
    """
    return (json.dumps(value)
            .replace("<", "\\u003c").replace(">", "\\u003e")
            .replace("&", "\\u0026")
            .replace("\u2028", "\\u2028").replace("\u2029", "\\u2029"))


_VIZ_SLOT_RE = re.compile(r"/\*(GRAPH_DATA|ROOT_PATH|LINK_MODE)\*/")


def viz_html(root: Path, db: CrucibleDB, link_mode: str = "vscode") -> tuple[str, int, int]:
    """The knowledge-graph page; return (html, node count, edge count).

    link_mode "vscode" makes a click on an article open it in VS Code;
    "browse" (the page served by `crucible browse`) opens its browse page.
    """
    # Build graph data
    nodes = []
    node_ids = {}  # path -> index

    # Article nodes
    for a in db.list_articles():
        idx = len(nodes)
        node_ids[a["path"]] = idx
        nodes.append({
            "id": idx,
            "label": a["title"],
            "path": a["path"],
            "type": a["article_type"],
            "status": a.get("status", "draft"),
            "group": a["article_type"],
        })

    # Concept nodes
    for c in db.list_concepts():
        idx = len(nodes)
        node_ids[f"concept:{c['name']}"] = idx
        nodes.append({
            "id": idx,
            "label": c["name"],
            "type": "topic",
            "group": "topic",
        })

    edges = []

    # Article-to-article links
    for link in db.all_article_links():
        src = node_ids.get(link["from_path"])
        tgt = node_ids.get(link["to_path"])
        if src is not None and tgt is not None:
            edges.append({"source": src, "target": tgt, "type": "link"})

    # Article-to-concept links
    by_concept = db.articles_by_concept()
    for concept_name, articles in by_concept.items():
        concept_key = f"concept:{concept_name}"
        if concept_key not in node_ids:
            continue
        concept_idx = node_ids[concept_key]
        for a in articles:
            article_idx = node_ids.get(a["path"])
            if article_idx is not None:
                edges.append({"source": article_idx, "target": concept_idx,
                              "type": "concept"})

    # Derivation links
    for a in db.list_articles():
        for d in db.derivatives(a["path"]):
            src = node_ids.get(a["path"])
            tgt = node_ids.get(d["path"])
            if src is not None and tgt is not None:
                edges.append({"source": src, "target": tgt, "type": "derivation"})

    # One pass over the template, so a title that reads "/*ROOT_PATH*/" is
    # not itself replaced.
    slots = {
        "GRAPH_DATA": _script_json({"nodes": nodes, "edges": edges}),
        "ROOT_PATH": _script_json(str(root.resolve())),
        "LINK_MODE": _script_json(link_mode),
    }
    html = _VIZ_SLOT_RE.sub(lambda m: slots[m.group(1)], _VIZ_TEMPLATE)
    return html, len(nodes), len(edges)


_VIZ_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Crucible Knowledge Graph</title>
<style>
  body { margin: 0; background: #1a1a2e; font-family: system-ui, sans-serif; }
  svg { width: 100vw; height: 100vh; }
  .node text { font-size: 11px; pointer-events: none; }
  .link { stroke-opacity: 0.4; }
  .link.link-type-link { stroke: #888; }
  .link.link-type-concept { stroke: #444; stroke-dasharray: 3,3; }
  .link.link-type-derivation { stroke: #e94560; }
  #tooltip {
    position: absolute; padding: 8px 12px; background: #16213e;
    color: #eee; border-radius: 6px; font-size: 13px;
    pointer-events: none; display: none; max-width: 300px;
    border: 1px solid #0f3460;
  }
  #legend {
    position: absolute; top: 12px; left: 12px; background: #16213e;
    color: #eee; padding: 12px; border-radius: 6px; font-size: 12px;
    border: 1px solid #0f3460;
  }
  #legend div { margin: 4px 0; }
  #legend span { display: inline-block; width: 12px; height: 12px;
    border-radius: 50%; margin-right: 6px; vertical-align: middle; }
  #search {
    position: absolute; top: 12px; right: 12px;
    padding: 8px 12px; border-radius: 6px; border: 1px solid #0f3460;
    background: #16213e; color: #eee; font-size: 14px; width: 220px;
  }
  #search::placeholder { color: #666; }
</style>
</head>
<body>
<div id="tooltip"></div>
<div id="legend">
  <strong>Crucible Knowledge Graph</strong>
  <div><span style="background:#e94560"></span>Concept article</div>
  <div><span style="background:#0f3460"></span>Summary</div>
  <div><span style="background:#533483"></span>Comparison</div>
  <div><span style="background:#1a8a5c"></span>Method</div>
  <div><span style="background:#e9a045"></span>Topic tag</div>
  <div style="margin-top:8px; color:#888">Drag to rearrange. Scroll to zoom.</div>
</div>
<input id="search" type="text" placeholder="Search nodes...">
<svg></svg>
<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
const data = /*GRAPH_DATA*/;

const rootPath = /*ROOT_PATH*/;
const linkMode = /*LINK_MODE*/;

// Article types, and "topic" for concept (tag) nodes; matches the legend.
const colors = {
  concept: "#e94560", summary: "#0f3460", comparison: "#533483",
  method: "#1a8a5c", topic: "#e9a045"
};
function nodeColor(d) {
  return colors[d.group] || "#888";
}
function nodeRadius(d) {
  return d.path ? 10 : 6;
}
// Browse URL of a wiki article (path relative to .crucible/wiki/).
function browseHref(path) {
  const prefix = ".crucible/wiki/";
  if (path.startsWith(prefix)) path = path.slice(prefix.length);
  return "/" + path.split("/").map(encodeURIComponent).join("/");
}

const svg = d3.select("svg");
const width = window.innerWidth, height = window.innerHeight;
const g = svg.append("g");

svg.call(d3.zoom().scaleExtent([0.1, 8]).on("zoom", (e) => {
  g.attr("transform", e.transform);
}));

const sim = d3.forceSimulation(data.nodes)
  .force("link", d3.forceLink(data.edges).id(d => d.id).distance(80))
  .force("charge", d3.forceManyBody().strength(-200))
  .force("center", d3.forceCenter(width / 2, height / 2))
  .force("collision", d3.forceCollide().radius(20));

const link = g.append("g").selectAll("line")
  .data(data.edges).join("line")
  .attr("class", d => "link link-type-" + d.type)
  .attr("stroke-width", d => d.type === "derivation" ? 2 : 1);

const node = g.append("g").selectAll("g")
  .data(data.nodes).join("g")
  .call(d3.drag()
    .on("start", (e, d) => { if (!e.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
    .on("drag", (e, d) => { d.fx = e.x; d.fy = e.y; })
    .on("end", (e, d) => { if (!e.active) sim.alphaTarget(0); d.fx = null; d.fy = null; }));

node.append("circle")
  .attr("r", nodeRadius)
  .attr("fill", nodeColor)
  .attr("stroke", "#fff")
  .attr("stroke-width", 1.5);

node.append("text")
  .text(d => d.label.length > 25 ? d.label.slice(0, 23) + "..." : d.label)
  .attr("dx", 14).attr("dy", 4)
  .attr("fill", "#ccc");

const tooltip = d3.select("#tooltip");
node.on("mouseover", (e, d) => {
  // Built from text nodes: titles and paths are data, never markup.
  tooltip.text("");
  tooltip.append("strong").text(d.label);
  const line = (s) => { tooltip.append("br"); tooltip.append("span").text(s); };
  if (d.type) line("Type: " + d.type);
  if (d.status) line("Status: " + d.status);
  if (d.path) line(d.path);
  tooltip.style("display", "block");
}).on("mousemove", (e) => {
  tooltip.style("left", (e.pageX + 12) + "px").style("top", (e.pageY - 12) + "px");
}).on("mouseout", () => tooltip.style("display", "none"))
.on("click", (e, d) => {
  if (linkMode === "browse") {
    window.location.href = d.path ? browseHref(d.path)
      : "/_topics#" + encodeURIComponent(d.label);
  } else if (d.path) {
    window.location.href = "vscode://file/" + rootPath + "/" + d.path;
  }
});
node.style("cursor", d => d.path || linkMode === "browse" ? "pointer" : "default");

sim.on("tick", () => {
  link.attr("x1", d => d.source.x).attr("y1", d => d.source.y)
      .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
  node.attr("transform", d => "translate(" + d.x + "," + d.y + ")");
});

d3.select("#search").on("input", function() {
  const q = this.value.toLowerCase();
  node.select("circle").attr("opacity", d =>
    !q || d.label.toLowerCase().includes(q) ? 1 : 0.15);
  node.select("text").attr("opacity", d =>
    !q || d.label.toLowerCase().includes(q) ? 1 : 0.1);
  link.attr("opacity", d => {
    if (!q) return 1;
    const sn = data.nodes[typeof d.source === "object" ? d.source.id : d.source];
    const tn = data.nodes[typeof d.target === "object" ? d.target.id : d.target];
    return (sn.label.toLowerCase().includes(q) || tn.label.toLowerCase().includes(q)) ? 1 : 0.05;
  });
});
</script>
</body>
</html>
"""
