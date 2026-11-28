"""Distilling sources by running an LLM agent non-interactively.

Crucible holds no model client: an agent harness (Claude Code, Codex,
Gemini CLI, or any command) does the reading and writing, guided by the
crucible skill. `crucible distill` builds the prompt for one source and
runs the agent in the project root, one source at a time.

An agent is named by a preset or given as a command template. In a
template, the token {prompt} is replaced by the prompt (as one argument,
never through a shell); a template without it gets the prompt on stdin.
"""

import shlex
from pathlib import Path

# Tools a Claude Code agent needs to distill: crucible itself, reading
# the source and the wiki, and writing articles. Nothing else runs.
CLAUDE_TOOLS = ["Bash(crucible *)", "Read", "Write", "Edit", "Glob", "Grep"]

PRESETS: dict[str, list[str]] = {
    "claude": ["claude", "-p", "{prompt}", "--allowedTools", *CLAUDE_TOOLS],
    "codex": ["codex", "exec", "--full-auto", "{prompt}"],
    "gemini": ["gemini", "--yolo", "-p", "{prompt}"],
}

DEFAULT_AGENT = "claude"


def agent_argv(agent: str, prompt: str) -> tuple[list[str], str | None]:
    """The command to run for agent and the stdin to give it.

    agent is a preset name (see PRESETS) or a command template. Returns
    (argv, stdin): stdin is the prompt when the template has no {prompt}
    token, else None. Raises ValueError for an empty or unparseable
    template.
    """
    if agent in PRESETS:
        template = list(PRESETS[agent])
    else:
        try:
            template = shlex.split(agent)
        except ValueError as e:
            raise ValueError(f"cannot parse the agent command {agent!r}: {e}") from None
    if not template:
        raise ValueError("the agent command is empty")
    if "{prompt}" in template:
        return [prompt if t == "{prompt}" else t for t in template], None
    return template, prompt


def describe_agent(agent: str) -> str:
    """The agent command as a shell would show it, with the prompt elided."""
    argv, stdin = agent_argv(agent, "<prompt>")
    shown = shlex.join(argv)
    return shown + (" < <prompt>" if stdin is not None else "")


def build_prompt(root: Path, source: dict, key: str, text_path: Path | None,
                 skill_path: Path | None) -> str:
    """The instructions for distilling one source into the crucible at root."""
    stored = root / source["path"]
    lines = [
        f"Distill one source into the crucible knowledge base at {root}.",
        "",
        f"Source: {source.get('title') or source['path']}",
        f"Cite key: {key}",
        f"Stored file: {stored}",
    ]
    if text_path is not None and text_path != stored and text_path.exists():
        lines.append(f"Extracted text: {text_path} (read this rather than the "
                     "original if the original is a PDF or HTML)")
    if source.get("url"):
        lines.append(f"URL: {source['url']}")
    lines.append("")
    if skill_path is not None and skill_path.exists():
        lines.append(f"Follow the crucible skill's distillation workflow and "
                     f"article format: read {skill_path} first if it is not "
                     "already loaded.")
    else:
        lines.append("Follow the crucible conventions: run `crucible help all` "
                     "for the article format and workflow.")
    lines += [
        "",
        "This run is non-interactive: nobody is available to approve a plan,",
        "so make the plan yourself and carry it out. In particular:",
        "",
        "1. Check what the wiki already covers (`crucible concepts`, "
        "`crucible search \"...\"`) before writing, and update existing "
        "articles rather than duplicating them.",
        f"2. Write a summary article for the source, and create or update "
        f"concept and method articles where the source adds something. "
        f"Cite the source as cite:{key} and list {key} in :SOURCE_KEYS:.",
        "3. Set :STATUS: draft on articles you create, and update "
        ":MODIFIED: on every article you change.",
        "4. Only change files under .crucible/wiki/. Do not ingest, delete "
        "or move sources.",
        "5. When done, run `crucible sync`, then `crucible lint` and fix any "
        "problems your articles introduced.",
        "6. End with a short list of the articles you created and updated.",
    ]
    return "\n".join(lines)
