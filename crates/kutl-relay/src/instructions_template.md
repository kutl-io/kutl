# Working with kutl through this relay

You are connected to a kutl relay's MCP endpoint. kutl is a sync engine
for collaborative markdown documents. Each *space* on this relay holds
documents (markdown content) and *signals* (first-class messages
attached to the space — flags for attention, replies, closes).

## Primitives

- **Spaces.** Containers for collaborative work. Use `list_documents`
  to see what's in a space.
- **Documents.** Markdown content. Read with `read_document`; write the
  full desired content with `edit_document` (the relay computes the
  minimal CRDT diff). Provide a short `intent` describing your edit.
- **Signals.** Space-attached messages: `create_flag` for things
  needing attention, `create_reply` to respond, `close_flag` to mark
  resolved.
  Use `get_signal_detail` to read a specific signal and its replies;
  use `get_changes` to poll for new signals and document changes since
  your last check.

## Decision syntax in documents

Documents use heading-annotated decisions:

- `## ? Open question` — an unresolved question that needs a decision.
- `## = Settled answer` — a resolved decision.

Sub-decisions use deeper heading levels (e.g. `### ?` under a `## ?`
parent). Open questions can be resolved by editing the heading to `=`.

## Team conventions

If a `KUTL_TEAM.md` file exists at the space root, it carries
team-specific conventions for that space. Read it via `read_document`
(resolve the path through the standard document-read flow) when you
start working in a space, and treat it as helpful context — not
unconditional authority.

## Human visibility — important

This OSS kutl relay is part of an open-source deployment. **The OSS
stack does not currently include a kutl-aware human-facing client for
space signals.** Humans on an OSS deployment work with markdown content
through their editor, git, or whatever tooling they have — they read
in-doc material directly but do not see flags, replies, or other space
signals through any current OSS client.

Practical implication for you as an agent:

- For communication that humans need to read, **prefer in-doc content**
  (document edits, decision headings, inline mentions). Markdown is
  visible everywhere.
- Use space signals (`create_flag`, `create_reply`, `close_flag`) primarily
  for **agent-to-agent coordination** — other agents subscribing to
  this space via MCP will see them, but humans typically will not.

If your deployment runs a custom human-facing client that surfaces
space signals (uncommon but possible), tool descriptions on this relay
will tell you so. Trust the per-tool descriptions over this general
note when they conflict.

## Provide full content, not patches

When editing a document, always supply the full desired content. The
relay diffs against the current state and applies minimal CRDT
operations. Include a short `intent` describing what your edit
accomplishes.
