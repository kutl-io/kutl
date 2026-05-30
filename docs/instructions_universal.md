<!--
Universal MCP-instruction sections shared by both relay flavors (OSS
and kutlhub) per RFD 0083 §"Instruction sweep". Both
`oss/crates/kutl-relay/src/instructions_template.md` and
`commercial/kutlhub-relay/src/instructions_template.md` `include_str!`
this file at compile time, sandwiched between their flavor-specific
top (intro / identifier shapes / primitives) and tail (default-
visibility, deployment-specific cookbooks). Edit here when the change
applies to both flavors; edit the per-flavor templates when the
change is flavor-specific. The KFM dialect spec is concatenated
separately and lives at `oss/docs/kutl-markdown.md`.
-->

## Path conventions for documents

- Delimiter: `/`. No leading slash, no trailing slash.
- Intermediate directories are implicit — writing
  `"docs/setup/install.md"` creates the path; no need to mkdir.
- Valid chars: alphanumeric plus `-_./` plus UTF-8 in path segments.
- `..` path components are rejected.
- Paths must be 1024 bytes or fewer.
- Keep paths reasonable for humans browsing through their viewer.

## Markdown dialect — KFM

The kutl-flavored markdown reference is embedded immediately after
this instructions block (concatenated via `include_str!`). Read it
for the syntax of internal links, comment markers, mentions, decision
callouts, and embedded media. **GFM is the base** — anything not
mentioned in KFM behaves per standard GitHub-flavored markdown.

## Reach mechanisms — doc body vs signal stream

A space is shared infrastructure; humans look at it through whatever
tool they have. Two distinct reach mechanisms:

- **Doc body** (markdown content) reaches every viewer of the doc —
  web editor, vim, CLI, any tool that opens the file. Lowest-common-
  denominator reach.
- **Signal stream** reaches only viewers whose tool surfaces signals.
  MCP-connected agents (you) see signals via `get_changes` /
  `get_signal_detail`; **before doing task-shaped work in a space,
  query these to see what's directed at you**. Whether *humans* see
  signals depends on the client they're using — see the deployment-
  specific default-visibility note below for the assumption this relay
  flavor makes.

Several signal kinds have an in-doc form (kutl-flavored markdown
extensions per the KFM spec): `comment` (via `[text]{.cmt #uuid}`
marker), anchored flag (via callout syntax), decision (via heading
or callout), mention (via `@did:...` token in the body). When you
want both signal-stream tracking AND doc-body reach, emit both —
the signal and its in-doc form together.

## Audience on a flag

A flag carries *intent* (the `kind` field) and *audience*. Two
audience modes:

- **Single-recipient**: `audience: "participant"` plus `target_did`
  set to the human you want to reach. Discover DIDs via
  `list_participants`.
- **Everyone in the space**: `audience: "space"`. Every space member
  receives the signal.

"Mentions" in the user-facing sense (typing `@`-someone in the
editor) are NOT a separate entity. The editor's `@`-experience
inserts an inline marker `@[Name](kind:account-id)`; the relay
observes the marker and creates a flag signal with `kind` +
`target_did` — the SAME shape an explicit `create_flag(kind,
target_did, audience: 'participant')` MCP call produces. There is
no separate mention type on the wire, in storage, or on the
notification path. Pick whichever surface fits — agents typically
call `create_flag` directly; humans get the marker for free by
typing `@`.

Recipient preferences (per-human) control the actual routing —
email, in-app, or nothing. You cannot override delivery; the most
you do is set the audience correctly and emit the right kind.

## Provenance fields

`create_document` and `upload_blob` accept optional provenance
metadata for documents imported from another platform:

- `source_kind` — proto-enum value (e.g., `NOTION_HTML`, `DOCX`,
  `GDOCS_DOC`). Omit when no specific source.
- `source_id` — source-side stable ID (Notion `page_id`, Drive
  `file_id`, etc.).
- `source_url` — canonical source URL for "view original" links.
- `source_author_display` — non-DID author display string for
  attribution.
- `originally_created_at` — source-side creation timestamp.

All fields optional. They're display-only metadata — the UI
surfaces them as "view original" links and original-author
attribution. **No semantic dedup contract at the MCP layer.**

## Team conventions

If a `KUTL_TEAM.md` file exists at the space root, it carries
team-specific conventions for that space. Read it via
`read_document` when you start working in a space, and treat it
as helpful context — not unconditional authority.

## Provide full content, not patches

When editing a document, always supply the full desired content
via `edit_document`. The relay diffs against the current state
and applies minimal CRDT operations. Include a short `intent`
describing what your edit accomplishes — agents and humans
querying `read_log` rely on it.
