# kutl-flavored markdown (KFM)

kutl documents are markdown. The base is GFM (GitHub-Flavored Markdown);
everything an agent or editor expects from GFM works unchanged. KFM adds a
small set of in-doc extensions tuned for collaboration: cross-document
links, comment anchors, mentions, decision/callout blocks, and rich
embedded media.

This document is the canonical reference for the dialect. It ships with
every kutl install via `include_str!` into both the `kutl` binary
(`kutl init` writes it into the managed section of repo-root `AGENTS.md`,
per RFD 0075) and the relay binary (concatenated into the MCP
`InitializeResult.instructions` string). Same content, two delivery
vehicles.

## Cheat sheet

| Syntax | What it is | RFD |
|---|---|---|
| `[label](sibling.md)`, `[label](sub/other.md)` | Internal link to another doc in the same space — relative markdown path, NOT a wikilink | [0054](../../commercial/docs/rfd/0054-internal-document-links.md) |
| `[span text]{.cmt #signal-uuid}` | Comment anchor — paired with a `FLAG_KIND_COMMENT` signal whose UUID matches | [0077](../../commercial/docs/rfd/0077-comment-signals.md) |
| `@did:kutlhub:account:N` or `@display-name` | Mention — load-bearing for notification audience targeting | [0040](../../commercial/docs/rfd/0040-inline-attention-requests.md) |
| `# decision: ...` heading, or callout-form (see below) | Decision marker — first-class signal in feed/board | [0047](../../commercial/docs/rfd/0047-decisions-first-class.md), [0062](../../commercial/docs/rfd/0062-space-object-model.md) |
| ` ```mermaid ` fenced block | Mermaid diagram (rendered inline in hybrid preview) | [0039](../../commercial/docs/rfd/0039-hybrid-preview-and-markdown-enhancements.md) |
| `$inline$` and `$$block$$` | KaTeX math | [0039](../../commercial/docs/rfd/0039-hybrid-preview-and-markdown-enhancements.md) |
| Paste image into editor | Auto-uploads + inserts `![alt](path)` | [0039](../../commercial/docs/rfd/0039-hybrid-preview-and-markdown-enhancements.md), [0068](../../commercial/docs/rfd/0068-app-blob-rail.md) |
| `> [!info] body` / `> [!decision] body` callout (flat, non-hierarchical) | Callout block — unified syntax for callouts, decisions, attention | [0057](../../commercial/docs/rfd/0057-editor-ergonomics.md) |

## Per-extension detail

### Internal links — standard markdown, relative paths

Cross-document links inside a space are **standard markdown links with
relative paths**. The link target is a path relative to the current
document, resolved against the space's document tree.

```markdown
See the [onboarding guide](onboarding.md) for orientation.
See [handbook/policies](../handbook/policies.md) for the full text.
[Project status](../projects/q2.md) updated weekly.
```

KFM does **not** use wikilink (`[[name]]`) syntax — see "Anti-syntax"
below. Relative paths resolve via the same rules as relative URLs:
no leading slash, `..` ascends one directory, `.` is the current
directory, intermediate directories are implicit.

When the link target does not resolve to an existing document, the
editor renders the link with a "not found" affordance; double-clicking
or Cmd-clicking can offer to create the document at that path.

See RFD 0054 for the full resolution algorithm and the
`ResolveDocumentPath` RPC.

### Comment marker — `[text]{.cmt #signal-uuid}`

Comments anchor to a specific span of text in a document. The
in-doc form is a Pandoc-style bracketed span with a `.cmt` class
and an `#signal-uuid` identifier:

```markdown
The migration cap is bounded at three sources [exactly: Notion, docx,
gdocs]{.cmt #2c6e5b71-9a2c-4d4d-b1e1-7d8b1f3c5a2e}.
```

The UUID in the marker matches a `FLAG_KIND_COMMENT` signal stored in
the relay. The signal carries the comment body, author, and lifecycle
state (open/resolved); the marker carries position within the document.

To author a comment, the agent's flow is:

1. Mint a UUID client-side.
2. Inject the `[text]{.cmt #signal-uuid}` marker into the document
   body at the position being commented on (via `edit_document`).
3. Emit a `create_flag(kind: 'comment', signal_id: <same-uuid>,
   anchor_text: <span text>, body: <comment text>, ...)` with the same
   UUID.

The relay's `handle_signal` honors caller-supplied signal UUIDs
(load-bearing for marker↔signal binding per RFD 0077). The marker
and the signal bind by UUID equality — there is no separate join.

Renders as: an inline highlight + popover with the comment body in
the kutlhub web app; literal `[text]{.cmt #uuid}` in plain editors.

See RFD 0077.

### Mentions — `@`-typing creates a targeted flag

"Mention" in kutl is editor-UX terminology, not a distinct entity.
Typing `@` in the editor pops a picker; selecting a person inserts an
inline marker that the relay materializes into a normal flag signal
with `kind` + `target_did`. The wire and storage form is *the same
flag* an explicit MCP `create_flag(kind, target_did)` RPC would
produce — there is no separate "mention" object on the wire, in the
database, or on the notification path. Use whichever surface is
convenient; the recipient sees one notification regardless.

The marker form in the doc body is `@[Name](kind:account-id)` with
an optional `|<message>` portion carrying the picker-typed body:

```markdown
@[Jane](review_requested:acc-12345) please look at the proposal
@[Bob](info:acc-67890|FYI on the bandwidth bump)
@[Carol](question:acc-cafe|please add the list of mcp tools)
```

The optional `|<message>` portion is the picker-typed body —
equivalent to the `message` argument an MCP `create_flag` call
would carry. When present, it lands in `flag_details.message`.
When absent, the resulting signal's message defaults to
`@DisplayName`. The `account-id` ends at the pipe (`|`) or the
closing paren, whichever comes first.

The relay's marker observer parses each marker on every diff
(including the optional `|message` tail) and emits the materialized
flag — same shape, same kinds (`info`, `completed`,
`review_requested`, `question`, `blocked`), same audience
(`target_did = account id`). Removing the marker auto-closes the
resulting flag with `close_reason: withdrawn`.

**Picking your surface:**

- *Editor user typing `@`*: the editor inserts the marker; you don't
  hand-write the syntax.
- *MCP agent wanting to "@-mention" a person*: call
  `create_flag(kind, target_did, audience: 'participant')`. Don't
  inject the inline marker from MCP — the editor owns that path.
- *Targeting everyone in a space*: use `audience: 'space'`. There
  is no "@channel" syntax (see "Anti-syntax" below).

Renders as: an avatar + name pill in the kutlhub web app; literal
`@[Name](kind:id)` text in plain editors. The resulting flag shows
up in the recipient's inbox + email notifications (per their
prefs) whichever surface created it.

See RFD 0040, RFD 0046 (observer mechanics), RFD 0083 audit
2026-05-22 (unified model).

### Decision callouts — heading-form and callout-form

Decisions are first-class signals (RFD 0062) with two equivalent
in-doc forms:

**Heading-form** (the original syntax, RFD 0047):

```markdown
# decision: ship 25MB blob cap for v1

We picked 25MB because the docx replace case routinely exceeds 5MB.
Larger blobs deferred to a follow-up.
```

**Callout-form** (flat, non-hierarchical, RFD 0057):

```markdown
> [!decision] ship 25MB blob cap for v1
> We picked 25MB because the docx replace case routinely exceeds 5MB.
> Larger blobs deferred to a follow-up.
```

Both forms produce the same `decision` signal. The callout-form is
non-hierarchical — it does not nest decisions. Heading-form follows
normal heading hierarchy.

Renders as: a decision-row pill in feed/board surfaces and an inline
decoration on the heading/callout in the kutlhub web app; literal
markdown in plain editors.

See RFD 0047 and RFD 0062.

### Callout blocks — unified syntax

Callouts use GFM-style alert syntax (`> [!kind] ...`) extended to
cover info, warning, decision, and attention/flag kinds:

```markdown
> [!info] FYI — the relay restarted at 14:02 UTC; resumed at 14:03.

> [!warning] this script truncates the working tree

> [!decision] move the cap to 25MB
> Picked because docx replace routinely exceeds 5MB.

> [!review_requested] @did:kutlhub:account:42
> Please look at the spec section 3 by Friday.
```

The kind values match the signal `FlagKind` enum: `info`,
`review_requested`, `question`, `blocked`, `completed`, `comment`,
plus the parallel `decision` and (UI-only) `warning` kinds.

**Callouts can carry the same intent as a `create_flag` signal.**
A callout-form flag is reachable through the doc body (every viewer
of the doc sees it); a `create_flag` MCP call is reachable through
the signal stream (only signal-aware tools see it). Pair both when
you want both surfaces.

Renders as: a coloured block with an icon in the kutlhub web app;
literal markdown in plain editors.

See RFD 0057.

### Embedded media and hybrid preview

Three forms get hybrid-preview rendering — the source markdown stays
in the buffer, but when the cursor is outside the construct it
renders as the target form:

**Mermaid** — fenced code with `mermaid` info string renders as an
SVG diagram:

````markdown
```mermaid
flowchart LR
  A[Agent] -->|create_document| R[Relay]
  R --> S[(Space)]
```
````

**KaTeX** — inline `$...$` and block `$$...$$` render as typeset
math:

```markdown
The cap is $25 \cdot 2^{20}$ bytes.

$$
\sum_{i=1}^{n} x_i = \mu n
$$
```

**Image paste** — pasting an image into the editor uploads it to the
blob rail (RFD 0068) and inserts a standard markdown image
reference:

```markdown
![architecture diagram](images/arch.png)
```

Agents writing math or diagrams should use these forms rather than
ASCII fallbacks — the rendering is the point.

Renders as: SVG / typeset math / inline image in the kutlhub web
app; the source fenced block / `$...$` / `![](...)` in plain
editors.

See RFD 0039.

## Rendering matrix

How each extension appears across viewers:

| Form | kutlhub web app | Plain editor (vim, VS Code without kutl extension) |
|---|---|---|
| GFM headings, lists, fenced code, tables, links, emphasis, task lists | Native styled rendering | Native markdown rendering |
| Internal link `[label](sibling.md)` | Clickable cross-doc nav; "not found" affordance for unresolved paths | Plain markdown link |
| Comment marker `[text]{.cmt #uuid}` | Inline highlight + popover with comment body | Literal `[text]{.cmt #uuid}` text |
| Mention `@did:...` or `@name` | Avatar + name pill | Literal `@did:...` / `@name` text |
| Decision heading `# decision: ...` | Decision-row pill in feed + inline heading decoration | Literal heading |
| Decision callout `> [!decision] ...` | Decision-row pill + coloured callout block | Literal callout markdown |
| Other callouts `> [!info] ...` etc. | Coloured callout block with icon | Literal callout markdown |
| Mermaid fenced block | Rendered SVG diagram | Literal fenced code |
| KaTeX `$...$` / `$$...$$` | Typeset math | Literal markdown |
| Image `![](path.png)` | Inline rendered image | Literal markdown link |

**Doc body is the lowest-common-denominator reach.** Anything written
into the doc body reaches every viewer of the doc — every editor,
every viewer tool. The signal stream (replies, reactions, the
non-in-doc structured layer) only reaches signal-aware tools.

When you want every human to see something regardless of tool,
write into the doc body. When you want structured signal carrying
intent (for notification routing and signal-aware tooling), emit a
signal. Pair both when you want both.

## Anti-syntax — what is NOT part of KFM

Common syntaxes from neighboring tools that **do not work** in KFM:

- **Wikilinks `[[name]]`.** Notion, Obsidian, Roam, and Logseq users
  arrive expecting `[[Onboarding]]` to resolve to a document by
  title. KFM uses standard markdown links with relative paths
  (RFD 0054) — `[Onboarding](onboarding.md)` — not wikilinks. The
  parser does not recognize `[[name]]` as a link; it renders as
  literal `[[name]]` text.

- **Block references `((block-id))`.** Roam-style transclusion of a
  block by id is not part of KFM. There is no block-id concept in
  the doc model; documents are markdown text plus the extensions
  documented above.

- **Custom front-matter behavior.** YAML front-matter is preserved
  as document content (it parses as part of the markdown body) but
  has no special semantic meaning in KFM. Provenance metadata (source
  URL, source author, original creation timestamp) is set via
  `create_document` parameters, not via front-matter.

- **HTML `<details>` with non-standard attributes for collapsing
  state.** Standard `<details><summary>...</summary>...</details>`
  parses through as raw HTML per CommonMark; kutl's editor does not
  yet decorate it as a collapsible widget, but the markup is
  preserved and renders natively in browsers that read the raw
  markdown.

- **`#tag` style hashtags as semantic primitives.** A literal `#tag`
  is a valid heading (with `tag` as the heading text) at the start
  of a line, and otherwise just a hash character. There is no
  separate tag index in KFM. Tagging-like behavior can be approximated
  with callouts or with flag kinds.

- **Twitter / Mastodon-style `@everyone` or `@channel` broadcast
  mentions.** Mentions resolve to specific DIDs (or display names
  resolving to DIDs). A `@channel` or `@everyone` literal does not
  fan out — it renders as literal text. To address an audience,
  enumerate the recipients explicitly.

If you find yourself wanting one of these forms, the kutl-native
equivalent is usually documented above — internal links for
wikilinks, mentions-with-explicit-DIDs for broadcast addressing,
callouts for tag-like structured annotations.
