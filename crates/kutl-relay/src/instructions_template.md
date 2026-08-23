# Working with kutl through this relay

You are connected to a kutl relay's MCP endpoint. kutl is a sync engine
for collaborative markdown documents. Each *space* on this relay holds
documents (markdown content) and *signals* (first-class messages
attached to the space — flags for attention, replies, closes,
comments anchored to text spans).

## Identifier shapes

- **`space_id`** — typically a slug-shaped string (e.g.,
  `"acme-handbook"`) in OSS. Opaque to you; discover via `list_spaces`.
- **`document_id`** — either a UUID (e.g.,
  `"01923456-78ab-cdef-..."`) OR a within-space path (e.g.,
  `"onboarding/intro.md"`). Tools that take `document_id` accept
  either form interchangeably; you don't need to convert.
- **`signal_id`** — UUID. Returned by `create_flag` / `create_reply`,
  consumed by `get_signal_detail` / `close_flag`.

## Primitives

- **Spaces.** Containers for collaborative work. Use `list_spaces` to
  enumerate destinations you're authorized for; use `list_documents`
  to see what's in a space.
- **Documents.** Markdown content addressed by UUID or path.
  - `create_document` to add a new document (with optional provenance
    fields for imports).
  - `read_document` to read existing content.
  - `edit_document` to update existing content — pass the `version`
    your `read_document` returned as `base_version`, plus the full
    markdown you want; only what you changed is applied, so a
    participant's concurrent work survives. Provide a short
    `intent` string describing your edit.
  - `upload_blob` for binary content (images, PDFs, .docx).
    Create-or-replace at a path; bytes are opaque.
- **Signals.** Space-attached messages of several kinds:
  - `create_flag` — attention signal. Kinds: `info`,
    `review_requested`, `question`, `blocked`, `completed`.
  - `create_comment` — a remark anchored to a span of document text.
    Its own verb because it needs a matching inline marker.
  - `create_reply` — respond to a signal.
  - `close_flag` — mark a flag resolved.
  - `get_signal_detail` — read a specific signal with its replies.
  - `get_changes` — poll for new signals + document changes since
    your last check.
