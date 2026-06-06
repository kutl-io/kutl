//! MCP tool definitions and argument parsing for space-scoped operations.

use kutl_proto::sync;
use serde_json::Value;

use crate::mcp::{ToolCallResult, ToolDefinition};

// ---------------------------------------------------------------------------
// McpInstructionsProvider trait — extension point for cloud-mode instructions
// ---------------------------------------------------------------------------

/// Canonical kutl-flavored markdown (KFM) dialect spec. Source of
/// truth lives at `oss/docs/kutl-markdown.md`; the same file is
/// embedded into the `kutl` CLI binary via a separate `include_str!`,
/// guaranteeing both vehicles ship the byte-identical reference. See
/// RFD 0083 §"Markdown dialect — Distribution".
pub const KFM_SPEC: &str = include_str!("../../../docs/kutl-markdown.md");

/// Cloud-mode scope-1 (tool-generic) instructions content for the OSS
/// relay. Embedded at compile time. Agents connecting to the OSS relay
/// directly via MCP receive this string in `InitializeResult.instructions`.
///
/// Composition (top-to-bottom):
/// 1. Flavor-specific *top* — intro, identifier shapes, primitives.
/// 2. Universal middle — path conventions, KFM pointer, reach
///    mechanisms, audience, provenance, team conventions, edit
///    discipline. Shared verbatim with the kutlhub-relay template via
///    `oss/docs/instructions_universal.md`.
/// 3. Flavor-specific *tail* — default-visibility assumption.
/// 4. KFM dialect spec — concatenated last so MCP-only clients get
///    the markdown reference in the same `instructions` string.
pub const OSS_INSTRUCTIONS: &str = concat!(
    include_str!("instructions_template.md"),
    "\n\n",
    include_str!("../../../docs/instructions_universal.md"),
    "\n\n",
    include_str!("instructions_template_tail.md"),
    "\n\n---\n\n",
    include_str!("../../../docs/kutl-markdown.md"),
);

/// Extension point for relay implementations to provide cloud-mode
/// instructions. The default returns `OSS_INSTRUCTIONS` via
/// `DefaultInstructionsProvider`; extension hosts may supply their own
/// provider returning deployment-flavored content. Override is
/// **replacement**, not extension — extension hosts do not inherit the
/// default content, just the trait.
pub trait McpInstructionsProvider: Send + Sync {
    /// Cloud-mode instructions string to include in MCP
    /// `InitializeResult.instructions`.
    fn instructions(&self) -> &str;
}

/// Default provider for the OSS relay. Returns `OSS_INSTRUCTIONS`.
pub struct DefaultInstructionsProvider;

impl McpInstructionsProvider for DefaultInstructionsProvider {
    fn instructions(&self) -> &str {
        OSS_INSTRUCTIONS
    }
}

// ---------------------------------------------------------------------------
// McpToolProvider trait — extension point for extension hosts
// ---------------------------------------------------------------------------

/// Extension point for relay implementations to provide additional MCP tools.
/// The relay registers base signal tools; extension hosts add tools
/// that require the UX layer (reactions, reopen).
pub trait McpToolProvider: Send + Sync {
    /// Additional tools beyond the base set.
    fn extra_tools(&self) -> Vec<ToolDefinition> {
        Vec::new()
    }

    /// Replacements for base tools by name. Used by extension hosts
    /// (kutlhub-relay) to override OSS-conservative tool descriptions —
    /// notably the visibility-default text that asserts what humans see —
    /// with deployment-flavored versions. The default returns an empty
    /// vec; `signal_tools` and `tool_definitions_with_provider` replace
    /// matching-name entries in the base list with the overrides.
    /// See RFD 0083 §"V1 implementation: relay flavor as the practical
    /// default-visibility proxy".
    fn override_tools(&self) -> Vec<ToolDefinition> {
        Vec::new()
    }
}

/// No-op provider for the OSS relay.
pub struct NoopToolProvider;
impl McpToolProvider for NoopToolProvider {}

// ---------------------------------------------------------------------------
// Tool list assembly
// ---------------------------------------------------------------------------

/// Returns base signal tools plus any extras from the provider.
pub fn signal_tools(provider: &dyn McpToolProvider) -> Vec<ToolDefinition> {
    let mut tools = vec![
        create_flag_tool(),
        create_reply_tool(),
        close_flag_tool(),
        get_signal_detail_tool(),
    ];
    apply_overrides(&mut tools, provider.override_tools());
    tools.extend(provider.extra_tools());
    tools
}

/// Return all MCP tool definitions (non-signal tools + base signal tools).
pub fn tool_definitions() -> Vec<ToolDefinition> {
    tool_definitions_with_provider(&NoopToolProvider)
}

/// Return all MCP tool definitions with extra tools from a provider.
pub fn tool_definitions_with_provider(provider: &dyn McpToolProvider) -> Vec<ToolDefinition> {
    let mut defs = vec![
        read_document_tool(),
        list_documents_tool(),
        list_spaces_tool(),
        read_log_tool(),
        list_participants_tool(),
        status_tool(),
        create_document_tool(),
        edit_document_tool(),
        upload_blob_tool(),
        get_changes_tool(),
    ];
    defs.extend(signal_tools(provider));
    defs
}

/// Replace entries in `tools` whose `name` matches an override entry.
/// Used by extension hosts to swap OSS-conservative tool descriptions
/// for deployment-flavored versions.
///
/// An override targeting a name that does not appear in `tools` is a
/// caller bug — usually the base tool was renamed without updating
/// the override. We panic in debug builds (loud during tests) and log
/// at warn in release (visible at startup) so the silent-drop failure
/// mode never reaches a deployed agent.
fn apply_overrides(tools: &mut [ToolDefinition], overrides: Vec<ToolDefinition>) {
    for override_def in overrides {
        if let Some(slot) = tools.iter_mut().find(|t| t.name == override_def.name) {
            *slot = override_def;
        } else {
            debug_assert!(
                false,
                "override targets unknown base tool `{}` — base tool renamed without updating override",
                override_def.name,
            );
            tracing::warn!(
                tool = %override_def.name,
                "tool override has no matching base tool; rename drift suspected",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Non-signal tool definitions
// ---------------------------------------------------------------------------

fn read_document_tool() -> ToolDefinition {
    ToolDefinition {
        name: "read_document".into(),
        description: "Read the current text content of a document. Returns \
                      `{ content, version }` where `content` is the full \
                      markdown body (no frontmatter wrapping, no metadata \
                      envelope) and `version` is an opaque CRDT-version \
                      string useful for follow-up diffing.\n\n\
                      Use this to inspect a document before editing it, or \
                      after `get_changes` surfaces a `document_changes` entry \
                      with a new `edited_at`. Discover `document_id` values \
                      via `list_documents` (UUID) — both UUIDs and within-space \
                      paths are accepted here.\n\n\
                      Errors with `NotTextDocument` when the target is a blob \
                      (image, PDF, .docx, etc.) — use the blob download path \
                      for those. Errors with `DocumentNotFound` when no such \
                      document exists in the space.\n\n\
                      Do NOT use to list documents (use `list_documents`) or \
                      to fetch change history (use `read_log`)."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "document_id": {
                    "type": "string",
                    "description": "Document identifier within the space. Accepts a UUID or a within-space path."
                }
            },
            "required": ["space_id", "document_id"]
        }),
    }
}

fn list_documents_tool() -> ToolDefinition {
    ToolDefinition {
        name: "list_documents".into(),
        description: "Enumerate documents in a space. Returns an array of \
                      `{ document_id, path, content_type, subscriber_count, \
                      ...provenance? }` records — `content_type` is one of \
                      `\"text\"`, `\"blob\"`, `\"empty\"`, or `\"unknown\"` \
                      (when the doc is registered but not yet loaded into \
                      memory). Order is registry-iteration order (NOT stable \
                      across calls; do not rely on it).\n\n\
                      Provenance fields (`source_kind`, `source_id`, \
                      `source_url`, `source_author_display`, \
                      `originally_created_at`, `ingestion_job_id`) appear \
                      only when set by an import — natively-authored \
                      documents omit them entirely. Useful for \
                      \"view original\" surfacing and post-import \
                      verification.\n\n\
                      Use this to discover `document_id` UUIDs for \
                      `read_document`, `edit_document`, or `create_flag`, and \
                      to enumerate paths for navigation.\n\n\
                      **WARNING — no pagination in v1.** The full list is \
                      returned in a single response; large spaces will \
                      return very large payloads and may exceed MCP client \
                      limits. Do NOT assume bounded results. Spaces with \
                      thousands of documents are not safe to enumerate via \
                      this tool today; a paginated variant is a follow-up.\n\n\
                      Do NOT use to read content (use `read_document`) or to \
                      poll for activity (use `get_changes`)."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                }
            },
            "required": ["space_id"]
        }),
    }
}

fn read_log_tool() -> ToolDefinition {
    ToolDefinition {
        name: "read_log".into(),
        description: "Read the per-change history of a single document. \
                      Returns an array of `{ id, author_did, intent, \
                      timestamp, boundary, full_rewrite }` entries in \
                      reverse-chronological order (most recent first). \
                      `intent` is the human-readable description supplied at \
                      edit time; `timestamp` is Unix milliseconds; \
                      `full_rewrite` flags bulk replacements.\n\n\
                      Use this to investigate WHO changed a document and WHY \
                      — particularly for diagnosing recent surprises before \
                      proposing further edits, or for building a per-document \
                      activity narrative.\n\n\
                      `limit` caps how many entries come back (most recent \
                      first); omit to return the entire history. **No cursor \
                      / no pagination in v1** — large change histories with \
                      `limit` unset return everything in one response. Blob \
                      documents return an empty array (no text-change log).\n\n\
                      Do NOT use to poll for new space-wide activity (use \
                      `get_changes` — that surfaces signals + cross-document \
                      changes). Do NOT use to read current content (use \
                      `read_document`)."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "document_id": {
                    "type": "string",
                    "description": "Document identifier within the space. Accepts a UUID or a within-space path."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of log entries to return (most recent first). Defaults to all."
                }
            },
            "required": ["space_id", "document_id"]
        }),
    }
}

fn list_participants_tool() -> ToolDefinition {
    ToolDefinition {
        name: "list_participants".into(),
        description: "List participants currently connected to a space. \
                      Returns an array of `{ did, connection_type }` records \
                      where `connection_type` is `\"websocket\"` (an active \
                      WS subscriber — usually a kutl daemon or browser \
                      client) or `\"mcp\"` (an MCP session). DIDs are unique \
                      across the response; a participant connected over both \
                      transports appears once.\n\n\
                      Use this to discover the `target_did` value for \
                      `create_flag(audience: 'participant', target_did: ...)` \
                      — the natural next action when you want to direct a \
                      flag at a specific human or agent.\n\n\
                      Snapshot of LIVE connections only — participants who \
                      are members of the space but currently offline do not \
                      appear. Do NOT use as an authoritative membership \
                      roster."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                }
            },
            "required": ["space_id"]
        }),
    }
}

fn status_tool() -> ToolDefinition {
    ToolDefinition {
        name: "status".into(),
        description: "Get aggregate counts for a space. Returns \
                      `{ document_count, subscriber_count, mcp_session_count }`:\n\
                      - `document_count` — loaded documents in the space \
                        (registry-driven; reflects what `list_documents` \
                        would return).\n\
                      - `subscriber_count` — total active WS subscriptions \
                        across all documents in the space (NOT unique \
                        participants — one daemon subscribed to N docs \
                        contributes N).\n\
                      - `mcp_session_count` — MCP sessions on the relay \
                        process. Note: this is process-global, not \
                        space-scoped, because MCP sessions today are \
                        space-agnostic.\n\n\
                      Use this for a quick health/scale sanity check before \
                      heavier operations (e.g. is the space empty? is anyone \
                      else connected to see what I'm about to do?). For the \
                      per-DID list of connected participants, use \
                      `list_participants`; for the document inventory, use \
                      `list_documents`."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                }
            },
            "required": ["space_id"]
        }),
    }
}

fn edit_document_tool() -> ToolDefinition {
    ToolDefinition {
        name: "edit_document".into(),
        description: "Updates the content of an EXISTING document. The relay diffs \
                      current vs new content and applies minimal CRDT operations. \
                      To create a new document, use `create_document` — \
                      `edit_document` errors when the requested document does not \
                      exist.\n\n\
                      The `document_id` argument accepts either a UUID (e.g. \
                      `11111111-1111-4111-8111-111111111111`) or a within-space \
                      path (e.g. `handbook/onboarding.md`). Both forms resolve to \
                      the same underlying document; paths are looked up via the \
                      space's path index.\n\n\
                      Other participants see the edit when they call `get_changes`: \
                      the document appears in `document_changes` with an updated \
                      `edited_at` timestamp. Use `create_flag` when you need to \
                      draw specific attention to your edit or ask for input."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "document_id": {
                    "type": "string",
                    "description": "Document identifier within the space. Accepts a UUID or a within-space path. The document must already exist; use create_document to create new documents."
                },
                "content": {
                    "type": "string",
                    "description": "The full desired document content. Use real newlines, not escaped \\n sequences."
                },
                "intent": {
                    "type": "string",
                    "description": "Human-readable description of what this edit does."
                },
                "snippet": {
                    "type": "string",
                    "description": "Optional short preview of the notable change for the activity feed. If omitted, the relay computes one from the diff."
                }
            },
            "required": ["space_id", "document_id", "content", "intent"]
        }),
    }
}

fn create_document_tool() -> ToolDefinition {
    ToolDefinition {
        name: "create_document".into(),
        description: "Creates a new text document at the given path with initial \
                      content. Returns the assigned `document_id` (UUID). To \
                      update an existing document, use `edit_document`.\n\n\
                      Provenance fields are optional display metadata for imports: \
                      pass what you have so the UI can show a \"view original\" \
                      link and original-author attribution. The MCP surface does \
                      NOT dedup on `(source_kind, source_id)` — repeat calls \
                      create separate documents unless you supply distinct \
                      `path`s, and a path collision is a plain error.\n\n\
                      Space creation is not an MCP capability — discover \
                      existing destinations via `list_spaces`. Humans create \
                      spaces through their relay's normal flow (`kutl init` \
                      for OSS / file-sync, the desktop app, or the kutlhub \
                      web UI for the hosted product)."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "path": {
                    "type": "string",
                    "description": "Within-space markdown path, e.g. `handbook/onboarding.md`. No leading slash."
                },
                "content": {
                    "type": "string",
                    "description": "Initial markdown content. Use real newlines, not escaped \\n sequences. Empty string is allowed."
                },
                "source_kind": {
                    "type": "integer",
                    "description": "Optional SourceKind enum value (e.g. 1=NOTION_HTML, 10=DOCX, 20=GDOCS_DOC). Leave unset for native authoring or unknown sources."
                },
                "source_id": {
                    "type": "string",
                    "description": "Optional source-side stable ID (e.g. Notion page_id, Drive file_id)."
                },
                "source_url": {
                    "type": "string",
                    "description": "Optional canonical source URL for a \"view original\" link."
                },
                "source_author_display": {
                    "type": "string",
                    "description": "Optional non-DID author display string captured from the source."
                },
                "originally_created_at": {
                    "type": "integer",
                    "description": "Optional source-side creation timestamp in Unix milliseconds."
                },
                "ingestion_job_id": {
                    "type": "string",
                    "description": "Optional ingestion job UUID. Set by the format-service worker on its built-in path; cross-MCP callers typically leave this unset."
                }
            },
            "required": ["space_id", "path", "content"]
        }),
    }
}

fn upload_blob_tool() -> ToolDefinition {
    ToolDefinition {
        name: "upload_blob".into(),
        description: "Creates or replaces a binary document (image, PDF, .docx, \
                      etc.) at the given within-space `path`. Returns the \
                      assigned `document_id` (stable across replace) and a \
                      relay-relative `content_url` suitable for embedding in \
                      markdown (e.g. `![alt](docimages/diagram.png)` from a \
                      sibling document).\n\n\
                      Bytes must be base64-encoded over the JSON-RPC wire. The \
                      decoded byte length must not exceed the relay's blob cap \
                      (see `KUTL_MAX_BLOB_BYTES`).\n\n\
                      `content_type` is optional and currently advisory — the \
                      relay's blob layer is MIME-agnostic by design (RFD 0043) \
                      and does NOT persist the MIME today; downstream rendering \
                      paths (e.g., browser download) derive content type at read \
                      time. Pass it when you know it, for forward-compat with \
                      future provenance plumbing.\n\n\
                      Replace semantics: re-uploading at the same path returns \
                      the same `document_id`; previous bytes are discarded \
                      (no versioning). Provenance fields follow leave-as-is-on-\
                      omit — supplying a field overwrites, omitting preserves \
                      the existing value. A path already used by a text \
                      document is rejected — text and blob are not \
                      interchangeable at the same path."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "path": {
                    "type": "string",
                    "description": "Within-space path for the blob, e.g. `docimages/diagram.png`. No leading slash."
                },
                "content_type": {
                    "type": "string",
                    "description": "Optional MIME type of the bytes (e.g. `image/png`, `application/pdf`). Advisory only today — the relay does not persist it; future browser-download paths derive MIME at read time."
                },
                "bytes": {
                    "type": "string",
                    "description": "Blob content as base64-encoded bytes (standard alphabet, with or without padding)."
                },
                "source_kind": {
                    "type": "integer",
                    "description": "Optional SourceKind enum value. See `create_document` for the enum semantics."
                },
                "source_id": {
                    "type": "string",
                    "description": "Optional source-side stable ID."
                },
                "source_url": {
                    "type": "string",
                    "description": "Optional canonical source URL."
                },
                "source_author_display": {
                    "type": "string",
                    "description": "Optional non-DID author display string."
                },
                "originally_created_at": {
                    "type": "integer",
                    "description": "Optional source-side creation timestamp in Unix milliseconds."
                }
            },
            "required": ["space_id", "path", "bytes"]
        }),
    }
}

fn list_spaces_tool() -> ToolDefinition {
    ToolDefinition {
        name: "list_spaces".into(),
        description: "Lists spaces the connected DID is authorised for. Returns \
                      an array of `{ space_id, slug, name }` records. Returns an \
                      empty array when the caller has no spaces.\n\n\
                      Use this to discover destinations before calling \
                      `create_document`, `edit_document`, `upload_blob`, or any \
                      space-scoped tool. The OSS relay enumerates from its \
                      in-process registry filtered by the `authorized_keys` \
                      file; deployment flavors with a membership backend \
                      filter by actual space memberships."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {},
            "required": []
        }),
    }
}

fn get_changes_tool() -> ToolDefinition {
    ToolDefinition {
        name: "get_changes".into(),
        description: "Poll for new activity in a space since the caller's \
                      previous call. Returns \
                      `{ signals, document_changes, checkpoint }`:\n\
                      - `signals` — proto `Signal` records emitted since the \
                        last cursor: flags, replies, reactions, closes, \
                        reopens. Each carries `id`, `signal_type`, \
                        `author_did`, `document_id`, `timestamp`, and \
                        type-specific fields. For a flag worth investigating, \
                        pass its `id` to `get_signal_detail` to fetch the \
                        full reply thread + reactions.\n\
                        - `document_changes` — `RegistryEntry` records for \
                        documents registered, renamed, deleted, or edited \
                        since the cursor. An entry with an updated \
                        `edited_at` means the body changed; call \
                        `read_document` to see the new content.\n\
                      - `checkpoint` — opaque string for failure-recovery \
                        replay. Pass it back via the `checkpoint` argument \
                        on the next call to re-deliver from that point if \
                        the previous response was lost mid-processing. \
                        Normal operation omits this — the server tracks the \
                        per-DID (or per-session, for anonymous callers) \
                        cursor automatically.\n\n\
                      Use this as the primary inbox-style polling primitive \
                      for agents working a space over time. Idempotent within \
                      a single agent's cursor; not idempotent across \
                      different agents (each has its own cursor).\n\n\
                      Returns an empty response on deployments without a \
                      change backend (OSS relay in pure ephemeral mode). Do \
                      NOT use to fetch a single signal's detail — use \
                      `get_signal_detail` for that."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "The space to check for changes."
                },
                "checkpoint": {
                    "type": "string",
                    "description": "Resume from a previous checkpoint. Pass the checkpoint \
                        value from a prior response to replay from that point. Omit for \
                        normal operation."
                }
            },
            "required": ["space_id"]
        }),
    }
}

// ---------------------------------------------------------------------------
// Signal tool definitions (OSS base set)
// ---------------------------------------------------------------------------

/// Create a flag signal to draw attention to something in a document.
/// Default-visibility framing for the `create_flag` tool description.
///
/// Extracted as a const so commercial deployments (RFD 0083 §"V1
/// implementation: relay flavor as the practical default-visibility
/// proxy") can replace just this paragraph when overriding the tool
/// description. The conservative OSS framing assumes signals reach
/// MCP-connected agents only; kutlhub-relay overrides with "humans
/// will see the signals eventually" via the web app + email
/// notifications.
pub const CREATE_FLAG_VISIBILITY_DEFAULT_OSS: &str = "Visibility on this OSS relay: signals reach MCP-connected agents only — no signal-rendering \
     tool is bundled today. If you want humans in editors/file-sync to see anything, also write \
     into the document body (an inline callout, a header, the document content itself) via \
     `create_document`/`edit_document`.";

/// Tool description body for `create_flag`. Concatenated with the
/// visibility-default paragraph at construction time.
const CREATE_FLAG_DESCRIPTION_BODY: &str = "\
Create a flag signal (a structured, kind-tagged note) in a space. Flags carry intent — \
the kind says what *type* of attention this warrants, the message says what about, and the \
audience says who.

Kinds (use the one that matches the intent — picking the right kind is how downstream \
notification routing distinguishes informational chatter from things that block work):
- `info` — informational; no action expected.
- `review_requested` — \"please look at this and respond.\"
- `question` — open question that needs an answer.
- `blocked` — something can't proceed until this is resolved.
- `completed` — marks completion of work.
- `comment` — a remark anchored to a specific span of text in a document; pair with a marker emit \
  (see comment flow below).

Audience — `flag + audience`:
- The flag kind carries the *intent* (`review_requested`, `question`, etc.).
- For a single-recipient flag, use `audience: 'participant'` with `target_did` set to the human \
  you want to reach. Discover DIDs via `list_participants`.
- For everyone in the space, use `audience: 'space'`. Each recipient's notification preferences \
  control how they're reached (email, in-app, or nothing); you cannot override delivery.

Comment-kind flow (`kind = 'comment'`):
1. Mint a UUID client-side.
2. Inject `[text]{.cmt #signal-uuid}` into the document body via `edit_document` at the \
   position you want to comment on (the wrapped span is what's being commented on).
3. Call `create_flag(kind: 'comment', signal_id: <same uuid>, anchor_text: <wrapped span>, \
   message: <comment body>, ...)`.

The matching UUID is load-bearing — it ties the inline marker in document content to the signal \
record. Both `signal_id` and `anchor_text` are required for `kind = 'comment'`. For other kinds \
these fields are silently ignored if supplied.

";

fn create_flag_tool() -> ToolDefinition {
    create_flag_tool_with_visibility(CREATE_FLAG_VISIBILITY_DEFAULT_OSS)
}

/// Compose `create_flag`'s tool definition with a caller-supplied
/// visibility-default paragraph. Used by extension hosts (kutlhub-relay)
/// to assert their own visibility default while reusing the rest of the
/// description verbatim.
pub fn create_flag_tool_with_visibility(visibility: &str) -> ToolDefinition {
    let description = format!("{CREATE_FLAG_DESCRIPTION_BODY}{visibility}");
    ToolDefinition {
        name: "create_flag".into(),
        description,
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "document_id": {
                    "type": "string",
                    "description": "Document this flag relates to. Must be a UUID (use list_documents to discover ids; paths are not accepted)."
                },
                "kind": {
                    "type": "string",
                    "enum": ["info", "completed", "review_requested", "question", "blocked", "comment"],
                    "description": "The type of flag. See the description for one-line semantics per kind."
                },
                "message": {
                    "type": "string",
                    "description": "Message body of the flag (the comment body for kind=comment)."
                },
                "audience": {
                    "type": "string",
                    "enum": ["space", "participant"],
                    "description": "Who should see this: 'space' for everyone, 'participant' for a specific person."
                },
                "target_did": {
                    "type": "string",
                    "description": "DID of the target person. Required when audience is 'participant'. Use list_participants to discover DIDs."
                },
                "signal_id": {
                    "type": "string",
                    "description": "Caller-supplied UUID for the signal. REQUIRED when kind='comment' — must match the UUID embedded in the [text]{.cmt #signal-uuid} marker injected via edit_document. Silently ignored for other kinds."
                },
                "anchor_text": {
                    "type": "string",
                    "description": "The wrapped doc span being commented on (posterity snapshot per RFD 0077). REQUIRED when kind='comment'. Silently ignored for other kinds."
                }
            },
            "required": ["space_id", "document_id", "kind", "message", "audience"]
        }),
    }
}

/// Reply to a signal (flag, decision, or another reply).
/// OSS-conservative visibility paragraph for `create_reply`. Extension
/// hosts override by passing a different string to
/// [`create_reply_tool_with_visibility`].
pub const CREATE_REPLY_VISIBILITY_DEFAULT_OSS: &str = "**Visibility on this OSS relay**: replies are out-of-doc — they live in the signal store \
     and reach only MCP-connected callers and clients that consume the signal stream. No kutl \
     OSS client renders signals today (file-sync mode is text-only), so a human reading the \
     document body via their editor will NOT see your reply. If you need humans on a file-sync \
     deployment to see your response, write into the document body via `edit_document` instead \
     (or in addition).";

const CREATE_REPLY_DESCRIPTION_BODY: &str = "Reply to a parent signal (a flag, decision, or earlier \
     reply). Returns the new reply's `signal_id` (UUID). The reply is persisted via the change \
     backend (when one is configured) and surfaces on future `get_changes` calls and inside the \
     parent's `get_signal_detail` response.\n\n\
     Use this to answer a `question`/`review_requested` flag you've been pulled into, or to \
     extend a thread on a decision. Pass `parent_reply_id` to thread under a specific earlier \
     reply; omit it for top-level replies under the parent signal.\n\n\
     ";

const CREATE_REPLY_DESCRIPTION_TAIL: &str = "\n\nDo NOT use to close a resolved flag — use `close_flag` (and `reopen_flag` on \
     commercial deployments to undo).";

fn create_reply_tool() -> ToolDefinition {
    create_reply_tool_with_visibility(CREATE_REPLY_VISIBILITY_DEFAULT_OSS)
}

/// Compose `create_reply`'s tool definition with a caller-supplied
/// visibility-default paragraph.
pub fn create_reply_tool_with_visibility(visibility: &str) -> ToolDefinition {
    let description =
        format!("{CREATE_REPLY_DESCRIPTION_BODY}{visibility}{CREATE_REPLY_DESCRIPTION_TAIL}");
    ToolDefinition {
        name: "create_reply".into(),
        description,
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "parent_signal_id": {
                    "type": "string",
                    "description": "ID of the signal to reply to."
                },
                "parent_reply_id": {
                    "type": "string",
                    "description": "ID of a prior reply this is a response to. Omit for top-level replies."
                },
                "body": {
                    "type": "string",
                    "description": "Text of the reply."
                }
            },
            "required": ["space_id", "parent_signal_id", "body"]
        }),
    }
}

/// OSS-conservative visibility paragraph for `close_flag`. Extension
/// hosts override via [`close_flag_tool_with_visibility`].
pub const CLOSE_FLAG_VISIBILITY_DEFAULT_OSS: &str = "**Visibility on this OSS relay**: closes are out-of-doc — they affect signal-store \
     state only and are visible to MCP-connected agents and signal-aware clients. No kutl OSS \
     client renders signals today, so a human on a file-sync deployment will not notice the close \
     unless you also edit the document body. The flag persists in the signal store; closing only \
     updates its lifecycle state.";

const CLOSE_FLAG_DESCRIPTION_BODY: &str = "Mark a flag as closed with a resolution reason. Emits a close event on the signal stream; \
     the flag's `closed_at` is populated when `get_signal_detail` is next called. Returns an empty \
     success result — the signal_id you supplied IS the identifier (no new id is minted).\n\n\
     `reason` is one of `resolved` (done — the default if omitted), `declined` (intentionally not \
     doing this), or `withdrawn` (no longer relevant). `close_note` is an optional free-text \
     explanation that travels with the close event for downstream rendering.\n\n\
     Use this for flags YOUR agent opened and has now finished (the natural inverse of \
     `create_flag`), or for flags delegated to you that you've completed. Use `reopen_flag` \
     (commercial relay only) if you closed by mistake or new information arrives.\n\n\
     ";

const CLOSE_FLAG_DESCRIPTION_TAIL: &str = "\n\nDo NOT use to delete a flag (no delete operation exists). Do NOT use to acknowledge \
     without resolving — use `react_to_signal` (commercial) or a reply via `create_reply` for that.";

/// Close a flag signal with a resolution.
fn close_flag_tool() -> ToolDefinition {
    close_flag_tool_with_visibility(CLOSE_FLAG_VISIBILITY_DEFAULT_OSS)
}

/// Compose `close_flag`'s tool definition with a caller-supplied
/// visibility-default paragraph.
pub fn close_flag_tool_with_visibility(visibility: &str) -> ToolDefinition {
    let description =
        format!("{CLOSE_FLAG_DESCRIPTION_BODY}{visibility}{CLOSE_FLAG_DESCRIPTION_TAIL}");
    ToolDefinition {
        name: "close_flag".into(),
        description,
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "signal_id": {
                    "type": "string",
                    "description": "ID of the flag signal to close."
                },
                "reason": {
                    "type": "string",
                    "enum": ["resolved", "declined", "withdrawn"],
                    "description": "Reason for closing: 'resolved' (done), 'declined' (won't do), 'withdrawn' (no longer relevant)."
                },
                "close_note": {
                    "type": "string",
                    "description": "Optional note explaining the closure."
                }
            },
            "required": ["space_id", "signal_id"]
        }),
    }
}

/// Get full details of a signal including replies and reactions.
fn get_signal_detail_tool() -> ToolDefinition {
    ToolDefinition {
        name: "get_signal_detail".into(),
        description: "Fetch one signal with its full reply thread and \
                      reactions in a single round-trip. Returns a \
                      `SignalDetail` record with the signal's core fields \
                      (`id`, `space_id`, `document_id`, `author_did`, \
                      `signal_type`, `timestamp`, `flag_kind`, `audience`, \
                      `target_did`, `message`, `anchor_text`, `closed_at`, \
                      parent pointers for replies, `body`) plus two nested \
                      arrays: `replies[]` (flat list of \
                      `{ id, parent_reply_id, author_did, body, created_at }`) \
                      and `reactions[]` (list of \
                      `{ actor_did, emoji, created_at }`).\n\n\
                      Use this as the natural follow-up when `get_changes` \
                      surfaces a signal_id you want to inspect in depth — \
                      e.g. before replying to a `review_requested` flag, \
                      pull the existing thread so your reply doesn't \
                      duplicate someone else's. Also the right tool for \
                      checking whether a flag has been closed (`closed_at` \
                      is set).\n\n\
                      Requires a change backend; deployments without one \
                      return an error. Errors with `SignalNotFound` when the \
                      signal does not exist in the supplied space.\n\n\
                      Do NOT use to enumerate ALL signals — use \
                      `get_changes` for the polling-style inbox view."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "signal_id": {
                    "type": "string",
                    "description": "ID of the signal to retrieve."
                }
            },
            "required": ["space_id", "signal_id"]
        }),
    }
}

// ---------------------------------------------------------------------------
// ParsedToolCall enum
// ---------------------------------------------------------------------------

/// Optional provenance metadata accepted by `create_document` and
/// `upload_blob`. All fields are display-only — the relay stores what
/// the caller supplies and surfaces it through the UI. The MCP layer
/// does NOT dedup on `(source_kind, source_id)`.
#[derive(Debug, Default, Clone)]
pub struct ProvenanceArgs {
    /// `SourceKind` enum value (proto uint32). 0 = native; see RFD 0081.
    pub source_kind: Option<u32>,
    /// Source-side stable ID.
    pub source_id: Option<String>,
    /// Canonical source URL.
    pub source_url: Option<String>,
    /// Free-form author display name from the source.
    pub source_author_display: Option<String>,
    /// Source-side creation timestamp in Unix milliseconds.
    pub originally_created_at_ms: Option<i64>,
    /// Ingestion job UUID. Set by the format-service worker on its
    /// built-in path; cross-MCP callers typically leave this unset.
    pub ingestion_job_id: Option<String>,
}

/// Parsed tool call — validated arguments ready for dispatch.
#[derive(Debug)]
pub enum ParsedToolCall {
    ReadDocument {
        space_id: String,
        document_id: String,
    },
    ListDocuments {
        space_id: String,
    },
    ReadLog {
        space_id: String,
        document_id: String,
        limit: Option<usize>,
    },
    ListParticipants {
        space_id: String,
    },
    Status {
        space_id: String,
    },
    EditDocument {
        space_id: String,
        document_id: String,
        content: String,
        intent: String,
        /// Agent-provided snippet for the activity feed. If empty, the relay
        /// computes one from the diff.
        snippet: String,
    },
    /// Create a brand-new text document at a path with initial content and
    /// optional provenance metadata.
    CreateDocument {
        space_id: String,
        path: String,
        content: String,
        provenance: ProvenanceArgs,
    },
    /// Upload (create or replace) a binary blob at a path with optional
    /// provenance metadata.
    UploadBlob {
        space_id: String,
        path: String,
        content_type: String,
        bytes: Vec<u8>,
        provenance: ProvenanceArgs,
    },
    /// List spaces the connected DID is authorised for.
    ListSpaces,
    GetChanges {
        space_id: String,
        checkpoint: Option<String>,
    },
    /// Create a flag signal.
    ///
    /// `signal_id` and `anchor_text` activate when `kind == FLAG_KIND_COMMENT`
    /// — they carry the inline marker UUID and the wrapped span for posterity
    /// (RFD 0077). For other kinds they are accepted-and-ignored at the
    /// parser layer (silently dropped); the handler does not require them
    /// to be absent.
    CreateFlag {
        space_id: String,
        document_id: String,
        kind: i32,
        message: String,
        audience: i32,
        target_did: String,
        /// Caller-supplied signal UUID, required for `kind == comment`.
        signal_id: Option<String>,
        /// Wrapped-span posterity snapshot, required for `kind == comment`.
        anchor_text: Option<String>,
    },
    /// Reply to an existing signal.
    CreateReply {
        space_id: String,
        parent_signal_id: String,
        parent_reply_id: Option<String>,
        body: String,
    },
    /// Close a flag signal with an optional reason.
    CloseFlag {
        space_id: String,
        signal_id: String,
        /// Close reason: `"resolved"`, `"declined"`, or `"withdrawn"`.
        /// When `None`, the server picks the default (`withdrawn` if the
        /// caller is the flag's author, otherwise `resolved`).
        reason: Option<String>,
        close_note: Option<String>,
    },
    /// Fetch the full detail of a single signal.
    GetSignalDetail {
        space_id: String,
        signal_id: String,
    },
    /// Add or remove a reaction on a signal (DB-backed deployments only).
    ReactToSignal {
        space_id: String,
        signal_id: String,
        emoji: String,
        remove: bool,
    },
    /// Reopen a previously closed flag signal (DB-backed deployments only).
    ReopenFlag {
        space_id: String,
        signal_id: String,
    },
}

// ---------------------------------------------------------------------------
// Tool call parsing
// ---------------------------------------------------------------------------

/// Parse and validate tool call arguments.
///
/// Returns `Err(ToolCallResult)` with an error result if validation fails.
pub fn parse_tool_call(name: &str, arguments: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    match name {
        "read_document" => {
            let space_id = require_string(arguments, "space_id")?;
            let document_id = require_string(arguments, "document_id")?;
            Ok(ParsedToolCall::ReadDocument {
                space_id,
                document_id,
            })
        }
        "list_documents" => {
            let space_id = require_string(arguments, "space_id")?;
            Ok(ParsedToolCall::ListDocuments { space_id })
        }
        "read_log" => {
            let space_id = require_string(arguments, "space_id")?;
            let document_id = require_string(arguments, "document_id")?;
            let limit = optional_usize(arguments, "limit")?;
            Ok(ParsedToolCall::ReadLog {
                space_id,
                document_id,
                limit,
            })
        }
        "list_participants" => {
            let space_id = require_string(arguments, "space_id")?;
            Ok(ParsedToolCall::ListParticipants { space_id })
        }
        "status" => {
            let space_id = require_string(arguments, "space_id")?;
            Ok(ParsedToolCall::Status { space_id })
        }
        "edit_document" => {
            let space_id = require_string(arguments, "space_id")?;
            let document_id = require_string(arguments, "document_id")?;
            let content = unescape_llm_newlines(&require_string(arguments, "content")?);
            let intent = require_string(arguments, "intent")?;
            let snippet = arguments
                .get("snippet")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            Ok(ParsedToolCall::EditDocument {
                space_id,
                document_id,
                content,
                intent,
                snippet,
            })
        }
        "create_document" => parse_create_document(arguments),
        "upload_blob" => parse_upload_blob(arguments),
        "list_spaces" => Ok(ParsedToolCall::ListSpaces),
        "get_changes" => {
            let space_id = require_string(arguments, "space_id")?;
            let checkpoint = optional_string(arguments, "checkpoint")?;
            Ok(ParsedToolCall::GetChanges {
                space_id,
                checkpoint,
            })
        }
        "create_flag" => parse_create_flag(arguments),
        "create_reply" => parse_create_reply(arguments),
        "close_flag" => parse_close_flag(arguments),
        "get_signal_detail" => parse_get_signal_detail(arguments),
        // Extension-host-only tools — the base relay doesn't define
        // these but extension hosts may register them via
        // McpToolProvider. The handler needs to parse them when
        // dispatching.
        "react_to_signal" => parse_react_to_signal(arguments),
        "reopen_flag" => parse_reopen_flag(arguments),
        _ => Err(ToolCallResult::error(format!("unknown tool: {name}"))),
    }
}

/// Maximum byte length of a within-space document path. Cheap upper
/// bound — long enough for nested directory structures (~16 levels at
/// 60 chars each), short enough to keep error messages bounded and
/// rule out adversarial inputs that try to wedge the registry.
const MAX_PATH_BYTES: usize = 1024;

/// Validate a within-space document path supplied by an MCP caller.
///
/// Shared by `create_document` and `upload_blob` so both reject the
/// same set of malformed inputs:
/// - empty path
/// - leading `/` (paths are within-space relative, not absolute)
/// - backslashes (within-space paths are POSIX-relative; a backslash
///   would let `dir\..\..\escape` slip past the `/`-only `..` split)
/// - `..` path components (refuse traversal up out of the space)
/// - NUL bytes (defensive — would break any filesystem-export surface)
/// - paths longer than `MAX_PATH_BYTES`
///
/// Today registry paths are opaque strings, but daemon `kutl surface`
/// and future filesystem-export paths will treat them as real paths;
/// validating at the MCP boundary stops a malformed value from
/// reaching those downstream consumers.
fn validate_within_space_path(path: &str) -> Result<(), ToolCallResult> {
    if path.is_empty() {
        return Err(ToolCallResult::error("path must not be empty"));
    }
    if path.starts_with('/') {
        return Err(ToolCallResult::error(
            "path must not start with `/` — paths are within-space relative",
        ));
    }
    if path.len() > MAX_PATH_BYTES {
        return Err(ToolCallResult::error(format!(
            "path exceeds {MAX_PATH_BYTES}-byte limit"
        )));
    }
    if path.contains('\0') {
        return Err(ToolCallResult::error("path must not contain NUL bytes"));
    }
    if path.contains('\\') {
        return Err(ToolCallResult::error(
            "path must not contain backslashes — within-space paths are POSIX-relative",
        ));
    }
    if path.split('/').any(|seg| seg == "..") {
        return Err(ToolCallResult::error(
            "path must not contain `..` components — traversal is not allowed",
        ));
    }
    Ok(())
}

/// Parse `create_document` arguments.
fn parse_create_document(args: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    let space_id = require_string(args, "space_id")?;
    let path = require_string(args, "path")?;
    validate_within_space_path(&path)?;
    let content = unescape_llm_newlines(&require_string(args, "content")?);
    let provenance = parse_provenance(args)?;
    Ok(ParsedToolCall::CreateDocument {
        space_id,
        path,
        content,
        provenance,
    })
}

/// Parse `upload_blob` arguments.
///
/// `content_type` is optional and accepted-but-not-persisted today —
/// the relay's blob layer is MIME-agnostic by design (RFD 0043). The
/// parser preserves what the caller sent (empty string when omitted)
/// so that a future plumbing change can pick it up without a schema
/// migration.
fn parse_upload_blob(args: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    use base64::Engine;
    let space_id = require_string(args, "space_id")?;
    let path = require_string(args, "path")?;
    validate_within_space_path(&path)?;
    let content_type = optional_string(args, "content_type")?.unwrap_or_default();
    let bytes_b64 = require_string(args, "bytes")?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(bytes_b64.as_bytes())
        .map_err(|e| ToolCallResult::error(format!("bytes is not valid base64: {e}")))?;
    let provenance = parse_provenance(args)?;
    Ok(ParsedToolCall::UploadBlob {
        space_id,
        path,
        content_type,
        bytes,
        provenance,
    })
}

/// Parse the shared optional provenance fields.
fn parse_provenance(args: &Value) -> Result<ProvenanceArgs, ToolCallResult> {
    Ok(ProvenanceArgs {
        source_kind: optional_u32(args, "source_kind")?,
        source_id: optional_string(args, "source_id")?,
        source_url: optional_string(args, "source_url")?,
        source_author_display: optional_string(args, "source_author_display")?,
        originally_created_at_ms: optional_i64(args, "originally_created_at")?,
        ingestion_job_id: optional_string(args, "ingestion_job_id")?,
    })
}

// ---------------------------------------------------------------------------
// Signal tool parsers
// ---------------------------------------------------------------------------

/// Parse `create_flag` arguments.
fn parse_create_flag(args: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    let space_id = require_string(args, "space_id")?;
    let document_id = require_uuid(args, "document_id")?;
    let kind = parse_flag_kind(&require_string(args, "kind")?)?;
    let message = require_string(args, "message")?;
    let audience = parse_audience_type(&require_string(args, "audience")?)?;
    let target_did = args
        .get("target_did")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if audience == i32::from(sync::AudienceType::Participant) && target_did.is_empty() {
        return Err(ToolCallResult::error(
            "target_did is required when audience is 'participant'",
        ));
    }
    // A 'space'-audience flag is a broadcast; carrying a specific target_did
    // is semantically malformed. Reject it so the confusion surfaces as a
    // clear error rather than silent data duplication.
    if audience == i32::from(sync::AudienceType::Space) && !target_did.is_empty() {
        return Err(ToolCallResult::error(
            "target_did must be empty when audience is 'space' (space audience is a broadcast); use audience 'participant' to target a specific user",
        ));
    }

    let is_comment = kind == i32::from(sync::FlagKind::Comment);
    let supplied_signal_id = optional_uuid(args, "signal_id")?;
    let anchor_text = optional_string(args, "anchor_text")?;

    // Comment-kind invariants (RFD 0077): the inline marker is the
    // live anchor binding, so the matching UUID + posterity snapshot
    // must both be supplied. Missing either makes the marker point at
    // nothing, or surfaces a bodiless comment in the sidebar.
    if is_comment {
        if supplied_signal_id.is_none() {
            return Err(ToolCallResult::error(
                "signal_id is required when kind is 'comment' — pass the same UUID embedded in the [text]{.cmt #signal-uuid} marker via edit_document",
            ));
        }
        if anchor_text.as_deref().is_none_or(str::is_empty) {
            return Err(ToolCallResult::error(
                "anchor_text is required when kind is 'comment' — supply the wrapped span text for posterity",
            ));
        }
    }
    // Non-comment kinds: signal_id and anchor_text are silently
    // ignored. The lean is "more forgiving" — agents that probe with a
    // shared call template shouldn't get a parse error.
    let (final_signal_id, final_anchor_text) = if is_comment {
        (supplied_signal_id, anchor_text)
    } else {
        (None, None)
    };

    Ok(ParsedToolCall::CreateFlag {
        space_id,
        document_id,
        kind,
        message,
        audience,
        target_did,
        signal_id: final_signal_id,
        anchor_text: final_anchor_text,
    })
}

/// Parse `create_reply` arguments.
fn parse_create_reply(args: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    let space_id = require_string(args, "space_id")?;
    let parent_signal_id = require_uuid(args, "parent_signal_id")?;
    let parent_reply_id = optional_uuid(args, "parent_reply_id")?;
    let body = require_string(args, "body")?;
    Ok(ParsedToolCall::CreateReply {
        space_id,
        parent_signal_id,
        parent_reply_id,
        body,
    })
}

/// Parse `close_flag` arguments.
///
/// `reason` is optional: when omitted the server picks the default
/// (`withdrawn` if the caller is the flag's author, otherwise `resolved`).
/// When provided it must be one of `"resolved"`, `"declined"`, or
/// `"withdrawn"`.
fn parse_close_flag(args: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    let space_id = require_string(args, "space_id")?;
    let signal_id = require_uuid(args, "signal_id")?;
    let reason = optional_string(args, "reason")?;
    if let Some(ref r) = reason {
        match r.as_str() {
            "resolved" | "declined" | "withdrawn" => {}
            _ => {
                return Err(ToolCallResult::error(
                    "invalid reason: must be 'resolved', 'declined', or 'withdrawn'",
                ));
            }
        }
    }
    let close_note = optional_string(args, "close_note")?;
    Ok(ParsedToolCall::CloseFlag {
        space_id,
        signal_id,
        reason,
        close_note,
    })
}

/// Parse `get_signal_detail` arguments.
fn parse_get_signal_detail(args: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    let space_id = require_string(args, "space_id")?;
    let signal_id = require_uuid(args, "signal_id")?;
    Ok(ParsedToolCall::GetSignalDetail {
        space_id,
        signal_id,
    })
}

/// Parse `react_to_signal` arguments (DB-backed deployments only).
fn parse_react_to_signal(args: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    let space_id = require_string(args, "space_id")?;
    let signal_id = require_uuid(args, "signal_id")?;
    let emoji = require_string(args, "emoji")?;
    let remove = args
        .get("remove")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Ok(ParsedToolCall::ReactToSignal {
        space_id,
        signal_id,
        emoji,
        remove,
    })
}

/// Parse `reopen_flag` arguments (DB-backed deployments only).
fn parse_reopen_flag(args: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    let space_id = require_string(args, "space_id")?;
    let signal_id = require_uuid(args, "signal_id")?;
    Ok(ParsedToolCall::ReopenFlag {
        space_id,
        signal_id,
    })
}

// ---------------------------------------------------------------------------
// Enum parsers
// ---------------------------------------------------------------------------

/// Parse a flag kind string to proto enum value.
fn parse_flag_kind(s: &str) -> Result<i32, ToolCallResult> {
    match s {
        "info" => Ok(i32::from(sync::FlagKind::Info)),
        "completed" => Ok(i32::from(sync::FlagKind::Completed)),
        "review_requested" => Ok(i32::from(sync::FlagKind::ReviewRequested)),
        "question" => Ok(i32::from(sync::FlagKind::Question)),
        "blocked" => Ok(i32::from(sync::FlagKind::Blocked)),
        "comment" => Ok(i32::from(sync::FlagKind::Comment)),
        _ => Err(ToolCallResult::error(
            "invalid kind: must be one of info, completed, review_requested, question, blocked, comment",
        )),
    }
}

/// Parse an audience type string to proto enum value.
fn parse_audience_type(s: &str) -> Result<i32, ToolCallResult> {
    match s {
        "space" => Ok(i32::from(sync::AudienceType::Space)),
        "participant" => Ok(i32::from(sync::AudienceType::Participant)),
        _ => Err(ToolCallResult::error(
            "invalid audience: must be 'space' or 'participant'",
        )),
    }
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

/// Extract a required string field from a JSON object.
fn require_string(args: &Value, field: &str) -> Result<String, ToolCallResult> {
    args.get(field)
        .and_then(Value::as_str)
        .map(String::from)
        .ok_or_else(|| ToolCallResult::error(format!("missing required field: {field}")))
}

/// Extract a required field that must parse as a UUID. Surfaces the
/// raw value in the error so a caller passing a path (`daily-digest.md`)
/// gets a hint about what they did wrong rather than a bare type error
/// from the database driver.
fn require_uuid(args: &Value, field: &str) -> Result<String, ToolCallResult> {
    let raw = require_string(args, field)?;
    if uuid::Uuid::parse_str(&raw).is_err() {
        return Err(ToolCallResult::error(format!(
            "{field} must be a UUID; got {raw:?}. Use list_documents (or the id returned by a prior call) — paths and slugs are not accepted here."
        )));
    }
    Ok(raw)
}

/// Like `optional_string`, but if present the value must parse as a UUID.
fn optional_uuid(args: &Value, field: &str) -> Result<Option<String>, ToolCallResult> {
    match optional_string(args, field)? {
        None => Ok(None),
        Some(s) => {
            if uuid::Uuid::parse_str(&s).is_err() {
                return Err(ToolCallResult::error(format!(
                    "{field} must be a UUID; got {s:?}"
                )));
            }
            Ok(Some(s))
        }
    }
}

/// Extract an optional string field from a JSON object.
fn optional_string(args: &Value, field: &str) -> Result<Option<String>, ToolCallResult> {
    match args.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let s = v
                .as_str()
                .ok_or_else(|| ToolCallResult::error(format!("{field} must be a string")))?;
            Ok(Some(s.to_owned()))
        }
    }
}

/// Extract an optional `u32` field from a JSON object. Rejects negatives
/// and values that overflow `u32`.
fn optional_u32(args: &Value, field: &str) -> Result<Option<u32>, ToolCallResult> {
    match args.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let n = v.as_u64().ok_or_else(|| {
                ToolCallResult::error(format!("{field} must be a non-negative integer"))
            })?;
            u32::try_from(n)
                .map(Some)
                .map_err(|_| ToolCallResult::error(format!("{field} exceeds u32 range")))
        }
    }
}

/// Extract an optional `i64` field from a JSON object.
fn optional_i64(args: &Value, field: &str) -> Result<Option<i64>, ToolCallResult> {
    match args.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .map(Some)
            .ok_or_else(|| ToolCallResult::error(format!("{field} must be a signed integer"))),
    }
}

/// Extract an optional usize field from a JSON object.
fn optional_usize(args: &Value, field: &str) -> Result<Option<usize>, ToolCallResult> {
    match args.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let n = v.as_u64().ok_or_else(|| {
                ToolCallResult::error(format!("{field} must be a non-negative integer"))
            })?;
            #[allow(clippy::cast_possible_truncation)]
            Ok(Some(n as usize))
        }
    }
}

/// Fix double-escaped newlines from LLMs that send literal `\n` instead of
/// actual newlines in document content. Only applies when the content contains
/// zero real newlines but has `\n` sequences — a reliable signal that the
/// entire string was double-escaped. Content with any real newlines is left
/// untouched to avoid mangling intentional escapes (e.g. code examples).
fn unescape_llm_newlines(s: &str) -> String {
    if !s.contains('\n') && s.contains("\\n") {
        s.replace("\\n", "\n").replace("\\t", "\t")
    } else {
        s.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_instructions_provider_returns_oss_template() {
        let p = DefaultInstructionsProvider;
        let s = p.instructions();
        // Header from the rewritten OSS template.
        assert!(s.contains("# Working with kutl through this relay"));
        // OSS-flavor default-visibility assumption — the load-bearing
        // sentence agents need to read.
        assert!(s.contains("Signals here reach MCP-connected agents only"));
        // Identifier-shape section landed (new in RFD 0083 Phase 5).
        assert!(s.contains("Identifier shapes"));
        // Audience-on-a-flag section landed (RFD 0083 Phase 5).
        assert!(s.contains("Audience on a flag"));
    }

    #[test]
    fn test_default_instructions_includes_kfm_spec() {
        // Concatenation order: cloud-mode template, separator, KFM spec.
        let p = DefaultInstructionsProvider;
        let s = p.instructions();
        // KFM heading from oss/docs/kutl-markdown.md.
        assert!(s.contains("# kutl-flavored markdown (KFM)"));
        // Cheat-sheet content as a sanity check that the full file
        // is embedded, not just the header.
        assert!(s.contains("FLAG_KIND_COMMENT"));
        // Cloud-mode template still comes first.
        let template_idx = s
            .find("# Working with kutl through this relay")
            .expect("cloud-mode header present");
        let kfm_idx = s
            .find("# kutl-flavored markdown (KFM)")
            .expect("KFM header present");
        assert!(template_idx < kfm_idx, "template must precede KFM section");
    }

    #[test]
    fn test_kfm_spec_constant_matches_embedded_file() {
        // The KFM_SPEC constant is the byte-identical embedding of
        // `oss/docs/kutl-markdown.md`. Other crates compare against
        // this constant to confirm dual-embedding shipped from the
        // same source. Confirm it's non-empty and starts with the
        // expected header so any accidental path change surfaces here.
        assert!(KFM_SPEC.starts_with("# kutl-flavored markdown (KFM)"));
        assert!(KFM_SPEC.len() > 1_000, "KFM spec unexpectedly small");
    }

    #[test]
    fn test_oss_instructions_concat_hygiene() {
        // The `concat!` over `instructions_template.md` +
        // `instructions_universal.md` + `instructions_template_tail.md`
        // + KFM is the right way to compose this, but it's a common
        // place for a stray newline or missing separator to slip in.
        // Snapshot a few structural properties:
        //   - The cloud-mode header is the first heading and KFM is the
        //     last (concat order, byte-level).
        //   - Every universal section heading appears exactly once.
        //   - No triple-newline runs (the `\n\n` joiners shouldn't
        //     accidentally stack with file-trailing newlines).
        let s = OSS_INSTRUCTIONS;
        let oss_header_idx = s
            .find("# Working with kutl through this relay")
            .expect("OSS top header present");
        let kfm_header_idx = s
            .find("# kutl-flavored markdown (KFM)")
            .expect("KFM header present");
        assert!(
            oss_header_idx < kfm_header_idx,
            "OSS template header must precede KFM section"
        );

        // Each universal-section heading appears exactly once (between
        // top and tail). Catches accidental duplication on edit.
        for heading in [
            "## Path conventions for documents",
            "## Markdown dialect — KFM",
            "## Reach mechanisms — doc body vs signal stream",
            "## Audience on a flag",
            "## Provenance fields",
            "## Team conventions",
            "## Provide full content, not patches",
        ] {
            let count = s.matches(heading).count();
            assert_eq!(
                count, 1,
                "expected universal heading {heading:?} exactly once, found {count}",
            );
        }

        // Triple-newline catches a stack-up between a file's trailing
        // `\n` and the `\n\n` joiner.
        assert!(
            !s.contains("\n\n\n\n"),
            "OSS_INSTRUCTIONS contains a quadruple-newline run — likely a \
             concat!() separator stacking with a file-trailing newline"
        );
    }

    /// Stable test UUIDs. Real UUIDs because `parse_create_flag`,
    /// `parse_create_reply`, etc. now require the relevant fields to
    /// parse as UUID — the prior `"d1"`, `"sig-1"` literals would be
    /// rejected before the assertion they're testing ever runs.
    const TEST_DOC_ID: &str = "11111111-1111-4111-8111-111111111111";
    const TEST_SIGNAL_ID: &str = "22222222-2222-4222-8222-222222222222";
    const TEST_SIGNAL_ID_2: &str = "22222222-2222-4222-8222-222222222223";
    const TEST_SIGNAL_ID_3: &str = "22222222-2222-4222-8222-222222222224";
    const TEST_REPLY_ID: &str = "33333333-3333-4333-8333-333333333333";

    #[test]
    fn test_tool_definitions_count() {
        let defs = tool_definitions();
        assert_eq!(defs.len(), 14);

        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"read_document"));
        assert!(names.contains(&"list_documents"));
        assert!(names.contains(&"list_spaces"));
        assert!(names.contains(&"read_log"));
        assert!(names.contains(&"list_participants"));
        assert!(names.contains(&"status"));
        assert!(names.contains(&"create_document"));
        assert!(names.contains(&"edit_document"));
        assert!(names.contains(&"upload_blob"));
        assert!(names.contains(&"get_changes"));
        assert!(names.contains(&"create_flag"));
        assert!(names.contains(&"create_reply"));
        assert!(names.contains(&"close_flag"));
        assert!(names.contains(&"get_signal_detail"));
    }

    #[test]
    fn test_signal_tools_with_provider() {
        struct TestProvider;
        impl McpToolProvider for TestProvider {
            fn extra_tools(&self) -> Vec<ToolDefinition> {
                vec![ToolDefinition {
                    name: "extra_tool".into(),
                    description: "test".into(),
                    input_schema: serde_json::json!({"type": "object", "properties": {}}),
                }]
            }
        }
        let tools = signal_tools(&TestProvider);
        assert_eq!(tools.len(), 5);
        assert_eq!(tools[4].name, "extra_tool");
    }

    #[test]
    fn test_tool_definitions_have_schemas() {
        for def in tool_definitions() {
            assert_eq!(
                def.input_schema["type"], "object",
                "tool {} missing type",
                def.name
            );
            assert!(
                def.input_schema.get("properties").is_some(),
                "tool {} missing properties",
                def.name
            );
        }
    }

    #[test]
    fn test_parse_tool_call_read_document() {
        let args = serde_json::json!({"space_id": "p1", "document_id": "d1"});
        let parsed = parse_tool_call("read_document", &args).unwrap();
        assert!(
            matches!(parsed, ParsedToolCall::ReadDocument { space_id, document_id } if space_id == "p1" && document_id == "d1")
        );
    }

    #[test]
    fn test_parse_tool_call_missing_field() {
        let args = serde_json::json!({"space_id": "p1"});
        let err = parse_tool_call("read_document", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("document_id"));
    }

    #[test]
    fn test_parse_tool_call_unknown_tool() {
        let args = serde_json::json!({});
        let err = parse_tool_call("nonexistent", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("unknown tool"));
    }

    #[test]
    fn test_parse_tool_call_edit_document() {
        let args = serde_json::json!({
            "space_id": "p1",
            "document_id": "d1",
            "content": "new text",
            "intent": "update content"
        });
        let parsed = parse_tool_call("edit_document", &args).unwrap();
        assert!(
            matches!(parsed, ParsedToolCall::EditDocument { content, intent, snippet, .. } if content == "new text" && intent == "update content" && snippet.is_empty())
        );
    }

    #[test]
    fn test_parse_tool_call_edit_document_unescapes_newlines() {
        let args = serde_json::json!({
            "space_id": "p1",
            "document_id": "d1",
            "content": "# Title\\n\\nBody text\\n- item",
            "intent": "create doc"
        });
        let parsed = parse_tool_call("edit_document", &args).unwrap();
        match parsed {
            ParsedToolCall::EditDocument { content, .. } => {
                assert_eq!(content, "# Title\n\nBody text\n- item");
            }
            _ => panic!("expected EditDocument"),
        }
    }

    #[test]
    fn test_parse_tool_call_edit_document_preserves_intentional_escapes() {
        let args = serde_json::json!({
            "space_id": "p1",
            "document_id": "d1",
            "content": "# Example\n\n```\nprint(\"hello\\nworld\")\n```",
            "intent": "add code example"
        });
        let parsed = parse_tool_call("edit_document", &args).unwrap();
        match parsed {
            ParsedToolCall::EditDocument { content, .. } => {
                assert!(
                    content.contains("\\n"),
                    "should preserve literal \\n in code"
                );
                assert!(content.contains('\n'), "should have real newlines");
            }
            _ => panic!("expected EditDocument"),
        }
    }

    #[test]
    fn test_parse_tool_call_edit_document_with_snippet() {
        let args = serde_json::json!({
            "space_id": "p1",
            "document_id": "d1",
            "content": "new text",
            "intent": "update content",
            "snippet": "replaced header"
        });
        let parsed = parse_tool_call("edit_document", &args).unwrap();
        assert!(
            matches!(parsed, ParsedToolCall::EditDocument { snippet, .. } if snippet == "replaced header")
        );
    }

    #[test]
    fn test_parse_tool_call_read_log_with_limit() {
        let args = serde_json::json!({"space_id": "p1", "document_id": "d1", "limit": 10});
        let parsed = parse_tool_call("read_log", &args).unwrap();
        assert!(matches!(
            parsed,
            ParsedToolCall::ReadLog {
                limit: Some(10),
                ..
            }
        ));
    }

    #[test]
    fn test_parse_tool_call_read_log_without_limit() {
        let args = serde_json::json!({"space_id": "p1", "document_id": "d1"});
        let parsed = parse_tool_call("read_log", &args).unwrap();
        assert!(matches!(
            parsed,
            ParsedToolCall::ReadLog { limit: None, .. }
        ));
    }

    #[test]
    fn test_parse_tool_call_create_flag_space() {
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": TEST_DOC_ID,
            "kind": "review_requested",
            "message": "please review",
            "audience": "space"
        });
        let parsed = parse_tool_call("create_flag", &args).unwrap();
        assert!(matches!(
            parsed,
            ParsedToolCall::CreateFlag {
                kind: 3,
                audience: 2,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_tool_call_create_flag_participant() {
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": TEST_DOC_ID,
            "kind": "info",
            "message": "heads up",
            "audience": "participant",
            "target_did": "did:key:abc123"
        });
        let parsed = parse_tool_call("create_flag", &args).unwrap();
        assert!(matches!(
            parsed,
            ParsedToolCall::CreateFlag {
                kind: 1,
                audience: 1,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_tool_call_create_flag_participant_missing_target() {
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": TEST_DOC_ID,
            "kind": "info",
            "message": "heads up",
            "audience": "participant"
        });
        let err = parse_tool_call("create_flag", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("target_did"));
    }

    #[test]
    fn test_parse_tool_call_create_flag_space_with_target_did_rejected() {
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": TEST_DOC_ID,
            "kind": "info",
            "message": "broadcast",
            "audience": "space",
            "target_did": "did:key:abc123"
        });
        let err = parse_tool_call("create_flag", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("target_did must be empty"));
    }

    #[test]
    fn test_parse_tool_call_create_flag_invalid_kind() {
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": TEST_DOC_ID,
            "kind": "urgent",
            "message": "help",
            "audience": "space"
        });
        let err = parse_tool_call("create_flag", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("invalid kind"));
    }

    #[test]
    fn test_parse_tool_call_create_flag_path_string_rejected() {
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": "daily-digest.md",
            "kind": "info",
            "message": "look",
            "audience": "space"
        });
        let err = parse_tool_call("create_flag", &args).unwrap_err();
        assert!(err.is_error);
        let text = &err.content[0].text;
        assert!(text.contains("document_id must be a UUID"), "got: {text}");
        assert!(text.contains("daily-digest.md"), "got: {text}");
    }

    #[test]
    fn test_parse_tool_call_create_flag_comment_ok() {
        // kind = comment with both signal_id and anchor_text supplied — the
        // RFD 0077 marker↔signal binding precondition is satisfied.
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": TEST_DOC_ID,
            "kind": "comment",
            "message": "this needs a reword",
            "audience": "space",
            "signal_id": TEST_SIGNAL_ID,
            "anchor_text": "the old phrasing",
        });
        let parsed = parse_tool_call("create_flag", &args).unwrap();
        match parsed {
            ParsedToolCall::CreateFlag {
                kind,
                signal_id,
                anchor_text,
                ..
            } => {
                // FLAG_KIND_COMMENT = 6 per the proto.
                assert_eq!(kind, 6);
                assert_eq!(signal_id.as_deref(), Some(TEST_SIGNAL_ID));
                assert_eq!(anchor_text.as_deref(), Some("the old phrasing"));
            }
            _ => panic!("expected CreateFlag"),
        }
    }

    #[test]
    fn test_parse_tool_call_create_flag_comment_missing_signal_id() {
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": TEST_DOC_ID,
            "kind": "comment",
            "message": "nit",
            "audience": "space",
            "anchor_text": "wrapped span",
        });
        let err = parse_tool_call("create_flag", &args).unwrap_err();
        assert!(err.is_error);
        assert!(
            err.content[0].text.contains("signal_id is required"),
            "got: {}",
            err.content[0].text
        );
    }

    #[test]
    fn test_parse_tool_call_create_flag_comment_missing_anchor_text() {
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": TEST_DOC_ID,
            "kind": "comment",
            "message": "nit",
            "audience": "space",
            "signal_id": TEST_SIGNAL_ID,
        });
        let err = parse_tool_call("create_flag", &args).unwrap_err();
        assert!(err.is_error);
        assert!(
            err.content[0].text.contains("anchor_text is required"),
            "got: {}",
            err.content[0].text
        );
    }

    #[test]
    fn test_parse_tool_call_create_flag_comment_empty_anchor_text_rejected() {
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": TEST_DOC_ID,
            "kind": "comment",
            "message": "nit",
            "audience": "space",
            "signal_id": TEST_SIGNAL_ID,
            "anchor_text": "",
        });
        let err = parse_tool_call("create_flag", &args).unwrap_err();
        assert!(err.is_error);
        assert!(
            err.content[0].text.contains("anchor_text is required"),
            "got: {}",
            err.content[0].text
        );
    }

    #[test]
    fn test_parse_tool_call_create_flag_info_with_comment_params_silently_ignored() {
        // Per the lean: non-comment kinds tolerate stray signal_id /
        // anchor_text params (more forgiving). The parser drops them so
        // the relay never persists a fake anchor on an info flag.
        let args = serde_json::json!({
            "space_id": "s1",
            "document_id": TEST_DOC_ID,
            "kind": "info",
            "message": "fyi",
            "audience": "space",
            "signal_id": TEST_SIGNAL_ID,
            "anchor_text": "should be dropped",
        });
        let parsed = parse_tool_call("create_flag", &args).unwrap();
        match parsed {
            ParsedToolCall::CreateFlag {
                kind,
                signal_id,
                anchor_text,
                ..
            } => {
                // FLAG_KIND_INFO = 1.
                assert_eq!(kind, 1);
                assert!(signal_id.is_none(), "comment-only field must be dropped");
                assert!(anchor_text.is_none(), "comment-only field must be dropped");
            }
            _ => panic!("expected CreateFlag"),
        }
    }

    #[test]
    fn test_parse_tool_call_create_reply_basic() {
        let args = serde_json::json!({
            "space_id": "s1",
            "parent_signal_id": TEST_SIGNAL_ID,
            "body": "looks good to me"
        });
        let parsed = parse_tool_call("create_reply", &args).unwrap();
        match parsed {
            ParsedToolCall::CreateReply {
                space_id,
                parent_signal_id,
                parent_reply_id,
                body,
            } => {
                assert_eq!(space_id, "s1");
                assert_eq!(parent_signal_id, TEST_SIGNAL_ID);
                assert!(parent_reply_id.is_none());
                assert_eq!(body, "looks good to me");
            }
            _ => panic!("expected CreateReply"),
        }
    }

    #[test]
    fn test_parse_tool_call_create_reply_with_parent() {
        let args = serde_json::json!({
            "space_id": "s1",
            "parent_signal_id": TEST_SIGNAL_ID,
            "parent_reply_id": TEST_REPLY_ID,
            "body": "following up on that"
        });
        let parsed = parse_tool_call("create_reply", &args).unwrap();
        assert!(matches!(
            parsed,
            ParsedToolCall::CreateReply {
                parent_reply_id: Some(ref p),
                ..
            } if p == TEST_REPLY_ID
        ));
    }

    #[test]
    fn test_parse_tool_call_create_reply_missing_body() {
        let args = serde_json::json!({
            "space_id": "s1",
            "parent_signal_id": TEST_SIGNAL_ID
        });
        let err = parse_tool_call("create_reply", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("body"));
    }

    #[test]
    fn test_parse_tool_call_close_flag_resolved() {
        let args = serde_json::json!({
            "space_id": "s1",
            "signal_id": TEST_SIGNAL_ID,
            "reason": "resolved",
            "close_note": "all done"
        });
        let parsed = parse_tool_call("close_flag", &args).unwrap();
        match parsed {
            ParsedToolCall::CloseFlag {
                space_id,
                signal_id,
                reason,
                close_note,
            } => {
                assert_eq!(space_id, "s1");
                assert_eq!(signal_id, TEST_SIGNAL_ID);
                assert_eq!(reason.as_deref(), Some("resolved"));
                assert_eq!(close_note.as_deref(), Some("all done"));
            }
            _ => panic!("expected CloseFlag"),
        }
    }

    #[test]
    fn test_parse_tool_call_close_flag_declined_no_note() {
        let args = serde_json::json!({
            "space_id": "s1",
            "signal_id": TEST_SIGNAL_ID_2,
            "reason": "declined"
        });
        let parsed = parse_tool_call("close_flag", &args).unwrap();
        assert!(matches!(
            parsed,
            ParsedToolCall::CloseFlag {
                close_note: None,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_tool_call_close_flag_no_reason_defaults() {
        let args = serde_json::json!({
            "space_id": "s1",
            "signal_id": TEST_SIGNAL_ID_3
        });
        let parsed = parse_tool_call("close_flag", &args).unwrap();
        assert!(matches!(
            parsed,
            ParsedToolCall::CloseFlag { reason: None, .. }
        ));
    }

    #[test]
    fn test_parse_tool_call_close_flag_invalid_reason() {
        let args = serde_json::json!({
            "space_id": "s1",
            "signal_id": TEST_SIGNAL_ID,
            "reason": "ignored"
        });
        let err = parse_tool_call("close_flag", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("invalid reason"));
    }

    #[test]
    fn test_parse_tool_call_get_signal_detail() {
        let args = serde_json::json!({
            "space_id": "s1",
            "signal_id": TEST_SIGNAL_ID
        });
        let parsed = parse_tool_call("get_signal_detail", &args).unwrap();
        match parsed {
            ParsedToolCall::GetSignalDetail {
                space_id,
                signal_id,
            } => {
                assert_eq!(space_id, "s1");
                assert_eq!(signal_id, TEST_SIGNAL_ID);
            }
            _ => panic!("expected GetSignalDetail"),
        }
    }

    #[test]
    fn test_parse_tool_call_get_signal_detail_missing_id() {
        let args = serde_json::json!({"space_id": "s1"});
        let err = parse_tool_call("get_signal_detail", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("signal_id"));
    }

    #[test]
    fn test_parse_tool_call_react_to_signal() {
        let args = serde_json::json!({
            "space_id": "s1",
            "signal_id": TEST_SIGNAL_ID,
            "emoji": "\u{1f44d}",
            "remove": true
        });
        let parsed = parse_tool_call("react_to_signal", &args).unwrap();
        match parsed {
            ParsedToolCall::ReactToSignal { emoji, remove, .. } => {
                assert_eq!(emoji, "\u{1f44d}");
                assert!(remove);
            }
            _ => panic!("expected ReactToSignal"),
        }
    }

    #[test]
    fn test_parse_tool_call_create_document_basic() {
        let args = serde_json::json!({
            "space_id": "s1",
            "path": "handbook/onboarding.md",
            "content": "hello"
        });
        let parsed = parse_tool_call("create_document", &args).unwrap();
        match parsed {
            ParsedToolCall::CreateDocument {
                space_id,
                path,
                content,
                provenance,
            } => {
                assert_eq!(space_id, "s1");
                assert_eq!(path, "handbook/onboarding.md");
                assert_eq!(content, "hello");
                assert!(provenance.source_kind.is_none());
            }
            _ => panic!("expected CreateDocument"),
        }
    }

    #[test]
    fn test_parse_tool_call_create_document_with_provenance() {
        let args = serde_json::json!({
            "space_id": "s1",
            "path": "imports/page.md",
            "content": "body",
            "source_kind": 1,
            "source_id": "page-123",
            "source_url": "https://notion.so/page-123",
            "source_author_display": "Jane Doe",
            "originally_created_at": 1_700_000_000_000_i64,
            "ingestion_job_id": "33333333-3333-4333-8333-333333333333"
        });
        let parsed = parse_tool_call("create_document", &args).unwrap();
        match parsed {
            ParsedToolCall::CreateDocument { provenance, .. } => {
                assert_eq!(provenance.source_kind, Some(1));
                assert_eq!(provenance.source_id.as_deref(), Some("page-123"));
                assert_eq!(
                    provenance.source_url.as_deref(),
                    Some("https://notion.so/page-123")
                );
                assert_eq!(
                    provenance.source_author_display.as_deref(),
                    Some("Jane Doe")
                );
                assert_eq!(provenance.originally_created_at_ms, Some(1_700_000_000_000));
                assert_eq!(
                    provenance.ingestion_job_id.as_deref(),
                    Some("33333333-3333-4333-8333-333333333333")
                );
            }
            _ => panic!("expected CreateDocument"),
        }
    }

    #[test]
    fn test_parse_tool_call_create_document_rejects_leading_slash() {
        let args = serde_json::json!({
            "space_id": "s1",
            "path": "/handbook/onboarding.md",
            "content": "x"
        });
        let err = parse_tool_call("create_document", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("must not start with"));
    }

    #[test]
    fn test_parse_tool_call_upload_blob_basic() {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode([1_u8, 2, 3, 4]);
        let args = serde_json::json!({
            "space_id": "s1",
            "path": "docimages/x.png",
            "content_type": "image/png",
            "bytes": encoded
        });
        let parsed = parse_tool_call("upload_blob", &args).unwrap();
        match parsed {
            ParsedToolCall::UploadBlob {
                space_id,
                path,
                content_type,
                bytes,
                ..
            } => {
                assert_eq!(space_id, "s1");
                assert_eq!(path, "docimages/x.png");
                assert_eq!(content_type, "image/png");
                assert_eq!(bytes, vec![1, 2, 3, 4]);
            }
            _ => panic!("expected UploadBlob"),
        }
    }

    #[test]
    fn test_parse_tool_call_upload_blob_invalid_base64() {
        let args = serde_json::json!({
            "space_id": "s1",
            "path": "docimages/x.png",
            "content_type": "image/png",
            "bytes": "!!!not-base64!!!"
        });
        let err = parse_tool_call("upload_blob", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("base64"));
    }

    #[test]
    fn test_parse_tool_call_list_spaces() {
        let args = serde_json::json!({});
        let parsed = parse_tool_call("list_spaces", &args).unwrap();
        assert!(matches!(parsed, ParsedToolCall::ListSpaces));
    }

    #[test]
    fn test_parse_tool_call_reopen_flag() {
        let args = serde_json::json!({
            "space_id": "s1",
            "signal_id": TEST_SIGNAL_ID
        });
        let parsed = parse_tool_call("reopen_flag", &args).unwrap();
        assert!(matches!(parsed, ParsedToolCall::ReopenFlag { .. }));
    }

    #[test]
    fn test_validate_within_space_path_accepts_well_formed() {
        for ok in [
            "doc.md",
            "subdir/doc.md",
            "deeply/nested/dir/file.md",
            "with-dashes_and_underscores.md",
            "unicode-é-segment.md",
        ] {
            assert!(
                validate_within_space_path(ok).is_ok(),
                "expected ok for {ok:?}"
            );
        }
    }

    #[test]
    fn test_validate_within_space_path_rejects_malformed() {
        let cases: &[(&str, &str)] = &[
            ("", "empty"),
            ("/leading-slash.md", "must not start with"),
            ("dir/../escape.md", "`..` components"),
            ("..", "`..` components"),
            ("ok/../bad", "`..` components"),
            ("bad\0path.md", "NUL bytes"),
            ("dir\\..\\..\\escape", "backslash"),
            ("dir\\sub.md", "backslash"),
        ];
        for (input, expected_substring) in cases {
            let err = validate_within_space_path(input).expect_err("must reject");
            let msg = &err.content[0].text;
            assert!(
                msg.contains(expected_substring),
                "for input {input:?} expected error containing {expected_substring:?}, got {msg:?}",
            );
        }
        // Length cap — cheaper to build inline than as a const.
        let long = "a/".repeat(600);
        let err = validate_within_space_path(&long).expect_err("must reject long path");
        assert!(err.content[0].text.contains("exceeds"));
    }
}
