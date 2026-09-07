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
/// guaranteeing both vehicles ship the byte-identical reference.
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
/// that require the UX layer (reactions).
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
        create_comment_tool(),
        create_reply_tool(),
        list_signals_tool(),
        close_flag_tool(),
        reopen_flag_tool(),
        get_signal_detail_tool(),
    ];
    apply_overrides(&mut tools, provider.override_tools());
    tools.extend(provider.extra_tools());
    tools
}

/// Return all MCP tool definitions with extra tools from a provider.
pub fn tool_definitions_with_provider(provider: &dyn McpToolProvider) -> Vec<ToolDefinition> {
    let mut defs = vec![
        read_document_tool(),
        list_documents_tool(),
        list_spaces_tool(),
        read_log_tool(),
        list_participants_tool(),
        resolve_participant_tool(),
        status_tool(),
        create_document_tool(),
        edit_document_tool(),
        upload_blob_tool(),
        get_changes_tool(),
    ];
    defs.extend(signal_tools(provider));
    defs
}

/// The set of tool NAMES `provider` advertises via [`tool_definitions_with_provider`]
/// — the single authority for which tools a relay will EXECUTE.
///
/// The `tools/call` handler rejects any name absent from this set, so the
/// executable surface is *definitionally* equal to the advertised (`tools/list`)
/// surface: both derive from [`tool_definitions_with_provider`] with the same
/// active provider, so adding a tool to a provider makes it advertised AND
/// executable in one edit — there is no second allowlist to maintain or
/// reconcile. A provider that does not advertise a tool (e.g. the OSS
/// [`NoopToolProvider`], which — unlike the kutlhub provider — never adds
/// `react_to_signal`) therefore cannot execute it, even when a client names it
/// directly in an out-of-band `tools/call`.
pub fn advertised_tool_names(provider: &dyn McpToolProvider) -> std::collections::HashSet<String> {
    tool_definitions_with_provider(provider)
        .into_iter()
        .map(|def| def.name)
        .collect()
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
                      envelope) and `version` is an opaque token naming the \
                      text you were just handed. Keep it: `edit_document` \
                      requires it back as `base_version` and rejects an edit \
                      that arrives without one.\n\n\
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
                      ...provenance? }` records — `subscriber_count` is the \
                      WebSocket subscriptions on that document (a daemon \
                      counts once per document it holds; presence in the \
                      space is `list_participants`), `content_type` is one of \
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

/// Resolve a name a human typed to the DID a record must carry.
fn resolve_participant_tool() -> ToolDefinition {
    ToolDefinition {
        name: "resolve_participant".into(),
        description: "Resolve a participant's name to their DID, for addressing a signal to \
                      them. Returns an array of `{ did, name }` — usually one entry.\n\n\
                      Answers over everyone authorized in the space, not just whoever is \
                      connected, so a name resolves while its owner is away. That is the point: \
                      a signal waits for the person it names.\n\n\
                      A lookup, not a listing — it confirms a name you already have and will \
                      not enumerate a space's members. An empty array means nobody here answers \
                      to that name, which is not the same as the name being wrong: a \
                      participant with no name configured is reachable by DID only.\n\n\
                      Matching is exact. More than one entry means the name is ambiguous — ask \
                      rather than picking, because guessing sends someone else's mail."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "name": {
                    "type": "string",
                    "description": "The participant's name, matched exactly."
                }
            },
            "required": ["space_id", "name"]
        }),
    }
}

fn list_participants_tool() -> ToolDefinition {
    ToolDefinition {
        name: "list_participants".into(),
        description: "Who can act in this space. Returns an array of \
                      `{ did, name, connection_type }` records, one per actor, \
                      DID-deduped.\n\n\
                      Everyone authorized here is listed whether or not they \
                      are present. `connection_type` says which: `\"websocket\"` \
                      (a connection listening to the space: a kutl daemon or a \
                      `kutl mcp serve` session), `\"mcp\"` (a live MCP session), \
                      or `\"offline\"`. \
                      Being offline is not a reason to skip someone — a signal \
                      waits for the person it names, so addressing an absent \
                      participant is the normal case, not a mistake.\n\n\
                      `name` is what people call that DID, and is null when the \
                      relay knows no name for them — they are still reachable by \
                      DID. To go the other way, from a name someone typed to the \
                      DID a record carries, use `resolve_participant`.\n\n\
                      Use this to discover the `target_did` value for \
                      `create_flag(audience: 'participant', target_did: ...)` — \
                      the natural next action when you want to direct a flag at \
                      a specific human or agent."
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
                      `{ document_count, listener_count, mcp_session_count }`:\n\
                      - `document_count` — loaded documents in the space \
                        (registry-driven; reflects what `list_documents` \
                        would return).\n\
                      - `listener_count` — connections listening to the space \
                        right now (the parties present over websocket: daemons \
                        and `kutl mcp serve` sessions), one per connection.\n\
                      - `mcp_session_count` — MCP sessions on the relay \
                        process. Note: this is process-global, not \
                        space-scoped, because MCP sessions today are \
                        space-agnostic.\n\n\
                      Use this for a quick health/scale sanity check before \
                      heavier operations (e.g. is the space empty? is anyone \
                      else here to see what I'm about to do? — note that a \
                      `kutl mcp serve` session of your own counts as one \
                      listener). For the per-DID list, use \
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
        description: "Updates an EXISTING document by merging your changes into it.\n\n\
                      Pass `base_version` — the `version` your `read_document` \
                      of this document returned — along with the full content \
                      you want. The relay compares your content against the \
                      text you actually read and applies only what you \
                      changed, so whatever a participant added meanwhile \
                      survives.\n\n\
                      If a participant rewrote a region you also changed, or \
                      that region now sits in more than one place, the region \
                      is refused and named in `hunks_refused` while the rest \
                      of your edit lands. That is a success, not a failure: \
                      read the document again and reapply the refused region. \
                      Changes close together in the text travel as one region, \
                      so contesting one of them refuses both.\n\n\
                      The result carries no version, deliberately. To edit \
                      again, read again and use the `version` that read \
                      returns. Editing from content you did not read deletes \
                      what other participants wrote.\n\n\
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
                      `edited_at` timestamp. The `intent` you give is recorded on \
                      the change and is what `read_log` shows. Use `create_flag` \
                      when you need to draw specific attention to your edit or ask \
                      for input."
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
                "base_version": {
                    "type": "string",
                    "description": "The `version` string from your most recent read_document of this document. Pass it back unchanged; it tells the relay which text your edit was based on."
                },
                "content": {
                    "type": "string",
                    "description": "The whole document as you want it to read: the text you got from read_document with your changes made to it. Send all of it, not a diff, and do not compose it from anything you did not read. Use real newlines, not escaped \\n sequences."
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
            "required": ["space_id", "document_id", "base_version", "content", "intent"]
        }),
    }
}

fn create_document_tool() -> ToolDefinition {
    ToolDefinition {
        name: "create_document".into(),
        description: "Creates a new text document at the given path with initial \
                      content. Returns the assigned `document_id` (UUID). To \
                      update an existing document, read it with \
                      `read_document` and pass the `version` that read returns \
                      to `edit_document` as its `base_version`.\n\n\
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
                      relay's blob layer is MIME-agnostic by design \
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
        description: "What is new for you in a space since your last call. \
                      This is your inbox. Returns \
                      `{ signals, document_changes, checkpoint }`:\n\
                      - `signals` — the signals with activity since your \
                        last cursor: flags, replies, chats, decisions. Each \
                        carries `id`, `signal_type`, `author_did`, \
                        `document_id`, `timestamp`, and type-specific fields. \
                        For a flag worth investigating, pass its `id` to \
                        `get_signal_detail` to fetch the full reply thread + \
                        reactions. Closing, reopening, or editing a signal \
                        RE-SURFACES it here: the same `id` can appear again, \
                        carrying `event: CLOSED` and `close_reason` when it \
                        closed — treat entries as upserts by `id`, newest \
                        state wins.\n\
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
                        per-DID cursor automatically.\n\n\
                      Delivery is scoped to you, and only a flag carries an \
                      audience: a flag arrives here when it names you or the \
                      whole space. A flag naming a different participant does \
                      not, including one you sent yourself — your own \
                      space-wide flags come back, your own directed ones do \
                      not. Every other kind — replies, chats, decisions — \
                      addresses nobody and reaches everybody, so a thread \
                      stays legible to everyone who can see the signal it \
                      hangs off. There is no argument for asking on someone \
                      else's behalf: your authenticated identity is the \
                      filter.\n\n\
                      The cursor is per-DID and survives across sessions, so \
                      this reports only what arrived since you last looked. A \
                      signal opened before your cursor and still open will \
                      never appear in it — `list_signals` is how you ask what \
                      is currently open. Idempotent within a single agent's \
                      cursor; not idempotent across different agents (each \
                      has its own cursor).\n\n\
                      Keep calling it for as long as you work. Participants \
                      address you mid-task and nothing hands you what they \
                      said — this is the call that goes and gets it.\n\n\
                      Returns an empty response on deployments without a \
                      change backend. Do \
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
/// Extracted as a const so commercial deployments can replace just
/// this paragraph when overriding the tool
/// description. The conservative OSS framing assumes signals reach
/// MCP-connected agents only; kutlhub-relay overrides with "humans
/// will see the signals eventually" via the web app + email
/// notifications.
pub const CREATE_FLAG_VISIBILITY_DEFAULT_OSS: &str = "Visibility on this OSS relay: signals reach MCP-connected agents only — no signal-rendering \
     tool is bundled today. If you want humans in editors/file-sync to see anything, also write \
     into the document body (an inline callout, a header, the document content itself) via \
     `create_document`/`edit_document`.";

// ---------------------------------------------------------------------------
// Verb guidance shared with the CLI
//
// What each signal verb is FOR and when to reach for it — the half of a tool
// description that is not about how you invoke it. These name no tool, no
// argument and no flag, because `kutl signal <verb> --help` renders them
// verbatim beside its own invocation text. Anything that spells a caller's
// arguments belongs in that surface's own words, not here; what belongs here
// is the part an agent needs in order to decide to act at all.
// ---------------------------------------------------------------------------

/// Why an agent creates a flag, and what addressing one decides.
pub const CREATE_GUIDANCE: &str = "\
Create a flag when you need something from another participant: an answer you cannot derive, a \
review before you commit to an approach, or word that you are blocked. Editing a document \
changes the text; it does not ask anyone for anything. A flag is how a question gets in front \
of a person.

Addressing decides who owes you an answer. A flag aimed at one participant is theirs to \
answer. A space-wide flag is a broadcast — everyone sees it and nobody in particular owes a \
reply — so reach for it to inform rather than to ask.

Asking costs little and silence costs a lot. A choice you made alone because asking felt like \
an interruption is a choice nobody else agreed to, and the disagreement surfaces later, in \
the document, where it is more expensive.";

/// When to reply, and what a reply does not do.
pub const REPLY_GUIDANCE: &str = "\
Answer a flag you have been pulled into — a question put to you, or a review you were asked \
for — or extend a thread on a decision. Someone who addressed you and heard nothing cannot \
tell whether you disagreed or never looked.

A reply settles nothing by itself. When the question it answers is finished, close the signal \
as well.";

/// When to close a signal, and what closing is not.
pub const CLOSE_GUIDANCE: &str = "\
Close a signal you raised and have now finished, or one addressed to you that you have \
completed. The reason records how it ended, and everyone who reads the signal afterwards sees \
it.

Do NOT close to acknowledge something you have not settled — reply instead. Closing is not \
deleting: the signal and its thread stay readable, and a close can be undone.";

/// When to reopen a closed signal, and the misuse to avoid.
pub const REOPEN_GUIDANCE: &str = "\
Reopen when a signal was closed too early — new information arrived, the resolution did not \
hold, or the reason was wrong. Say why in a reply; a signal that reopens with no explanation \
reads as a mistake rather than a correction.

Do NOT reopen to get someone's attention again. Reopening corrects the record, it does not \
notify; raise a fresh signal for a new ask.";

/// Opening of the `create_flag` description, before the generated kind list.
const CREATE_FLAG_DESCRIPTION_HEAD: &str = "\
Create a flag signal (a structured, kind-tagged note) in a space. Flags carry intent — \
the kind says what *type* of attention this warrants, the message says what about, and the \
audience says who.

Kinds (use the one that matches the intent — picking the right kind is how downstream \
notification routing distinguishes informational chatter from things that block work):
";

/// Remainder of the `create_flag` description, after the generated kind list.
/// Concatenated with the visibility-default paragraph at construction time.
const CREATE_FLAG_DESCRIPTION_TAIL: &str = "
`comment` is NOT one of them — it has its own tool, `create_comment`. It is the one kind that \
requires a matching inline marker in the document body, so passing it here by accident would \
mint a comment anchored to nothing.

How to address one here:
- For a single recipient, use `audience: 'participant'` with `target_did` set to the human you \
  want to reach. Discover DIDs via `list_participants`.
- For everyone in the space, use `audience: 'space'`. Each recipient's notification preferences \
  control how they're reached (email, in-app, or nothing); you cannot override delivery.

To comment on a span of document text, use `create_comment` instead — it is the same record \
underneath, with the marker-binding arguments made mandatory rather than optional.

";

fn create_flag_tool() -> ToolDefinition {
    create_flag_tool_with_visibility(CREATE_FLAG_VISIBILITY_DEFAULT_OSS)
}

/// Compose `create_flag`'s tool definition with a caller-supplied
/// visibility-default paragraph. Used by extension hosts (kutlhub-relay)
/// to assert their own visibility default while reusing the rest of the
/// description verbatim.
pub fn create_flag_tool_with_visibility(visibility: &str) -> ToolDefinition {
    use std::fmt::Write as _;

    use kutl_proto::vocab::{AUTHORABLE_FLAG_KINDS, flag_kind_guidance, flag_kind_to_str};

    // Rendered from the shared vocabulary rather than spelled out, so a kind
    // reaches this description and the CLI's `--kind` help by the same edit.
    let kinds = AUTHORABLE_FLAG_KINDS
        .iter()
        .fold(String::new(), |mut acc, kind| {
            let _ = writeln!(
                acc,
                "- `{}` — {}.",
                flag_kind_to_str(i32::from(*kind)),
                flag_kind_guidance(*kind)
            );
            acc
        });
    let description = format!(
        "{CREATE_GUIDANCE}\n\n{CREATE_FLAG_DESCRIPTION_HEAD}{kinds}\
         {CREATE_FLAG_DESCRIPTION_TAIL}{visibility}"
    );
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
                    "enum": kutl_proto::vocab::flag_kind_names(AUTHORABLE_FLAG_KINDS),
                    "description": "The type of flag. See the description for one-line semantics per kind. For 'comment', use the create_comment tool."
                },
                "message": {
                    "type": "string",
                    "description": "Message body of the flag."
                },
                "audience": {
                    "type": "string",
                    "enum": ["space", "participant"],
                    "description": "Who should see this: 'space' for everyone, 'participant' for a specific person."
                },
                "target_did": {
                    "type": "string",
                    "description": "DID of the target person. Required when audience is 'participant'. Use list_participants to discover DIDs."
                }
            },
            "required": ["space_id", "document_id", "kind", "message", "audience"]
        }),
    }
}

/// Comment on a span of document text.
///
/// The same record `create_flag` mints — split out because the comment kind
/// alone requires a matching inline marker in the document body, and a caller
/// who reached it by passing a string would get a comment bound to nothing.
/// Making it a separate verb turns "did you mean to write a comment?" from a
/// runtime validation error into a question the caller answers by choosing a
/// tool.
fn create_comment_tool() -> ToolDefinition {
    ToolDefinition {
        name: "create_comment".into(),
        description: "Comment on a specific span of text in a document. Returns the \
                      comment's `signal_id` — which YOU supply, because the comment \
                      is bound to an inline marker in the document body.\n\n\
                      The flow, and all three steps are required:\n\
                      1. Mint a UUID client-side.\n\
                      2. Inject `[text]{.cmt #signal-uuid}` into the document via \
                      `edit_document`, wrapping the span you are commenting on.\n\
                      3. Call this tool with the SAME uuid as `signal_id`, the \
                      wrapped span as `anchor_text`, and your remark as `message`.\n\n\
                      The matching UUID is load-bearing: it is what ties the marker \
                      in the document to the comment record. `anchor_text` is a \
                      posterity snapshot — the live binding is the marker, \
                      so this is what the comment still shows after the span is \
                      edited away.\n\n\
                      Use this for a remark about a PARTICULAR passage. For anything \
                      about the document as a whole — a question, a review request, a \
                      blocker — use `create_flag`, which needs no marker."
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
                    "description": "Document containing the commented span. Must be a UUID."
                },
                "signal_id": {
                    "type": "string",
                    "description": "The UUID you embedded in the [text]{.cmt #signal-uuid} marker via edit_document. Must match exactly."
                },
                "anchor_text": {
                    "type": "string",
                    "description": "The wrapped doc span being commented on — a posterity snapshot."
                },
                "message": {
                    "type": "string",
                    "description": "The comment body."
                },
                "audience": {
                    "type": "string",
                    "enum": ["space", "participant"],
                    "description": "Who should see this: 'space' for everyone (the default for a comment), 'participant' for a specific person."
                },
                "target_did": {
                    "type": "string",
                    "description": "DID of the target person. Required when audience is 'participant'."
                }
            },
            "required": ["space_id", "document_id", "signal_id", "anchor_text", "message"]
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
     Pass `parent_reply_id` to thread under a specific earlier reply; omit it for top-level \
     replies under the parent signal.\n\n\
     ";

const CREATE_REPLY_DESCRIPTION_TAIL: &str = "\n\nDo NOT use to close a resolved flag — use `close_flag` (and `reopen_flag` to \
     undo).";

fn create_reply_tool() -> ToolDefinition {
    create_reply_tool_with_visibility(CREATE_REPLY_VISIBILITY_DEFAULT_OSS)
}

/// Compose `create_reply`'s tool definition with a caller-supplied
/// visibility-default paragraph.
pub fn create_reply_tool_with_visibility(visibility: &str) -> ToolDefinition {
    let description = format!(
        "{REPLY_GUIDANCE}\n\n{CREATE_REPLY_DESCRIPTION_BODY}{visibility}\
         {CREATE_REPLY_DESCRIPTION_TAIL}"
    );
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
     Decisions are document edits: closing a decision signal (a `## ? …` heading) flips its \
     heading to `## = …` in the document as YOUR edit, and the close record follows from that \
     edit. Only `resolved` applies — remove the heading to withdraw a decision — and \
     `close_note` lands as body text under the heading.\n\n\
     ";

const CLOSE_FLAG_DESCRIPTION_TAIL: &str = "\n\nThe inverse is `reopen_flag`. To acknowledge without settling, use \
     `react_to_signal` (commercial) or a reply via `create_reply`.";

/// Close a flag signal with a resolution.
fn close_flag_tool() -> ToolDefinition {
    close_flag_tool_with_visibility(CLOSE_FLAG_VISIBILITY_DEFAULT_OSS)
}

/// Compose `close_flag`'s tool definition with a caller-supplied
/// visibility-default paragraph.
pub fn close_flag_tool_with_visibility(visibility: &str) -> ToolDefinition {
    let description = format!(
        "{CLOSE_GUIDANCE}\n\n{CLOSE_FLAG_DESCRIPTION_BODY}{visibility}\
         {CLOSE_FLAG_DESCRIPTION_TAIL}"
    );
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

/// Reopen a previously closed flag signal — the inverse of `close_flag`.
fn reopen_flag_tool() -> ToolDefinition {
    ToolDefinition {
        name: "reopen_flag".into(),
        description: format!(
            "{REOPEN_GUIDANCE}\n\n\
             Reopen a previously-closed flag — the inverse of `close_flag`. Returns an empty \
             success result; the signal's `closed_at` is cleared and a REOPENED event surfaces \
             on the next `get_changes`, returning the flag to the active flag stream.\n\n\
             Decisions are document edits: reopening a decision signal flips its `## = …` \
             heading back to `## ? …` in the document as YOUR edit, and the reopen record \
             follows from that edit.\n\n\
             The explanatory reply goes through `create_reply`; a fresh ask goes through \
             `create_flag` with its own kind and audience."
        ),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "signal_id": {
                    "type": "string",
                    "description": "ID of the closed flag signal to reopen."
                }
            },
            "required": ["space_id", "signal_id"]
        }),
    }
}

/// List a space's signals with the same filters `kutl signal list` offers.
fn list_signals_tool() -> ToolDefinition {
    ToolDefinition {
        name: "list_signals".into(),
        description: "What is open in this space right now. Returns an array \
                      of summaries — `{ id, document_id, kind, message, \
                      flag_kind, audience, target_did, status, created_ms, \
                      closed_ms, author_did }` — in a stable order, with no \
                      nested replies or reactions (use `get_signal_detail` \
                      for one signal's full thread).\n\n\
                      A state query over the whole space, and explicitly NOT \
                      an inbox: it reports every signal matching the filters \
                      regardless of who it addresses or whether you have seen \
                      it before. `audience` and `target_did` are what say who \
                      each one is for — `space` is a broadcast, `participant` \
                      names one DID in `target_did`. For what has arrived FOR \
                      YOU since you last looked, call `get_changes`.\n\n\
                      Defaults to OPEN signals only: what still needs \
                      someone. Pass `status: 'all'` for history. Tombstoned \
                      signals are never returned.\n\n\
                      Two independent kind axes, and mixing them up is the \
                      common mistake. `kind` is the RECORD type — `flag`, \
                      `chat`, `decision`, `reply`. `flag_kind` is a flag's \
                      INTENT — `info`, `question`, `blocked`, … — so setting \
                      it implies `kind: 'flag'`.\n\n\
                      Use this to survey a space before acting — e.g. \
                      `flag_kind: 'question'` to find what is waiting on an \
                      answer, or `document_id` to see everything raised \
                      against one document. It is also the way to find a \
                      signal that predates your `get_changes` cursor and has \
                      been quiet since: the change feed re-serves a signal \
                      only when something happens to it, so one opened before \
                      your cursor and never touched again will not appear \
                      there."
            .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "status": {
                    "type": "string",
                    "enum": ["open", "closed", "all"],
                    "description": "Which lifecycle states to include. Defaults to 'open'."
                },
                "kind": {
                    "type": "string",
                    "enum": ["flag", "chat", "decision", "reply"],
                    "description": "Restrict to one record type."
                },
                "document_id": {
                    "type": "string",
                    "description": "Restrict to signals attached to this document (a UUID)."
                },
                "flag_kind": {
                    "type": "string",
                    "enum": kutl_proto::vocab::flag_kind_names(kutl_proto::vocab::FLAG_KINDS),
                    "description": "Restrict to flags of this intent kind. Implies kind='flag'."
                }
            },
            "required": ["space_id"]
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
                      parent pointers for replies, `body`) plus three nested \
                      arrays: `replies[]` (flat list of \
                      `{ id, parent_reply_id, author_did, body, created_at }`), \
                      `reactions[]` (list of \
                      `{ actor_did, emoji, created_at }`), and \
                      `transitions[]` — the lifecycle audit trail, oldest \
                      first, of `{ record_id, event, timestamp_ms, actor_did, \
                      close_reason, note }`. `event` is \
                      `created`/`closed`/`reopened`/`tombstoned`.\n\n\
                      The trail shows EVERY transition, not just the one that \
                      currently holds — so you can see that a flag was closed \
                      and reopened rather than only that it is open now, and \
                      read the note whoever closed it left.\n\n\
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
                      Do NOT use to enumerate signals — `list_signals` \
                      reports what is open in the space, and `get_changes` \
                      what has arrived for you."
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
    /// `SourceKind` enum value (proto uint32). 0 = native.
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
    ResolveParticipant {
        space_id: String,
        name: String,
    },
    Status {
        space_id: String,
    },
    EditDocument {
        space_id: String,
        document_id: String,
        /// The version token the caller's most recent read returned. Names the
        /// text the edit was composed against, so the relay can apply the
        /// caller's delta rather than its whole payload.
        base_version: String,
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
    /// — they carry the inline marker UUID and the wrapped span for posterity.
    /// For other kinds they are accepted-and-ignored at the
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
        /// When `None`, the relay closes as `"resolved"` — unconditionally,
        /// with no author check.
        reason: Option<String>,
        close_note: Option<String>,
    },
    /// List a space's signals, narrowed by the listing filters.
    ListSignals {
        space_id: String,
        status: String,
        kind: Option<String>,
        document_id: Option<String>,
        flag_kind: Option<String>,
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
        "resolve_participant" => {
            let space_id = require_string(arguments, "space_id")?;
            let name = require_string(arguments, "name")?;
            Ok(ParsedToolCall::ResolveParticipant { space_id, name })
        }
        "status" => {
            let space_id = require_string(arguments, "space_id")?;
            Ok(ParsedToolCall::Status { space_id })
        }
        "edit_document" => {
            let space_id = require_string(arguments, "space_id")?;
            let document_id = require_string(arguments, "document_id")?;
            let base_version = require_string(arguments, "base_version")?;
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
                base_version,
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
        "create_comment" => parse_create_comment(arguments),
        "list_signals" => parse_list_signals(arguments),
        "create_reply" => parse_create_reply(arguments),
        "close_flag" => parse_close_flag(arguments),
        "reopen_flag" => parse_reopen_flag(arguments),
        "get_signal_detail" => parse_get_signal_detail(arguments),
        // `react_to_signal` is extension-host-only: the OSS base does not
        // advertise it (only providers such as kutlhub add it via
        // `McpToolProvider`). Its parser lives here so the handler can dispatch
        // it where it IS advertised; the `tools/call` execution gate
        // (`advertised_tool_names`) keeps it from running where it is not.
        "react_to_signal" => parse_react_to_signal(arguments),
        _ => Err(ToolCallResult::error(format!("unknown tool: {name}"))),
    }
}

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
    crate::ids::check_within_space_path(path).map_err(ToolCallResult::error)
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
/// the relay's blob layer is MIME-agnostic by design. The
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

/// The audience/target pairing rule, enforced once for every MCP door that
/// accepts the pair.
///
/// A `participant` audience without a target addresses nobody; a `space`
/// audience WITH one is a broadcast carrying a specific recipient, which is
/// semantically malformed. Rejecting both surfaces the confusion as a clear
/// error rather than silent data duplication.
///
/// Extracted because `create_flag` and `create_comment` carried it verbatim,
/// error strings included — a comment IS a flag, so a divergence here would be
/// a second vocabulary for one record shape. This is the same rule
/// `authoring.rs::check_audience` enforces on the typed `Audience`; this copy
/// guards the untyped door, where the pair arrives as two loose arguments.
fn check_audience_pairing(audience: i32, target_did: &str) -> Result<(), ToolCallResult> {
    if audience == i32::from(sync::AudienceType::Participant) && target_did.is_empty() {
        return Err(ToolCallResult::error(
            "target_did is required when audience is 'participant'",
        ));
    }
    if audience == i32::from(sync::AudienceType::Space) && !target_did.is_empty() {
        return Err(ToolCallResult::error(
            "target_did must be empty when audience is 'space' (space audience is a broadcast); use audience 'participant' to target a specific user",
        ));
    }
    Ok(())
}

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
    check_audience_pairing(audience, &target_did)?;

    // `comment` is not one of this tool's kinds. It is the only kind that
    // needs a matching inline marker, so reaching it by passing a string would
    // mint a comment bound to nothing; `create_comment` takes the binding as
    // required arguments instead. Named explicitly rather than left to the
    // enum check so the error says where to go.
    if kind == i32::from(sync::FlagKind::Comment) {
        return Err(ToolCallResult::error(
            "kind 'comment' is created with the create_comment tool, which requires the signal_id and anchor_text that bind it to its inline marker",
        ));
    }
    // Other kinds carry no marker binding, so these two are not read at all.
    let (final_signal_id, final_anchor_text) = (None, None);

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

/// Parse `create_comment` arguments.
///
/// Produces the SAME [`ParsedToolCall::CreateFlag`] the comment kind always
/// produced — the split is at the tool surface, not in the record. What changes
/// is that `signal_id` and `anchor_text` are required by the schema rather than
/// conditionally validated after the fact, and `kind` is not a caller argument
/// at all: choosing this tool IS choosing the kind.
///
/// `audience` is optional here and defaults to `space`. A comment is a remark
/// on a passage, which is a public gesture by default; requiring the caller to
/// restate that on every call would be noise.
fn parse_create_comment(args: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    let space_id = require_string(args, "space_id")?;
    let document_id = require_uuid(args, "document_id")?;
    let message = require_string(args, "message")?;
    let signal_id = require_uuid(args, "signal_id")?;
    let anchor_text = require_string(args, "anchor_text")?;

    let audience = match optional_string(args, "audience")? {
        Some(a) => parse_audience_type(&a)?,
        None => i32::from(sync::AudienceType::Space),
    };
    let target_did = args
        .get("target_did")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    check_audience_pairing(audience, &target_did)?;

    Ok(ParsedToolCall::CreateFlag {
        space_id,
        document_id,
        kind: i32::from(sync::FlagKind::Comment),
        message,
        audience,
        target_did,
        signal_id: Some(signal_id),
        anchor_text: Some(anchor_text),
    })
}

/// Parse `list_signals` arguments.
///
/// Every filter is optional; `status` defaults to `open`. The VALUES are
/// re-parsed relay-side (`handle_mcp_list_signals`) against the shared
/// vocabularies rather than here, so the schema enum and the accepted set
/// cannot drift — this only shapes the call.
fn parse_list_signals(args: &Value) -> Result<ParsedToolCall, ToolCallResult> {
    let space_id = require_string(args, "space_id")?;
    let status = optional_string(args, "status")?.unwrap_or_else(|| "open".to_owned());
    let document_id = match optional_string(args, "document_id")? {
        Some(_) => Some(require_uuid(args, "document_id")?),
        None => None,
    };
    Ok(ParsedToolCall::ListSignals {
        space_id,
        status,
        kind: optional_string(args, "kind")?,
        document_id,
        flag_kind: optional_string(args, "flag_kind")?,
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
/// `reason` is optional: when omitted the relay closes as `"resolved"`,
/// unconditionally. When provided it must be one of `"resolved"`,
/// `"declined"`, or `"withdrawn"`.
///
/// An author-aware default (`withdrawn` when the caller authored the flag) is
/// deliberately NOT implemented: an author is if anything the MOST likely
/// person to close a flag they genuinely resolved — they raised the question
/// and read the answer — so the heuristic mislabels at least as often as it
/// helps, and `close_reason` is carried on an immutable record. The
/// agent-facing tool description promises `resolved`.
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

/// Parse `reopen_flag` arguments. Reopen appends a REOPENED record via the
/// same transition path as `close_flag`, so it works on any deployment.
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
///
/// Delegates to [`kutl_proto::vocab::flag_kind_from_str`].
fn parse_flag_kind(s: &str) -> Result<i32, ToolCallResult> {
    kutl_proto::vocab::flag_kind_from_str(s).ok_or_else(|| {
        ToolCallResult::error(format!(
            "invalid kind: must be one of {}",
            kutl_proto::vocab::flag_kind_names(kutl_proto::vocab::FLAG_KINDS).join(", ")
        ))
    })
}

/// Parse an audience type string to proto enum value.
///
/// Delegates to [`kutl_proto::vocab::audience_type_from_str`].
fn parse_audience_type(s: &str) -> Result<i32, ToolCallResult> {
    kutl_proto::vocab::authorable_audience_from_str(s).ok_or_else(|| {
        ToolCallResult::error(format!(
            "invalid audience: must be one of {}",
            kutl_proto::vocab::AUTHORABLE_AUDIENCES.join(", ")
        ))
    })
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
    crate::ids::check_uuid(field, &raw).map_err(|reason| {
        // The hint names no specific tool on purpose: this helper guards
        // `signal_id` and `parent_signal_id` as often as `document_id`, so any
        // tool-specific advice would be wrong for most of its callers.
        ToolCallResult::error(format!(
            "{reason}. Ids come from a prior call's response — paths and slugs are not accepted here."
        ))
    })?;
    Ok(raw)
}

/// Like `optional_string`, but if present the value must parse as a UUID.
fn optional_uuid(args: &Value, field: &str) -> Result<Option<String>, ToolCallResult> {
    match optional_string(args, field)? {
        None => Ok(None),
        Some(s) => {
            crate::ids::check_uuid(field, &s).map_err(ToolCallResult::error)?;
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

    /// `create_flag` teaches each kind's meaning from the shared vocabulary,
    /// so the CLI's `--kind` help and this description cannot drift apart.
    /// Fails the moment the description stops sharing.
    #[test]
    fn test_create_flag_description_carries_the_shared_guidance() {
        let description = create_flag_tool().description;
        for kind in kutl_proto::vocab::AUTHORABLE_FLAG_KINDS {
            let guidance = kutl_proto::vocab::flag_kind_guidance(*kind);
            assert!(
                description.contains(guidance),
                "{} must carry its shared meaning",
                kutl_proto::vocab::flag_kind_to_str(i32::from(*kind))
            );
        }
        // `comment` has its own tool; offering it here would mint a comment
        // anchored to nothing.
        assert!(
            !description.contains("- `comment` —"),
            "comment must not be offered as a create_flag kind"
        );
    }

    /// Each signal verb's tool description carries the shared guidance the CLI
    /// renders under `--help`. Fails the moment a description stops sharing.
    #[test]
    fn test_signal_tools_carry_the_shared_verb_guidance() {
        for (tool, guidance) in [
            (create_flag_tool(), CREATE_GUIDANCE),
            (create_reply_tool(), REPLY_GUIDANCE),
            (close_flag_tool(), CLOSE_GUIDANCE),
            (reopen_flag_tool(), REOPEN_GUIDANCE),
        ] {
            assert!(
                tool.description.contains(guidance),
                "{} must carry its shared verb guidance",
                tool.name
            );
        }
    }

    /// The shared guidance is rendered verbatim by a surface that spells its
    /// arguments differently, so it must name none of this surface's.
    #[test]
    fn test_verb_guidance_names_no_invocation() {
        for (label, guidance) in [
            ("create", CREATE_GUIDANCE),
            ("reply", REPLY_GUIDANCE),
            ("close", CLOSE_GUIDANCE),
            ("reopen", REOPEN_GUIDANCE),
        ] {
            for spelling in [
                "create_flag",
                "create_reply",
                "close_flag",
                "reopen_flag",
                "get_signal_detail",
                "list_participants",
                "target_did",
                "space_id",
                "--",
            ] {
                assert!(
                    !guidance.contains(spelling),
                    "{label} guidance must not name `{spelling}` — it is rendered by both surfaces"
                );
            }
        }
    }

    /// The MCP tool accepts only the two AUTHORABLE audiences.
    ///
    /// The six role audiences are retired for authoring: the tool schema names
    /// two audiences, and the retired ones are argument errors. Reading them is
    /// unaffected — a stored row or a legacy record still resolves through
    /// `vocab::audience_type_from_str`, which keeps all eight.
    #[test]
    fn test_parse_audience_type_accepts_only_authorable_audiences() {
        assert!(parse_audience_type("space").is_ok());
        assert!(parse_audience_type("participant").is_ok());
        for retired in [
            "human_owners",
            "human_editors",
            "human_viewers",
            "agent_owners",
            "agent_editors",
            "agent_viewers",
        ] {
            assert!(
                parse_audience_type(retired).is_err(),
                "{retired} is retired and must not be authorable"
            );
        }
        assert!(parse_audience_type("nope").is_err());
    }

    #[test]
    fn test_default_instructions_provider_returns_oss_template() {
        let p = DefaultInstructionsProvider;
        let s = p.instructions();
        // Header from the rewritten OSS template.
        assert!(s.contains("# Working with kutl through this relay"));
        // OSS-flavor default-visibility assumption — the load-bearing
        // sentence agents need to read.
        assert!(s.contains("Signals here reach MCP-connected agents only"));
        // Identifier-shape section present.
        assert!(s.contains("Identifier shapes"));
        // Audience-on-a-flag section present.
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
        // this constant to confirm dual-embedding ships from the
        // same source. Confirm it's non-empty and starts with the
        // expected header so any accidental path change surfaces here.
        assert!(KFM_SPEC.starts_with("# kutl-flavored markdown (KFM)"));
        assert!(KFM_SPEC.len() > 1_000, "KFM spec unexpectedly small");
    }

    #[test]
    fn test_instructions_never_offer_a_callout_as_a_decision_form() {
        // Only `## ?` / `## =` headings mint decisions; a callout is
        // rendering. Telling an agent otherwise is not a cosmetic error —
        // it is a decision the engine never records, written by an agent
        // that believed the instructions.
        assert!(
            !OSS_INSTRUCTIONS.contains("heading or callout for decisions"),
            "the reach guidance must not present a callout as a decision form"
        );
        assert!(
            OSS_INSTRUCTIONS.contains("`## ?` / `## =` heading for decisions"),
            "the reach guidance must name the tracked heading grammar"
        );
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
            "## Read before you edit",
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
    /// `parse_create_reply`, etc. require the relevant fields to
    /// parse as UUID — a bare literal like `"d1"` or `"sig-1"` would be
    /// rejected before the assertion it is testing ever runs.
    const TEST_DOC_ID: &str = "11111111-1111-4111-8111-111111111111";
    const TEST_SIGNAL_ID: &str = "22222222-2222-4222-8222-222222222222";
    const TEST_SIGNAL_ID_2: &str = "22222222-2222-4222-8222-222222222223";
    const TEST_SIGNAL_ID_3: &str = "22222222-2222-4222-8222-222222222224";
    const TEST_REPLY_ID: &str = "33333333-3333-4333-8333-333333333333";

    #[test]
    fn test_tool_definitions_count() {
        let defs = tool_definitions_with_provider(&NoopToolProvider);
        assert_eq!(defs.len(), 18);

        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"read_document"));
        assert!(names.contains(&"list_documents"));
        assert!(names.contains(&"list_spaces"));
        assert!(names.contains(&"read_log"));
        assert!(names.contains(&"list_participants"));
        assert!(names.contains(&"resolve_participant"));
        assert!(names.contains(&"status"));
        assert!(names.contains(&"create_document"));
        assert!(names.contains(&"edit_document"));
        assert!(names.contains(&"upload_blob"));
        assert!(names.contains(&"get_changes"));
        assert!(names.contains(&"create_flag"));
        assert!(names.contains(&"create_comment"));
        assert!(names.contains(&"create_reply"));
        assert!(names.contains(&"list_signals"));
        assert!(names.contains(&"close_flag"));
        assert!(names.contains(&"reopen_flag"));
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
        assert_eq!(tools.len(), 8);
        assert_eq!(tools[7].name, "extra_tool");
    }

    #[test]
    fn test_advertised_tool_names_excludes_unprovided_tool() {
        // The OSS NoopToolProvider advertises the base set and nothing else, so
        // a provider-gated tool like `react_to_signal` (added only by the
        // kutlhub provider) is NOT in the executable set — exactly what the
        // `tools/call` gate checks to keep it from running on an OSS relay.
        let names = advertised_tool_names(&NoopToolProvider);
        assert!(
            !names.contains("react_to_signal"),
            "react_to_signal must not be executable on a relay that does not advertise it"
        );
        // ...while every base tool remains executable.
        assert!(names.contains("create_flag"));
        assert!(names.contains("get_changes"));
        assert!(names.contains("get_signal_detail"));
    }

    #[test]
    fn test_advertised_tool_names_includes_provider_extras() {
        // A provider that advertises a tool (as the kutlhub provider does for
        // `react_to_signal`) makes it executable — advertised == executable, in
        // one edit, with no separate allowlist to reconcile.
        struct ReactProvider;
        impl McpToolProvider for ReactProvider {
            fn extra_tools(&self) -> Vec<ToolDefinition> {
                vec![ToolDefinition {
                    name: "react_to_signal".into(),
                    description: "test".into(),
                    input_schema: serde_json::json!({"type": "object", "properties": {}}),
                }]
            }
        }
        let names = advertised_tool_names(&ReactProvider);
        assert!(names.contains("react_to_signal"));
    }

    #[test]
    fn test_advertised_tool_names_matches_definitions() {
        // The gate's allowed set is DERIVED from the same function that renders
        // `tools/list`, so the executable and advertised surfaces cannot drift:
        // one name per definition, every definition present.
        let defs = tool_definitions_with_provider(&NoopToolProvider);
        let names = advertised_tool_names(&NoopToolProvider);
        assert_eq!(names.len(), defs.len(), "tool names must be unique");
        for def in &defs {
            assert!(names.contains(&def.name), "{} missing from names", def.name);
        }
    }

    #[test]
    fn test_tool_definitions_have_schemas() {
        for def in tool_definitions_with_provider(&NoopToolProvider) {
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
        let args = serde_json::json!({"space_id": "f64551fc-d6f0-4823-8b87-971cfb914464", "document_id": "d1"});
        let parsed = parse_tool_call("read_document", &args).unwrap();
        assert!(
            matches!(parsed, ParsedToolCall::ReadDocument { space_id, document_id } if space_id == "f64551fc-d6f0-4823-8b87-971cfb914464" && document_id == "d1")
        );
    }

    #[test]
    fn test_parse_tool_call_missing_field() {
        let args = serde_json::json!({"space_id": "f64551fc-d6f0-4823-8b87-971cfb914464"});
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
            "space_id": "f64551fc-d6f0-4823-8b87-971cfb914464",
            "document_id": "d1",
            "base_version": "kv1.AAAAAAAAAAA",
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
            "space_id": "f64551fc-d6f0-4823-8b87-971cfb914464",
            "document_id": "d1",
            "base_version": "kv1.AAAAAAAAAAA",
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
            "space_id": "f64551fc-d6f0-4823-8b87-971cfb914464",
            "document_id": "d1",
            "base_version": "kv1.AAAAAAAAAAA",
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
    fn test_parse_tool_call_edit_document_requires_a_base_version() {
        // Optional would defeat the point: a writer that omits its base is
        // exactly the writer whose payload silently deletes a peer's work.
        let args = serde_json::json!({
            "space_id": "f64551fc-d6f0-4823-8b87-971cfb914464",
            "document_id": "d1",
            "content": "new text",
            "intent": "update content"
        });
        let err = parse_tool_call("edit_document", &args).unwrap_err();
        assert!(err.is_error);
        assert!(
            err.content[0].text.contains("base_version"),
            "the error must name the missing field, got: {}",
            err.content[0].text
        );
    }

    #[test]
    fn test_edit_document_schema_advertises_base_version() {
        // The companion to the parse test above, and the half that is easy to
        // lose: a caller only learns an argument exists from the schema. A
        // handler that demands `base_version` while the schema hides it turns
        // every well-formed call into a rejection, and the caller has nothing
        // to read that would tell it why.
        let schema = edit_document_tool().input_schema;

        assert!(
            schema["properties"]["base_version"].is_object(),
            "edit_document must advertise base_version in properties, got: {}",
            schema["properties"]
        );

        let required: Vec<&str> = schema["required"]
            .as_array()
            .expect("edit_document schema has a required array")
            .iter()
            .map(|v| v.as_str().expect("required entries are strings"))
            .collect();
        assert!(
            required.contains(&"base_version"),
            "edit_document must list base_version as required — the handler \
             rejects the call without it, so advertising it as optional would \
             be a lie; got: {required:?}"
        );
    }

    #[test]
    fn test_parse_tool_call_edit_document_with_snippet() {
        let args = serde_json::json!({
            "space_id": "f64551fc-d6f0-4823-8b87-971cfb914464",
            "document_id": "d1",
            "base_version": "kv1.AAAAAAAAAAA",
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
        let args = serde_json::json!({"space_id": "f64551fc-d6f0-4823-8b87-971cfb914464", "document_id": "d1", "limit": 10});
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
        let args = serde_json::json!({"space_id": "f64551fc-d6f0-4823-8b87-971cfb914464", "document_id": "d1"});
        let parsed = parse_tool_call("read_log", &args).unwrap();
        assert!(matches!(
            parsed,
            ParsedToolCall::ReadLog { limit: None, .. }
        ));
    }

    #[test]
    fn test_parse_tool_call_create_flag_space() {
        let args = serde_json::json!({
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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

    /// `list_signals` defaults to open-only and leaves every other filter
    /// unset — everything in the space that still needs someone, which is what
    /// a caller passing only a `space_id` means. Not an inbox: it is scoped to
    /// the space, not to the caller.
    #[test]
    fn test_parse_tool_call_list_signals_defaults_to_open() {
        let args = serde_json::json!({ "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db" });
        match parse_tool_call("list_signals", &args).unwrap() {
            ParsedToolCall::ListSignals {
                status,
                kind,
                document_id,
                flag_kind,
                ..
            } => {
                assert_eq!(status, "open");
                assert!(kind.is_none() && document_id.is_none() && flag_kind.is_none());
            }
            _ => panic!("expected ListSignals"),
        }
    }

    /// A `document_id` filter must be a UUID — the same rule every other
    /// document-taking tool applies, so a caller cannot narrow by a path and
    /// get a silently empty list back.
    #[test]
    fn test_parse_tool_call_list_signals_rejects_non_uuid_document() {
        let args = serde_json::json!({
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
            "document_id": "notes.md",
        });
        let err = parse_tool_call("list_signals", &args).unwrap_err();
        assert!(err.is_error);
        assert!(
            err.content[0].text.contains("document_id must be a UUID"),
            "got: {}",
            err.content[0].text
        );
    }

    #[test]
    fn test_parse_tool_call_create_comment_ok() {
        // The marker↔signal binding arrives as required arguments
        // rather than conditionally-validated optional ones, and `kind` is not
        // a caller argument at all — choosing the tool chose it.
        let args = serde_json::json!({
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
            "document_id": TEST_DOC_ID,
            "message": "this needs a reword",
            "signal_id": TEST_SIGNAL_ID,
            "anchor_text": "the old phrasing",
        });
        let parsed = parse_tool_call("create_comment", &args).unwrap();
        match parsed {
            ParsedToolCall::CreateFlag {
                kind,
                signal_id,
                anchor_text,
                audience,
                ..
            } => {
                // FLAG_KIND_COMMENT = 6 per the proto — the SAME record the
                // comment kind always produced. The split is at the tool
                // surface, not in the record.
                assert_eq!(kind, 6);
                assert_eq!(signal_id.as_deref(), Some(TEST_SIGNAL_ID));
                assert_eq!(anchor_text.as_deref(), Some("the old phrasing"));
                assert_eq!(
                    audience,
                    i32::from(sync::AudienceType::Space),
                    "an omitted audience defaults to space"
                );
            }
            _ => panic!("expected CreateFlag"),
        }
    }

    #[test]
    fn test_parse_tool_call_create_comment_requires_the_marker_binding() {
        // Both halves of the binding are mandatory. Missing either is what used
        // to mint a comment anchored to nothing.
        let base = serde_json::json!({
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
            "document_id": TEST_DOC_ID,
            "message": "nit",
            "signal_id": TEST_SIGNAL_ID,
            "anchor_text": "wrapped span",
        });
        for missing in ["signal_id", "anchor_text"] {
            let mut args = base.clone();
            args.as_object_mut().unwrap().remove(missing);
            let err = parse_tool_call("create_comment", &args).unwrap_err();
            assert!(err.is_error);
            assert!(
                err.content[0].text.contains(missing),
                "the error must name the missing field, got: {}",
                err.content[0].text
            );
        }
    }

    #[test]
    fn test_parse_tool_call_create_flag_refuses_the_comment_kind() {
        // `comment` is not in create_flag's vocabulary. The refusal
        // names the tool that owns it — an agent that reaches here has a
        // recoverable mistake, not an unknown one.
        let args = serde_json::json!({
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
            "document_id": TEST_DOC_ID,
            "kind": "comment",
            "message": "this needs a reword",
            "audience": "space",
            "signal_id": TEST_SIGNAL_ID,
            "anchor_text": "the old phrasing",
        });
        let err = parse_tool_call("create_flag", &args).unwrap_err();
        assert!(err.is_error);
        assert!(
            err.content[0].text.contains("create_comment"),
            "the refusal must point at create_comment, got: {}",
            err.content[0].text
        );
    }

    #[test]
    fn test_parse_tool_call_create_flag_info_with_comment_params_silently_ignored() {
        // Non-comment kinds tolerate stray signal_id / anchor_text params
        // (more forgiving). The parser drops them so the relay never
        // persists a fake anchor on an info flag.
        let args = serde_json::json!({
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
                assert_eq!(space_id, "e8bc163c-82ee-4187-8328-8c7d4ac636db");
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
            "parent_signal_id": TEST_SIGNAL_ID
        });
        let err = parse_tool_call("create_reply", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("body"));
    }

    #[test]
    fn test_parse_tool_call_close_flag_resolved() {
        let args = serde_json::json!({
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
                assert_eq!(space_id, "e8bc163c-82ee-4187-8328-8c7d4ac636db");
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
            "signal_id": TEST_SIGNAL_ID
        });
        let parsed = parse_tool_call("get_signal_detail", &args).unwrap();
        match parsed {
            ParsedToolCall::GetSignalDetail {
                space_id,
                signal_id,
            } => {
                assert_eq!(space_id, "e8bc163c-82ee-4187-8328-8c7d4ac636db");
                assert_eq!(signal_id, TEST_SIGNAL_ID);
            }
            _ => panic!("expected GetSignalDetail"),
        }
    }

    #[test]
    fn test_parse_tool_call_get_signal_detail_missing_id() {
        let args = serde_json::json!({"space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db"});
        let err = parse_tool_call("get_signal_detail", &args).unwrap_err();
        assert!(err.is_error);
        assert!(err.content[0].text.contains("signal_id"));
    }

    #[test]
    fn test_parse_tool_call_react_to_signal() {
        let args = serde_json::json!({
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
                assert_eq!(space_id, "e8bc163c-82ee-4187-8328-8c7d4ac636db");
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
                assert_eq!(space_id, "e8bc163c-82ee-4187-8328-8c7d4ac636db");
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
            "space_id": "e8bc163c-82ee-4187-8328-8c7d4ac636db",
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
