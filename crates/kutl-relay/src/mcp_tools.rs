//! MCP tool definitions and argument parsing for space-scoped operations.

use kutl_proto::sync;
use serde_json::Value;

use crate::mcp::{ToolCallResult, ToolDefinition};

// ---------------------------------------------------------------------------
// McpInstructionsProvider trait — extension point for cloud-mode instructions
// ---------------------------------------------------------------------------

/// Cloud-mode scope-1 (tool-generic) instructions content for the OSS
/// relay. Embedded at compile time. Agents connecting to the OSS relay
/// directly via MCP receive this string in `InitializeResult.instructions`.
pub const OSS_INSTRUCTIONS: &str = include_str!("instructions_template.md");

/// Extension point for relay implementations to provide cloud-mode
/// instructions. The OSS relay returns `OSS_INSTRUCTIONS` via
/// `DefaultInstructionsProvider`; kutlhub-relay (commercial) implements
/// its own provider returning kutlhub-flavored content. Per RFD 0075,
/// override is **replacement**, not extension — kutlhub does not
/// inherit OSS content, just the trait.
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
// McpToolProvider trait — extension point for kutlhub-relay
// ---------------------------------------------------------------------------

/// Extension point for relay implementations to provide additional MCP tools.
/// The OSS relay registers base signal tools; kutlhub-relay adds tools that
/// require the UX layer (reactions, reopen).
pub trait McpToolProvider: Send + Sync {
    /// Additional tools beyond the base set.
    fn extra_tools(&self) -> Vec<ToolDefinition> {
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
        read_log_tool(),
        list_participants_tool(),
        status_tool(),
        edit_document_tool(),
        get_changes_tool(),
    ];
    defs.extend(signal_tools(provider));
    defs
}

// ---------------------------------------------------------------------------
// Non-signal tool definitions
// ---------------------------------------------------------------------------

fn read_document_tool() -> ToolDefinition {
    ToolDefinition {
        name: "read_document".into(),
        description: "Read the current content of a document.".into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "document_id": {
                    "type": "string",
                    "description": "Document identifier within the space."
                }
            },
            "required": ["space_id", "document_id"]
        }),
    }
}

fn list_documents_tool() -> ToolDefinition {
    ToolDefinition {
        name: "list_documents".into(),
        description: "List all documents in a space.".into(),
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
        description: "Read the change log for a document with author and intent metadata.".into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "Space identifier."
                },
                "document_id": {
                    "type": "string",
                    "description": "Document identifier within the space."
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
        description: "List participants (DIDs) connected to a space.".into(),
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
        description:
            "Get status information for a space (document count, connections, MCP sessions).".into(),
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
        description: "Edit a document by providing the full desired content. The relay \
                      diffs current vs new and applies minimal CRDT operations. \
                      Other participants will see that this document changed when \
                      they call get_changes (the document appears in \
                      document_changes with an updated edited_at timestamp). \
                      Use create_flag when you need to draw specific attention \
                      to your edit or ask for input."
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
                    "description": "Document identifier within the space."
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

fn get_changes_tool() -> ToolDefinition {
    ToolDefinition {
        name: "get_changes".into(),
        description: "Poll for new activity in a space. Returns signals (flags, \
                      replies) and document changes (new, renamed, deleted, or \
                      edited) since your last check. The server tracks your \
                      position automatically.\n\n\
                      When a document appears in document_changes with an \
                      edited_at timestamp, its content was modified. Call \
                      read_document to see the current content.\n\n\
                      The response includes a checkpoint field. If a previous call \
                      failed or you need to replay, pass that checkpoint value to \
                      resume."
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
fn create_flag_tool() -> ToolDefinition {
    ToolDefinition {
        name: "create_flag".into(),
        description: "Create a flag signal (a request for attention) in a space. \
                      Note: the OSS kutl deployment does not currently surface space \
                      signals to humans through any kutl-aware client — humans \
                      read markdown content through their editor or other tooling. \
                      Prefer in-doc content (decisions, mentions, document edits) \
                      for anything that needs human attention; use signals \
                      primarily for agent-to-agent coordination."
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
                    "description": "Document this flag relates to."
                },
                "kind": {
                    "type": "string",
                    "enum": ["info", "completed", "review_requested", "question", "blocked"],
                    "description": "The type of flag."
                },
                "message": {
                    "type": "string",
                    "description": "Message to include with the flag."
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

/// Reply to a signal (flag, decision, or another reply).
fn create_reply_tool() -> ToolDefinition {
    ToolDefinition {
        name: "create_reply".into(),
        description: "Reply to a flag or other parent signal. \
                      Note: see the human-visibility note on `create_flag` — replies \
                      on this OSS relay reach other agents subscribing via MCP, \
                      but are not surfaced to humans through any current kutl \
                      client."
            .into(),
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

/// Close a flag signal with a resolution.
fn close_flag_tool() -> ToolDefinition {
    ToolDefinition {
        name: "close_flag".into(),
        description: "Close a flag, marking it resolved. \
                      Note: see the human-visibility note on `create_flag`."
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
        description: "Fetch a signal with its full reply thread and reactions.".into(),
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
    GetChanges {
        space_id: String,
        checkpoint: Option<String>,
    },
    /// Create a flag signal.
    CreateFlag {
        space_id: String,
        document_id: String,
        kind: i32,
        message: String,
        audience: i32,
        target_did: String,
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
    /// Add or remove a reaction on a signal (kutlhub-only).
    ReactToSignal {
        space_id: String,
        signal_id: String,
        emoji: String,
        remove: bool,
    },
    /// Reopen a previously closed flag signal (kutlhub-only).
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
        // kutlhub-only tools — the OSS relay doesn't define these but
        // kutlhub-relay may register them via McpToolProvider. The handler
        // needs to parse them when dispatching.
        "react_to_signal" => parse_react_to_signal(arguments),
        "reopen_flag" => parse_reopen_flag(arguments),
        _ => Err(ToolCallResult::error(format!("unknown tool: {name}"))),
    }
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
    Ok(ParsedToolCall::CreateFlag {
        space_id,
        document_id,
        kind,
        message,
        audience,
        target_did,
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

/// Parse `react_to_signal` arguments (kutlhub-only).
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

/// Parse `reopen_flag` arguments (kutlhub-only).
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
        _ => Err(ToolCallResult::error(
            "invalid kind: must be one of info, completed, review_requested, question, blocked",
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
/// from Postgres.
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

/// Extract an optional usize field from a JSON object.
fn optional_usize(args: &Value, field: &str) -> Result<Option<usize>, ToolCallResult> {
    match args.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let n = v.as_u64().ok_or_else(|| {
                ToolCallResult::error(format!("{field} must be a positive integer"))
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
        assert!(s.contains("# Working with kutl through this relay"));
        assert!(s.contains("stack does not currently include"));
        assert!(s.contains("prefer in-doc content"));
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
        assert_eq!(defs.len(), 11);

        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"read_document"));
        assert!(names.contains(&"list_documents"));
        assert!(names.contains(&"read_log"));
        assert!(names.contains(&"list_participants"));
        assert!(names.contains(&"status"));
        assert!(names.contains(&"edit_document"));
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
    fn test_parse_tool_call_reopen_flag() {
        let args = serde_json::json!({
            "space_id": "s1",
            "signal_id": TEST_SIGNAL_ID
        });
        let parsed = parse_tool_call("reopen_flag", &args).unwrap();
        assert!(matches!(parsed, ParsedToolCall::ReopenFlag { .. }));
    }
}
