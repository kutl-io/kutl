//! MCP JSON-RPC types and protocol constants.
//!
//! Implements the subset of JSON-RPC 2.0 needed for MCP Streamable HTTP:
//! request/response envelopes, error codes, and initialize/tool types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// MCP protocol version we advertise.
pub const MCP_PROTOCOL_VERSION: &str = "2025-03-26";

// ---- JSON-RPC error codes ----

/// Parse error: invalid JSON.
pub const PARSE_ERROR: i64 = -32700;

/// Invalid JSON-RPC request.
pub const INVALID_REQUEST: i64 = -32600;

/// Method not found.
pub const METHOD_NOT_FOUND: i64 = -32601;

/// Invalid method parameters.
pub const INVALID_PARAMS: i64 = -32602;

/// Internal server error.
pub const INTERNAL_ERROR: i64 = -32603;

// ---- Core JSON-RPC types ----

/// An inbound JSON-RPC 2.0 request.
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    /// Must be `"2.0"`.
    pub jsonrpc: String,
    /// Request id — may be absent for notifications.
    pub id: Option<Value>,
    /// Method name.
    pub method: String,
    /// Method parameters.
    #[serde(default)]
    pub params: Value,
}

/// An outbound JSON-RPC 2.0 response.
#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    /// Always `"2.0"`.
    pub jsonrpc: String,
    /// Echoed request id.
    pub id: Value,
    /// Result on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// Error on failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    /// Build a success response.
    pub fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Build an error response.
    pub fn error(id: Value, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }
}

/// JSON-RPC error object.
#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    /// Numeric error code.
    pub code: i64,
    /// Human-readable message.
    pub message: String,
    /// Optional additional data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// A server-initiated JSON-RPC notification (no id).
#[derive(Debug, Serialize)]
pub struct JsonRpcNotification {
    /// Always `"2.0"`.
    pub jsonrpc: String,
    /// Notification method name.
    pub method: String,
    /// Notification parameters.
    pub params: Value,
}

// ---- MCP Initialize types ----

/// Parameters for `initialize` request.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    /// Protocol version the client wants.
    pub protocol_version: String,
    /// Client info.
    pub client_info: ClientInfo,
    /// Optional DID for self-identification when auth is disabled.
    /// Ignored when `require_auth` is true (DID comes from the token).
    #[serde(default)]
    pub did: Option<String>,
}

/// Client information sent during initialize.
#[derive(Debug, Deserialize)]
pub struct ClientInfo {
    /// Client name.
    pub name: String,
    /// Client version.
    #[serde(default)]
    pub version: String,
}

/// Result of the `initialize` handshake.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    /// Protocol version.
    pub protocol_version: String,
    /// Server capabilities.
    pub capabilities: ServerCapabilities,
    /// Server info.
    pub server_info: ServerInfo,
    /// Optional cloud-mode instructions string. Per RFD 0075, the
    /// relay's MCP `initialize` response carries scope-1 (tool-generic)
    /// content for agents connecting to the relay directly without
    /// going through `kutl watch`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

/// Server capabilities advertised during initialize.
#[derive(Debug, Serialize)]
pub struct ServerCapabilities {
    /// Tools capability (empty object = supported).
    pub tools: Value,
}

/// Server info returned during initialize.
#[derive(Debug, Serialize)]
pub struct ServerInfo {
    /// Server name.
    pub name: String,
    /// Server version.
    pub version: String,
}

// ---- MCP Tool types ----

/// A tool definition with JSON Schema input.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDefinition {
    /// Tool name.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// JSON Schema for tool arguments.
    pub input_schema: Value,
}

/// Parameters for `tools/call`.
#[derive(Debug, Deserialize)]
pub struct ToolCallParams {
    /// Tool name to invoke.
    pub name: String,
    /// Tool arguments.
    #[serde(default)]
    pub arguments: Value,
}

/// Result of a tool invocation.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallResult {
    /// Content items.
    pub content: Vec<ToolContent>,
    /// Whether this result represents an error.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub is_error: bool,
}

/// A content item in a tool result.
#[derive(Debug, Serialize, Deserialize)]
pub struct ToolContent {
    /// Content type — always `"text"` for now.
    #[serde(rename = "type")]
    pub content_type: String,
    /// Text content.
    pub text: String,
}

impl ToolCallResult {
    /// Build a successful text result.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent {
                content_type: "text".into(),
                text: text.into(),
            }],
            is_error: false,
        }
    }

    /// Build an error result.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent {
                content_type: "text".into(),
                text: message.into(),
            }],
            is_error: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_rpc_response_success_serializes() {
        let resp =
            JsonRpcResponse::success(serde_json::json!(1), serde_json::json!({"status": "ok"}));
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["jsonrpc"], "2.0");
        assert_eq!(json["id"], 1);
        assert!(json["result"].is_object());
        assert!(json.get("error").is_none());
    }

    #[test]
    fn test_json_rpc_response_error_serializes() {
        let resp = JsonRpcResponse::error(serde_json::json!(2), METHOD_NOT_FOUND, "not found");
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["jsonrpc"], "2.0");
        assert_eq!(json["id"], 2);
        assert!(json.get("result").is_none());
        assert_eq!(json["error"]["code"], METHOD_NOT_FOUND);
        assert_eq!(json["error"]["message"], "not found");
    }

    #[test]
    fn test_json_rpc_request_deserializes() {
        let json = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#;
        let req: JsonRpcRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.jsonrpc, "2.0");
        assert_eq!(req.id, Some(serde_json::json!(1)));
        assert_eq!(req.method, "tools/list");
    }

    #[test]
    fn test_json_rpc_request_without_id() {
        let json = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        let req: JsonRpcRequest = serde_json::from_str(json).unwrap();
        assert!(req.id.is_none());
    }

    #[test]
    fn test_tool_call_result_text() {
        let result = ToolCallResult::text("hello");
        assert!(!result.is_error);
        assert_eq!(result.content.len(), 1);
        assert_eq!(result.content[0].text, "hello");
    }

    #[test]
    fn test_tool_call_result_error() {
        let result = ToolCallResult::error("oops");
        assert!(result.is_error);
        assert_eq!(result.content[0].text, "oops");
    }

    #[test]
    fn test_initialize_params_deserializes() {
        let json = r#"{
            "protocolVersion": "2025-03-26",
            "clientInfo": { "name": "test-agent", "version": "1.0" }
        }"#;
        let params: InitializeParams = serde_json::from_str(json).unwrap();
        assert_eq!(params.protocol_version, "2025-03-26");
        assert_eq!(params.client_info.name, "test-agent");
    }

    #[test]
    fn test_notification_serializes() {
        let notif = JsonRpcNotification {
            jsonrpc: "2.0".into(),
            method: "notifications/tools/list_changed".into(),
            params: serde_json::json!({}),
        };
        let json = serde_json::to_value(&notif).unwrap();
        assert_eq!(json["jsonrpc"], "2.0");
        assert_eq!(json["method"], "notifications/tools/list_changed");
    }
}
