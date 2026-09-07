//! Shared MCP test client for the relay's `/mcp` endpoint.
//!
//! Not all helpers are used in every test file — `dead_code` is expected.

#![allow(dead_code)]

use ed25519_dalek::SigningKey;
use reqwest::header;

use super::{authenticate, authorize_did, test_keypair};

/// Send a JSON-RPC request to `http://{addr}/mcp`, attaching the bearer and,
/// when given, the `mcp-session-id` header.
pub async fn mcp_request(
    client: &reqwest::Client,
    addr: &str,
    token: &str,
    session_id: Option<&str>,
    method: &str,
    params: serde_json::Value,
) -> reqwest::Response {
    let mut req = client
        .post(format!("http://{addr}/mcp"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }));

    if let Some(sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }

    req.send().await.unwrap()
}

/// An authenticated MCP session: a freshly minted DID, the bearer it
/// authenticated with, and the session id `initialize` handed back.
pub struct McpSession {
    pub session_id: String,
    pub token: String,
    pub did: String,
    addr: String,
    client: reqwest::Client,
}

impl McpSession {
    /// Mint a keypair, enroll its DID in `keys_file` (so space-scoped tool
    /// calls accept it), authenticate, and initialize an MCP session.
    pub async fn open(addr: &str, keys_file: &tempfile::NamedTempFile) -> Self {
        let (did, signing_key) = test_keypair();
        authorize_did(keys_file.path(), &did);
        Self::initialize(addr, did, &signing_key).await
    }

    /// Like [`McpSession::open`] but the DID is never enrolled in `keys_file`:
    /// the relay still issues a session (the keys file gates space-scoped
    /// actions), so tests can probe what an unauthorized caller is shown.
    /// Takes the keys file the authorized variant writes so the two entry
    /// points swap freely at a call site.
    pub async fn open_unauthorized(addr: &str, _keys_file: &tempfile::NamedTempFile) -> Self {
        let (did, signing_key) = test_keypair();
        Self::initialize(addr, did, &signing_key).await
    }

    /// Authenticate `did` and run `initialize`, reading the session id from
    /// the `mcp-session-id` response header.
    async fn initialize(addr: &str, did: String, signing_key: &SigningKey) -> Self {
        let token = authenticate(&format!("http://{addr}"), &did, signing_key).await;
        let client = reqwest::Client::new();

        let resp = mcp_request(
            &client,
            addr,
            &token,
            None,
            "initialize",
            serde_json::json!({
                "protocolVersion": kutl_relay::mcp::MCP_PROTOCOL_VERSION,
                "clientInfo": {"name": "test-agent", "version": "0.1"}
            }),
        )
        .await;

        let session_id = resp
            .headers()
            .get("mcp-session-id")
            .expect("initialize should return mcp-session-id header")
            .to_str()
            .unwrap()
            .to_owned();

        Self {
            session_id,
            token,
            did,
            addr: addr.to_owned(),
            client,
        }
    }

    /// Send a raw JSON-RPC request on this session. The escape hatch for
    /// tests that assert on the envelope itself (HTTP status, notification
    /// wiring, plain-text tool results) rather than a decoded tool payload.
    pub async fn request(&self, method: &str, params: serde_json::Value) -> reqwest::Response {
        mcp_request(
            &self.client,
            &self.addr,
            &self.token,
            Some(&self.session_id),
            method,
            params,
        )
        .await
    }

    /// `tools/call` for `tool`, asserting HTTP 200 and returning the parsed
    /// JSON-RPC body without judging the tool-level outcome.
    async fn tools_call(&self, tool: &str, args: serde_json::Value) -> serde_json::Value {
        let resp = self
            .request(
                "tools/call",
                serde_json::json!({ "name": tool, "arguments": args }),
            )
            .await;
        assert_eq!(resp.status(), 200, "{tool} tools/call must return HTTP 200");
        resp.json().await.unwrap()
    }

    /// Call `tool`, assert it succeeded, and decode the JSON payload carried
    /// in its first text content item.
    pub async fn call_ok(&self, tool: &str, args: serde_json::Value) -> serde_json::Value {
        let body = self.tools_call(tool, args).await;
        assert!(
            !body["result"]["isError"].as_bool().unwrap_or(false),
            "{tool} must succeed, got: {body}"
        );
        let text = body["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("{tool} returns a text result: {body}"));
        serde_json::from_str(text).unwrap_or_else(|e| panic!("{tool} returns JSON ({e}): {text}"))
    }

    /// Call `tool`, assert it returned a tool-level error, and return the
    /// error text.
    pub async fn call_err(&self, tool: &str, args: serde_json::Value) -> String {
        let body = self.tools_call(tool, args).await;
        assert!(
            body["result"]["isError"].as_bool().unwrap_or(false),
            "{tool} must return an error result, got: {body}"
        );
        body["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("an error carries text: {body}"))
            .to_owned()
    }
}
