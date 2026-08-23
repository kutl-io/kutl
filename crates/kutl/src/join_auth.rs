//! Auth-adaptive join decision: given the relay's advertised auth model and
//! the local auth state, decide how `kutl join` proceeds — lazily, instead of
//! silently minting a DID before knowing the relay's policy.

use anyhow::{Result, bail};
use kutl_proto::sync::{AuthModel, ErrorCode, RelayPolicy};

/// What `kutl join` should do for the connection's identity/credentials, given
/// the relay's advertised [`AuthModel`]. The caller executes the chosen action.
#[derive(Debug, PartialEq, Eq)]
pub enum JoinAuthAction {
    /// No join-gate precondition: proceed to connect. Identity is supplied at
    /// connect time by the did:key challenge-response, so `kutl join` mints
    /// nothing here.
    Proceed,
    /// Account-required relay with a stored token: proceed with it.
    UseStoredToken,
    /// Account-required relay, no token, interactive terminal: prompt the user,
    /// then run the device flow.
    PromptThenDeviceFlow,
    /// Account-required relay, no token, non-interactive: fail with a clear,
    /// actionable error rather than silently minting an unusable identity.
    ErrorNotAuthenticated,
}

/// Decide the join auth action from the relay's advertised auth model and the
/// local auth state. Pure; performs no I/O.
pub fn decide_join_auth(
    auth_model: AuthModel,
    has_stored_token: bool,
    is_interactive: bool,
) -> JoinAuthAction {
    match auth_model {
        AuthModel::AccountRequired => {
            if has_stored_token {
                JoinAuthAction::UseStoredToken
            } else if is_interactive {
                JoinAuthAction::PromptThenDeviceFlow
            } else {
                JoinAuthAction::ErrorNotAuthenticated
            }
        }
        // Unspecified — treated permissively, consistent with the no-policy skip
        // in `apply_join_policy_gate` (a relay that serves no policy is handled
        // identically). A legacy relay that advertised the retired
        // AUTH_MODEL_ANONYMOUS_DID (wire value 1) also decodes here, since the
        // proto reservation collapses value 1 to Unspecified via the accessor's
        // unwrap-or-default.
        AuthModel::Unspecified => JoinAuthAction::Proceed,
    }
}

/// Map a relay `ResolveSpace`/join error to a user-facing message, applying the
/// anti-enumeration rule on the client side: an authentication failure
/// is explicit and actionable, while a not-found and a not-authorized result
/// collapse into one indistinguishable message (so a private space's existence
/// is not leaked to a non-member). Pairs with the relay-side collapse that makes
/// a not-member result arrive as the same error code as not-found.
pub fn map_join_error(code: i32, owner_slug: &str, relay: &str) -> String {
    if code == i32::from(ErrorCode::AuthFailed) {
        format!("not authenticated to {relay} — run `kutl auth login --relay {relay}`")
    } else {
        format!("no space {owner_slug}, or you do not have access")
    }
}

/// Resolve the WebSocket endpoint to connect to from the relay's advertised
/// policy and the HTTP discovery base it was fetched from. A proxy front door
/// advertises an explicit `relay_endpoint` and the client follows it; a direct
/// relay leaves it empty, so the client falls back to `{discovery_base}/ws`
/// (the OSS path).
pub fn effective_relay_endpoint(
    policy: Option<&RelayPolicy>,
    discovery_base_http: &str,
) -> Result<String> {
    match policy {
        Some(p) if !p.relay_endpoint.is_empty() => {
            if !same_origin(&p.relay_endpoint, discovery_base_http) {
                bail!(
                    "relay advertised a cross-origin endpoint {} for discovery base {discovery_base_http} — refusing to connect",
                    p.relay_endpoint
                );
            }
            Ok(p.relay_endpoint.clone())
        }
        _ => Ok(kutl_client::http_url_to_ws(discovery_base_http)),
    }
}

/// Whether a relay endpoint (ws or http URL) shares the scheme+host+port origin
/// of the HTTP discovery base that advertised it. Anti-redirect: the
/// client follows a policy-advertised endpoint/`auth_url` only when it stays on
/// the origin the user already targeted, so a policy can't silently redirect a
/// bearer-token connection (or the device flow) to a third party. An unparseable
/// URL on either side is treated as NOT same-origin (fail closed).
pub fn same_origin(ws_or_http_url: &str, http_base: &str) -> bool {
    let normalized = kutl_client::ws_url_to_http(ws_or_http_url);
    match (
        reqwest::Url::parse(&normalized),
        reqwest::Url::parse(http_base),
    ) {
        (Ok(a), Ok(b)) => a.origin() == b.origin(),
        _ => false,
    }
}

/// Whether a yes/no prompt answer is affirmative, defaulting to yes on an empty
/// response. Trims and lowercases; treats "", "y", and "yes" as yes.
pub fn affirmative_default_yes(answer: &str) -> bool {
    let a = answer.trim().to_lowercase();
    a.is_empty() || a == "y" || a == "yes"
}

#[cfg(test)]
mod tests {
    use super::{
        JoinAuthAction, affirmative_default_yes, decide_join_auth, effective_relay_endpoint,
        map_join_error, same_origin,
    };
    use kutl_proto::sync::{AuthModel, ErrorCode, RelayPolicy};

    #[test]
    fn test_decide_join_auth_unspecified_policy_proceeds() {
        // A relay that serves no policy (Unspecified) is permissive: proceed to
        // connect regardless of token/TTY, and let the connect-time did:key
        // challenge supply identity. A legacy relay advertising the retired
        // AUTH_MODEL_ANONYMOUS_DID (wire value 1) also lands here, because the
        // proto reservation decodes value 1 to Unspecified.
        assert_eq!(
            decide_join_auth(AuthModel::Unspecified, false, false),
            JoinAuthAction::Proceed
        );
        assert_eq!(
            decide_join_auth(AuthModel::Unspecified, true, true),
            JoinAuthAction::Proceed
        );
    }

    #[test]
    fn test_decide_join_auth_account_with_token_uses_token() {
        assert_eq!(
            decide_join_auth(AuthModel::AccountRequired, true, false),
            JoinAuthAction::UseStoredToken
        );
    }

    #[test]
    fn test_decide_join_auth_account_no_token_interactive_prompts() {
        assert_eq!(
            decide_join_auth(AuthModel::AccountRequired, false, true),
            JoinAuthAction::PromptThenDeviceFlow
        );
    }

    #[test]
    fn test_decide_join_auth_account_no_token_noninteractive_errors() {
        assert_eq!(
            decide_join_auth(AuthModel::AccountRequired, false, false),
            JoinAuthAction::ErrorNotAuthenticated
        );
    }

    #[test]
    fn test_map_join_error_auth_failed_is_explicit() {
        let msg = map_join_error(
            i32::from(ErrorCode::AuthFailed),
            "alice/proj",
            "wss://relay",
        );
        assert!(msg.contains("not authenticated"), "got: {msg}");
        assert!(msg.contains("kutl auth login"), "got: {msg}");
    }

    #[test]
    fn test_map_join_error_non_auth_message_is_unified() {
        // map_join_error collapses every non-auth relay error into one message
        // that does not reveal whether the space exists — the CLI half of the
        // anti-enumeration rule. What makes that safe is the RELAY
        // collapse (not-member and not-found arrive as the same `InvalidMessage`
        // code), which a CLI string test cannot observe; it is proven end-to-end
        // by `join_policy_gate`. Here we only pin the CLI's non-leaking,
        // owner_slug-bearing message against accidental format regressions.
        let msg = map_join_error(
            i32::from(ErrorCode::InvalidMessage),
            "alice/proj",
            "wss://relay",
        );
        assert_eq!(msg, "no space alice/proj, or you do not have access");
    }

    #[test]
    fn test_effective_relay_endpoint_follows_advertised() {
        // A front door advertises an explicit same-origin endpoint; the client
        // follows it rather than assuming {origin}/ws.
        let policy = RelayPolicy {
            relay_endpoint: "wss://kutlhub.com/relay/ws".to_owned(),
            ..Default::default()
        };
        assert_eq!(
            effective_relay_endpoint(Some(&policy), "https://kutlhub.com").unwrap(),
            "wss://kutlhub.com/relay/ws"
        );
    }

    #[test]
    fn test_effective_relay_endpoint_falls_back_to_origin_ws() {
        // Empty relay_endpoint (a direct relay / OSS) -> {discovery_base}/ws.
        let direct = RelayPolicy::default();
        assert_eq!(
            effective_relay_endpoint(Some(&direct), "https://relay.example.com").unwrap(),
            "wss://relay.example.com/ws"
        );
        // No policy served at all -> the same fallback.
        assert_eq!(
            effective_relay_endpoint(None, "http://localhost:9100").unwrap(),
            "ws://localhost:9100/ws"
        );
    }

    #[test]
    fn test_effective_relay_endpoint_rejects_cross_origin() {
        // Anti-redirect: a policy fetched from one origin must not point the
        // bearer-token connection at a different host.
        let evil = RelayPolicy {
            relay_endpoint: "wss://evil.example/ws".to_owned(),
            ..Default::default()
        };
        assert!(effective_relay_endpoint(Some(&evil), "https://kutlhub.com").is_err());
    }

    #[test]
    fn test_same_origin() {
        // ws/http scheme equivalence + path-insensitive: front door is same-origin.
        assert!(same_origin(
            "wss://kutlhub.com/relay/ws",
            "https://kutlhub.com"
        ));
        assert!(same_origin(
            "wss://kutlhub.com/relay/ws",
            "https://kutlhub.com/relay"
        ));
        // Different host, different port, and unparseable are all NOT same-origin.
        assert!(!same_origin("wss://evil.example/ws", "https://kutlhub.com"));
        assert!(!same_origin(
            "wss://kutlhub.com:9999/ws",
            "https://kutlhub.com"
        ));
        assert!(!same_origin("garbage", "https://kutlhub.com"));
    }

    #[test]
    fn test_affirmative_default_yes() {
        // Default (empty) is yes; y/yes (any case, with whitespace) is yes.
        assert!(affirmative_default_yes(""));
        assert!(affirmative_default_yes("y"));
        assert!(affirmative_default_yes("Y\n"));
        assert!(affirmative_default_yes("  yes  "));
        // Anything else is no.
        assert!(!affirmative_default_yes("n"));
        assert!(!affirmative_default_yes("no"));
        assert!(!affirmative_default_yes("nope"));
    }
}
