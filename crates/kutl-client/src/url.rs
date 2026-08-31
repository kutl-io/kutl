//! URL conversion utilities for relay communication.

/// Accepted relay URL schemes, in the form the user must supply them.
const RELAY_SCHEMES: [&str; 2] = ["ws://", "wss://"];

/// Validate a user-supplied relay URL before it is persisted.
///
/// A relay URL must use the `ws://` or `wss://` scheme and carry a non-empty
/// host. Anything else (a bare hostname, an `http(s)://` URL, or a scheme with
/// no host) is rejected so it never lands in `space.json` where it would fail
/// opaquely at connect time.
///
/// The host is the segment between the scheme and the first `/`, with any
/// `user@` prefix and `:port` suffix stripped — those may be empty, but the
/// host itself must not be.
///
/// This is a lightweight structural check, not a full RFC 3986 parse; it
/// deliberately avoids pulling in a URL-parsing dependency for a single
/// boundary validation.
pub fn validate_relay_url(raw: &str) -> anyhow::Result<()> {
    let trimmed = raw.trim();
    let Some(scheme) = RELAY_SCHEMES.iter().find(|s| trimmed.starts_with(**s)) else {
        anyhow::bail!(
            "invalid relay URL {raw:?}: must start with ws:// or wss:// (e.g. ws://host:port/ws or wss://relay.example/ws)"
        );
    };

    let after_scheme = &trimmed[scheme.len()..];
    // Authority is everything up to the path/query/fragment.
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    // Drop optional userinfo and port to isolate the host.
    let host = authority
        .rsplit('@')
        .next()
        .unwrap_or(authority)
        .split(':')
        .next()
        .unwrap_or("");

    if host.is_empty() {
        anyhow::bail!(
            "invalid relay URL {raw:?}: missing host (expected ws://host:port/ws or wss://relay.example/ws)"
        );
    }
    Ok(())
}

/// Normalize a relay URL to ensure it ends with `/ws`.
///
/// The relay WebSocket endpoint is always at `/ws`. Users and config
/// may omit the path, so this function canonicalizes:
///
/// - `ws://host:9100` → `ws://host:9100/ws`
/// - `ws://host:9100/` → `ws://host:9100/ws`
/// - `ws://host:9100/ws` → `ws://host:9100/ws` (no-op)
/// - `wss://relay.example.com/ws` → unchanged
pub fn normalize_relay_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/ws") {
        trimmed.to_owned()
    } else {
        format!("{trimmed}/ws")
    }
}

/// Convert a WebSocket URL to an HTTP base URL.
///
/// `ws://host:port/ws` -> `http://host:port`
/// `wss://host:port/ws` -> `https://host:port`
pub fn ws_url_to_http(ws_url: &str) -> String {
    let url = if let Some(rest) = ws_url.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = ws_url.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        ws_url.to_owned()
    };
    // Strip trailing /ws path if present.
    url.trim_end_matches("/ws").to_owned()
}

/// Convert a relay URL (which may be WS or HTTP) to an HTTP base URL.
///
/// Handles all scheme variants:
/// - `ws://host:port/ws`  -> `http://host:port`
/// - `wss://host:port/ws` -> `https://host:port`
/// - `http://host:port`   -> `http://host:port` (passthrough, strips trailing `/ws` and `/`)
/// - `https://host:port`  -> `https://host:port` (passthrough)
pub fn relay_url_to_http(url: &str) -> String {
    let trimmed = url.trim();
    // If it's already HTTP, strip trailing /ws and trailing slash.
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed
            .trim_end_matches("/ws")
            .trim_end_matches('/')
            .to_owned();
    }
    // Convert ws:// -> http://, wss:// -> https://.
    ws_url_to_http(trimmed)
}

/// Convert an HTTP base URL to a WebSocket URL with the `/ws` path.
///
/// `http://host:port` -> `ws://host:port/ws`
/// `https://host:port` -> `wss://host:port/ws`
///
/// The `/ws` suffix is always appended; use [`normalize_relay_url`] if
/// the input is already a WebSocket URL.
pub fn http_url_to_ws(http_url: &str) -> String {
    let base = http_url.trim_end_matches('/');
    let stripped = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base.to_owned()
    };
    // Strip any existing /ws before appending, then re-append.
    let stripped = stripped.trim_end_matches("/ws");
    format!("{stripped}/ws")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_relay_url_accepts_ws_and_wss() {
        assert!(validate_relay_url("ws://127.0.0.1:9100/ws").is_ok());
        assert!(validate_relay_url("wss://relay.example/ws").is_ok());
    }

    #[test]
    fn test_validate_relay_url_rejects_bad_input() {
        for bad in [
            "not-a-url",
            "http://x/ws",
            "ws://",
            "",
            // Subtle empty-host cases the host-extraction logic guards; pin
            // them so a future refactor can't silently reopen the hole.
            "ws:///ws",           // triple slash — empty authority
            "ws://:9100/ws",      // port only, no host
            "ws://user@:9100/ws", // userinfo present but empty host
            "wss://",             // bare scheme
        ] {
            let err = validate_relay_url(bad).expect_err(&format!("{bad:?} should be rejected"));
            // The message must name the offending value so users can spot it.
            assert!(
                format!("{err}").contains(&format!("{bad:?}")),
                "error should quote the bad value {bad:?}: {err}"
            );
        }
    }

    #[test]
    fn test_ws_url_to_http() {
        assert_eq!(
            ws_url_to_http("ws://localhost:9100/ws"),
            "http://localhost:9100"
        );
        assert_eq!(
            ws_url_to_http("wss://relay.example.com/ws"),
            "https://relay.example.com"
        );
        assert_eq!(
            ws_url_to_http("ws://localhost:9100"),
            "http://localhost:9100"
        );
    }

    #[test]
    fn test_normalize_relay_url_appends_ws() {
        assert_eq!(
            normalize_relay_url("ws://localhost:9100"),
            "ws://localhost:9100/ws"
        );
    }

    #[test]
    fn test_normalize_relay_url_strips_trailing_slash() {
        assert_eq!(
            normalize_relay_url("ws://localhost:9100/"),
            "ws://localhost:9100/ws"
        );
    }

    #[test]
    fn test_normalize_relay_url_preserves_ws() {
        assert_eq!(
            normalize_relay_url("ws://localhost:9100/ws"),
            "ws://localhost:9100/ws"
        );
        assert_eq!(
            normalize_relay_url("wss://relay.example.com/ws"),
            "wss://relay.example.com/ws"
        );
    }

    #[test]
    fn test_ws_url_to_http_passthrough() {
        assert_eq!(
            ws_url_to_http("http://already-http:8080"),
            "http://already-http:8080"
        );
    }

    #[test]
    fn test_http_url_to_ws_plain() {
        assert_eq!(
            http_url_to_ws("http://localhost:9100"),
            "ws://localhost:9100/ws"
        );
    }

    #[test]
    fn test_http_url_to_ws_tls() {
        assert_eq!(
            http_url_to_ws("https://relay.example.com"),
            "wss://relay.example.com/ws"
        );
    }

    #[test]
    fn test_http_url_to_ws_trailing_slash() {
        assert_eq!(
            http_url_to_ws("https://relay.example.com/"),
            "wss://relay.example.com/ws"
        );
    }

    #[test]
    fn test_http_url_to_ws_with_path() {
        // URL with a non-ws path: still appends /ws (caller should pass a base URL)
        assert_eq!(
            http_url_to_ws("https://example.com/invites/abc"),
            "wss://example.com/invites/abc/ws"
        );
    }

    #[test]
    fn test_relay_url_to_http_ws() {
        assert_eq!(
            relay_url_to_http("ws://localhost:9100/ws"),
            "http://localhost:9100"
        );
    }

    #[test]
    fn test_relay_url_to_http_wss() {
        assert_eq!(
            relay_url_to_http("wss://relay.example.com/ws"),
            "https://relay.example.com"
        );
    }

    #[test]
    fn test_relay_url_to_http_already_http() {
        assert_eq!(
            relay_url_to_http("http://localhost:9100"),
            "http://localhost:9100"
        );
    }

    #[test]
    fn test_relay_url_to_http_already_http_with_ws() {
        assert_eq!(
            relay_url_to_http("http://localhost:9100/ws"),
            "http://localhost:9100"
        );
    }

    #[test]
    fn test_relay_url_to_http_https_trailing_slash() {
        assert_eq!(
            relay_url_to_http("https://relay.example.com/"),
            "https://relay.example.com"
        );
    }
}
