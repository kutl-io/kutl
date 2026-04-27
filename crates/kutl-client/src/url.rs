//! URL conversion utilities for relay communication.

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
            normalize_relay_url("wss://relay.kutlhub.com/ws"),
            "wss://relay.kutlhub.com/ws"
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
            http_url_to_ws("https://relay.kutlhub.com"),
            "wss://relay.kutlhub.com/ws"
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
