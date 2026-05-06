//! Server-rendered HTML helpers and web admin page handlers.
//!
//! These routes are mounted when an `InviteBackend` is configured
//! (OSS relay with `data_dir`). They provide a minimal admin UI for
//! managing spaces and invite codes, using plain HTML forms and inline CSS.
//!
//! Three pages are served:
//! - `GET /` — space list with "Generate invite" buttons
//! - `GET /spaces/{space_id}` — space detail with invite management
//! - `GET /join/{code}` — invite landing page for browser users

use std::fmt::Write as _;
use std::sync::Arc;

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::{Extension, Form};
use serde::Deserialize;

use kutl_core::{MS_PER_HOUR, MS_PER_SECOND, SECONDS_PER_HOUR, SECONDS_PER_MINUTE};

use crate::invite_backend::{InviteBackend, InviteBackendError};
use crate::space_backend::SpaceBackend;

// ---------------------------------------------------------------------------
// HTML layout
// ---------------------------------------------------------------------------

/// Inline CSS for all admin pages — dark theme matching kutl's OKLCH palette.
const PAGE_CSS: &str = r"
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0a0a1a;
    color: #e0e0e0;
    font-family: system-ui, -apple-system, sans-serif;
    font-size: 15px;
    line-height: 1.5;
    padding: 2rem;
  }
  h1 { font-size: 1.5rem; font-weight: 600; margin-bottom: 1.5rem; color: #f0f0f0; }
  h2 { font-size: 1.1rem; font-weight: 600; margin-bottom: 1rem; color: #f0f0f0; }
  a { color: #3b82f6; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .card {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 8px;
    padding: 1rem 1.25rem;
    margin-bottom: 0.75rem;
  }
  .card-row {
    display: flex;
    align-items: center;
    gap: 1rem;
  }
  .card-name { flex: 1; font-weight: 500; }
  .card-meta { font-size: 0.8rem; color: #94a3b8; font-family: monospace; }
  .btn {
    display: inline-block;
    padding: 0.35rem 0.75rem;
    border-radius: 6px;
    border: none;
    cursor: pointer;
    font-size: 0.85rem;
    font-weight: 500;
    text-decoration: none;
    background: #3b82f6;
    color: #fff;
  }
  .btn:hover { background: #2563eb; text-decoration: none; }
  .btn-danger { background: #dc2626; }
  .btn-danger:hover { background: #b91c1c; }
  .badge {
    display: inline-block;
    font-size: 0.75rem;
    padding: 0.15rem 0.5rem;
    border-radius: 4px;
    font-weight: 500;
  }
  .badge-ok { background: #14532d; color: #4ade80; }
  .badge-exp { background: #450a0a; color: #fca5a5; }
  .back { margin-bottom: 1.25rem; font-size: 0.9rem; }
  .empty { color: #64748b; font-style: italic; margin-top: 0.5rem; }
  .monospace { font-family: monospace; font-size: 0.85rem; }
  .cmd-block {
    background: #0f172a;
    border: 1px solid #334155;
    border-radius: 6px;
    padding: 0.75rem 1rem;
    font-family: monospace;
    font-size: 0.85rem;
    color: #94a3b8;
    margin-top: 0.5rem;
    word-break: break-all;
  }
  .join-box { max-width: 480px; margin: 0 auto; text-align: center; }
  .join-box .card { text-align: left; margin-top: 1.5rem; }
  .join-title { font-size: 1.8rem; margin-bottom: 0.5rem; }
  .join-sub { color: #94a3b8; margin-bottom: 2rem; }
  .open-btn {
    display: inline-block;
    padding: 0.75rem 1.75rem;
    background: #3b82f6;
    color: #fff;
    border-radius: 8px;
    font-size: 1rem;
    font-weight: 600;
    text-decoration: none;
    margin-top: 1rem;
  }
  .open-btn:hover { background: #2563eb; text-decoration: none; }
";

/// Render a complete HTML page with the given `title` and `body` HTML.
///
/// Uses an inline dark theme matching kutl's OKLCH palette. No external
/// resources, no JavaScript.
pub(crate) fn page(title: &str, body: &str) -> String {
    let title_esc = html_escape(title);
    format!(
        "<!DOCTYPE html>\n\
         <html lang=\"en\">\n\
         <head>\n\
         <meta charset=\"UTF-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{title_esc}</title>\n\
         <style>{PAGE_CSS}</style>\n\
         </head>\n\
         <body>\n\
         {body}\n\
         </body>\n\
         </html>"
    )
}

/// Escape a string for safe inclusion in HTML text content and attributes.
///
/// Replaces `&`, `<`, `>`, `"`, and `'` with their HTML entity equivalents.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

// ---------------------------------------------------------------------------
// Relay URL helper
// ---------------------------------------------------------------------------

/// Shared web state threaded through handlers via `Extension`.
#[derive(Clone)]
pub(crate) struct WebState {
    /// WebSocket URL for the relay (e.g. `ws://localhost:9100`).
    ///
    /// Derived from `external_url` if set, otherwise constructed from
    /// `host:port`. HTTP(S) schemes are converted to WS(S).
    pub relay_ws_url: String,
    /// HTTP base URL for the relay (e.g. `http://localhost:9100`).
    pub relay_http_url: String,
}

impl WebState {
    /// Build `WebState` from optional `external_url`, `host`, and `port`.
    pub(crate) fn new(external_url: Option<&str>, host: &str, port: u16) -> Self {
        let http_base =
            external_url.map_or_else(|| format!("http://{host}:{port}"), ToString::to_string);

        // Convert http(s):// to ws(s)://.
        // NOTE: this mirrors kutl_client::http_url_to_ws — keep in sync.
        let ws_base = if let Some(rest) = http_base.strip_prefix("https://") {
            format!("wss://{rest}")
        } else if let Some(rest) = http_base.strip_prefix("http://") {
            format!("ws://{rest}")
        } else {
            // Already a ws:// URL or unknown scheme — pass through.
            http_base.clone()
        };

        Self {
            relay_ws_url: ws_base,
            relay_http_url: http_base,
        }
    }
}

// ---------------------------------------------------------------------------
// GET / — space list
// ---------------------------------------------------------------------------

/// `GET /` — list all registered spaces with "Generate invite" buttons.
///
/// Requires both a `SpaceBackend` and `InviteBackend` to be mounted.
///
/// **Auth:** unauthenticated. The OSS relay has no HTTP auth layer.
/// Deployers should restrict access via private networks or a reverse
/// proxy with SSO (e.g. `OAuth2` Proxy, Authelia).
pub(crate) async fn handle_index(
    Extension(space_backend): Extension<Arc<dyn SpaceBackend>>,
    Extension(_invite_backend): Extension<Arc<dyn InviteBackend>>,
) -> Response {
    let spaces = match space_backend.list_spaces().await {
        Ok(s) => s,
        Err(e) => {
            let body = format!(
                "<h1>kutl relay</h1><p style='color:#fca5a5'>error listing spaces: {}</p>",
                html_escape(&e.to_string())
            );
            return Html(page("kutl relay", &body)).into_response();
        }
    };

    let mut rows = String::new();
    if spaces.is_empty() {
        rows.push_str("<p class=\"empty\">No spaces registered yet.</p>");
    } else {
        for space in &spaces {
            let name_esc = html_escape(&space.name);
            let id_esc = html_escape(&space.space_id);
            let _ = write!(
                rows,
                r#"<div class="card">
  <div class="card-row">
    <span class="card-name"><a href="/spaces/{id_esc}">{name_esc}</a></span>
    <span class="card-meta">{id_esc}</span>
    <form method="POST" action="/invites/form" style="display:inline">
      <input type="hidden" name="space_id" value="{id_esc}">
      <button type="submit" class="btn">Generate invite</button>
    </form>
  </div>
</div>"#
            );
        }
    }

    let body = format!("<h1>kutl relay \u{2014} spaces</h1>\n{rows}");
    Html(page("kutl relay", &body)).into_response()
}

// ---------------------------------------------------------------------------
// GET /spaces/{space_id} — space detail
// ---------------------------------------------------------------------------

/// `GET /spaces/{space_id}` — space detail with invite list and controls.
///
/// **Auth:** unauthenticated — see [`handle_index`].
pub(crate) async fn handle_space_detail(
    Extension(space_backend): Extension<Arc<dyn SpaceBackend>>,
    Extension(invite_backend): Extension<Arc<dyn InviteBackend>>,
    Path(space_id): Path<String>,
) -> Response {
    // Point lookup by UUID instead of scanning all spaces.
    let space = match space_backend.resolve_by_id(&space_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Html(page("not found", "<p>space not found</p>")),
            )
                .into_response();
        }
        Err(e) => {
            let body = format!(
                "<p style='color:#fca5a5'>error: {}</p>",
                html_escape(&e.to_string())
            );
            return Html(page("error", &body)).into_response();
        }
    };

    let invites = match invite_backend.list_invites(&space_id) {
        Ok(i) => i,
        Err(e) => {
            let body = format!(
                "<h1>{}</h1><p style='color:#fca5a5'>error listing invites: {}</p>",
                html_escape(&space.name),
                html_escape(&e.to_string())
            );
            return Html(page(&space.name, &body)).into_response();
        }
    };

    let now = kutl_core::env::now_ms();
    let mut invite_rows = String::new();

    if invites.is_empty() {
        invite_rows.push_str("<p class=\"empty\">No invite codes yet.</p>");
    } else {
        for inv in &invites {
            let code_esc = html_escape(&inv.code);
            let expired = inv.expires_at.is_some_and(|exp| exp <= now);
            let badge = if expired {
                r#"<span class="badge badge-exp">expired</span>"#
            } else {
                r#"<span class="badge badge-ok">valid</span>"#
            };

            let expiry_text = match inv.expires_at {
                None => "no expiry".to_owned(),
                Some(exp) => {
                    if expired {
                        "expired".to_owned()
                    } else {
                        let remaining_secs = (exp - now) / MS_PER_SECOND;
                        format_duration_secs(remaining_secs)
                    }
                }
            };

            let space_id_esc = html_escape(&space_id);
            let _ = write!(
                invite_rows,
                r#"<div class="card">
  <div class="card-row">
    <span class="card-name monospace">{code_esc}</span>
    {badge}
    <span class="card-meta">{expiry_text}</span>
    <form method="POST" action="/invites/revoke" style="display:inline">
      <input type="hidden" name="code" value="{code_esc}">
      <input type="hidden" name="space_id" value="{space_id_esc}">
      <button type="submit" class="btn btn-danger">Revoke</button>
    </form>
  </div>
</div>"#
            );
        }
    }

    let name_esc = html_escape(&space.name);
    let sid_esc = html_escape(&space_id);
    let body = format!(
        r#"<div class="back"><a href="/">&#8592; all spaces</a></div>
<h1>{name_esc}</h1>
<p class="card-meta" style="margin-bottom:1.25rem">{sid_esc}</p>
<div style="display:flex; align-items:center; gap:1rem; margin-bottom:1rem">
  <h2 style="margin:0">Invite codes</h2>
  <form method="POST" action="/invites/form">
    <input type="hidden" name="space_id" value="{sid_esc}">
    <button type="submit" class="btn">Generate invite</button>
  </form>
</div>
{invite_rows}"#
    );

    Html(page(&space.name, &body)).into_response()
}

// ---------------------------------------------------------------------------
// GET /join/{code} — invite landing page
// ---------------------------------------------------------------------------

/// `GET /join/{code}` — browser-facing invite landing page.
///
/// Shows the space name, an "Open in kutl" button that links to a
/// `kutl://join?...` URL, and a fallback CLI command. The existing
/// `GET /invites/{code}` endpoint remains the JSON endpoint for
/// programmatic (CLI/daemon) access.
///
/// **Auth:** unauthenticated — see [`handle_index`].
pub(crate) async fn handle_join_page(
    Extension(invite_backend): Extension<Arc<dyn InviteBackend>>,
    Extension(web_state): Extension<WebState>,
    Path(code): Path<String>,
) -> Response {
    let info = match invite_backend.validate_invite(&code) {
        Ok(Some(info)) => info,
        Ok(None) => {
            let body = r#"<div class="join-box">
  <h1 class="join-title">Invite not found</h1>
  <p class="join-sub">This invite link is invalid or has already been used.</p>
</div>"#;
            return (StatusCode::NOT_FOUND, Html(page("invite not found", body))).into_response();
        }
        Err(InviteBackendError::Expired(_)) => {
            let body = r#"<div class="join-box">
  <h1 class="join-title">Invite expired</h1>
  <p class="join-sub">This invite link has expired. Ask the space owner for a new one.</p>
</div>"#;
            return (StatusCode::GONE, Html(page("invite expired", body))).into_response();
        }
        Err(e) => {
            let body = format!(
                r#"<div class="join-box">
  <h1 class="join-title">Error</h1>
  <p class="join-sub">{}</p>
</div>"#,
                html_escape(&e.to_string())
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(page("error", &body)),
            )
                .into_response();
        }
    };

    let space_name_esc = html_escape(&info.space_name);
    let space_id_esc = html_escape(&info.space_id);
    let code_esc = html_escape(&code);
    let ws_url_esc = html_escape(&web_state.relay_ws_url);
    let http_url_esc = html_escape(&web_state.relay_http_url);

    // Build the kutl:// deep-link URL.
    let kutl_url = format!(
        "kutl://join?relay={ws}&space_id={sid}&space_name={name}",
        ws = percent_encode(&web_state.relay_ws_url),
        sid = percent_encode(&info.space_id),
        name = percent_encode(&info.space_name),
    );
    let kutl_url_esc = html_escape(&kutl_url);

    // Build the fallback CLI command.
    let invite_url = format!("{http_url_esc}/join/{code_esc}");
    let cli_cmd = format!("kutl join {invite_url}");

    let body = format!(
        r#"<div class="join-box">
  <h1 class="join-title">Join <em>{space_name_esc}</em></h1>
  <p class="join-sub">You've been invited to collaborate on this space.</p>
  <p><strong>Space:</strong> {space_name_esc}</p>
  <p class="card-meta">{space_id_esc}</p>
  <p><strong>Relay:</strong> <span class="monospace">{ws_url_esc}</span></p>
  <a href="{kutl_url_esc}" class="open-btn">Open in kutl</a>
  <div class="card" style="margin-top:2rem">
    <h2>Or use the CLI</h2>
    <div class="cmd-block">{cli_cmd}</div>
  </div>
</div>"#
    );

    Html(page(&format!("Join {}", info.space_name), &body)).into_response()
}

// ---------------------------------------------------------------------------
// POST /invites/form — form-based invite creation
// ---------------------------------------------------------------------------

/// Form body for invite creation via HTML form submission.
#[derive(Deserialize)]
pub(crate) struct CreateInviteForm {
    /// Space UUID to create the invite for.
    pub space_id: String,
    /// Optional expiry in hours.
    pub expires_in_hours: Option<u32>,
}

/// `POST /invites/form` — create an invite and redirect to the space detail page.
///
/// Accepts `application/x-www-form-urlencoded` and responds with `303 See Other`
/// redirecting to `/spaces/{space_id}`. This allows browser HTML forms to trigger
/// invite creation without JavaScript.
///
/// **Auth:** unauthenticated — see [`handle_index`].
pub(crate) async fn handle_create_invite_form(
    Extension(invite_backend): Extension<Arc<dyn InviteBackend>>,
    Form(form): Form<CreateInviteForm>,
) -> Response {
    let expires_at = form.expires_in_hours.map(|h| {
        let now = kutl_core::env::now_ms();
        now + i64::from(h) * MS_PER_HOUR
    });

    let space_id = form.space_id.clone();

    match invite_backend.create_invite(&space_id, expires_at) {
        Ok(_) => Redirect::to(&format!("/spaces/{}", percent_encode(&space_id))).into_response(),
        Err(InviteBackendError::SpaceNotFound(_)) => (
            StatusCode::NOT_FOUND,
            Html(page("not found", "<p>space not found</p>")),
        )
            .into_response(),
        Err(e) => {
            let body = format!(
                "<p style='color:#fca5a5'>error creating invite: {}</p>",
                html_escape(&e.to_string())
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(page("error", &body)),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// POST /invites/revoke — form-based invite revocation
// ---------------------------------------------------------------------------

/// Form body for invite revocation via HTML form submission.
#[derive(Deserialize)]
pub(crate) struct RevokeInviteForm {
    /// Invite code to revoke.
    pub code: String,
    /// Space UUID to redirect back to after revocation.
    pub space_id: String,
}

/// `POST /invites/revoke` — revoke an invite and redirect to the space detail page.
///
/// Accepts `application/x-www-form-urlencoded` and responds with `303 See Other`.
///
/// **Auth:** unauthenticated — see [`handle_index`].
pub(crate) async fn handle_revoke_invite_form(
    Extension(invite_backend): Extension<Arc<dyn InviteBackend>>,
    Form(form): Form<RevokeInviteForm>,
) -> Response {
    let space_id = form.space_id.clone();
    // Ignore whether the invite existed — idempotent from the user's perspective.
    let _ = invite_backend.revoke_invite(&form.code);
    Redirect::to(&format!("/spaces/{}", percent_encode(&space_id))).into_response()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format a duration given in seconds as a human-readable string.
///
/// Returns strings like "2h 15m", "45m", or "< 1m".
fn format_duration_secs(secs: i64) -> String {
    if secs <= 0 {
        return "< 1m".to_owned();
    }
    let hours = secs / SECONDS_PER_HOUR;
    let minutes = (secs % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE;
    match (hours, minutes) {
        (0, m) if m < 1 => "< 1m".to_owned(),
        (0, m) => format!("{m}m"),
        (h, 0) => format!("{h}h"),
        (h, m) => format!("{h}h {m}m"),
    }
}

/// Percent-encode a string for use in URLs.
///
/// Encodes all non-unreserved characters (letters, digits, `-`, `_`, `.`, `~`).
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            b => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_html_escape_special_chars() {
        assert_eq!(html_escape("<script>"), "&lt;script&gt;");
        assert_eq!(html_escape("a & b"), "a &amp; b");
        assert_eq!(html_escape(r#"say "hi""#), "say &quot;hi&quot;");
        assert_eq!(html_escape("it's"), "it&#39;s");
    }

    #[test]
    fn test_percent_encode_unreserved() {
        assert_eq!(percent_encode("hello"), "hello");
        assert_eq!(percent_encode("hello-world"), "hello-world");
        assert_eq!(percent_encode("hello world"), "hello%20world");
        assert_eq!(percent_encode("ws://host:9100"), "ws%3A%2F%2Fhost%3A9100");
    }

    #[test]
    fn test_web_state_http_to_ws() {
        let s = WebState::new(Some("http://relay.example.com"), "0.0.0.0", 9100);
        assert_eq!(s.relay_ws_url, "ws://relay.example.com");
        assert_eq!(s.relay_http_url, "http://relay.example.com");
    }

    #[test]
    fn test_web_state_https_to_wss() {
        let s = WebState::new(Some("https://relay.example.com"), "0.0.0.0", 9100);
        assert_eq!(s.relay_ws_url, "wss://relay.example.com");
        assert_eq!(s.relay_http_url, "https://relay.example.com");
    }

    #[test]
    fn test_web_state_fallback() {
        let s = WebState::new(None, "localhost", 9100);
        assert_eq!(s.relay_ws_url, "ws://localhost:9100");
        assert_eq!(s.relay_http_url, "http://localhost:9100");
    }

    #[test]
    fn test_format_duration_secs_hours_and_minutes() {
        assert_eq!(format_duration_secs(7500), "2h 5m");
    }

    #[test]
    fn test_format_duration_secs_exact_hours() {
        assert_eq!(format_duration_secs(3600), "1h");
    }

    #[test]
    fn test_format_duration_secs_minutes() {
        assert_eq!(format_duration_secs(300), "5m");
    }

    #[test]
    fn test_format_duration_secs_less_than_minute() {
        assert_eq!(format_duration_secs(30), "< 1m");
        assert_eq!(format_duration_secs(0), "< 1m");
        assert_eq!(format_duration_secs(-5), "< 1m");
    }

    #[test]
    fn test_page_escapes_title() {
        let html = page("<script>", "body");
        assert!(html.contains("&lt;script&gt;"));
        assert!(!html.contains("<script>"));
    }
}
