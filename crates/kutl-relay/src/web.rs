//! Server-rendered HTML helpers and web admin page handlers.
//!
//! These routes are mounted when an `InviteBackend` is configured
//! (OSS relay with `data_dir`). They provide a minimal admin UI for
//! managing spaces and invite codes, using plain HTML forms, a compiled-in
//! stylesheet, and a small inline clipboard script for the "Copy invite link" button.
//!
//! Three pages are served:
//! - `GET /` — space list with "Generate invite" buttons
//! - `GET /spaces/{space_id}` — space detail with invite management
//! - `GET /join/{code}` — invite landing page for browser users

use std::fmt::Write as _;
use std::sync::Arc;

use axum::extract::Path;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::{Extension, Form};
use serde::Deserialize;

use kutl_core::{MS_PER_HOUR, MS_PER_SECOND, SECONDS_PER_HOUR, SECONDS_PER_MINUTE};

use crate::invite_backend::{InviteBackend, InviteBackendError, InviteRecord};
use crate::space_backend::{SpaceBackend, SpaceBackendError};
use crate::spaces;

// ---------------------------------------------------------------------------
// HTML layout
// ---------------------------------------------------------------------------

/// Brand stylesheet for all relay web pages, compiled into the binary.
/// Source: `src/assets/relay.css` (kutl.io OKLCH tokens, light + dark).
const PAGE_CSS: &str = include_str!("assets/relay.css");

/// Inline cuttlefish logo for the page header. Inherits color via
/// `currentColor`. Trademarked — see crate `NOTICE`.
const KUTL_LOGO_SVG: &str = include_str!("assets/kutl-logo.svg");

/// Standalone SVG favicon, served at `/favicon.svg`. Trademarked — see `NOTICE`.
const FAVICON_SVG: &str = include_str!("assets/favicon.svg");

/// Rajdhani `SemiBold` (Latin subset, OFL), served at `/assets/rajdhani.woff2`.
const RAJDHANI_WOFF2: &[u8] = include_bytes!("assets/rajdhani-latin-600-normal.woff2");

/// `Cache-Control` for the embedded static assets. One year
/// (`31_536_000` s) is the conventional "effectively forever" max-age; the
/// assets only change on a binary upgrade, so a long immutable TTL is safe.
const IMMUTABLE_CACHE: &str = "public, max-age=31536000, immutable";

/// Inline clipboard script for the "Copy invite link" buttons on the
/// space-detail page.
///
/// Each button carries the invite code in `data-code`; the absolute link is
/// built from `location.origin` at click time so it reflects whatever host the
/// admin is browsing (correct behind a reverse proxy, where the relay does not
/// know its own external URL). Falls back to a temporary textarea +
/// `execCommand` — and finally a `prompt` — when the async Clipboard API is
/// unavailable, which it is on plain-HTTP LAN origins (the API is gated to
/// secure contexts).
const COPY_INVITE_SCRIPT: &str = r"<script>
function kutlCopyInvite(btn) {
  var url = location.origin + '/join/' + btn.dataset.code;
  var done = function () {
    var prev = btn.textContent;
    btn.textContent = 'Copied!';
    setTimeout(function () { btn.textContent = prev; }, 1200);
  };
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(url).then(done, function () {
      window.prompt('Copy this invite link:', url);
    });
    return;
  }
  var ta = document.createElement('textarea');
  ta.value = url;
  ta.style.position = 'fixed';
  ta.style.opacity = '0';
  document.body.appendChild(ta);
  ta.focus();
  ta.select();
  try { document.execCommand('copy'); done(); }
  catch (e) { window.prompt('Copy this invite link:', url); }
  finally { document.body.removeChild(ta); }
}
document.querySelectorAll('.copy-invite').forEach(function (btn) {
  btn.addEventListener('click', function () { kutlCopyInvite(btn); });
});
</script>";

/// Render a complete HTML page with the given `title` and `body` HTML.
///
/// Wraps the body with kutl-relay branding (a favicon link and a header with the
/// cuttlefish logo and wordmark) and the bundled stylesheet, which adapts to
/// light and dark via `prefers-color-scheme`. The CSS and logo are compiled into
/// the binary; any JavaScript is supplied inline by the page body.
pub(crate) fn page(title: &str, body: &str) -> String {
    let title_esc = html_escape(title);
    format!(
        "<!DOCTYPE html>\n\
         <html lang=\"en\">\n\
         <head>\n\
         <meta charset=\"UTF-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\n\
         <title>{title_esc}</title>\n\
         <style>{PAGE_CSS}</style>\n\
         </head>\n\
         <body>\n\
         <header class=\"brand-header\"><a href=\"/\" aria-label=\"kutl relay home\">\
         <span class=\"brand-logo\">{KUTL_LOGO_SVG}</span>\
         <span class=\"brand-name\">kutl relay</span></a></header>\n\
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
            // Log the detailed backend error; render a generic message so
            // SQL fragments and storage internals are not exposed.
            tracing::error!(error = %e, "failed to list spaces");
            let body = "<h1>Spaces</h1><p class='error'>could not list spaces</p>".to_owned();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(page("kutl relay", &body)),
            )
                .into_response();
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

    let create_space = r#"<details class="create-space">
  <summary class="cta">+ New space</summary>
  <form method="POST" action="/spaces/form" class="create-form">
    <input type="text" name="name" aria-label="space name" placeholder="space-name" required pattern="[a-z0-9]+(-[a-z0-9]+)*" minlength="3" maxlength="50">
    <button type="submit" class="btn">Create</button>
  </form>
</details>"#;

    let body = format!("<h1>Spaces</h1>\n{create_space}\n{rows}");
    Html(page("kutl relay", &body)).into_response()
}

// ---------------------------------------------------------------------------
// GET /spaces/{space_id} — space detail
// ---------------------------------------------------------------------------

/// Render the invite-code cards for the space-detail page.
///
/// `now` is the current epoch-millis used to flag expiry. The per-card "Copy
/// invite link" button is emitted only for invites that are still redeemable,
/// so an expired link is never offered for copying.
fn render_invite_rows(invites: &[InviteRecord], now: i64, space_id: &str) -> String {
    if invites.is_empty() {
        return r#"<p class="empty">No invite codes yet.</p>"#.to_owned();
    }

    let space_id_esc = html_escape(space_id);
    let mut rows = String::new();
    for inv in invites {
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
                    format_duration_secs((exp - now) / MS_PER_SECOND)
                }
            }
        };

        // Only offer a copy affordance for invites that can still be
        // redeemed; copying an expired link would mislead.
        let copy_button = if expired {
            String::new()
        } else {
            format!(
                r#"<button type="button" class="btn copy-invite" data-code="{code_esc}">Copy invite link</button>"#
            )
        };

        let _ = write!(
            rows,
            r#"<div class="card">
  <div class="card-row">
    <span class="card-name monospace">{code_esc}</span>
    {badge}
    <span class="card-meta">{expiry_text}</span>
    {copy_button}
    <form method="POST" action="/invites/revoke" style="display:inline">
      <input type="hidden" name="code" value="{code_esc}">
      <input type="hidden" name="space_id" value="{space_id_esc}">
      <button type="submit" class="btn btn-danger">Revoke</button>
    </form>
  </div>
</div>"#
        );
    }
    rows
}

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
            // Generic message to the caller; details only in the server log.
            tracing::error!(space_id = %space_id, error = %e, "failed to resolve space");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(page("error", "<p class='error'>could not load space</p>")),
            )
                .into_response();
        }
    };

    let invites = match invite_backend.list_invites(&space_id) {
        Ok(i) => i,
        Err(e) => {
            tracing::error!(space_id = %space_id, error = %e, "failed to list invites");
            let body = format!(
                "<h1>{}</h1><p class='error'>could not list invites</p>",
                html_escape(&space.name),
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(page(&space.name, &body)),
            )
                .into_response();
        }
    };

    let now = kutl_core::env::now_ms();
    let invite_rows = render_invite_rows(&invites, now, &space_id);

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
{invite_rows}
{COPY_INVITE_SCRIPT}"#
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
                "<p class='error'>error creating invite: {}</p>",
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
// POST /spaces/form — form-based space creation
// ---------------------------------------------------------------------------

/// Form body for space creation via HTML form submission.
#[derive(Deserialize)]
pub(crate) struct CreateSpaceForm {
    /// Desired space name (slug rules — see [`spaces::validate_space_name`]).
    pub name: String,
}

/// `POST /spaces/form` — create a space and redirect to its detail page.
///
/// Accepts `application/x-www-form-urlencoded` and responds with `303 See Other`
/// redirecting to `/spaces/{space_id}` on success. This is the explicit space
/// registration path; unlike the implicit WebSocket path, it persists the space
/// in the configured `SpaceBackend`.
///
/// Mirrors the validate→register→map-errors logic of the JSON
/// `POST /spaces/register` handler but renders HTML error pages: the name is
/// validated via [`spaces::validate_space_name`] (invalid → 400), then registered
/// (`NameConflict` → 409, other → 500).
///
/// **Auth:** unauthenticated — see [`handle_index`].
pub(crate) async fn handle_create_space_form(
    Extension(space_backend): Extension<Arc<dyn SpaceBackend>>,
    Form(form): Form<CreateSpaceForm>,
) -> Response {
    if let Err(msg) = spaces::validate_space_name(&form.name) {
        let body = format!(
            "<p class='error'>invalid space name: {}</p>",
            html_escape(msg)
        );
        return (StatusCode::BAD_REQUEST, Html(page("invalid name", &body))).into_response();
    }

    match space_backend.register(&form.name).await {
        Ok(space) => {
            Redirect::to(&format!("/spaces/{}", percent_encode(&space.space_id))).into_response()
        }
        Err(SpaceBackendError::NameConflict(_)) => {
            let body = format!(
                "<p class='error'>space name already taken: {}</p>",
                html_escape(&form.name)
            );
            (StatusCode::CONFLICT, Html(page("name taken", &body))).into_response()
        }
        Err(SpaceBackendError::InvalidName(ref msg)) => {
            let body = format!(
                "<p class='error'>invalid space name: {}</p>",
                html_escape(msg)
            );
            (StatusCode::BAD_REQUEST, Html(page("invalid name", &body))).into_response()
        }
        Err(e) => {
            tracing::error!(name = %form.name, error = %e, "failed to register space");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(page(
                    "error",
                    "<p class='error'>could not create space — please try again</p>",
                )),
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
    match invite_backend.revoke_invite(&form.code) {
        // Ignore the not-found boolean — revocation is idempotent from the
        // user's perspective. Only a storage failure is surfaced.
        Ok(_) => Redirect::to(&format!("/spaces/{}", percent_encode(&space_id))).into_response(),
        Err(e) => {
            tracing::error!(
                space_id = %space_id,
                error = %e,
                "failed to revoke invite"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(page(
                    "error",
                    "<p class='error'>could not revoke invite — please try again</p>",
                )),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// GET /favicon.svg, GET /assets/rajdhani.woff2 — embedded static assets
// ---------------------------------------------------------------------------

/// `GET /favicon.svg` — the embedded SVG favicon.
pub(crate) async fn handle_favicon() -> Response {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, IMMUTABLE_CACHE),
        ],
        FAVICON_SVG,
    )
        .into_response()
}

/// `GET /assets/rajdhani.woff2` — the embedded Rajdhani `SemiBold` subset (OFL).
pub(crate) async fn handle_font_rajdhani() -> Response {
    (
        [
            (header::CONTENT_TYPE, "font/woff2"),
            (header::CACHE_CONTROL, IMMUTABLE_CACHE),
        ],
        RAJDHANI_WOFF2,
    )
        .into_response()
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

    #[test]
    fn test_page_includes_brand_header_and_favicon() {
        let html = page("title", "<p>hi</p>");
        assert!(
            html.contains(r#"<link rel="icon" type="image/svg+xml" href="/favicon.svg">"#),
            "page head should link the SVG favicon"
        );
        assert!(
            html.contains(r#"class="brand-header""#),
            "page should render the brand header"
        );
        assert!(
            html.contains("kutl relay"),
            "header should show the wordmark"
        );
        assert!(html.contains("<p>hi</p>"), "body should be embedded");
    }
}
