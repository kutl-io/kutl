//! `kutl status` — collect and render client diagnostic state.
//!
//! Works whether the daemon is up or down. All collection
//! is file-or-network from the CLI process; no IPC to the daemon.
//!
//! Submodules:
//! - [`schema`] — stable JSON-serializable types.
//! - [`collect`] — file-system collection (registry, identity, PIDs).
//! - [`probe`] — relay reachability probe (network).
//! - [`format`] — human-readable renderer.

mod collect;
mod format;
mod probe;
mod schema;

use std::fmt::Write as _;

pub use collect::collect_static;
pub use format::render_human;
pub use probe::probe_relays;
pub use schema::ClientStatus;

use schema::{IdentityInfo, RelayInfo, SpaceInfo};

/// Reason a space is downgraded to unhealthy because its relay did not answer
/// the reachability probe.
const RELAY_UNREACHABLE_REASON: &str = "relay unreachable";

/// Reconcile each space's health against its relay's probed reachability.
///
/// [`collect_static`] sets `healthy` from local filesystem checks only, and
/// [`probe_relays`] fills in relay reachability independently. This folds the
/// two together: a space whose relay did not answer the probe is downgraded to
/// unhealthy so `kutl status` doesn't report a stranded space as healthy.
///
/// A space already unhealthy for a stronger, local reason (path missing, config
/// unreadable) keeps that reason — relay reachability is only the tie-breaker
/// for spaces that passed the static checks. Call this AFTER [`probe_relays`].
pub fn reconcile_space_health(snapshot: &mut ClientStatus) {
    for space in &mut snapshot.spaces {
        if !space.healthy {
            // Already flagged for a stronger, local reason; leave it.
            continue;
        }
        let relay_reachable = snapshot
            .relays
            .iter()
            .find(|r| r.url == space.relay_url)
            .is_none_or(|r| r.reachable);
        if !relay_reachable {
            space.healthy = false;
            space.unhealthy_reason = Some(RELAY_UNREACHABLE_REASON.to_owned());
        }
    }
}

/// Warning wording emitted, per frozen document, for a space with entries in
/// `docs_at_op_cap`. The `{doc}` placeholder is the document's relative path.
///
/// Shared by the aggregate `kutl status` renderer and the focused space render
/// so the two can never drift.
pub(super) const FROZEN_DOC_WARNING: &str =
    "! {doc} is at the edit-history cap; its edits no longer sync";

/// Warning wording emitted, per approaching document, for a space with
/// entries in `docs_approaching_op_cap` — the early warning while edits still
/// sync. Same sharing rule as [`FROZEN_DOC_WARNING`].
pub(super) const APPROACHING_DOC_WARNING: &str =
    "! {doc} is approaching the edit-history cap; split or compact it before edits stop syncing";

/// Render the registered spaces as a human-readable list — the body of
/// `kutl space list`.
///
/// One line per registered space: health mark, name, root path, and relay
/// URL (or the unhealthy reason for a space that could not be loaded).
/// Returns a short "no spaces registered" notice when the registry is empty.
pub fn render_space_list(status: &ClientStatus) -> String {
    let mut out = String::new();
    if status.spaces.is_empty() {
        let _ = writeln!(out, "no spaces registered");
        return out;
    }
    let _ = writeln!(
        out,
        "spaces ({count} registered):",
        count = status.spaces.len()
    );
    for space in &status.spaces {
        write_space_line(&mut out, space);
    }
    out
}

/// Write one focused `kutl space list` line for a single registered space,
/// followed by one warning line per document frozen at the op cap.
///
/// Renders the same fields as the aggregate `kutl status` space section
/// (health mark, name, path, relay + last-activity, or the unhealthy reason)
/// so the focused view can't silently drop information the aggregate carries.
fn write_space_line(out: &mut String, space: &SpaceInfo) {
    let mark = if space.healthy { "✓" } else { "✗" };
    if space.healthy {
        let _ = writeln!(
            out,
            "  {mark} {name} {path} relay={relay} last-activity={activity} signals={signals}",
            name = space.name,
            path = space.path,
            relay = space.relay_url,
            activity = space_activity(space),
            signals = space.open_signals,
        );
    } else {
        let _ = writeln!(
            out,
            "  {mark} {name} {path} <{reason}>",
            name = space.name,
            path = space.path,
            reason = space.unhealthy_reason.as_deref().unwrap_or("unhealthy"),
        );
    }
    write_frozen_docs(out, space);
}

/// Format a space's last-activity age, falling back to `—` when unknown.
///
/// Shared by the aggregate `kutl status` renderer and the focused space render
/// so the two never drift.
pub(super) fn space_activity(space: &SpaceInfo) -> String {
    match space.last_activity_seconds {
        Some(secs) => format::format_age(secs),
        None => "—".into(),
    }
}

/// Write one [`FROZEN_DOC_WARNING`] line per document frozen at the op cap
/// and one [`APPROACHING_DOC_WARNING`] line per document nearing it.
///
/// Shared by the aggregate `kutl status` renderer and the focused space render
/// so the op-cap warning wording never drifts between the two.
pub(super) fn write_frozen_docs(out: &mut String, space: &SpaceInfo) {
    for doc in &space.docs_at_op_cap {
        let _ = writeln!(
            out,
            "    {line}",
            line = FROZEN_DOC_WARNING.replace("{doc}", doc)
        );
    }
    for doc in &space.docs_approaching_op_cap {
        let _ = writeln!(
            out,
            "    {line}",
            line = APPROACHING_DOC_WARNING.replace("{doc}", doc)
        );
    }
}

/// Render the focused space section — the body of `kutl space status`.
///
/// The caller has already scoped the snapshot to the space enclosing the
/// current directory, so this renders the space line(s) under a plain
/// `space:` header — never the registry count, which would misreport the
/// scoped view. Deliberately omits the daemon and identity sections that
/// the aggregate `kutl status` carries.
pub fn render_space_status(status: &ClientStatus) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "space:");
    for space in &status.spaces {
        write_space_line(&mut out, space);
    }
    write_relays(&mut out, &status.relays);
    out
}

/// Append the relays section to `out` when any relay is present.
fn write_relays(out: &mut String, relays: &[RelayInfo]) {
    if relays.is_empty() {
        return;
    }
    let _ = writeln!(out, "relays:");
    for relay in relays {
        if relay.reachable {
            let _ = writeln!(out, "  {url}   reachable", url = relay.url);
        } else {
            let err = relay.error.as_deref().unwrap_or("not probed");
            let _ = writeln!(out, "  {url}   unreachable ({err})", url = relay.url);
        }
    }
}

/// Render the focused daemon section — the body of `kutl daemon status`.
///
/// Shows the daemon liveness line (and the desktop tray line when running).
/// Deliberately omits the spaces, relays, and identity sections.
pub fn render_daemon_status(status: &ClientStatus) -> String {
    let mut out = String::new();
    write_daemon(&mut out, status);
    out
}

/// Write the daemon liveness line into `out`, followed by the desktop tray
/// line when the tray app is running.
///
/// Shared by the aggregate `kutl status` renderer and the focused
/// `kutl daemon status` render so the two never drift.
pub(super) fn write_daemon(out: &mut String, status: &ClientStatus) {
    match (status.daemon.running, status.daemon.pid) {
        (true, Some(pid)) => {
            let _ = writeln!(
                out,
                "daemon: running (PID {pid}, $KUTL_HOME={home})",
                home = status.kutl_home
            );
        }
        _ => {
            let _ = writeln!(
                out,
                "daemon: not running ($KUTL_HOME={home})",
                home = status.kutl_home
            );
        }
    }
    if let (true, Some(pid)) = (status.desktop.running, status.desktop.pid) {
        let _ = writeln!(out, "desktop: running (PID {pid})");
    }
}

/// Render the focused identity section — the body of `kutl auth status`.
///
/// Shows the auth/identity summary and its static fields (DID, name,
/// email). Deliberately omits the daemon, spaces, and relays sections.
pub fn render_auth_status(status: &ClientStatus) -> String {
    let mut out = String::new();
    write_identity(&mut out, status.identity.as_ref());
    out
}

/// Write the identity section into `out`. Header line summarises auth state;
/// indented sub-lines carry the static identity fields (DID, name, email).
/// Shared by the aggregate `kutl status` renderer and `kutl auth status` so
/// the two never drift.
pub(super) fn write_identity(out: &mut String, identity: Option<&IdentityInfo>) {
    let Some(id) = identity else {
        let _ = writeln!(out, "identity: none provisioned");
        return;
    };

    if id.from_env {
        let prefix = id.token_prefix.as_deref().unwrap_or("");
        let _ = writeln!(out, "identity: token from $KUTL_TOKEN ({prefix}…)");
    } else if let Some(prefix) = id.token_prefix.as_deref() {
        let label = match (id.display_name.as_deref(), id.account_id.as_deref()) {
            (Some(name), Some(acct)) => format!("{name} ({acct})"),
            (Some(s), None) | (None, Some(s)) => s.to_owned(),
            (None, None) => format!("token {prefix}…"),
        };
        match id.relay_url.as_deref() {
            Some(url) => {
                let _ = writeln!(out, "identity: {label} @ {url}");
            }
            None => {
                let _ = writeln!(out, "identity: {label}");
            }
        }
    } else {
        let _ = writeln!(out, "identity: local (no relay token yet)");
    }

    if let Some(did) = id.did.as_deref() {
        let _ = writeln!(out, "  did:   {did}");
    }
    if let Some(name) = id.display_name.as_deref() {
        let _ = writeln!(out, "  name:  {name}");
    }
    if let Some(email) = id.email.as_deref() {
        let _ = writeln!(out, "  email: {email}");
    }
}

#[cfg(test)]
mod tests {
    use super::schema::{DaemonInfo, RelayInfo, SpaceInfo};
    use super::*;

    /// Build a `ClientStatus` fixture with the given spaces and no daemon,
    /// identity, or relays — enough to exercise the render helpers.
    fn fixture_with_spaces(spaces: Vec<SpaceInfo>) -> ClientStatus {
        let relays = spaces
            .iter()
            .filter(|s| !s.relay_url.is_empty())
            .map(|s| RelayInfo {
                url: s.relay_url.clone(),
                reachable: false,
                error: None,
            })
            .collect();
        ClientStatus {
            kutl_home: "/tmp/.kutl".into(),
            daemon: DaemonInfo {
                running: false,
                pid: None,
            },
            desktop: DaemonInfo {
                running: false,
                pid: None,
            },
            identity: None,
            spaces,
            relays,
        }
    }

    /// Build a healthy `SpaceInfo` fixture.
    fn healthy_space(name: &str, path: &str, relay: &str) -> SpaceInfo {
        SpaceInfo {
            path: path.into(),
            name: name.into(),
            space_id: format!("{name}-id"),
            relay_url: relay.into(),
            healthy: true,
            unhealthy_reason: None,
            last_activity_seconds: Some(5),
            docs_at_op_cap: vec![],
            docs_approaching_op_cap: vec![],
            open_signals: 0,
            surface_target: None,
        }
    }

    /// Build a healthy `SpaceInfo` fixture with a document frozen at the op cap.
    fn frozen_doc_space(name: &str, path: &str, relay: &str, doc: &str) -> SpaceInfo {
        let mut space = healthy_space(name, path, relay);
        space.docs_at_op_cap = vec![doc.into()];
        space
    }

    /// Both renderers must carry the approaching-cap early warning — a doc
    /// nearing the cap that status stays silent about becomes the wedge
    /// discovered only at the freeze.
    #[test]
    fn test_space_render_carries_approaching_cap_warning() {
        let mut space = healthy_space("alpha", "/x/alpha", "ws://r1/ws");
        space.docs_approaching_op_cap = vec!["rfd/hot.md".into()];
        let status = fixture_with_spaces(vec![space]);
        let rendered = render_space_list(&status);
        assert!(
            rendered.contains("! rfd/hot.md is approaching the edit-history cap"),
            "rendered:\n{rendered}"
        );
    }

    #[test]
    fn test_reconcile_space_health_downgrades_unreachable_relay() {
        let mut status =
            fixture_with_spaces(vec![healthy_space("alpha", "/x/alpha", "ws://r1/ws")]);
        // The matching relay probed as unreachable.
        status.relays = vec![RelayInfo {
            url: "ws://r1/ws".into(),
            reachable: false,
            error: Some("connection refused".into()),
        }];

        reconcile_space_health(&mut status);

        assert!(
            !status.spaces[0].healthy,
            "a space whose relay is unreachable must be unhealthy"
        );
        assert_eq!(
            status.spaces[0].unhealthy_reason.as_deref(),
            Some("relay unreachable"),
        );
    }

    #[test]
    fn test_reconcile_space_health_leaves_reachable_relay_untouched() {
        let mut status =
            fixture_with_spaces(vec![healthy_space("alpha", "/x/alpha", "ws://r1/ws")]);
        status.relays = vec![RelayInfo {
            url: "ws://r1/ws".into(),
            reachable: true,
            error: None,
        }];

        reconcile_space_health(&mut status);

        assert!(
            status.spaces[0].healthy,
            "a space with a reachable relay stays healthy"
        );
        assert!(status.spaces[0].unhealthy_reason.is_none());
    }

    #[test]
    fn test_render_space_list_shows_frozen_doc_warning() {
        let status = fixture_with_spaces(vec![frozen_doc_space(
            "alpha",
            "/x/alpha",
            "ws://r1/ws",
            "notes/big.md",
        )]);

        let out = render_space_list(&status);

        assert!(
            out.contains("! notes/big.md is at the edit-history cap; its edits no longer sync"),
            "space list must warn about the frozen doc:\n{out}"
        );
    }

    #[test]
    fn test_render_space_status_shows_frozen_doc_warning() {
        let status = fixture_with_spaces(vec![frozen_doc_space(
            "alpha",
            "/x/alpha",
            "ws://r1/ws",
            "notes/big.md",
        )]);

        let out = render_space_status(&status);

        assert!(
            out.contains("! notes/big.md is at the edit-history cap; its edits no longer sync"),
            "space status must warn about the frozen doc:\n{out}"
        );
    }

    #[test]
    fn test_focused_and_aggregate_share_frozen_warning() {
        // The focused space render and the aggregate `kutl status` render must
        // emit the identical frozen-doc warning line so they can't drift.
        let make = || {
            fixture_with_spaces(vec![frozen_doc_space(
                "alpha",
                "/x/alpha",
                "ws://r1/ws",
                "notes/big.md",
            )])
        };
        let warning = "! notes/big.md is at the edit-history cap; its edits no longer sync";
        assert!(
            render_human(&make()).contains(warning),
            "aggregate must warn:\n{out}",
            out = render_human(&make())
        );
        assert!(
            render_space_list(&make()).contains(warning),
            "focused must warn:\n{out}",
            out = render_space_list(&make())
        );
    }

    #[test]
    fn test_render_space_list_shows_open_signal_count() {
        let mut space = healthy_space("alpha", "/x/alpha", "ws://r1/ws");
        space.open_signals = 4;
        let status = fixture_with_spaces(vec![space]);

        let out = render_space_list(&status);

        assert!(
            out.contains("signals=4"),
            "focused space list must show the open-signal count:\n{out}"
        );
    }

    #[test]
    fn test_render_space_list_shows_all_spaces() {
        let status = fixture_with_spaces(vec![
            healthy_space("alpha", "/x/alpha", "ws://r1/ws"),
            healthy_space("beta", "/y/beta", "ws://r2/ws"),
        ]);

        let out = render_space_list(&status);

        assert!(!out.is_empty(), "render must be non-empty");
        for needle in [
            "alpha",
            "/x/alpha",
            "ws://r1/ws",
            "beta",
            "/y/beta",
            "ws://r2/ws",
        ] {
            assert!(out.contains(needle), "missing {needle} in:\n{out}");
        }
    }

    #[test]
    fn test_render_space_list_empty_registry() {
        let status = fixture_with_spaces(vec![]);
        let out = render_space_list(&status);
        assert!(out.contains("no spaces registered"), "got:\n{out}");
    }

    /// A `ClientStatus` fixture that populates every section: a running
    /// daemon, one healthy space, one reachable relay, and a full identity.
    /// Used by the focused-render tests to prove each helper shows its own
    /// section and omits the others.
    fn fixture_all_sections() -> ClientStatus {
        ClientStatus {
            kutl_home: "/tmp/.kutl".into(),
            daemon: DaemonInfo {
                running: true,
                pid: Some(4242),
            },
            desktop: DaemonInfo {
                running: false,
                pid: None,
            },
            identity: Some(IdentityInfo {
                did: Some("did:key:zAlice".into()),
                display_name: Some("Alice".into()),
                email: Some("alice@example.com".into()),
                account_id: Some("acct-9".into()),
                token_prefix: Some("kutl_abc1234".into()),
                relay_url: Some("wss://relay.example.com/ws".into()),
                from_env: false,
            }),
            spaces: vec![healthy_space("alpha", "/x/alpha", "ws://r1/ws")],
            relays: vec![RelayInfo {
                url: "ws://r1/ws".into(),
                reachable: true,
                error: None,
            }],
        }
    }

    #[test]
    fn test_render_space_status_shows_spaces_and_relays_only() {
        let out = render_space_status(&fixture_all_sections());
        assert!(out.contains("alpha"), "should show the space:\n{out}");
        assert!(out.contains("/x/alpha"), "should show the root:\n{out}");
        assert!(out.contains("ws://r1/ws"), "should show the relay:\n{out}");
        assert!(
            out.contains("reachable"),
            "should show reachability:\n{out}"
        );
        // Omits the daemon and identity sections.
        assert!(!out.contains("daemon:"), "must omit daemon:\n{out}");
        assert!(!out.contains("identity:"), "must omit identity:\n{out}");
        assert!(!out.contains("Alice"), "must omit identity:\n{out}");
    }

    #[test]
    fn test_render_daemon_status_shows_daemon_only() {
        let out = render_daemon_status(&fixture_all_sections());
        assert!(
            out.contains("daemon: running (PID 4242"),
            "should show the daemon line:\n{out}"
        );
        // Omits spaces, relays, and identity.
        assert!(!out.contains("alpha"), "must omit spaces:\n{out}");
        assert!(!out.contains("relays:"), "must omit relays:\n{out}");
        assert!(!out.contains("identity:"), "must omit identity:\n{out}");
    }

    #[test]
    fn test_render_auth_status_shows_identity_only() {
        let out = render_auth_status(&fixture_all_sections());
        assert!(out.contains("Alice"), "should show display name:\n{out}");
        assert!(
            out.contains("did:key:zAlice"),
            "should show the DID:\n{out}"
        );
        assert!(
            out.contains("alice@example.com"),
            "should show the email:\n{out}"
        );
        // Omits daemon and spaces.
        assert!(!out.contains("daemon:"), "must omit daemon:\n{out}");
        assert!(!out.contains("alpha"), "must omit spaces:\n{out}");
    }

    #[test]
    fn test_render_auth_status_none_provisioned() {
        let status = fixture_with_spaces(vec![]);
        let out = render_auth_status(&status);
        assert!(out.contains("identity: none provisioned"), "got:\n{out}");
    }
}
