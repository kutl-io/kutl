//! File-based DID authorization for the open-source relay.
//!
//! Reads a plain-text file listing allowed DIDs (one per line), SSH
//! `authorized_keys`-shaped. Each line is a DID followed by optional
//! whitespace-separated `key=value` options:
//!
//! ```text
//! did:key:z…                                  # bare: full access, never expires
//! did:key:z… scope=<uuid>,<uuid> expiry=<unix_ms> name=<text> notes=<text>
//! ```
//!
//! `name` is what other participants see and address this DID by, so it is
//! public by definition. `notes` is an operator annotation that never leaves
//! the relay. They are separate fields precisely so neither role forces the
//! other's visibility: one field serving both would mean either hiding a name
//! nobody can then use, or publishing a line like `revoke after the contract`.
//!
//! A **bare** DID (no options) authorizes every space forever. A scoped entry
//! authorizes only the listed spaces; an entry with `expiry` is rejected once
//! `now_ms >= expiry`. Comments (`#`) and blank lines are ignored. The parsed
//! file is cached in memory and re-read only when its modification time or
//! length changes, so an edit takes effect on the next check without a relay
//! restart while per-op checks avoid a full read+parse of an unchanged file.
//!
//! **Malformed options fail CLOSED.** A line whose option tokens are all
//! well-formed is parsed normally; a bare DID with no option tokens keeps its
//! full-access-forever default. But if ANY option token is malformed — an
//! unparseable `expiry`, a token missing `=`, an empty/whitespace scope, a
//! `name` that could not be typed as an argument, or an **unrecognized key** —
//! the entire entry is DROPPED with a warning: that DID
//! gets no access from that line. A typo must never silently widen access (drop
//! an `expiry` so an entry never expires, or degrade a scoped entry to
//! all-spaces-forever). This is a security file, so the conservative default is
//! deny.

use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::SystemTime;

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

use tracing::warn;

/// Whether `value` is usable as a name a caller may type to address someone.
///
/// Checked where the file is parsed rather than where a name is used, so a
/// malformed one is an operator's problem at load time instead of a caller's at
/// send time. Rejects: the empty string; anything containing whitespace (it
/// would not survive being typed as one argument); a leading `-` (it would read
/// as a flag); and anything DID-shaped, because a surface resolving `--to` uses
/// a DID verbatim and must never have to guess which of the two it was handed.
///
/// Uniqueness is deliberately NOT checked here. This file is hand-edited, and a
/// duplicated name must never cost anyone access; the ambiguity is reported
/// where a name is resolved instead.
fn is_addressable_name(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('-')
        && !value.contains(char::is_whitespace)
        && !value.starts_with("did:")
}

/// Whether `query` — what a caller typed — names the participant called
/// `name`.
///
/// **The** participant-name comparison. One accessor answers the name
/// question for both backends — this file's own entries and the membership
/// backend's roster — so the two cannot disagree about whether `Egon` finds
/// `egon`; each answers by calling this rather than comparing for itself. A
/// backend querying storage has to fold case there too — an
/// ASCII-case-insensitive collation, not a bare equality — and this is the
/// rule it is matching.
///
/// Case-insensitive, and otherwise exact. Case is the one axis where a
/// difference is never a different person: names are written lowercase in a
/// file and typed capitalised by anyone treating them as proper nouns, which
/// cost two real addressing attempts in a demo run. Prefixes and near-misses
/// stay refused — those CAN name someone else, and guessing sends their mail.
///
/// ASCII-only folding, deliberately: `to_lowercase` on arbitrary Unicode is
/// locale-shaped, and an identifier that resolves differently by locale is
/// worse than one that resolves strictly.
#[must_use]
pub fn name_matches(name: &str, query: &str) -> bool {
    name.eq_ignore_ascii_case(query)
}

/// Whether `query` — one or more `/`-separated segments — names the
/// participant whose full name is `name`.
///
/// Matches whole trailing segments: `accountant` and `cfo/accountant`
/// both name `boris/cfo/accountant`; `fo/accountant` does not. Each
/// segment folds case by the same rule as [`name_matches`], and every
/// surface that resolves a typed name must come through here — two
/// resolvers matching separately is how they drift.
#[must_use]
pub fn name_path_matches(name: &str, query: &str) -> bool {
    // An empty query must never resolve anyone: without this guard it
    // matches any name ending in '/', because splitting "" on '/' yields
    // one empty segment that trivially equals a trailing empty segment.
    if query.is_empty() {
        return false;
    }
    let name_segs: Vec<&str> = name.split('/').collect();
    let query_segs: Vec<&str> = query.split('/').collect();
    if query_segs.len() > name_segs.len() {
        return false;
    }
    name_segs
        .iter()
        .rev()
        .zip(query_segs.iter().rev())
        .all(|(n, q)| name_matches(n, q))
}

/// A single parsed line from the authorized-keys file.
///
/// A bare DID has `scope = None` and `expiry_ms = None`, authorizing every
/// space forever. `scope = Some(set)` restricts to the listed space IDs;
/// `expiry_ms = Some(ms)` rejects the entry once `now_ms >= ms`.
struct AuthorizedEntry {
    /// The DID this entry authorizes.
    did: String,
    /// Allowed space IDs, or `None` for all spaces.
    scope: Option<HashSet<String>>,
    /// Expiry in Unix milliseconds, or `None` for never-expiring.
    expiry_ms: Option<i64>,
    /// What participants call this DID, or `None` when the entry names nobody.
    ///
    /// Read by [`AuthorizedKeys::name_for`] and never by an access decision: a
    /// name is a label, and authorization keys on the DID alone.
    name: Option<String>,
}

/// A cached parse of the authorized-keys file, keyed on the file's modification
/// time and length so a check can serve the parsed entries without re-reading
/// disk while the file is unchanged.
///
/// `mtime` is `Option<SystemTime>` because a filesystem may not report a
/// modification time; when it is `None` we still cache on `len` alone, and a
/// same-length edit on such a filesystem falls back to re-reading only if the
/// length also changes (an accepted, rare limitation — most edits change one or
/// both).
struct CacheState {
    /// The file's modification time at parse, if the filesystem reports one.
    mtime: Option<SystemTime>,
    /// The file's length in bytes at parse (guards against coarse `mtime`).
    len: u64,
    /// The parsed entries, shared so a cache hit clones only an `Arc` pointer.
    entries: Arc<[AuthorizedEntry]>,
}

/// File-based DID authorization list.
///
/// Authorization checks serve an in-memory cache of the parsed file, re-reading
/// from disk only when the file's modification time or length changes. This
/// keeps live reload (an append/edit takes effect on the next check) while
/// avoiding a full read+parse on every per-op authorization check.
pub struct AuthorizedKeys {
    path: PathBuf,
    /// Cached parse of the file, invalidated by an `mtime`/`len` change.
    cache: RwLock<Option<CacheState>>,
    /// Set once the file has been reported unreadable, cleared on a successful
    /// read; gates the warning so a persistently missing file logs once, not
    /// once per per-op check.
    unreadable_warned: AtomicBool,
    /// Test-only count of how many times THIS instance read and parsed the
    /// file from disk. Per instance, not per process: a count that every
    /// `AuthorizedKeys` anywhere in the test binary contributes to is a count
    /// two tests running at once can inflate for each other.
    #[cfg(test)]
    read_count: AtomicUsize,
}

impl AuthorizedKeys {
    /// Create a new `AuthorizedKeys` backed by the given file path.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            cache: RwLock::new(None),
            unreadable_warned: AtomicBool::new(false),
            #[cfg(test)]
            read_count: AtomicUsize::new(0),
        }
    }

    /// How many times this instance has read and parsed the file from disk.
    ///
    /// Cache MISSES only, which is what proves the mtime cache serves repeat
    /// calls without re-reading.
    #[cfg(test)]
    pub(crate) fn read_count(&self) -> usize {
        self.read_count.load(Ordering::SeqCst)
    }

    /// What participants call `did`, or `None` when no entry names it.
    ///
    /// A name is a label, never identity: it is resolved at a surface boundary
    /// and the DID is what reaches the wire, so renaming someone cannot orphan
    /// anything already addressed to them. Absence is legal — a bare entry
    /// names nobody and stays addressable by DID.
    ///
    /// Where two entries name the same DID differently, the first wins. That is
    /// arbitrary but harmless: unlike [`authorize`](Self::authorize), which
    /// combines entries most-permissively because access is at stake, a display
    /// name has no safe-vs-unsafe direction to pick.
    pub fn name_for(&self, did: &str) -> Option<String> {
        self.load_entries()
            .iter()
            .find(|e| e.did == did && e.name.is_some())
            .and_then(|e| e.name.clone())
    }

    /// The DIDs named `name` that may reach `space_id` at `now_ms`.
    ///
    /// The inverse of [`name_for`](Self::name_for), and the lookup a surface
    /// resolving a typed recipient uses. Answers over the AUTHORIZED set rather
    /// than who is connected: a flag's whole purpose is to wait for someone, so
    /// a name must resolve when its owner is away.
    ///
    /// Scope and expiry are honoured through [`authorize`](Self::authorize), so
    /// a name never resolves to someone this space cannot reach — a DID scoped
    /// to another space is invisible here even if its name matches.
    ///
    /// Returns every match rather than picking: a duplicated name is an
    /// ambiguity for the caller to report, never one for this to silently
    /// resolve. Matches any whole trailing segment path of the stored name:
    /// `accountant` and `cfo/accountant` both resolve `boris/cfo/accountant`,
    /// but `fo/accountant` does not. Partial-segment matches (near-misses) are
    /// refused; an interactive picker should show what was found.
    pub fn dids_named(&self, name: &str, space_id: &str, now_ms: i64) -> Vec<String> {
        let mut found: Vec<String> = Vec::new();
        for entry in self.load_entries().iter() {
            if entry
                .name
                .as_deref()
                .is_some_and(|n| name_path_matches(n, name))
                && self.authorize(&entry.did, space_id, now_ms)
                && !found.contains(&entry.did)
            {
                found.push(entry.did.clone());
            }
        }
        found
    }

    /// Every DID authorized to reach `space_id` at `now_ms`, in file order.
    ///
    /// The actor set of a space: who may act here, whether or not they are
    /// connected right now. Deduped, because one DID may be listed on several
    /// lines that combine most-permissively.
    ///
    /// A **bare** DID authorizes every space, so it is an actor of every space
    /// and appears in each one's list. That is the truth about what it can do,
    /// not an oversight: an entry that should belong to one space carries a
    /// `scope`.
    pub fn dids_for_space(&self, space_id: &str, now_ms: i64) -> Vec<String> {
        let mut found: Vec<String> = Vec::new();
        for entry in self.load_entries().iter() {
            if self.authorize(&entry.did, space_id, now_ms) && !found.contains(&entry.did) {
                found.push(entry.did.clone());
            }
        }
        found
    }

    /// Check whether `did` appears in the authorized keys file at all.
    ///
    /// A thin presence check that ignores scope and expiry — it answers "is this
    /// DID listed?", not "may it reach space X now?". Any caller enforcing an
    /// access decision must use [`authorize`](Self::authorize), which honours
    /// scope and expiry. This is used only as a coarse presence gate on the
    /// kutlhub membership path (where the real decision is by membership) and by
    /// tests. Entries dropped for malformed options (fail closed) are absent
    /// here too. If the file does not exist or cannot be read, logs a warning
    /// and returns `false`.
    pub fn is_authorized(&self, did: &str) -> bool {
        self.load_entries().iter().any(|e| e.did == did)
    }

    /// Authorize `did` for `space_id` at `now_ms`: true iff the DID is listed AND
    /// (its scope is unset OR contains `space_id`) AND (its expiry is unset OR
    /// `now_ms < expiry`). A bare DID (no options) authorizes every space forever.
    ///
    /// Multiple lines may list the same DID; they combine with **most-permissive
    /// (OR) semantics** — `any` entry that grants access wins, matching SSH's
    /// `authorized_keys` (any matching line authorizes). So a bare line for a DID
    /// grants all spaces even if a *narrower* scoped/expiring line for the same
    /// DID also exists — to restrict a DID, it must have no widening line.
    ///
    /// Reflects the current file (live reload): an append/edit takes effect on
    /// the next call, since a modification-time or length change invalidates the
    /// in-memory cache. If the file does not exist or cannot be read, logs a
    /// warning and returns `false` (fail closed).
    pub fn authorize(&self, did: &str, space_id: &str, now_ms: i64) -> bool {
        self.load_entries().iter().any(|entry| {
            entry.did == did
                && entry
                    .scope
                    .as_ref()
                    .is_none_or(|scope| scope.contains(space_id))
                && entry.expiry_ms.is_none_or(|expiry| now_ms < expiry)
        })
    }

    /// Return the parsed authorized-keys entries, serving an in-memory cache.
    ///
    /// The file is `stat`'d on every call; if its modification time and length
    /// match the cached parse, the cached entries are returned without touching
    /// the file contents (the fast, per-op path). Otherwise the file is read and
    /// parsed and the cache is refreshed.
    ///
    /// Fails CLOSED: if the file is missing or its metadata cannot be read, logs
    /// a warning (only on a state change, to avoid per-op log spam) and returns
    /// an empty list. A transient stat/read error leaves any prior cache intact
    /// rather than poisoning it. Lines with malformed options are dropped (also
    /// fail closed), so the list may be shorter than the file's non-comment lines.
    fn load_entries(&self) -> Arc<[AuthorizedEntry]> {
        let (mtime, len) = match std::fs::metadata(&self.path) {
            Ok(meta) => (meta.modified().ok(), meta.len()),
            Err(e) => return self.on_unreadable(&e),
        };

        // Fast path: the file is unchanged since the last parse — clone the Arc.
        {
            let guard = self.read_cache();
            if let Some(state) = guard.as_ref()
                && state.mtime == mtime
                && state.len == len
            {
                return Arc::clone(&state.entries);
            }
        }

        // Miss: read+parse, then refresh the cache. A concurrent double-read is
        // harmless (both parse the same file; last writer wins).
        let entries: Arc<[AuthorizedEntry]> = match self.read_lines() {
            Ok(entries) => Arc::from(entries),
            Err(e) => return self.on_unreadable(&e),
        };

        // A successful read clears the unreadable-warned latch so a later
        // outage warns again.
        self.unreadable_warned.store(false, Ordering::Relaxed);
        let mut guard = self.write_cache();
        *guard = Some(CacheState {
            mtime,
            len,
            entries: Arc::clone(&entries),
        });
        entries
    }

    /// Fail-closed handling for a missing/unreadable file: warn once per outage
    /// (not per per-op call) and return an empty entry list.
    ///
    /// The cache is left untouched — a transient stat/read blip does not poison
    /// a previously-good parse; the next successful call simply re-reads. Every
    /// call still returns empty here, so authorization fails closed regardless.
    fn on_unreadable(&self, err: &io::Error) -> Arc<[AuthorizedEntry]> {
        if !self.unreadable_warned.swap(true, Ordering::Relaxed) {
            warn!(path = %self.path.display(), error = %err, "failed to read authorized keys file");
        }
        Arc::from(Vec::new())
    }

    /// Read the cache, tolerating a poisoned lock (serve rather than panic).
    fn read_cache(&self) -> std::sync::RwLockReadGuard<'_, Option<CacheState>> {
        self.cache.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Write the cache, tolerating a poisoned lock (serve rather than panic).
    fn write_cache(&self) -> std::sync::RwLockWriteGuard<'_, Option<CacheState>> {
        self.cache.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Read the file and parse each non-comment, non-blank line into an entry.
    ///
    /// Lines with malformed options are dropped (fail closed), so the returned
    /// list may be shorter than the number of non-comment lines.
    fn read_lines(&self) -> io::Result<Vec<AuthorizedEntry>> {
        #[cfg(test)]
        self.read_count.fetch_add(1, Ordering::SeqCst);
        let content = std::fs::read_to_string(&self.path)?;
        let entries = content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .filter_map(|line| self.parse_line(line))
            .collect();
        Ok(entries)
    }

    /// Parse one line into an [`AuthorizedEntry`], or `None` if any option token
    /// is malformed (fail closed).
    ///
    /// The first whitespace-separated token is the DID; each remaining
    /// `key=value` token sets `scope`, `expiry` or `name` (`notes` is a
    /// recognized operator annotation — accepted and discarded, never part of
    /// the decision). A bare DID (no option tokens) parses to a
    /// full-access-forever entry. If ANY token is malformed — missing `=`, an
    /// unparseable `expiry`, an empty/whitespace scope (or a scope list
    /// containing an empty element), a name that is not addressable, or an
    /// unrecognized key — the whole line is DROPPED with a warning so a typo
    /// never widens access. Unknown keys are treated as malformed (the
    /// conservative choice).
    fn parse_line(&self, line: &str) -> Option<AuthorizedEntry> {
        let mut tokens = line.split_whitespace();
        let did = tokens
            .next()
            .expect("non-empty line has at least one token")
            .to_owned();

        let mut entry = AuthorizedEntry {
            did,
            scope: None,
            expiry_ms: None,
            name: None,
        };

        for token in tokens {
            let Some((key, value)) = token.split_once('=') else {
                warn!(
                    path = %self.path.display(),
                    did = %entry.did,
                    token,
                    "dropping authorized_keys entry: malformed option (expected key=value)"
                );
                return None;
            };
            match key {
                "scope" => {
                    // A scope with an empty element (including a wholly empty
                    // `scope=`) is malformed — fail closed rather than resolve
                    // to a scope set that never matches or that leaks intent.
                    let spaces: HashSet<String> = value.split(',').map(str::to_owned).collect();
                    if value.is_empty() || spaces.iter().any(|s| s.trim().is_empty()) {
                        warn!(
                            path = %self.path.display(),
                            did = %entry.did,
                            value,
                            "dropping authorized_keys entry: malformed scope (empty space id)"
                        );
                        return None;
                    }
                    entry.scope = Some(spaces);
                }
                "expiry" => {
                    let Ok(ms) = value.parse::<i64>() else {
                        warn!(
                            path = %self.path.display(),
                            did = %entry.did,
                            value,
                            "dropping authorized_keys entry: malformed expiry (expected unix milliseconds)"
                        );
                        return None;
                    };
                    entry.expiry_ms = Some(ms);
                }
                "name" => {
                    if !is_addressable_name(value) {
                        warn!(
                            path = %self.path.display(),
                            did = %entry.did,
                            value,
                            "dropping authorized_keys entry: malformed name (non-empty, \
                             no whitespace, no leading '-', and must not look like a DID)"
                        );
                        return None;
                    }
                    entry.name = Some(value.to_owned());
                }
                // An operator annotation, deliberately never stored: it exists
                // so a note can be written without the unrecognized-key rule
                // dropping the entry, and so `name` can be published without
                // carrying whatever an operator wrote about the person.
                "notes" => {}
                _ => {
                    warn!(
                        path = %self.path.display(),
                        did = %entry.did,
                        key,
                        "dropping authorized_keys entry: unrecognized option key"
                    );
                    return None;
                }
            }
        }

        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::AuthorizedKeys;

    fn write_keys(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("create temp keys file");
        file.write_all(contents.as_bytes())
            .expect("write temp keys file");
        file.flush().expect("flush temp keys file");
        file
    }

    /// A name is readable back, and its absence is legal rather than an error.
    #[test]
    fn test_name_for_reads_the_named_entry() {
        let file = write_keys(
            "did:key:zNamed name=alice\n\
             did:key:zBare\n\
             did:key:zBoth scope=s1 name=bob notes=on-call-until-friday\n",
        );
        let keys = AuthorizedKeys::new(file.path().to_path_buf());

        assert_eq!(keys.name_for("did:key:zNamed").as_deref(), Some("alice"));
        assert_eq!(keys.name_for("did:key:zBoth").as_deref(), Some("bob"));
        assert_eq!(
            keys.name_for("did:key:zBare"),
            None,
            "a bare entry names nobody"
        );
        assert_eq!(keys.name_for("did:key:zAbsent"), None);
    }

    /// `notes` is accepted so an operator can annotate without the
    /// unrecognized-key rule denying that DID, and it never becomes a name.
    #[test]
    fn test_notes_is_accepted_and_never_surfaces() {
        let file = write_keys("did:key:zA scope=s1 notes=revoke-after-the-contract\n");
        let keys = AuthorizedKeys::new(file.path().to_path_buf());

        assert!(
            keys.authorize("did:key:zA", "s1", 0),
            "notes must not deny access"
        );
        assert_eq!(
            keys.name_for("did:key:zA"),
            None,
            "an operator note must never be served as a name"
        );
    }

    /// A name that could not be typed as one argument fails closed, like every
    /// other malformed option — the entry is dropped, not silently unnamed.
    #[test]
    fn test_unaddressable_name_drops_the_entry() {
        for bad in ["name=", "name=-dash", "name=did:key:zEvil"] {
            let file = write_keys(&format!("did:key:zA scope=s1 {bad}\n"));
            let keys = AuthorizedKeys::new(file.path().to_path_buf());
            assert!(
                !keys.authorize("did:key:zA", "s1", 0),
                "{bad} must drop the entry rather than widen or half-apply it"
            );
        }
        // Whitespace cannot reach the parser as one token — `name=a b` splits,
        // and the stray `b` is a token with no `=`, which fails closed anyway.
        let file = write_keys("did:key:zA scope=s1 name=a b\n");
        let keys = AuthorizedKeys::new(file.path().to_path_buf());
        assert!(!keys.authorize("did:key:zA", "s1", 0));
    }

    /// `label` was renamed to `name`, and the fail-closed rule means the old
    /// spelling is now an unrecognized key rather than a silently ignored one.
    #[test]
    fn test_retired_label_key_fails_closed() {
        let file = write_keys("did:key:zA scope=s1 label=alice\n");
        let keys = AuthorizedKeys::new(file.path().to_path_buf());
        assert!(
            !keys.authorize("did:key:zA", "s1", 0),
            "the retired `label` key must fail closed, not read as a name"
        );
    }

    /// A name resolves over the authorized set, honouring scope and expiry, and
    /// reports every match rather than picking one.
    #[test]
    fn test_dids_named_resolves_within_reach_only() {
        let file = write_keys(
            "did:key:zAlice scope=s1 name=alice\n\
             did:key:zElsewhere scope=s2 name=remote\n\
             did:key:zExpired scope=s1 expiry=100 name=gone\n\
             did:key:zTwinA scope=s1 name=twin\n\
             did:key:zTwinB scope=s1 name=twin\n",
        );
        let keys = AuthorizedKeys::new(file.path().to_path_buf());

        assert_eq!(keys.dids_named("alice", "s1", 0), vec!["did:key:zAlice"]);

        // Scoped to another space: not reachable here, so not resolvable here.
        assert!(
            keys.dids_named("remote", "s1", 0).is_empty(),
            "a name must not resolve to a DID this space cannot reach"
        );

        // Expiry is the access rule, so it governs resolution too.
        assert_eq!(keys.dids_named("gone", "s1", 50), vec!["did:key:zExpired"]);
        assert!(
            keys.dids_named("gone", "s1", 100).is_empty(),
            "an expired entry must stop resolving when it stops authorizing"
        );

        // Ambiguity is reported, never resolved.
        assert_eq!(
            keys.dids_named("twin", "s1", 0),
            vec!["did:key:zTwinA", "did:key:zTwinB"],
            "a duplicated name must return every match for the caller to refuse"
        );

        assert!(keys.dids_named("nobody", "s1", 0).is_empty());

        // Case is the one axis where a difference never means a different
        // person: a name written lowercase in the file is typed capitalised by
        // anyone treating it as a proper noun. Two real addressing attempts in
        // a demo run were refused this way before the rule was folded.
        assert_eq!(keys.dids_named("Alice", "s1", 0), vec!["did:key:zAlice"]);
        assert_eq!(keys.dids_named("ALICE", "s1", 0), vec!["did:key:zAlice"]);

        // Everything else stays exact. A prefix CAN name someone else, and
        // resolving one would send that person's mail.
        assert!(keys.dids_named("ali", "s1", 0).is_empty());
        assert!(keys.dids_named("alice2", "s1", 0).is_empty());

        // Folding does not collapse two people into one: entries differing
        // only by case are an ambiguity to report, not a match to pick.
        assert_eq!(keys.dids_named("TWIN", "s1", 0).len(), 2);
    }

    #[test]
    fn test_repeat_calls_read_file_once() {
        // Before the cache fix, each of these N calls did a full read+parse.
        const N: usize = 1000;

        let file = write_keys("did:key:zA scope=s1 expiry=1000\n");
        let keys = AuthorizedKeys::new(file.path().to_path_buf());

        for _ in 0..N {
            assert!(keys.authorize("did:key:zA", "s1", 500));
        }
        // 1000 authorize() calls with an unchanged file => exactly one disk read.
        assert_eq!(
            keys.read_count(),
            1,
            "{N} calls on an unchanged file should read the file once, not {N} times"
        );
    }

    #[test]
    fn test_edit_invalidates_cache_live_reload() {
        let mut file = write_keys("did:key:zLive scope=s1\n");
        let keys = AuthorizedKeys::new(file.path().to_path_buf());

        // First check populates the cache (one read).
        assert!(keys.authorize("did:key:zLive", "s1", 0));
        assert!(!keys.authorize("did:key:zLive", "s2", 0));
        assert_eq!(keys.read_count(), 1);

        // Append a broader grant. Bump the mtime explicitly: on coarse-mtime
        // filesystems a fast append may not move the mtime, but the length
        // change alone still invalidates the cache — and this makes the reload
        // robust either way.
        file.as_file()
            .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(2))
            .expect("bump mtime");
        writeln!(file, "did:key:zLive").expect("append bare grant");
        file.flush().expect("flush appended grant");

        // The next check must re-read and reflect the new content (live reload).
        assert!(keys.authorize("did:key:zLive", "s2", 0));
        assert_eq!(
            keys.read_count(),
            2,
            "an edited file must trigger exactly one additional read"
        );
    }

    #[test]
    fn test_cache_preserves_authorization_correctness() {
        // A cache hit must return byte-for-byte identical decisions.
        let file = write_keys(
            "# comment\n\
             did:key:zBare\n\
             did:key:zScoped scope=s1,s2 expiry=1000 name=agent\n\
             did:key:zBad scope=s1 expiry=not-a-number\n",
        );
        let keys = AuthorizedKeys::new(file.path().to_path_buf());

        // Repeat every check twice to exercise miss-then-hit; decisions match.
        for _ in 0..2 {
            // Bare DID: everywhere, forever.
            assert!(keys.is_authorized("did:key:zBare"));
            assert!(keys.authorize("did:key:zBare", "anything", i64::MAX));
            // Scoped + expiring.
            assert!(keys.authorize("did:key:zScoped", "s1", 500));
            assert!(keys.authorize("did:key:zScoped", "s2", 500));
            assert!(!keys.authorize("did:key:zScoped", "s3", 500));
            assert!(!keys.authorize("did:key:zScoped", "s1", 1000)); // at expiry
            // Malformed line drops the entry (fail closed).
            assert!(!keys.is_authorized("did:key:zBad"));
            assert!(!keys.authorize("did:key:zBad", "s1", 0));
            // Unlisted DID.
            assert!(!keys.is_authorized("did:key:zNope"));
            assert!(!keys.authorize("did:key:zNope", "s1", 0));
        }
    }

    #[test]
    fn test_missing_file_fails_closed_and_is_cached() {
        let keys = AuthorizedKeys::new(std::path::PathBuf::from(
            "/nonexistent/kutl/authorized_keys",
        ));
        // Repeated checks on a missing file all deny and never reach read_lines
        // (metadata() fails first), so the read counter stays put.
        for _ in 0..100 {
            assert!(!keys.is_authorized("did:key:zAnything"));
        }
        assert_eq!(keys.read_count(), 0);
    }

    #[test]
    fn test_name_path_matches_suffix_segments() {
        assert!(super::name_path_matches(
            "boris/cfo/accountant",
            "accountant"
        ));
        assert!(super::name_path_matches(
            "boris/cfo/accountant",
            "cfo/accountant"
        ));
        assert!(super::name_path_matches(
            "boris/cfo/accountant",
            "boris/cfo/accountant"
        ));
        assert!(super::name_path_matches(
            "boris/cfo/accountant",
            "CFO/Accountant"
        )); // case folds
        assert!(!super::name_path_matches(
            "boris/cfo/accountant",
            "fo/accountant"
        )); // whole segments only
        assert!(!super::name_path_matches(
            "boris/cfo/accountant",
            "boris/accountant"
        )); // suffix, not subset
        assert!(!super::name_path_matches("accountant", "cfo/accountant")); // query longer than name
        assert!(super::name_path_matches("egon", "Egon")); // single-segment == old behavior
    }

    #[test]
    fn test_name_path_matches_rejects_empty_query() {
        // A trailing '/' in `name` splits to a trailing empty segment; an
        // empty query must not trivially match it.
        assert!(!super::name_path_matches("boris/", ""));
        assert!(!super::name_path_matches("boris", ""));
    }

    #[test]
    fn test_dids_named_resolves_path_suffix() {
        let file = write_keys(
            "did:key:zA scope=s1 name=boris/cfo/accountant\n\
             did:key:zB scope=s2 name=boris/cfo/accountant\n",
        );
        let keys = AuthorizedKeys::new(file.path().to_path_buf());

        // Resolves by single-segment suffix in s1.
        assert_eq!(keys.dids_named("accountant", "s1", 0), vec!["did:key:zA"]);
        // Resolves by two-segment suffix in s1.
        assert_eq!(
            keys.dids_named("cfo/accountant", "s1", 0),
            vec!["did:key:zA"]
        );
        // Resolves by full path in s1.
        assert_eq!(
            keys.dids_named("boris/cfo/accountant", "s1", 0),
            vec!["did:key:zA"]
        );
        // Same name in s2 resolves to the s2 DID only.
        assert_eq!(keys.dids_named("accountant", "s2", 0), vec!["did:key:zB"]);
    }
}
