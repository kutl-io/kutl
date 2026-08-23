//! Persisted relay signing identities, stored at
//! `$KUTL_HOME/known_relays.json`.
//!
//! Modelled on ssh's `known_hosts`: the first time a client meets a relay it
//! records the identity that relay advertised, and every later connection is
//! checked against that record. A relay presenting a DID this client has never
//! seen at that URL is surfaced loudly.
//!
//! **Why persistence is load-bearing, and not merely tidy.** A relay's
//! `MATERIALIZER` signature speaks on an author's behalf, so
//! [`kutl_signals::verify_record`] will only honour it from a relay the caller
//! trusts. That trust must come from a durable record established before the
//! current connection: a per-connection pin re-initialized from what the relay
//! itself advertises would make the trusted set whatever the current
//! connection claims to be, and a pin that is established by the party being
//! checked is not a pin.
//!
//! **A set, not a value.** Key rotation is legitimate, and a client that
//! discarded the old DID would stop verifying every record signed before the
//! rotation — records are immutable and are never re-signed, so their signing
//! key must stay trusted for as long as they exist. Entries therefore
//! accumulate in the order they were first seen.
//!
//! **Recorded, not enforced.** An unrecognized DID warns and is then added; it
//! does not refuse the connection. Verification is advisory,
//! and a relay rotating its signing key must not be able
//! to take document sync down with it — sync does not depend on this identity
//! at all. What the client gets is a durable record and a loud log line, which
//! is what makes a substituted identity visible after the fact.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Every relay identity this client has seen, keyed by relay URL.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct KnownRelays {
    /// One entry per relay URL.
    #[serde(default)]
    pub relays: Vec<KnownRelay>,
}

/// The identities seen at one relay URL.
#[derive(Debug, Serialize, Deserialize)]
pub struct KnownRelay {
    /// The relay URL, exactly as the client was configured with it.
    ///
    /// Identities are scoped to the URL rather than global: two relays are two
    /// principals, and a DID trusted for one says nothing about the other.
    pub url: String,
    /// Every DID seen at this URL, oldest first.
    ///
    /// More than one entry means a rotation was observed. Order is the audit
    /// trail — the last entry is what the relay currently presents.
    pub dids: Vec<String>,
}

/// What observing a relay's advertised identity meant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayPinOutcome {
    /// The relay advertised no identity at all.
    ///
    /// Not an error: an identity-less relay is a supported configuration, and
    /// its records are tier-3 asserted. Nothing is recorded, because there is
    /// nothing to record.
    NoIdentity,
    /// First contact with this relay URL — trusted on first use, now recorded.
    FirstUse,
    /// The advertised DID is one this client already knew.
    Known,
    /// A DID not previously seen at this URL. Added, keeping the earlier ones.
    Rotated {
        /// The DIDs known before this one, oldest first.
        previous: Vec<String>,
    },
}

impl KnownRelays {
    /// Record `relay_did` as an identity of `relay_url` and return the full
    /// trust set for that URL alongside what the observation meant.
    ///
    /// The whole read-modify-write runs under an exclusive lock, because one
    /// daemon process per space means several may meet the same relay at once
    /// and an interleaved write would silently drop a rotation entry.
    ///
    /// An empty `relay_did` (the relay advertises no identity) records nothing
    /// and returns an empty trust set — trusting no materializer claim, which
    /// is the honest reading of "this relay declined to identify itself."
    ///
    /// # Errors
    ///
    /// Returns an error if `$KUTL_HOME` cannot be resolved or created, if the
    /// existing file is present but unparseable, or if the write fails.
    pub fn observe(relay_url: &str, relay_did: &str) -> Result<(Vec<String>, RelayPinOutcome)> {
        Self::observe_at(None, relay_url, relay_did)
    }

    /// [`Self::observe`] against an explicit file rather than the default
    /// `$KUTL_HOME/known_relays.json`.
    ///
    /// The path is a parameter because the daemon runs one worker per space and
    /// a test binary runs many workers in one process: pointing them at the
    /// developer's real `~/.kutl` would both pollute it and serialize every
    /// worker on one lock. Production passes `None` and gets the shared record,
    /// which is the correct behaviour there — a relay's identity is a property
    /// of the install, not of one space.
    ///
    /// # Errors
    ///
    /// As [`Self::observe`].
    pub fn observe_at(
        path: Option<&Path>,
        relay_url: &str,
        relay_did: &str,
    ) -> Result<(Vec<String>, RelayPinOutcome)> {
        if relay_did.is_empty() {
            return Ok((Vec::new(), RelayPinOutcome::NoIdentity));
        }

        let path = match path {
            Some(p) => p.to_path_buf(),
            None => known_relays_path()?,
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create the kutl home dir {}", parent.display())
            })?;
        }
        let _lock = crate::file_lock::lock_exclusive(&path)?;

        let mut known = Self::load(&path)?;
        let outcome = known.record(relay_url, relay_did);
        if outcome != RelayPinOutcome::Known {
            let json = serde_json::to_string_pretty(&known)
                .context("failed to serialize the known-relays file")?;
            crate::file_lock::write_atomic(&path, &json)?;
        }

        let trusted = known
            .dids_for(relay_url)
            .map(<[String]>::to_vec)
            .unwrap_or_default();
        Ok((trusted, outcome))
    }

    /// Load from `path`, or return an empty set when the file does not exist.
    ///
    /// A present-but-unparseable file is an error rather than a silent reset:
    /// this is a trust record, and quietly starting over would re-trust
    /// whatever the next relay to connect happens to present.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(data) => serde_json::from_str(&data)
                .with_context(|| format!("failed to parse {}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).with_context(|| format!("failed to read {}", path.display())),
        }
    }

    /// The DIDs known for `relay_url`, oldest first.
    #[must_use]
    pub fn dids_for(&self, relay_url: &str) -> Option<&[String]> {
        self.relays
            .iter()
            .find(|r| r.url == relay_url)
            .map(|r| r.dids.as_slice())
    }

    /// Add `relay_did` to `relay_url`'s entry, reporting what that meant.
    fn record(&mut self, relay_url: &str, relay_did: &str) -> RelayPinOutcome {
        let Some(entry) = self.relays.iter_mut().find(|r| r.url == relay_url) else {
            self.relays.push(KnownRelay {
                url: relay_url.to_owned(),
                dids: vec![relay_did.to_owned()],
            });
            return RelayPinOutcome::FirstUse;
        };
        if entry.dids.iter().any(|d| d == relay_did) {
            return RelayPinOutcome::Known;
        }
        let previous = entry.dids.clone();
        entry.dids.push(relay_did.to_owned());
        RelayPinOutcome::Rotated { previous }
    }
}

/// Path to the known-relays file: `$KUTL_HOME/known_relays.json`.
///
/// # Errors
///
/// Returns an error if `$KUTL_HOME` cannot be resolved.
pub fn known_relays_path() -> Result<PathBuf> {
    Ok(crate::dirs::kutl_home()?.join("known_relays.json"))
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use tempfile::TempDir;

    use super::*;

    /// Point `$KUTL_HOME` at a temp dir for the duration of a test.
    fn with_kutl_home<T>(f: impl FnOnce() -> T) -> T {
        let dir = TempDir::new().unwrap();
        // SAFETY: env-mutating tests are serialized via `#[serial]`.
        unsafe { std::env::set_var("KUTL_HOME", dir.path()) };
        let out = f();
        unsafe { std::env::remove_var("KUTL_HOME") };
        out
    }

    const RELAY: &str = "wss://relay.example/ws/sync";
    const DID_A: &str = "did:key:zRelayA";
    const DID_B: &str = "did:key:zRelayB";

    /// First contact records the identity; the next connection recognizes it.
    #[test]
    #[serial]
    fn test_first_use_then_known() {
        with_kutl_home(|| {
            let (trusted, outcome) = KnownRelays::observe(RELAY, DID_A).unwrap();
            assert_eq!(outcome, RelayPinOutcome::FirstUse);
            assert_eq!(trusted, vec![DID_A.to_owned()]);

            let (trusted, outcome) = KnownRelays::observe(RELAY, DID_A).unwrap();
            assert_eq!(outcome, RelayPinOutcome::Known);
            assert_eq!(trusted, vec![DID_A.to_owned()]);
        });
    }

    /// A rotation is reported AND keeps the old DID trusted — records signed
    /// under the previous key are immutable and would stop verifying otherwise.
    #[test]
    #[serial]
    fn test_rotation_is_reported_and_keeps_the_old_did() {
        with_kutl_home(|| {
            KnownRelays::observe(RELAY, DID_A).unwrap();
            let (trusted, outcome) = KnownRelays::observe(RELAY, DID_B).unwrap();
            assert_eq!(
                outcome,
                RelayPinOutcome::Rotated {
                    previous: vec![DID_A.to_owned()]
                }
            );
            assert_eq!(
                trusted,
                vec![DID_A.to_owned(), DID_B.to_owned()],
                "both keys must stay trusted, oldest first"
            );
        });
    }

    /// Identities are scoped per URL — trusting one relay must not trust another.
    #[test]
    #[serial]
    fn test_identities_are_scoped_per_relay_url() {
        with_kutl_home(|| {
            KnownRelays::observe(RELAY, DID_A).unwrap();
            let (trusted, outcome) =
                KnownRelays::observe("wss://other.example/ws/sync", DID_B).unwrap();
            assert_eq!(outcome, RelayPinOutcome::FirstUse);
            assert_eq!(trusted, vec![DID_B.to_owned()]);

            let known = KnownRelays::load(&known_relays_path().unwrap()).unwrap();
            assert_eq!(known.dids_for(RELAY), Some([DID_A.to_owned()].as_slice()));
        });
    }

    /// A relay that advertises no identity records nothing and trusts nothing.
    #[test]
    #[serial]
    fn test_no_identity_records_nothing() {
        with_kutl_home(|| {
            let (trusted, outcome) = KnownRelays::observe(RELAY, "").unwrap();
            assert_eq!(outcome, RelayPinOutcome::NoIdentity);
            assert!(trusted.is_empty());
            assert!(
                !known_relays_path().unwrap().exists(),
                "an identity-less relay must not create the file"
            );
        });
    }

    /// A corrupt trust record is fatal, not silently reset — starting over
    /// would re-trust whatever presents itself next.
    #[test]
    #[serial]
    fn test_corrupt_file_is_an_error() {
        with_kutl_home(|| {
            let path = known_relays_path().unwrap();
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, "{not json").unwrap();
            assert!(KnownRelays::observe(RELAY, DID_A).is_err());
        });
    }

    /// The pin survives a process restart — the property the whole file exists
    /// for.
    #[test]
    #[serial]
    fn test_pin_survives_reload() {
        with_kutl_home(|| {
            KnownRelays::observe(RELAY, DID_A).unwrap();
            // A fresh load, as a restarted daemon would do.
            let reloaded = KnownRelays::load(&known_relays_path().unwrap()).unwrap();
            assert_eq!(
                reloaded.dids_for(RELAY),
                Some([DID_A.to_owned()].as_slice())
            );
        });
    }
}
