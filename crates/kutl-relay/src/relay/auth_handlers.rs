//! Auth/handshake family of the relay actor: WebSocket handshake
//! authentication, DID challenge-response and device-flow handlers, and
//! the space/connection ACL checks that gate every space-scoped command.
//!
//! Child module of the relay actor (`super`) so the `impl Relay` block here
//! reaches the actor's private fields directly. `process_command` in
//! relay.rs routes auth commands to these handlers, and `mcp.rs` reaches
//! `authorize_space` as a sibling.

use kutl_proto::sync::{self, ErrorCode};
use tracing::{debug, info, warn};

use crate::acl::{AuthError as AclError, AuthorizedSpace};
use crate::auth::{AuthError, DeviceTokenResponse};
use crate::protocol::{
    MIN_SUPPORTED_PROTOCOL_MAJOR, encode_envelope, refusal, wrap_handshake_ack,
    wrap_refused_handshake_ack,
};
use crate::spaces;

use super::{ConnId, Relay};

/// Response to an [`RelayCommand::AuthChallenge`](super::RelayCommand::AuthChallenge).
pub struct ChallengeResponse {
    /// Base64url-encoded nonce that the client must sign.
    pub nonce: String,
    /// Expiry timestamp in Unix milliseconds.
    pub expires_at: i64,
}

/// Response to an [`RelayCommand::AuthVerify`](super::RelayCommand::AuthVerify).
pub struct VerifyResponse {
    /// Bearer token to include in the WebSocket handshake.
    pub token: String,
    /// Expiry timestamp in Unix milliseconds.
    pub expires_at: i64,
}

/// Response to an [`RelayCommand::CreateDeviceRequest`](super::RelayCommand::CreateDeviceRequest).
pub struct DeviceRequestResponse {
    /// Opaque device code for CLI polling.
    pub device_code: String,
    /// Human-readable code displayed to the user (e.g. "ABCD-1234").
    pub user_code: String,
    /// Expiry timestamp in Unix milliseconds.
    pub expires_at: i64,
}

/// The refusal every authorization path returns: the space named, nothing
/// about why (the reason is logged, not told to the caller).
fn not_authorized(space_id: &str) -> AclError {
    AclError::NotAuthorized {
        space_id: space_id.to_owned(),
    }
}

impl Relay {
    pub(super) async fn handle_handshake(&mut self, conn_id: ConnId, msg: &sync::Handshake) {
        info!(conn_id, client_name = %msg.client_name, display_name = %msg.display_name, "handshake received");

        // Refuse a client whose protocol major is below the floor this relay
        // advertises. Checked BEFORE authentication so an outdated client is
        // told what is actually wrong instead of being handed an auth error
        // it cannot act on — and so the refusal costs no token validation.
        //
        // Admitting one is worse than refusing it: the frames it depends on
        // are gone, so it would authenticate, subscribe, and then sit there
        // receiving nothing, with no error to surface. Loud beats silent.
        if msg.protocol_version_major < MIN_SUPPORTED_PROTOCOL_MAJOR {
            warn!(
                conn_id,
                client_major = msg.protocol_version_major,
                min_supported = MIN_SUPPORTED_PROTOCOL_MAJOR,
                client_name = %msg.client_name,
                "refusing client below the minimum supported protocol major"
            );
            let ack = wrap_refused_handshake_ack(
                &self.config,
                refusal(
                    ErrorCode::VersionMismatch,
                    format!(
                        "relay requires protocol version {MIN_SUPPORTED_PROTOCOL_MAJOR} or later \
                         (this client speaks {}); upgrade kutl",
                        msg.protocol_version_major
                    ),
                ),
            );
            self.send_ack(conn_id, &encode_envelope(&ack));
            self.connections.remove(&conn_id);
            return;
        }

        // Authentication is mandatory: every connection must present
        // a valid bearer token in the handshake before it is accepted.
        let now = kutl_core::now_ms();
        let result = self
            .auth
            .validate_token_with_backend(
                &msg.auth_token,
                now,
                self.session_backend.as_deref(),
                self.pat_backend.as_deref(),
            )
            .await;
        match result {
            Ok((did, pat_ctx)) => {
                info!(conn_id, %did, "authenticated");
                self.authenticated.insert(conn_id, (did, pat_ctx));
            }
            Err(e) => {
                debug!(conn_id, error = %e, "authentication failed");
                let ack = wrap_refused_handshake_ack(
                    &self.config,
                    refusal(ErrorCode::AuthFailed, e.to_string()),
                );
                self.send_ack(conn_id, &encode_envelope(&ack));
                self.connections.remove(&conn_id);
                return;
            }
        }

        let relay_did = self.signing_identity().map(|id| id.did());
        // Re-seed can only be advertised with a RECORD LOG (nowhere to store
        // pushed history without one). The log is a trait, not file segments
        // specifically, so a Postgres-backed deployment qualifies too.
        let has_record_log = self.record_log.is_configured();
        let ack = wrap_handshake_ack(&self.config, relay_did, has_record_log);
        self.send_ack(conn_id, &encode_envelope(&ack));
    }

    pub(super) fn handle_auth_challenge(
        &mut self,
        did: &str,
    ) -> Result<ChallengeResponse, AuthError> {
        let now = kutl_core::now_ms();
        let (nonce, expires_at) = self.auth.create_challenge(did, now)?;
        Ok(ChallengeResponse { nonce, expires_at })
    }

    pub(super) async fn handle_auth_verify(
        &mut self,
        did: &str,
        nonce: &str,
        signature: &str,
    ) -> Result<VerifyResponse, AuthError> {
        let now = kutl_core::now_ms();
        let (token, expires_at) = self
            .auth
            .verify_challenge_with_backend(
                did,
                nonce,
                signature,
                now,
                self.session_backend.as_deref(),
            )
            .await?;
        Ok(VerifyResponse { token, expires_at })
    }

    pub(super) fn handle_create_device_request(&mut self) -> DeviceRequestResponse {
        let now = kutl_core::now_ms();
        let (device_code, user_code, expires_at) = self.auth.create_device_request(now);
        DeviceRequestResponse {
            device_code,
            user_code,
            expires_at,
        }
    }

    pub(super) fn handle_poll_device(
        &mut self,
        device_code: &str,
    ) -> Result<DeviceTokenResponse, AuthError> {
        let now = kutl_core::now_ms();
        self.auth.poll_device(device_code, now)
    }

    pub(super) fn handle_authorize_device(
        &mut self,
        user_code: &str,
        token: String,
        account_id: String,
        display_name: String,
    ) -> Result<(), AuthError> {
        let now = kutl_core::now_ms();
        self.auth
            .authorize_device(user_code, token, account_id, display_name, now)
    }

    /// Authoritative `author_did` to record for a connection, overriding any
    /// client-supplied value.
    ///
    /// Returns the DID the connection authenticated as (via bearer token or
    /// PAT). Any `author_did` or `participant_did` field arriving over the wire
    /// must be replaced with this value to prevent an authenticated peer from
    /// impersonating a different user in signals, presence, edit metadata, or
    /// lifecycle events. Authentication is mandatory, so an
    /// established connection always has an authenticated identity.
    pub(super) fn authoritative_author_did(&self, conn_id: ConnId) -> &str {
        self.authenticated
            .get(&conn_id)
            .map(|(id, _)| id.as_str())
            .expect("connection is authenticated (every authoring path calls authorize_conn first)")
    }

    /// The token this connection authenticated with, when it used one.
    ///
    /// A token binds to the account's key, so a person and their agent
    /// authenticate as the same DID. The DID alone therefore cannot say which
    /// of the two acted; this can, and surfaces are expected to name the agent
    /// rather than the person it acts for. `None` means the connection
    /// authenticated as the person directly.
    pub(super) fn authoritative_via_pat_id(&self, conn_id: ConnId) -> Option<String> {
        self.authenticated
            .get(&conn_id)
            .and_then(|(_, pat)| pat.as_ref())
            .map(|pat| pat.pat_id.clone())
    }

    /// The full authoring identity of a connection: the authoritative DID
    /// plus the PAT it authenticated with, when it used one.
    ///
    /// The WS counterpart of `authorize_mcp_caller`'s [`AuthorIdentity`] —
    /// one identity shape for both authoring surfaces, so a door on either
    /// side records the same attribution for the same caller. Every WS door
    /// that authors something (a signal, an edit) takes its identity from
    /// here rather than from [`Self::authoritative_author_did`] alone: the
    /// DID half by itself cannot distinguish an agent from its person, and a
    /// door that reads only it silently credits the person.
    pub(super) fn authoritative_identity(&self, conn_id: ConnId) -> super::AuthorIdentity {
        super::AuthorIdentity {
            did: self.authoritative_author_did(conn_id).to_owned(),
            via_pat_id: self.authoritative_via_pat_id(conn_id),
        }
    }

    /// Resolve the caller's account ID from their authenticated identity,
    /// through [`Self::account_for_identity`]. Resolves to `None` when the
    /// identity maps to no account or the backend cannot answer (logged);
    /// the connection paths that call this treat both as "no account".
    pub(super) fn resolve_account_id(
        &self,
        conn_id: ConnId,
    ) -> impl Future<Output = Option<String>> + Send + use<> {
        // The identity is copied into the future only when the lookup will
        // reach the backend; the account fast path needs no copy.
        let resolution = self.authenticated.get(&conn_id).map(|(identity, _)| {
            let did = spaces::extract_account_id(identity)
                .is_none()
                .then(|| identity.clone());
            (did, self.account_for_identity(identity))
        });
        async move {
            let (did, resolve) = resolution?;
            match resolve.await {
                Ok(Some(account_id)) => Some(account_id),
                Ok(None) => {
                    debug!(conn_id, did = %did.as_deref().unwrap_or_default(), "no account found for DID");
                    None
                }
                Err(e) => {
                    warn!(conn_id, error = %e, "failed to resolve DID to account");
                    None
                }
            }
        }
    }

    /// Resolve an identity string to its account id: the `account:<uuid>`
    /// form directly (historical; PAT auth issues `did:key` identities now,
    /// and the form survives only in backfilled provenance), else the
    /// membership backend's custodied-key lookup. The ONE identity → account
    /// rule; the connection paths and the MCP handlers all read it.
    ///
    /// `Ok(None)` is an unknown identity, and so is a relay with no membership
    /// backend. `Err` is the backend failing to answer, which each caller
    /// maps for itself: a surface that cannot tell "unknown" from
    /// "unavailable" must not treat the second as the first.
    pub(super) fn account_for_identity(
        &self,
        identity: &str,
    ) -> impl Future<Output = anyhow::Result<Option<String>>> + Send + use<> {
        let fast = spaces::extract_account_id(identity).map(str::to_owned);
        let slow = fast
            .is_none()
            .then(|| (identity.to_owned(), self.membership_backend.clone()));
        async move {
            if let Some(account_id) = fast {
                return Ok(Some(account_id));
            }
            let Some((identity, Some(membership))) = slow else {
                return Ok(None);
            };
            membership.resolve_did_to_account(&identity).await
        }
    }

    /// Authorize an identity string for access to a space.
    ///
    /// Returns an [`AuthorizedSpace`] on success. The decision tree:
    /// 0. If the connection was PAT-authed, verify the token is scoped for
    ///    this space before any membership check.
    /// 1. Account-based identity (`account:<uuid>`) — check `space_memberships`.
    /// 2. DID-based identity — resolve via `custodied_keys`, check `space_memberships`.
    /// 3. Fall back to authorized keys file (OSS relay).
    pub(super) fn authorize_space(
        &self,
        identity: &str,
        space_id: &str,
        pat_hash: Option<&str>,
    ) -> impl Future<Output = Result<AuthorizedSpace, AclError>> + Send + use<> {
        // THE space-id boundary. Every space-scoped operation authorizes first,
        // so parsing here means no consumer downstream has to re-check — and a
        // malformed id is refused as unauthorized rather than surfacing a dozen
        // different "must be a UUID" errors from wherever it happened to reach
        // first. Space ids are relay-minted UUIDs (`SpaceBackend::register`), so
        // a non-UUID names no space that exists; unauthorized is the truth.
        let parsed = crate::acl::SpaceId::parse(space_id).map_err(|e| {
            debug!(%space_id, error = %e, "rejected: space_id is not a UUID");
            not_authorized(space_id)
        });
        let identity = identity.to_owned();
        // The PAT scope check needs both the token hash and a backend.
        let pat = pat_hash.map(str::to_owned).zip(self.pat_backend.clone());
        let membership = self.membership_backend.clone();
        let authorized_keys = self.authorized_keys.clone();
        async move {
            let parsed = parsed?;
            let space_id = parsed.as_str();

            // Step 0: PAT space-scoping — the token may restrict which spaces
            // the holder can access regardless of membership.
            if let Some((hash, backend)) = pat.as_ref() {
                match backend.check_scope(hash, space_id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        debug!(%space_id, "rejected: PAT not scoped for this space");
                        return Err(not_authorized(space_id));
                    }
                    Err(e) => {
                        debug!(error = %e, "PAT scope check failed");
                        return Err(not_authorized(space_id));
                    }
                }
            }

            // Startup check in build_app_with_backends_and_web guarantees that
            // at least one of membership_backend or authorized_keys is present.
            // If we somehow reach this point without either, reject rather than
            // silently allowing.

            // Database-backed ACL checks.
            if let Some(membership) = membership.as_ref() {
                // Account-based identity (PAT auth) — check space_memberships table.
                if let Some(account_id) = spaces::extract_account_id(&identity) {
                    return match membership.check_membership(space_id, account_id).await {
                        Ok(Some(_role)) => Ok(AuthorizedSpace::new_unchecked(parsed)),
                        Ok(None) => {
                            debug!(%account_id, %space_id, "rejected: not a member of this space");
                            Err(not_authorized(space_id))
                        }
                        Err(e) => {
                            debug!(error = %e, %space_id, "membership check failed");
                            Err(not_authorized(space_id))
                        }
                    };
                }

                // DID-based identity (challenge-response auth): resolve DID ->
                // account via custodied_keys, then check space_memberships. A
                // DID the directory does not know falls through to the keys
                // file; a directory failure is logged as the degradation it
                // is, then falls through the same way.
                match membership.resolve_did_to_account(&identity).await {
                    Ok(Some(account_id)) => {
                        return match membership.check_membership(space_id, &account_id).await {
                            Ok(Some(_role)) => Ok(AuthorizedSpace::new_unchecked(parsed)),
                            Ok(None) => {
                                debug!(%account_id, %space_id, "rejected: not a member of this space");
                                Err(not_authorized(space_id))
                            }
                            Err(e) => {
                                debug!(error = %e, %space_id, "membership check failed");
                                Err(not_authorized(space_id))
                            }
                        };
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!(error = %e, %space_id, "DID resolution failed; membership unknown");
                    }
                }
            }

            // Fall back to authorized_keys file (OSS relay). A bare DID authorizes
            // every space forever; a scoped/expiring entry is confined to its
            // spaces and rejected once expired.
            if let Some(authorized_keys) = authorized_keys.as_ref()
                && authorized_keys.authorize(&identity, space_id, kutl_core::now_ms())
            {
                return Ok(AuthorizedSpace::new_unchecked(parsed));
            }

            debug!(did = %identity, %space_id, "rejected: not authorized");
            Err(not_authorized(space_id))
        }
    }

    /// Authorize a WebSocket connection for access to a space.
    ///
    /// Looks up the connection's authenticated identity and delegates to
    /// [`authorize_space`]. Authentication is mandatory, so a
    /// connection with no authenticated identity is rejected.
    pub(super) fn authorize_conn(
        &self,
        conn_id: ConnId,
        space_id: &str,
    ) -> impl Future<Output = Result<AuthorizedSpace, AclError>> + Send + use<> {
        let decision = self.authenticated.get(&conn_id).map(|(identity, pat_ctx)| {
            self.authorize_space(
                identity,
                space_id,
                pat_ctx.as_ref().map(|p| p.pat_hash.as_str()),
            )
        });
        async move {
            match decision {
                Some(decision) => decision.await,
                None => Err(AclError::NotAuthenticated),
            }
        }
    }
}
