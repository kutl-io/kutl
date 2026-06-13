//! Auth/handshake family of the relay actor: WebSocket handshake
//! authentication, DID challenge-response and device-flow handlers, and
//! the space/connection ACL checks that gate every space-scoped command.
//!
//! Child module of the relay actor (`super`) so the `impl Relay` block here
//! reaches the actor's private fields directly. Pure relocation from
//! relay.rs — behavior and dispatch are unchanged; `process_command` in
//! relay.rs still routes auth commands to these handlers, and `mcp.rs`
//! reaches `authorize_space` as a sibling.

use kutl_proto::sync::{self, ErrorCode};
use tracing::{debug, info, warn};

use crate::acl::{AuthError as AclError, AuthorizedSpace};
use crate::auth::{AuthError, DeviceTokenResponse};
use crate::protocol::{encode_envelope, wrap_error, wrap_handshake_ack};
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

impl Relay {
    pub(super) async fn handle_handshake(&mut self, conn_id: ConnId, msg: &sync::Handshake) {
        info!(conn_id, client_name = %msg.client_name, display_name = %msg.display_name, "handshake received");

        if self.config.require_auth {
            let auth = self
                .auth
                .as_ref()
                .expect("auth store exists when require_auth is true");
            let now = kutl_core::now_ms();
            let result = auth
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
                    let err = wrap_error(ErrorCode::AuthFailed, e.to_string());
                    self.send_ack(conn_id, &encode_envelope(&err));
                    self.connections.remove(&conn_id);
                    return;
                }
            }
        }

        let ack = wrap_handshake_ack(&self.config);
        self.send_ack(conn_id, &encode_envelope(&ack));
    }

    pub(super) fn handle_auth_challenge(
        &mut self,
        did: &str,
    ) -> Result<ChallengeResponse, AuthError> {
        let auth = self.auth.as_mut().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        let (nonce, expires_at) = auth.create_challenge(did, now)?;
        Ok(ChallengeResponse { nonce, expires_at })
    }

    pub(super) async fn handle_auth_verify(
        &mut self,
        did: &str,
        nonce: &str,
        signature: &str,
    ) -> Result<VerifyResponse, AuthError> {
        let auth = self.auth.as_mut().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        let (token, expires_at) = auth
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

    pub(super) fn handle_create_device_request(
        &mut self,
    ) -> Result<DeviceRequestResponse, AuthError> {
        let auth = self.auth.as_mut().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        let (device_code, user_code, expires_at) = auth.create_device_request(now);
        Ok(DeviceRequestResponse {
            device_code,
            user_code,
            expires_at,
        })
    }

    pub(super) fn handle_poll_device(
        &mut self,
        device_code: &str,
    ) -> Result<DeviceTokenResponse, AuthError> {
        let auth = self.auth.as_mut().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        auth.poll_device(device_code, now)
    }

    pub(super) fn handle_authorize_device(
        &mut self,
        user_code: &str,
        token: String,
        account_id: String,
        display_name: String,
    ) -> Result<(), AuthError> {
        let auth = self.auth.as_mut().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        auth.authorize_device(user_code, token, account_id, display_name, now)
    }

    /// Authoritative `author_did` to record for a connection, overriding any
    /// client-supplied value.
    ///
    /// When `require_auth` is enabled, returns the DID the connection
    /// authenticated as (via bearer token or PAT). Any `author_did` or
    /// `participant_did` field arriving over the wire must be replaced with
    /// this value to prevent an authenticated peer from impersonating a
    /// different user in signals, presence, edit metadata, or lifecycle
    /// events.
    ///
    /// When `require_auth` is disabled (dev/simulation mode), the relay has
    /// no authenticated identity to substitute and returns `None` — callers
    /// fall back to the client-supplied field, preserving the existing
    /// unauthenticated behaviour.
    pub(super) fn authoritative_author_did(&self, conn_id: ConnId) -> Option<&str> {
        if !self.config.require_auth {
            return None;
        }
        self.authenticated.get(&conn_id).map(|(id, _)| id.as_str())
    }

    /// Resolve the caller's account ID from their authenticated identity.
    ///
    /// Handles both `account:<uuid>` (PAT auth) and DID (challenge-response
    /// auth via `custodied_keys`). Returns `None` if the identity cannot be
    /// resolved to an account.
    pub(super) async fn resolve_account_id(
        &self,
        conn_id: ConnId,
        membership: &dyn crate::membership_backend::MembershipBackend,
    ) -> Option<String> {
        let (identity, _) = self.authenticated.get(&conn_id)?;
        if let Some(account_id) = spaces::extract_account_id(identity) {
            return Some(account_id.to_owned());
        }
        // DID-based identity — resolve via custodied_keys.
        match membership.resolve_did_to_account(identity).await {
            Ok(Some(account_id)) => Some(account_id),
            Ok(None) => {
                debug!(conn_id, did = %identity, "no account found for DID");
                None
            }
            Err(e) => {
                warn!(conn_id, error = %e, "failed to resolve DID to account");
                None
            }
        }
    }

    /// Authorize an identity string for access to a space.
    ///
    /// Returns an [`AuthorizedSpace`] on success. The decision tree:
    /// 0. If the connection was PAT-authed, verify the token is scoped for
    ///    this space before any membership check.
    /// 1. If `require_auth` is false, allow.
    /// 2. If no database and no authorized keys file, allow (dev/test mode).
    /// 3. Account-based identity (`account:<uuid>`) — check `space_memberships`.
    /// 4. DID-based identity — resolve via `custodied_keys`, check `space_memberships`.
    /// 5. Fall back to authorized keys file (OSS relay).
    pub(super) async fn authorize_space(
        &self,
        identity: &str,
        space_id: &str,
        pat_hash: Option<&str>,
    ) -> Result<AuthorizedSpace, AclError> {
        // Step 0: PAT space-scoping — the token may restrict which spaces
        // the holder can access regardless of membership.
        if let Some(hash) = pat_hash
            && let Some(ref backend) = self.pat_backend
        {
            match backend.check_scope(hash, space_id).await {
                Ok(false) => {
                    debug!(%space_id, "rejected: PAT not scoped for this space");
                    return Err(AclError::NotAuthorized {
                        space_id: space_id.to_owned(),
                    });
                }
                Err(e) => {
                    debug!(error = %e, "PAT scope check failed");
                    return Err(AclError::NotAuthorized {
                        space_id: space_id.to_owned(),
                    });
                }
                Ok(true) => {}
            }
        }
        if !self.config.require_auth {
            return Ok(AuthorizedSpace::new_unchecked(space_id.to_owned()));
        }

        // Startup check in build_app_with_backends_and_web guarantees that
        // at least one of membership_backend or authorized_keys is present
        // when require_auth is true. If we somehow reach this point without
        // either, reject rather than silently allowing.

        // Database-backed ACL checks.
        if let Some(ref membership) = self.membership_backend {
            // Account-based identity (PAT auth) — check space_memberships table.
            if let Some(account_id) = spaces::extract_account_id(identity) {
                match membership.check_membership(space_id, account_id).await {
                    Ok(Some(_role)) => {
                        return Ok(AuthorizedSpace::new_unchecked(space_id.to_owned()));
                    }
                    Ok(None) => {
                        debug!(%account_id, %space_id, "rejected: not a member of this space");
                        return Err(AclError::NotAuthorized {
                            space_id: space_id.to_owned(),
                        });
                    }
                    Err(e) => {
                        debug!(error = %e, %space_id, "membership check failed");
                        return Err(AclError::NotAuthorized {
                            space_id: space_id.to_owned(),
                        });
                    }
                }
            }

            // DID-based identity (challenge-response auth).
            // Try resolving DID -> account via custodied_keys, then check
            // space_memberships.
            if let Ok(Some(account_id)) = membership.resolve_did_to_account(identity).await {
                match membership.check_membership(space_id, &account_id).await {
                    Ok(Some(_role)) => {
                        return Ok(AuthorizedSpace::new_unchecked(space_id.to_owned()));
                    }
                    Ok(None) => {
                        debug!(%account_id, %space_id, "rejected: not a member of this space");
                        return Err(AclError::NotAuthorized {
                            space_id: space_id.to_owned(),
                        });
                    }
                    Err(e) => {
                        debug!(error = %e, %space_id, "membership check failed");
                        return Err(AclError::NotAuthorized {
                            space_id: space_id.to_owned(),
                        });
                    }
                }
            }
        }

        // Fall back to authorized_keys file (OSS relay).
        if let Some(ref authorized_keys) = self.authorized_keys
            && authorized_keys.is_authorized(identity)
        {
            return Ok(AuthorizedSpace::new_unchecked(space_id.to_owned()));
        }

        debug!(did = %identity, %space_id, "rejected: not authorized");
        Err(AclError::NotAuthorized {
            space_id: space_id.to_owned(),
        })
    }

    /// Authorize a WebSocket connection for access to a space.
    ///
    /// Looks up the connection's authenticated identity and delegates to
    /// [`authorize_space`]. When `require_auth` is false, skips identity
    /// lookup (connections may not have authenticated).
    pub(super) async fn authorize_conn(
        &self,
        conn_id: ConnId,
        space_id: &str,
    ) -> Result<AuthorizedSpace, AclError> {
        if !self.config.require_auth {
            return Ok(AuthorizedSpace::new_unchecked(space_id.to_owned()));
        }
        let (identity, pat_ctx) = self
            .authenticated
            .get(&conn_id)
            .ok_or(AclError::NotAuthenticated)?;
        let pat_hash = pat_ctx.as_ref().map(|p| p.pat_hash.as_str());
        self.authorize_space(identity, space_id, pat_hash).await
    }
}
