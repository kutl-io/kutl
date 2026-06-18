//! Space ops + signals family of the relay actor: space registration and
//! resolution, invitation joins, membership and document listings, and the
//! WebSocket flag-signal path with its shared persist-then-broadcast core.
//!
//! Child module of the relay actor (`super`) so the `impl Relay` block here
//! reaches the actor's private fields directly. Pure relocation from
//! relay.rs — behavior and dispatch are unchanged; `process_command` in
//! relay.rs still routes space and signal commands to these handlers, and
//! `mcp.rs` reaches `relay_flag_signal` as a sibling (the MCP create-flag
//! path).

use kutl_proto::sync::{self, ErrorCode};
use tracing::{debug, error, info, warn};

use crate::observer::SignalCreatedEvent;
use crate::protocol::{encode_envelope, signal_flag_envelope};
use crate::spaces;

use super::{ConnId, Relay};

impl Relay {
    // -----------------------------------------------------------------------
    // Space ops handlers (require a configured database)
    // -----------------------------------------------------------------------

    /// Handle `RegisterSpace` — create a new space via the space backend.
    pub(super) async fn handle_register_space(
        &self,
        name: &str,
    ) -> Result<crate::space_backend::RegisteredSpace, crate::space_backend::SpaceBackendError>
    {
        if let Err(msg) = spaces::validate_space_name(name) {
            return Err(crate::space_backend::SpaceBackendError::InvalidName(
                msg.to_owned(),
            ));
        }
        let backend = self.space_backend.as_ref().ok_or_else(|| {
            crate::space_backend::SpaceBackendError::Storage("space backend not configured".into())
        })?;
        let result = backend.register(name).await?;
        info!(space_id = %result.space_id, name = %result.name, "space registered");
        Ok(result)
    }

    /// Handle `ResolveSpace` — look up a space by name via the space backend.
    pub(super) async fn handle_resolve_space(
        &self,
        name: &str,
    ) -> Result<
        Option<crate::space_backend::RegisteredSpace>,
        crate::space_backend::SpaceBackendError,
    > {
        let backend = self.space_backend.as_ref().ok_or_else(|| {
            crate::space_backend::SpaceBackendError::Storage("space backend not configured".into())
        })?;
        let result = backend.resolve_by_name(name).await?;
        if let Some(ref space) = result {
            info!(space_id = %space.space_id, name = %space.name, "space resolved");
        }
        Ok(result)
    }

    /// Handle `JoinSpace` — accept an invitation code and join the space.
    pub(super) async fn handle_join_space_op(&self, conn_id: ConnId, msg: &sync::JoinSpace) {
        let Some(ref membership) = self.membership_backend else {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                "operation not supported on this relay",
            );
            return;
        };

        let Some(account_id) = self.resolve_account_id(conn_id, membership.as_ref()).await else {
            self.send_error(conn_id, ErrorCode::AuthFailed, "not authenticated");
            return;
        };

        match membership.accept_invitation(&msg.code, &account_id).await {
            Ok(result) => {
                self.send_payload(
                    conn_id,
                    sync::sync_envelope::Payload::JoinSpaceResult(sync::JoinSpaceResult {
                        space_id: result.space_id,
                        space_name: result.space_name,
                        space_slug: result.space_slug,
                        owner_account_id: result.owner_account_id,
                    }),
                );
            }
            Err(e) => {
                warn!(conn_id, error = %e, "join space failed");
                self.send_error(conn_id, ErrorCode::InvalidMessage, &e.to_string());
            }
        }
    }

    /// Handle `ResolveSpace` — look up a space by owner slug and space slug.
    pub(super) async fn handle_resolve_space_op(&self, conn_id: ConnId, msg: &sync::ResolveSpace) {
        let Some(ref membership) = self.membership_backend else {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                "operation not supported on this relay",
            );
            return;
        };

        let Some(account_id) = self.resolve_account_id(conn_id, membership.as_ref()).await else {
            self.send_error(conn_id, ErrorCode::AuthFailed, "not authenticated");
            return;
        };

        match membership
            .resolve_space_by_slugs(&msg.owner, &msg.slug)
            .await
        {
            Ok(Some(record)) => {
                // Verify caller is a member.
                match membership
                    .check_membership(&record.space_id, &account_id)
                    .await
                {
                    Ok(Some(_role)) => {
                        let relay_url = self.config.external_url.clone().unwrap_or_default();
                        self.send_payload(
                            conn_id,
                            sync::sync_envelope::Payload::ResolveSpaceResult(
                                sync::ResolveSpaceResult {
                                    space_id: record.space_id,
                                    space_name: record.space_name,
                                    relay_url,
                                },
                            ),
                        );
                    }
                    Ok(None) => {
                        // Anti-enumeration (RFD 0050): a non-member must not be
                        // able to distinguish an existing private space from a
                        // non-existent one. Return the identical not-found error.
                        self.send_error(conn_id, ErrorCode::InvalidMessage, "space not found");
                    }
                    Err(e) => {
                        // This arm is reachable only when the space EXISTS
                        // (resolve returned Some). Anti-enumeration (RFD 0050):
                        // emit the byte-identical response the resolve-error arm
                        // sends, so an internal failure on an existing space is
                        // indistinguishable from one on a non-existent space.
                        // (AuthFailed would otherwise leak existence: the CLI
                        // maps it to a distinct "run kutl auth login" message.)
                        debug!(conn_id, error = %e, "membership check failed");
                        self.send_error(
                            conn_id,
                            ErrorCode::InvalidMessage,
                            "internal error resolving space",
                        );
                    }
                }
            }
            Ok(None) => {
                self.send_error(conn_id, ErrorCode::InvalidMessage, "space not found");
            }
            Err(e) => {
                warn!(conn_id, error = %e, "resolve space failed");
                self.send_error(
                    conn_id,
                    ErrorCode::InvalidMessage,
                    "internal error resolving space",
                );
            }
        }
    }

    /// Handle `ListMySpaces` — return all spaces the caller is a member of.
    pub(super) async fn handle_list_my_spaces_op(
        &self,
        conn_id: ConnId,
        _msg: &sync::ListMySpaces,
    ) {
        let Some(ref membership) = self.membership_backend else {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                "operation not supported on this relay",
            );
            return;
        };

        let Some(account_id) = self.resolve_account_id(conn_id, membership.as_ref()).await else {
            self.send_error(conn_id, ErrorCode::AuthFailed, "not authenticated");
            return;
        };

        match membership.list_spaces_for_account(&account_id).await {
            Ok(memberships) => {
                let space_infos: Vec<sync::SpaceInfo> = memberships
                    .into_iter()
                    .map(|m| sync::SpaceInfo {
                        space_id: m.space_id,
                        name: m.space_name,
                        slug: m.space_slug,
                        owner_account_id: m.owner_account_id,
                        role: m.role,
                    })
                    .collect();
                self.send_payload(
                    conn_id,
                    sync::sync_envelope::Payload::ListMySpacesResult(sync::ListMySpacesResult {
                        spaces: space_infos,
                    }),
                );
            }
            Err(e) => {
                warn!(conn_id, error = %e, "list spaces failed");
                self.send_error(
                    conn_id,
                    ErrorCode::InvalidMessage,
                    "internal error listing spaces",
                );
            }
        }
    }

    /// Handle `ListSpaceDocuments` — return all active documents in the space registry.
    ///
    /// Also registers the connection as a space watcher so it receives
    /// lifecycle broadcasts (register/rename/unregister) even if it hasn't
    /// subscribed to any specific document yet.
    pub(super) async fn handle_list_space_documents(
        &mut self,
        conn_id: ConnId,
        msg: &sync::ListSpaceDocuments,
    ) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                return;
            }
        };

        // Register as a space watcher for lifecycle broadcasts.
        self.space_watchers
            .entry(msg.space_id.clone())
            .or_default()
            .insert(conn_id);

        let documents: Vec<sync::DocumentInfo> = self
            .registries
            .get(&msg.space_id)
            .map(|reg| {
                reg.active_entries()
                    .map(|(_, entry)| sync::DocumentInfo {
                        document_id: entry.document_id.clone(),
                        path: entry.path.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let has_conn = self.connections.contains_key(&conn_id);
        info!(
            conn_id,
            space_id = %msg.space_id,
            document_count = documents.len(),
            has_conn,
            "sending ListSpaceDocumentsResult"
        );

        self.send_payload(
            conn_id,
            sync::sync_envelope::Payload::ListSpaceDocumentsResult(
                sync::ListSpaceDocumentsResult {
                    space_id: msg.space_id.clone(),
                    documents,
                },
            ),
        );
    }

    // -----------------------------------------------------------------------
    // Signal handler (flag payload accepted over WebSocket)
    // -----------------------------------------------------------------------

    /// Handle an incoming signal from a WebSocket client.
    ///
    /// Only flag payloads are accepted over WebSocket. Reply and decision
    /// signals are created exclusively through MCP.
    pub(super) async fn handle_signal(&mut self, conn_id: ConnId, msg: &sync::Signal) {
        // Only flag payloads arrive via WebSocket.
        let Some(sync::signal::Payload::Flag(flag)) = &msg.payload else {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                "only flag signals accepted over websocket",
            );
            return;
        };

        let space_id = &msg.space_id;
        let _authorized = match self.authorize_conn(conn_id, space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                return;
            }
        };

        let document_id = match &msg.document_id {
            Some(d) if !d.is_empty() => d.as_str(),
            _ => {
                self.send_error(
                    conn_id,
                    ErrorCode::InvalidMessage,
                    "flag signal requires document_id",
                );
                return;
            }
        };
        // Catch path-string document_ids before they reach the durable
        // backend's `::uuid` cast and surface as opaque DB errors. Signals
        // index documents by UUID; path-typed callers are bugs we want
        // to fail loudly.
        if uuid::Uuid::parse_str(document_id).is_err() {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                &format!("document_id must be a UUID; got {document_id:?}"),
            );
            return;
        }

        // Reject UNSPECIFIED audience.
        if flag.audience_type == i32::from(sync::AudienceType::Unspecified) {
            self.send_error(conn_id, ErrorCode::InvalidMessage, "audience type required");
            return;
        }

        // Reject PARTICIPANT without target_did.
        if flag.audience_type == i32::from(sync::AudienceType::Participant)
            && flag.target_did.as_ref().is_none_or(String::is_empty)
        {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                "participant audience requires target_did",
            );
            return;
        }

        // Author identity is authoritative when the relay authenticated
        // this connection. Any client-supplied `author_did` would otherwise
        // let an authenticated member impersonate a different DID in signals
        // (including DM targets and persisted `signals.author_did`).
        let author_did = self
            .authoritative_author_did(conn_id)
            .map_or_else(|| msg.author_did.clone(), str::to_owned);

        // Client-supplied signal id is honored when present and well-
        // formed. Used by the ingestion-worker's comment-materialization
        // path: the `[text]{.cmt #<uuid>}` marker in the
        // imported document content must reference the SAME UUID as the
        // FLAG_KIND_COMMENT signal so the relay's CommentTracker can tie
        // marker to signal on next merge. Without honoring the supplied
        // id the relay would mint a different one and the marker would
        // point at a non-existent signal. Empty id keeps the legacy
        // behavior (relay mints).
        let supplied_id: Option<&str> = if msg.id.is_empty() {
            None
        } else {
            if uuid::Uuid::parse_str(&msg.id).is_err() {
                self.send_error(
                    conn_id,
                    ErrorCode::InvalidMessage,
                    &format!("signal id must be a UUID; got {id:?}", id = msg.id),
                );
                return;
            }
            Some(msg.id.as_str())
        };

        if let Err(e) = self
            .relay_flag_signal(
                supplied_id,
                space_id,
                document_id,
                &author_did,
                flag.kind,
                flag.audience_type,
                &flag.message,
                flag.target_did.as_deref().unwrap_or(""),
                // The WS `FlagPayload` doesn't carry `anchor_text` —
                // comment-anchor posterity on the WS path comes from
                // the document body (CommentTracker on merge), not
                // from the WS signal envelope.
                None,
                msg.timestamp,
                Some(&author_did),
                Some(conn_id),
                // WS signal-create uses DID challenge-response; no PAT
                // is involved in the WS auth path, so via_pat_id is
                // always None on this code path.
                None,
            )
            .await
        {
            // Persistence failed → no broadcast happened, no observer
            // event fired. Tell the WS client honestly so it can retry
            // or surface the failure to its user.
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                &format!("failed to persist flag signal: {e}"),
            );
        }
    }

    /// Shared core of flag signal fanout.
    ///
    /// Uses `supplied_id` as the signal id when present (well-formed UUID
    /// pre-validated by the caller); otherwise mints a fresh v4 UUID.
    /// Builds the relay envelope, broadcasts to the appropriate subscribers,
    /// notifies the observer, and awaits the backend write synchronously.
    /// Returns the resolved signal id.
    ///
    /// Called from both the WS path (`handle_signal`) and the MCP path
    /// (`handle_mcp_create_flag`).
    ///
    /// `anchor_text` — RFD 0077 comment-kind posterity snapshot. `Some` for
    /// `FLAG_KIND_COMMENT` signals with an inline marker; ignored (and
    /// persisted as `None`) for other kinds. Threaded through to the change
    /// backend; not part of the broadcast envelope (the inline marker in
    /// document content is the live anchor binding).
    ///
    /// `skip_did` — when `Some`, all connections authenticated as that DID are
    /// excluded from the broadcast (used to suppress echo back to the author).
    /// `skip_conn` — when `Some`, this specific connection is also excluded
    /// (fallback for unauthenticated connections where DID-based skip doesn't work).
    /// For `PARTICIPANT` audience the skip is irrelevant because delivery is
    /// restricted to `target_did` and `author_did` regardless.
    ///
    /// **Persistence order.** The change-backend write is awaited
    /// before broadcast or observer notification. If persistence
    /// fails, the function returns the error and NEITHER the WS
    /// broadcast nor the observer event fires. This is deliberate:
    /// before the fix, the broadcast/observer ran first and a
    /// persistence error was logged-and-swallowed, leaving callers
    /// (notably the MCP path) with a phantom-success response and
    /// downstream subscribers with state divergent from the DB. The
    /// extra latency of one DB round-trip before broadcast is worth
    /// the failure honesty.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn relay_flag_signal(
        &mut self,
        supplied_id: Option<&str>,
        space_id: &str,
        document_id: &str,
        author_did: &str,
        kind: i32,
        audience_type: i32,
        message: &str,
        target_did: &str,
        anchor_text: Option<&str>,
        timestamp: i64,
        skip_did: Option<&str>,
        skip_conn: Option<ConnId>,
        via_pat_id: Option<&str>,
    ) -> Result<String, crate::change_backend::ChangeError> {
        let signal_id = supplied_id.map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);

        // Persist FIRST. A failure here halts the whole emit so the
        // caller (MCP tool response, WS reply) sees the truth instead
        // of a phantom-success broadcast that never made it to the DB.
        if let Some(ref backend) = self.change_backend {
            backend
                .record_signal(
                    &signal_id,
                    space_id,
                    Some(document_id),
                    author_did,
                    "flag",
                    timestamp,
                    Some(kind),
                    Some(audience_type),
                    Some(message),
                    Some(target_did),
                    anchor_text,
                    None,
                    None,
                    None,
                    via_pat_id,
                )
                .await
                .inspect_err(|e| {
                    error!(signal_id = %signal_id, error = %e, "failed to persist flag signal — skipping broadcast");
                })?;
        }

        let envelope = signal_flag_envelope(
            &signal_id,
            space_id,
            document_id,
            author_did,
            kind,
            audience_type,
            message,
            target_did,
            timestamp,
        );
        let bytes = encode_envelope(&envelope);

        let is_participant = audience_type == i32::from(sync::AudienceType::Participant);

        // Collect the broadcast recipients first, then fan out — keeps the
        // delivery loop independent of the borrows used to select recipients.
        let mut recipients: Vec<ConnId> = Vec::new();
        if is_participant {
            // DM mode: deliver only to target_did and author_did connections.
            for &cid in self.connections.keys() {
                if let Some((did, _)) = self.authenticated.get(&cid)
                    && (*did == target_did || *did == author_did)
                {
                    recipients.push(cid);
                }
            }
        } else {
            // Broadcast to all unique subscribers in the space, skipping any
            // connections authenticated as skip_did (the author).
            let mut seen = std::collections::HashSet::new();
            for (key, slot) in &self.documents {
                if key.space_id != space_id {
                    continue;
                }
                for (sub_conn_id, _) in slot.subscribers.active_entries() {
                    let is_skipped = skip_conn == Some(sub_conn_id)
                        || skip_did.is_some_and(|skip| {
                            self.authenticated
                                .get(&sub_conn_id)
                                .is_some_and(|(d, _)| d.as_str() == skip)
                        });
                    if !is_skipped
                        && seen.insert(sub_conn_id)
                        && self.connections.contains_key(&sub_conn_id)
                    {
                        recipients.push(sub_conn_id);
                    }
                }
            }
        }
        for cid in recipients {
            self.send_broadcast(cid, &bytes);
        }

        self.observer.on_signal_created(SignalCreatedEvent {
            id: signal_id.clone(),
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            author_did: author_did.to_owned(),
            signal_type: "flag".to_owned(),
            timestamp: kutl_core::env::now_ms(),
            flag_kind: Some(kind.to_string()),
            audience: Some(audience_type.to_string()),
            target_did: Some(target_did.to_owned()),
            message: Some(message.to_owned()),
            parent_signal_id: None,
            parent_reply_id: None,
            body: None,
            via_pat_id: via_pat_id.map(str::to_owned),
        });

        Ok(signal_id)
    }
}
