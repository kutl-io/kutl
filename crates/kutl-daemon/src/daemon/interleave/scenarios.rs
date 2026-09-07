//! Convergence properties pinned at every point of a schedule.
//!
//! Each scenario runs a real joiner against a scripted peer, moving the wire
//! one step at a time, and checks the outcome the same way at the end: what
//! is on the joiner's disk, what it persisted, what the relay's registry
//! holds, and which frames each side sent. A property is enumerated over
//! the joiner's whole startup schedule: the peer acts at step 0, at step 1,
//! ..., after the last step, and the joiner must converge from every one.

use std::collections::HashMap;

use kutl_proto::protocol::{
    RegisterDocumentMetadata, register_document_envelope, rename_document_envelope,
    submit_flag_envelope, subscribe_envelope, subscribe_signals_envelope, sync_ops_envelope,
};
use kutl_proto::sync::{ChangeMetadata, FlagKind, sync_envelope::Payload};
use kutl_relay::config::RelayConfig;

use super::{Interleaver, Lane, PartyId, Step, payload_kind};

/// The joiner's own file, present before it starts.
const LOCAL_PATH: &str = "local.md";
const LOCAL_CONTENT: &str = "mine\n";
/// The document the peer publishes, and where it renames it to.
const PEER_PATH: &str = "peer.md";
const PEER_RENAMED_PATH: &str = "renamed/peer.md";
const PEER_CONTENT: &str = "theirs\n";

/// What the peer does at its step of the joiner's schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerMove {
    /// Register [`PEER_PATH`], subscribe, fill it.
    Publish,
    /// [`Self::Publish`], then rename it to [`PEER_RENAMED_PATH`] at once,
    /// so the register and the rename are both in flight to the joiner.
    PublishAndRename,
}

impl PeerMove {
    /// Where the peer's document must end up.
    fn final_path(self) -> &'static str {
        match self {
            Self::Publish => PEER_PATH,
            Self::PublishAndRename => PEER_RENAMED_PATH,
        }
    }

    /// The most placements the joiner may make for the peer's document
    /// without a bounce: one to land it, plus one to move it when the rename
    /// arrives after it landed. A document the snapshot already showed at
    /// its final path costs none, since the startup reconcile writes it
    /// without a placement.
    fn placement_ceiling(self) -> u64 {
        match self {
            Self::Publish => 1,
            Self::PublishAndRename => 2,
        }
    }
}

/// Text ops inserting `text` into an empty document, as a peer's worker
/// sends them for a new file.
fn text_ops(author: &str, text: &str) -> (Vec<u8>, Vec<ChangeMetadata>) {
    let mut doc = kutl_core::Document::new();
    // The agent name is a short session label; the author DID rides the change.
    let agent = doc.register_agent("peer").unwrap();
    doc.edit(
        agent,
        author,
        "file change",
        kutl_core::Boundary::Explicit,
        |ctx| ctx.insert(0, text),
    )
    .unwrap();
    doc.delta_since(&[])
}

/// A peer already in the space: connected and listening, its handshake and
/// listen answered before the joiner exists.
async fn join_peer(h: &mut Interleaver, space: &str) -> PartyId {
    let peer = h.add_peer(space);
    h.connect(peer).await;
    h.send(peer, &subscribe_signals_envelope(space, None));
    h.process_all(peer).await;
    h.deliver_all(peer).await;
    peer
}

/// Queue the peer's publication of `doc` at `path`: register, subscribe,
/// fill with [`PEER_CONTENT`]. Returns the registration's stamp, the causal
/// floor a later rename carries.
fn peer_publish(
    h: &mut Interleaver,
    peer: PartyId,
    space: &str,
    doc: &str,
    path: &str,
) -> Option<kutl_proto::sync::Hlc> {
    let register = h.stamp(peer, "register");
    let registered_hlc = register.hlc.clone();
    h.send(
        peer,
        &register_document_envelope(
            space,
            doc,
            path,
            Some(register),
            RegisterDocumentMetadata::default(),
        ),
    );
    h.send(peer, &subscribe_envelope(space, doc));
    let (ops, metadata) = text_ops(h.did(peer), PEER_CONTENT);
    h.send(
        peer,
        &sync_ops_envelope(space, doc, ops, metadata, HashMap::new()),
    );
    registered_hlc
}

/// The peer registers `doc` at [`PEER_PATH`], subscribes to it and fills it
/// with [`PEER_CONTENT`], then renames it if `mv` says so, all processed
/// by the relay before the schedule moves on.
async fn peer_moves(h: &mut Interleaver, peer: PartyId, space: &str, doc: &str, mv: PeerMove) {
    let registered_hlc = peer_publish(h, peer, space, doc, PEER_PATH);
    if mv == PeerMove::PublishAndRename {
        let rename = h.stamp(peer, "rename");
        h.send(
            peer,
            &rename_document_envelope(
                space,
                doc,
                PEER_PATH,
                PEER_RENAMED_PATH,
                Some(rename),
                registered_hlc,
            ),
        );
    }
    h.process_all(peer).await;
}

/// The visible files under a space root, relative and sorted.
fn visible_files(root: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = walkdir::WalkDir::new(root)
        .into_iter()
        // The root is a temp dir, whose own name starts with a dot.
        .filter_entry(|e| e.depth() == 0 || !e.file_name().to_string_lossy().starts_with('.'))
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .map(|e| {
            e.path()
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

/// The one convergence check, over a stopped joiner: it holds both
/// documents at their paths with their content, persisted and confirmed;
/// the relay lists exactly those two; the joiner claimed the peer's
/// document by subscription alone, never by re-registering, renaming or
/// unregistering anything; and the peer heard the joiner's registration.
/// `run` names the schedule in every message.
fn assert_converged(
    h: &Interleaver,
    joiner: PartyId,
    peer: PartyId,
    space: &str,
    doc: &str,
    mv: PeerMove,
    run: &str,
) {
    let (peer_path, placement_ceiling) = (mv.final_path(), mv.placement_ceiling());
    let root = h.space_root_of(joiner).to_path_buf();
    let sent: Vec<&'static str> = h.sent(joiner).iter().map(payload_kind).collect();

    assert_eq!(
        std::fs::read_to_string(root.join(peer_path))
            .ok()
            .as_deref(),
        Some(PEER_CONTENT),
        "{run}: the peer's file on the joiner's disk; joiner sent {sent:?}"
    );
    assert_eq!(
        std::fs::read_to_string(root.join(LOCAL_PATH)).unwrap(),
        LOCAL_CONTENT,
        "{run}"
    );
    let mut expected_files = [LOCAL_PATH.to_owned(), peer_path.to_owned()];
    expected_files.sort();
    assert_eq!(visible_files(&root), expected_files, "{run}");

    let state = h.persisted_state(joiner);
    let peer_entry = state
        .documents
        .get(peer_path)
        .unwrap_or_else(|| panic!("{run}: the peer's document is tracked"));
    assert_eq!(peer_entry.id, doc, "{run}");
    assert!(
        peer_entry.confirmed,
        "{run}: the peer's document is confirmed"
    );
    assert!(
        state.documents.get(LOCAL_PATH).is_some_and(|e| e.confirmed),
        "{run}: the local file is confirmed"
    );

    let registry = h.relay().registry(space).expect("the space exists");
    assert_eq!(
        registry.active_entries().count(),
        2,
        "{run}: the relay lists exactly the two documents"
    );
    assert_eq!(
        registry
            .get_by_path(peer_path)
            .map(|e| e.document_id.as_str()),
        Some(doc),
        "{run}"
    );
    let local_id = registry
        .get_by_path(LOCAL_PATH)
        .expect("the local file is registered")
        .document_id
        .clone();

    assert!(
        h.sent(joiner)
            .iter()
            .any(|p| matches!(p, Payload::Subscribe(s) if s.document_id == doc)),
        "{run}: the joiner subscribed to the peer's document; sent {sent:?}"
    );
    let unwanted: Vec<&Payload> = h
        .sent(joiner)
        .iter()
        .filter(|p| match p {
            Payload::RegisterDocument(r) => r.path == peer_path || r.document_id == doc,
            Payload::RenameDocument(_) | Payload::UnregisterDocument(_) => true,
            _ => false,
        })
        .collect();
    assert!(
        unwanted.is_empty(),
        "{run}: the joiner re-arbitrated a settled document: {unwanted:?}"
    );

    let received = h.received(peer);
    assert!(
        received
            .iter()
            .any(|p| matches!(p, Payload::RegisterDocument(r) if r.document_id == local_id)),
        "{run}: the peer heard the joiner's registration; received {:?}",
        received.iter().map(payload_kind).collect::<Vec<_>>()
    );
    assert!(
        received.iter().any(
            |p| matches!(p, Payload::RegisterDocumentAck(a) if a.document_id == doc && a.success)
        ),
        "{run}: the peer's registration was acknowledged"
    );

    let placements = Interleaver::placements(space);
    assert!(
        placements <= placement_ceiling,
        "{run}: the joiner placed the peer's document {placements} times; more than \
         {placement_ceiling} means it bounced through a stale path"
    );
}

/// Run a joiner's startup against a peer that makes `mv` at step `at` of
/// the schedule (`None`: never), and return the schedule taken up to the
/// point the wire first went idle. With a move, the outcome is checked.
async fn joiner_startup(mv: PeerMove, at: Option<usize>) -> Vec<Step> {
    let space = uuid::Uuid::new_v4().to_string();
    let doc = uuid::Uuid::new_v4().to_string();
    let mut h = Interleaver::new(RelayConfig::default().outbound_capacity, false);
    let peer = join_peer(&mut h, &space).await;
    let root = h.space_root();
    std::fs::write(root.join(LOCAL_PATH), LOCAL_CONTENT).unwrap();
    let joiner = h.add_daemon(root, &space);
    h.connect(joiner).await;

    let mut trace = Vec::new();
    let mut moved = false;
    while let Some(step) = {
        if at == Some(trace.len()) {
            peer_moves(&mut h, peer, &space, &doc, mv).await;
            moved = true;
        }
        h.next_step().await
    } {
        h.take(step).await;
        trace.push(step);
    }
    if at.is_some() && !moved {
        // The peer acts after the joiner's startup settled: a live session.
        peer_moves(&mut h, peer, &space, &doc, mv).await;
        h.run_to_idle().await;
    }
    h.stop(joiner).await.expect("the joiner exits cleanly");
    if let Some(at) = at {
        let run = format!("{mv:?} at step {at} of {trace:?}");
        assert_converged(&h, joiner, peer, &space, &doc, mv, &run);
    }
    trace
}

/// Enumerate `mv` over every step of the joiner's startup schedule, the
/// step after it included: the joiner must converge from each. The prefix of
/// each schedule before the move equals the undisturbed schedule, which is
/// the harness's own proof that the steps are the test's and not the clock's.
async fn at_every_startup_step(mv: PeerMove) {
    let baseline = joiner_startup(mv, None).await;
    assert!(
        baseline.len() > 4,
        "a startup takes several steps: {baseline:?}"
    );
    for at in 0..=baseline.len() {
        let trace = joiner_startup(mv, Some(at)).await;
        assert_eq!(
            &trace[..at],
            &baseline[..at],
            "the schedule before the peer's {mv:?} at step {at} differs from the undisturbed one"
        );
    }
}

/// A peer's document published at ANY point of the joiner's startup reaches
/// the joiner: before its handshake, between its listen and its snapshot,
/// while its snapshot is in flight, during its scan, or once it is live.
#[tokio::test(start_paused = true)]
async fn test_joiner_converges_whatever_step_the_peer_publishes_at() {
    at_every_startup_step(PeerMove::Publish).await;
}

/// A document registered and renamed at ANY point of the joiner's startup
/// lands at its renamed path and nowhere else. Between the joiner's listen
/// and its snapshot both lifecycle frames precede the snapshot in relay
/// order yet reach the joiner after it (the ack lane outruns the ctrl
/// lane), so the joiner sees the final path first and the history second.
#[tokio::test(start_paused = true)]
async fn test_joiner_converges_whatever_step_the_peer_renames_at() {
    at_every_startup_step(PeerMove::PublishAndRename).await;
}

/// A data lane this small pauses a signal stream after a few undelivered
/// catch-ups: the relay keeps one slot in four for document traffic, so
/// three frames waiting on a four-slot lane leave no room for a signal.
const SMALL_LANE: usize = 4;
/// Documents the peer publishes in a burst: as many catch-ups as the small
/// lane holds before signals must yield.
const BURST_DOCS: usize = SMALL_LANE - 1;

/// How the joiner gets its signal stream back after the relay paused it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Recovery {
    /// The pause notice reaches the joiner, which re-subscribes from its
    /// cursor: the relay re-serves what the pause missed as a page.
    Resubscribe,
    /// The connection dies before the notice arrives; the next session's
    /// listen carries the cursor and picks the missed record up.
    Reconnect,
}

/// Queue a space-wide flag from the peer with `client_ref`, and process it.
async fn peer_flags(h: &mut Interleaver, peer: PartyId, space: &str, client_ref: &str) {
    h.send(
        peer,
        &submit_flag_envelope(client_ref, space, None, FlagKind::Info, client_ref, None),
    );
    h.process_all(peer).await;
}

/// The signal id the relay acked for the peer's submit `client_ref`.
fn acked_signal(h: &Interleaver, peer: PartyId, client_ref: &str) -> String {
    h.received(peer)
        .iter()
        .find_map(|p| match p {
            Payload::SignalAck(a) if a.client_ref == client_ref && a.success => {
                Some(a.signal_id.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("the peer's submit {client_ref} was acked"))
}

/// The signal ids of the records the joiner has ingested into its own
/// segments.
fn ingested_signals(root: &std::path::Path, space: &str) -> std::collections::BTreeSet<String> {
    let dir = kutl_signals::segment::signals_root(root).join(space);
    if !dir.exists() {
        return std::collections::BTreeSet::new();
    }
    kutl_signals::segment::SegmentStore::load(&dir)
        .unwrap()
        .records
        .into_iter()
        .filter(|r| !r.record_id.is_empty())
        .map(|r| r.id)
        .collect()
}

/// A joiner behind on a burst of catch-ups has its signal stream paused by
/// a signal it had no room for, stays a listener, and recovers the record
/// it missed: by re-subscribing when the notice reaches it, or by
/// reconnecting before it does. Either way it ends with every document, the
/// missed record, and the next live one.
async fn paused_stream_recovers(recovery: Recovery) {
    let space = uuid::Uuid::new_v4().to_string();
    let mut h = Interleaver::new(SMALL_LANE, true);
    let peer = join_peer(&mut h, &space).await;
    let root = h.space_root();
    let joiner = h.add_daemon(root.clone(), &space);
    h.connect(joiner).await;
    h.run_to_idle().await;

    // The burst: the registrations reach the joiner, it subscribes to each,
    // and every catch-up lands on its data lane undelivered.
    let docs: Vec<(String, String)> = (0..BURST_DOCS)
        .map(|i| (uuid::Uuid::new_v4().to_string(), format!("d{i}.md")))
        .collect();
    for (doc, path) in &docs {
        peer_publish(&mut h, peer, &space, doc, path);
    }
    h.process_all(peer).await;
    // The peer reads its own replies promptly; only the joiner falls behind.
    h.deliver_all(peer).await;
    h.deliver_all(joiner).await;
    h.quiesce().await;
    h.process_all(joiner).await;
    assert_eq!(
        h.pending(joiner, Lane::Data),
        BURST_DOCS,
        "the burst's catch-ups wait on the joiner's data lane; joiner sent {:?}",
        h.sent(joiner).iter().map(payload_kind).collect::<Vec<_>>()
    );

    // A signal now finds the joiner's lane out of room: its stream pauses,
    // and the notice joins the subscribe replies on its ack lane.
    let acks_before = h.pending(joiner, Lane::Ack);
    peer_flags(&mut h, peer, &space, "first").await;
    assert_eq!(
        h.pending(joiner, Lane::Ack),
        acks_before + 1,
        "the pause notice waits on the joiner's ack lane"
    );

    match recovery {
        Recovery::Resubscribe => {}
        Recovery::Reconnect => {
            h.disconnect(joiner).await;
            h.connect(joiner).await;
        }
    }
    h.run_to_idle().await;
    peer_flags(&mut h, peer, &space, "second").await;
    h.run_to_idle().await;

    let first = acked_signal(&h, peer, "first");
    let second = acked_signal(&h, peer, "second");
    h.stop(joiner).await.expect("the joiner exits cleanly");

    let ingested = ingested_signals(&root, &space);
    assert!(
        ingested.contains(&first) && ingested.contains(&second),
        "{recovery:?}: the joiner holds the missed record and the live one; has {ingested:?}, \
         joiner sent {:?}",
        h.sent(joiner).iter().map(payload_kind).collect::<Vec<_>>()
    );
    for (_, path) in &docs {
        assert_eq!(
            std::fs::read_to_string(root.join(path)).ok().as_deref(),
            Some(PEER_CONTENT),
            "{recovery:?}: {path} on the joiner's disk"
        );
    }
    let mut expected: Vec<String> = docs.iter().map(|(_, p)| p.clone()).collect();
    expected.sort();
    assert_eq!(visible_files(&root), expected, "{recovery:?}");
    let pauses = Interleaver::stream_pauses(&space);
    let sent: Vec<&'static str> = h.sent(joiner).iter().map(payload_kind).collect();
    match recovery {
        Recovery::Resubscribe => {
            assert_eq!(
                pauses, 1,
                "the notice reached the joiner once (space {space}, joiner sent {sent:?})"
            );
        }
        Recovery::Reconnect => {
            assert_eq!(
                pauses, 0,
                "the notice died with the connection (space {space}, joiner sent {sent:?})"
            );
        }
    }
}

/// See [`paused_stream_recovers`]: recovery by re-subscribe.
#[tokio::test(start_paused = true)]
async fn test_paused_signal_stream_resumes_by_resubscribe() {
    paused_stream_recovers(Recovery::Resubscribe).await;
}

/// See [`paused_stream_recovers`]: recovery by reconnect.
#[tokio::test(start_paused = true)]
async fn test_paused_signal_stream_resumes_by_reconnect() {
    paused_stream_recovers(Recovery::Reconnect).await;
}
