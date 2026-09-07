//! Two writers, one document, one of them holding a stale read.
//!
//! `edit_document` applies the difference between the base a caller names and
//! the content it sends, so text a peer added after that read is never
//! expressed as a deletion. A base that cannot be used is refused with the
//! remedy for the rung it hit — each one is fixed differently, and the message
//! is the only place a caller learns which.

mod common;

use std::io::Write;

use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

use common::mcp::McpSession;

/// The space every test here writes to. The in-memory boot materializes it on
/// first touch, so no registration step is needed.
const SPACE: &str = "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635";

/// The relay refuses a base more than this many CHANGES behind. Ops are
/// deliberately not what is counted, and two tests here pin the difference.
const CHANGES_BEHIND_BOUND: usize = 25;

/// Seed for the frontier-ordering tests, long enough that every position they
/// forge lies inside its oplog — so what refuses those tokens is the order of
/// the positions, not their size.
const ORDERING_SEED: &str = "# Menu\n\n## Starter\n\n## Main\n\n## Pudding\n";

async fn start_mcp_relay() -> (String, tempfile::NamedTempFile) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let config = RelayConfig {
        port: 0,
        relay_name: "test-edit-merge".into(),
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };
    let (app, _relay_handle, _flush_handle) = common::build_in_memory_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, keys_file)
}

/// One authenticated MCP caller, so a test reads "peer writes, we write"
/// rather than threading session state through every call.
struct Writer {
    s: McpSession,
}

impl Writer {
    async fn join(addr: &str, keys: &tempfile::NamedTempFile) -> Self {
        Self {
            s: McpSession::open(addr, keys).await,
        }
    }

    /// Create a document and return its UUID.
    async fn create(&self, path: &str, content: &str) -> String {
        let result = self
            .s
            .call_ok(
                "create_document",
                serde_json::json!({ "space_id": SPACE, "path": path, "content": content }),
            )
            .await;
        result["document_id"]
            .as_str()
            .expect("create_document returns document_id")
            .to_owned()
    }

    /// Read a document, returning its content and the version token that names
    /// what was read.
    async fn read(&self, document_id: &str) -> (String, String) {
        let result = self
            .s
            .call_ok(
                "read_document",
                serde_json::json!({ "space_id": SPACE, "document_id": document_id }),
            )
            .await;
        (
            result["content"]
                .as_str()
                .expect("read_document returns content")
                .to_owned(),
            result["version"]
                .as_str()
                .expect("read_document returns version")
                .to_owned(),
        )
    }

    /// Write `content` against the base named by `base_version`, asserting the
    /// edit was accepted and returning its result payload.
    async fn edit_ok(
        &self,
        document_id: &str,
        base_version: &str,
        content: &str,
        intent: &str,
    ) -> serde_json::Value {
        self.s
            .call_ok(
                "edit_document",
                edit_args(document_id, base_version, content, intent),
            )
            .await
    }

    /// Write `content` against the base named by `base_version`, asserting the
    /// edit was refused and returning the refusal text.
    async fn edit_err(
        &self,
        document_id: &str,
        base_version: &str,
        content: &str,
        intent: &str,
    ) -> String {
        self.s
            .call_err(
                "edit_document",
                edit_args(document_id, base_version, content, intent),
            )
            .await
    }

    /// Read, then write the result of `revise` applied to what was read — a
    /// peer doing the ordinary thing, against a base it just took. Returns the
    /// edit's result, so a caller can see how much work the write was.
    async fn read_then_edit(
        &self,
        document_id: &str,
        revise: impl Fn(&str) -> String,
    ) -> serde_json::Value {
        let (content, version) = self.read(document_id).await;
        self.edit_ok(document_id, &version, &revise(&content), "peer edit")
            .await
    }

    /// How many changes the document's log holds. A write that placed nothing
    /// must add none.
    async fn log_len(&self, document_id: &str) -> usize {
        self.s
            .call_ok(
                "read_log",
                serde_json::json!({ "space_id": SPACE, "document_id": document_id }),
            )
            .await
            .as_array()
            .expect("read_log returns an array")
            .len()
    }
}

/// The `edit_document` argument object both edit outcomes send.
fn edit_args(
    document_id: &str,
    base_version: &str,
    content: &str,
    intent: &str,
) -> serde_json::Value {
    serde_json::json!({
        "space_id": SPACE,
        "document_id": document_id,
        "base_version": base_version,
        "content": content,
        "intent": intent,
    })
}

/// A document of `blocks` nine-line blocks, every line distinct.
///
/// Distinct because a hunk is placed by searching for its text, and repeated
/// lines make that search ambiguous. Nine lines because consecutive `value`
/// lines then sit far enough apart to be separate hunks — so one edit touching
/// every block is one change carrying many operations, which is the difference
/// the staleness bound is counting.
fn blocky_document(blocks: usize) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    for i in 0..blocks {
        write!(
            out,
            "## S{i}\n{i}a\n{i}b\n{i}c\nvalue {i}\n{i}d\n{i}e\n{i}f\n{i}g\n"
        )
        .expect("writing into a String cannot fail");
    }
    out
}

#[tokio::test]
async fn test_stale_write_keeps_the_other_writer_section() {
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let peer = Writer::join(&addr, &keys).await;
    let doc = ours
        .create("menu.md", "# Menu\n\n## Starter\n\n## Main\n")
        .await;

    // Our writer reads, and then takes its time composing.
    let (read_content, read_version) = ours.read(&doc).await;

    // A peer lands a whole section in the meantime.
    peer.read_then_edit(&doc, |current| {
        format!("{current}\n## Peter\n\nkosher check\n")
    })
    .await;

    // Our writer writes what it composed, from the OLD read. Its content has
    // no trace of the peer's section, so a diff taken against current content
    // would express that absence as a deletion.
    let ours_content = read_content.replace("## Main\n", "## Main\n\nrisotto\n");
    let result = ours
        .edit_ok(&doc, &read_version, &ours_content, "propose main")
        .await;
    assert_eq!(
        result["hunks_refused"],
        serde_json::json!([]),
        "nothing conflicts: {result}"
    );

    let (after, _) = ours.read(&doc).await;
    assert!(after.contains("risotto"), "our change landed: {after:?}");
    assert!(
        after.contains("kosher check"),
        "the peer's section survived: {after:?}"
    );
}

#[tokio::test]
async fn test_partial_merge_lands_what_it_can_and_reports_the_rest() {
    // Partial success is success: the caller must be able to tell which part
    // of its edit did not land WITHOUT discarding the part that did.
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let peer = Writer::join(&addr, &keys).await;
    // The two regions our writer touches sit far enough apart to be separate
    // hunks; changes closer together than that share one and refuse together.
    let doc = ours
        .create(
            "menu.md",
            "# Menu\n\n## Starter\n\nsoup\n\n## Sides\n\nbread\n\nrice\n\n## Main\n\npasta\n",
        )
        .await;

    let (read_content, read_version) = ours.read(&doc).await;

    // The peer rewrites one of the two regions our writer is about to change.
    peer.read_then_edit(&doc, |current| current.replace("pasta", "gnocchi"))
        .await;

    let ours_content = read_content
        .replace("soup", "gazpacho")
        .replace("pasta", "risotto");
    let result = ours
        .edit_ok(&doc, &read_version, &ours_content, "two courses")
        .await;

    let refused = result["hunks_refused"]
        .as_array()
        .unwrap_or_else(|| panic!("edit_document reports hunks_refused: {result}"));
    assert_eq!(
        refused.len(),
        1,
        "only the contested region refuses: {result}"
    );
    // The refusal names the section that did not land, so the caller can
    // reapply that part instead of re-sending all of it. Ordinary markdown:
    // the region opens on the blank line above the heading, and naming the
    // blank would tell the caller nothing.
    assert!(
        refused[0].as_str().is_some_and(|r| r.contains("## Main")),
        "the refusal names the region it could not place: {result}"
    );

    let (after, _) = ours.read(&doc).await;
    assert!(
        after.contains("gazpacho"),
        "the uncontested change landed: {after:?}"
    );
    assert!(
        after.contains("gnocchi"),
        "the peer's rewrite stands: {after:?}"
    );
    assert!(
        !after.contains("risotto"),
        "the contested change did not overwrite the peer: {after:?}"
    );
}

#[tokio::test]
async fn test_edit_refuses_a_token_from_another_document() {
    // Frontier indices from one document are usually valid on another, so
    // without this the caller merges against a plausible wrong base.
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let doc_a = ours.create("a.md", "# A\n").await;
    let doc_b = ours.create("b.md", "# B\n").await;

    let (_, version_a) = ours.read(&doc_a).await;
    let text = ours
        .edit_err(&doc_b, &version_a, "# B changed\n", "wrong token")
        .await;
    assert!(
        text.contains("different document"),
        "the refusal must name the mix-up: {text}"
    );
}

#[tokio::test]
async fn test_edit_refuses_a_malformed_token() {
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let doc = ours.create("menu.md", "# Menu\n").await;

    let text = ours
        .edit_err(&doc, "not-a-token", "# Changed\n", "bad token")
        .await;
    assert!(
        text.contains("read_document"),
        "the refusal must name the remedy: {text}"
    );
}

#[tokio::test]
async fn test_edit_refuses_a_base_too_many_changes_behind() {
    // A base far enough behind that a clean merge would still be arguing with
    // settled work. Counted in changes, so a quiet document imposes no bound.
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let peer = Writer::join(&addr, &keys).await;
    let doc = ours.create("log.md", "# Log\n").await;

    let (read_content, read_version) = ours.read(&doc).await;

    for n in 0..=CHANGES_BEHIND_BOUND {
        peer.read_then_edit(&doc, move |current| format!("{current}entry {n}\n"))
            .await;
    }

    let text = ours
        .edit_err(
            &doc,
            &read_version,
            &format!("{read_content}ours\n"),
            "very late",
        )
        .await;
    assert!(
        text.contains("edits have landed since that version"),
        "the refusal must say how far behind the base is: {text}"
    );
    assert!(
        text.contains("read the document again"),
        "the refusal must name the remedy: {text}"
    );
}

#[tokio::test]
async fn test_the_staleness_bound_counts_changes_not_operations() {
    // Counting changes rather than operations is a deliberate choice: a
    // participant's edit is one thing that happened to the document, however
    // much text it moved. One sweeping edit here carries far more operations
    // than the bound allows changes, and must NOT be what refuses a base.
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let peer = Writer::join(&addr, &keys).await;
    let doc = ours.create("book.md", &blocky_document(15)).await;

    let (read_content, read_version) = ours.read(&doc).await;

    // One change, touching every block.
    let peer_edit = peer
        .read_then_edit(&doc, |current| current.replace("value ", "changed "))
        .await;
    let ops = peer_edit["ops_applied"]
        .as_u64()
        .expect("edit_document reports ops_applied");
    assert!(
        ops > CHANGES_BEHIND_BOUND as u64,
        "the fixture must put more OPS behind our writer than the bound allows \
         CHANGES, or this test cannot tell the two apart: {peer_edit}"
    );

    // Our writer appends where the peer touched nothing, from the old base.
    let result = ours
        .edit_ok(
            &doc,
            &read_version,
            &format!("{read_content}## Ours\n"),
            "one change behind",
        )
        .await;
    assert_eq!(
        result["hunks_refused"],
        serde_json::json!([]),
        "one change behind is not stale, whatever it cost to make: {result}"
    );

    let (after, _) = ours.read(&doc).await;
    assert!(after.contains("## Ours"), "our change landed: {after:?}");
    assert!(
        after.contains("changed 7"),
        "the peer's sweep stands: {after:?}"
    );
}

#[tokio::test]
async fn test_an_edit_whose_every_region_refuses_is_still_a_success() {
    // Partial success taken to its limit: nothing placed at all. Still an Ok
    // response carrying its refusals — a caller told "error" discards work
    // that a partial merge would have landed, and learns nothing about why.
    // And nothing is written: no change recorded, content untouched.
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let peer = Writer::join(&addr, &keys).await;
    let doc = ours.create("menu.md", "# Menu\n\n## Main\n\npasta\n").await;

    let (read_content, read_version) = ours.read(&doc).await;
    peer.read_then_edit(&doc, |current| current.replace("pasta", "gnocchi"))
        .await;

    let log_before = ours.log_len(&doc).await;
    let result = ours
        .edit_ok(
            &doc,
            &read_version,
            &read_content.replace("pasta", "risotto"),
            "contest main",
        )
        .await;
    assert_eq!(
        result["ops_applied"],
        serde_json::json!(0),
        "nothing was placed: {result}"
    );
    assert_eq!(
        result["hunks_refused"].as_array().map(Vec::len),
        Some(1),
        "the one region it wanted comes back refused: {result}"
    );

    let (after, _) = ours.read(&doc).await;
    assert_eq!(
        after, "# Menu\n\n## Main\n\ngnocchi\n",
        "the peer's text stands untouched: {after:?}"
    );
    assert_eq!(
        ours.log_len(&doc).await,
        log_before,
        "a write that placed nothing records no change"
    );
}

#[tokio::test]
async fn test_edit_refuses_a_token_whose_base_is_not_this_history() {
    // Frontier indices are local to one relay's oplog, so a token that
    // travelled would name a real position holding text the caller never saw.
    // Forged here because a single relay cannot mint one.
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let doc = ours.create("menu.md", "# Menu\n").await;

    let forged = kutl_relay::doc_version::mint(SPACE, &doc, "text this relay never held", &[]);
    let text = ours
        .edit_err(&doc, &forged, "# Changed\n", "wrong base")
        .await;
    assert!(
        text.contains("does not describe this document's history"),
        "the refusal must name the mismatch: {text}"
    );
}

#[tokio::test]
async fn test_edit_refuses_a_frontier_past_this_document_history() {
    // A token is a plain hash of public identifiers, so any caller can build
    // one naming an operation that does not exist. Reconstructing content at
    // such a point panics inside the engine, which would take the relay actor
    // down with it — the base is rejected before it gets there.
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let doc = ours.create("menu.md", "# Menu\n").await;

    let forged = kutl_relay::doc_version::mint(SPACE, &doc, "", &[9999]);
    let text = ours
        .edit_err(&doc, &forged, "# Changed\n", "impossible base")
        .await;
    assert!(
        text.contains("does not describe this document's history"),
        "the refusal must name the mismatch: {text}"
    );

    // The relay is still answering: the forged token was refused, not fatal.
    let (after, _) = ours.read(&doc).await;
    assert_eq!(after, "# Menu\n", "the document is untouched: {after:?}");
}

#[tokio::test]
async fn test_edit_refuses_a_frontier_that_is_not_ascending() {
    // Being inside the oplog is not enough. The engine walks a frontier on the
    // assumption that it ascends, so a descending pair takes it off the rails
    // exactly as a position past the end does — and is just as reachable,
    // since a caller can build whatever token it likes. Both positions here
    // sit well inside this document's oplog: what refuses them is their order.
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let doc = ours.create("menu.md", ORDERING_SEED).await;

    let forged = kutl_relay::doc_version::mint(SPACE, &doc, "", &[5, 2]);
    let text = ours
        .edit_err(&doc, &forged, "# Changed\n", "unsorted base")
        .await;
    assert!(
        text.contains("does not describe this document's history"),
        "the refusal must name the mismatch: {text}"
    );

    // The relay is still answering: the forged token was refused, not fatal.
    let (after, _) = ours.read(&doc).await;
    assert_eq!(after, ORDERING_SEED, "the document is untouched: {after:?}");
}

#[tokio::test]
async fn test_edit_refuses_a_frontier_with_a_repeated_position() {
    // A frontier names each position once. To the engine a repeat is not a
    // harmless duplicate — it breaks the same assumption a descending pair
    // does, and takes the relay actor down with it if it gets that far.
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let doc = ours.create("menu.md", ORDERING_SEED).await;

    let forged = kutl_relay::doc_version::mint(SPACE, &doc, "", &[3, 3]);
    let text = ours
        .edit_err(&doc, &forged, "# Changed\n", "repeated position")
        .await;
    assert!(
        text.contains("does not describe this document's history"),
        "the refusal must name the mismatch: {text}"
    );

    let (after, _) = ours.read(&doc).await;
    assert_eq!(after, ORDERING_SEED, "the document is untouched: {after:?}");
}

#[tokio::test]
async fn test_an_edit_offers_no_version_to_write_again_from() {
    // A merge leaves the document holding the caller's content AND whatever a
    // peer contributed, so a token minted for the caller here would name text
    // it has never read. Writing against that token would express the peer's
    // section as a deletion — the defect the merge just prevented — and
    // nothing would catch it, because such a token describes the document
    // perfectly. So an edit hands back no version at all, and a caller that
    // wants to write again reads again.
    let (addr, keys) = start_mcp_relay().await;
    let ours = Writer::join(&addr, &keys).await;
    let peer = Writer::join(&addr, &keys).await;
    let doc = ours
        .create("menu.md", "# Menu\n\n## Starter\n\n## Main\n")
        .await;

    let (read_content, read_version) = ours.read(&doc).await;
    peer.read_then_edit(&doc, |current| {
        format!("{current}\n## Peter\n\nkosher check\n")
    })
    .await;

    let result = ours
        .edit_ok(
            &doc,
            &read_version,
            &read_content.replace("## Main\n", "## Main\n\nrisotto\n"),
            "propose main",
        )
        .await;
    assert!(
        result.get("new_version").is_none(),
        "an edit must not hand back a version: {result}"
    );
    // Named field or not: nothing token-shaped may leave here, or a caller
    // will find it and write against it.
    assert!(
        result
            .as_object()
            .expect("edit_document returns an object")
            .values()
            .all(|v| !v.as_str().is_some_and(|s| s.starts_with("kv1."))),
        "no field may carry a version token: {result}"
    );

    // The supported way on: read again, and the base that read names carries
    // the peer's section, so the next write keeps it.
    let (after, next_version) = ours.read(&doc).await;
    let result = ours
        .edit_ok(
            &doc,
            &next_version,
            &format!("{after}\n## Pudding\n"),
            "add a course",
        )
        .await;
    assert_eq!(
        result["hunks_refused"],
        serde_json::json!([]),
        "the edit after a fresh read must place cleanly: {result}"
    );

    let (final_content, _) = ours.read(&doc).await;
    for expected in ["risotto", "kosher check", "## Pudding"] {
        assert!(
            final_content.contains(expected),
            "{expected} must survive both writes: {final_content:?}"
        );
    }
}
