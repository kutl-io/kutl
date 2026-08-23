//! Tests for the file-based DID authorization system.

use std::io::Write;

use kutl_relay::authorized_keys::AuthorizedKeys;

#[test]
fn test_authorized_did_is_accepted() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "did:key:z6MkTestAuthorizedKey123").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    assert!(keys.is_authorized("did:key:z6MkTestAuthorizedKey123"));
}

#[test]
fn test_unauthorized_did_is_rejected() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "did:key:z6MkAllowedKey999").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    assert!(!keys.is_authorized("did:key:z6MkNotInTheFile"));
}

#[test]
fn test_comments_and_blank_lines_ignored() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "# this is a comment").unwrap();
    writeln!(file).unwrap();
    writeln!(file, "did:key:z6MkValidKey1").unwrap();
    writeln!(file, "  # indented comment").unwrap();
    writeln!(file, "   ").unwrap();
    writeln!(file, "did:key:z6MkValidKey2").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    assert!(keys.is_authorized("did:key:z6MkValidKey1"));
    assert!(keys.is_authorized("did:key:z6MkValidKey2"));
    assert!(!keys.is_authorized("# this is a comment"));
    assert!(!keys.is_authorized(""));
}

#[test]
fn test_missing_file_denies_access() {
    let keys = AuthorizedKeys::new(std::path::PathBuf::from(
        "/nonexistent/path/authorized_keys",
    ));
    assert!(!keys.is_authorized("did:key:z6MkAnything"));
}

#[test]
fn test_bare_did_authorizes_every_space_forever() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "did:key:zA").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    // Bare DID: present, no scope, no expiry — authorized anywhere, always.
    assert!(keys.is_authorized("did:key:zA"));
    assert!(keys.authorize("did:key:zA", "anything", 99999));
    assert!(keys.authorize("did:key:zA", "another-space", i64::MAX));
}

#[test]
fn test_scoped_and_expiring_entry_authorization() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "did:key:zB scope=s1,s2 expiry=1000 name=agent-x").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    // In scope, before expiry.
    assert!(keys.authorize("did:key:zB", "s1", 500));
    assert!(keys.authorize("did:key:zB", "s2", 500));
    // Out of scope.
    assert!(!keys.authorize("did:key:zB", "s3", 500));
    // Expired: now_ms >= expiry (2000 >= 1000).
    assert!(!keys.authorize("did:key:zB", "s1", 2000));
    // Boundary: exactly at expiry is expired (not < 1000).
    assert!(!keys.authorize("did:key:zB", "s1", 1000));
    // Presence check still works for a scoped entry.
    assert!(keys.is_authorized("did:key:zB"));
}

#[test]
fn test_unlisted_did_never_authorized() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "did:key:zB scope=s1 expiry=1000").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    assert!(!keys.authorize("did:key:zUnlisted", "s1", 500));
    assert!(!keys.is_authorized("did:key:zUnlisted"));
}

#[test]
fn test_malformed_expiry_token_drops_entry() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    // Malformed expiry — fail CLOSED: the whole entry (DID + scope) is dropped
    // so that DID gets no access from this line. A parse typo must never widen
    // access.
    writeln!(file, "did:key:zC scope=s1 expiry=not-a-number").unwrap();
    writeln!(file, "did:key:zD").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    // zC is dropped entirely — no presence, no authorization anywhere/anytime.
    assert!(!keys.is_authorized("did:key:zC"));
    assert!(!keys.authorize("did:key:zC", "s1", i64::MAX));
    assert!(!keys.authorize("did:key:zC", "s1", 0));
    assert!(!keys.authorize("did:key:zC", "s2", 0));
    // The rest of the file is unaffected — only the malformed line drops.
    assert!(keys.authorize("did:key:zD", "anything", 0));
    assert!(keys.is_authorized("did:key:zD"));
}

#[test]
fn test_option_token_missing_equals_drops_entry() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    // An option token with no `=` is malformed — fail CLOSED, drop the entry
    // rather than silently degrading it to a bare (all-spaces-forever) DID.
    writeln!(file, "did:key:zE scope=s1 garbage-token").unwrap();
    writeln!(file, "did:key:zF").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    assert!(!keys.is_authorized("did:key:zE"));
    assert!(!keys.authorize("did:key:zE", "s1", 0));
    assert!(!keys.authorize("did:key:zE", "anything", 0));
    // Unaffected sibling line.
    assert!(keys.authorize("did:key:zF", "anything", 0));
}

#[test]
fn test_unknown_option_key_drops_entry() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    // An unknown option key is malformed — fail CLOSED, drop the entry.
    writeln!(file, "did:key:zG scope=s1 mystery=value").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    assert!(!keys.is_authorized("did:key:zG"));
    assert!(!keys.authorize("did:key:zG", "s1", 0));
}

#[test]
fn test_malformed_scope_drops_entry() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    // An empty/whitespace scope is malformed — fail CLOSED, drop the entry
    // rather than resolving to an empty scope set (which would authorize
    // nothing) or a widened one.
    writeln!(file, "did:key:zH scope=").unwrap();
    writeln!(file, "did:key:zI scope=s1,,s2").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    assert!(!keys.is_authorized("did:key:zH"));
    assert!(!keys.authorize("did:key:zH", "s1", 0));
    // A scope list with an empty element is malformed too.
    assert!(!keys.is_authorized("did:key:zI"));
    assert!(!keys.authorize("did:key:zI", "s1", 0));
}

#[test]
fn test_scoped_did_does_not_authorize_other_space() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "did:key:zScoped scope=spaceA").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    // In scope: authorized.
    assert!(keys.authorize("did:key:zScoped", "spaceA", 0));
    // Out of scope: not authorized — must not enumerate spaceB.
    assert!(!keys.authorize("did:key:zScoped", "spaceB", 0));
}

#[test]
fn test_expired_did_authorizes_nothing() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "did:key:zExpired expiry=1000").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    // Before expiry: authorized anywhere (no scope).
    assert!(keys.authorize("did:key:zExpired", "anything", 500));
    // At/after expiry: authorized nowhere.
    assert!(!keys.authorize("did:key:zExpired", "anything", 1000));
    assert!(!keys.authorize("did:key:zExpired", "anything", 2000));
}

#[test]
fn test_comments_and_blank_lines_ignored_with_options() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "# comment").unwrap();
    writeln!(file).unwrap();
    writeln!(file, "did:key:zE scope=s1").unwrap();
    file.flush().unwrap();

    let keys = AuthorizedKeys::new(file.path().to_path_buf());
    assert!(keys.authorize("did:key:zE", "s1", 0));
    assert!(!keys.is_authorized("# comment"));
}
