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
