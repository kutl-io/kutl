use uuid::Uuid;

use super::Lattice;

/// Tracks document existence, path, and lifecycle timestamps.
///
/// Merge semantics: field-by-field max for timestamps. Path comes from
/// whichever of `registered_at` or `renamed_at` is latest (LWW). On tie,
/// lexicographically larger path wins (arbitrary but deterministic).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocRecord {
    /// Document UUID.
    pub document_id: Uuid,
    /// Current document path. Determined by the latest timestamp.
    pub path: String,
    /// When the document was registered (millis since epoch). 0 if unknown.
    pub registered_at: u64,
    /// When the document was last renamed. 0 if never renamed.
    pub renamed_at: u64,
    /// When the document was deleted. 0 if alive.
    pub deleted_at: u64,
}

impl DocRecord {
    /// Whether the document is alive (not deleted, or revived after deletion).
    pub fn is_alive(&self) -> bool {
        self.deleted_at == 0
            || self.renamed_at > self.deleted_at
            || self.registered_at > self.deleted_at
    }

    /// The current path, determined by the latest of `registered_at`/`renamed_at`.
    pub fn current_path(&self) -> &str {
        &self.path
    }
}

impl Lattice for DocRecord {
    fn merge(&mut self, other: &Self) {
        debug_assert_eq!(
            self.document_id, other.document_id,
            "cannot merge DocRecords with different document_ids"
        );

        // Determine which source has the latest operation (for path selection).
        let self_latest = self.registered_at.max(self.renamed_at);
        let other_latest = other.registered_at.max(other.renamed_at);

        // Timestamps: field-by-field max.
        self.registered_at = self.registered_at.max(other.registered_at);
        self.renamed_at = self.renamed_at.max(other.renamed_at);
        self.deleted_at = self.deleted_at.max(other.deleted_at);

        // Path: from the source with the latest operation. Tie → max path.
        if other_latest > self_latest || (other_latest == self_latest && other.path > self.path) {
            self.path.clone_from(&other.path);
        }
    }

    fn lattice_le(&self, other: &Self) -> bool {
        self.registered_at <= other.registered_at
            && self.renamed_at <= other.renamed_at
            && self.deleted_at <= other.deleted_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Fixed UUID for property tests (identity doesn't affect merge).
    const TEST_UUID: Uuid = Uuid::from_bytes([
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ]);

    fn arb_doc_record() -> impl Strategy<Value = DocRecord> {
        ("[a-z]{1,8}", 0u64..100, 0u64..100, 0u64..100).prop_map(
            |(path, registered_at, renamed_at, deleted_at)| DocRecord {
                document_id: TEST_UUID,
                path,
                registered_at,
                renamed_at,
                deleted_at,
            },
        )
    }

    proptest! {
        #[test]
        fn commutativity(a in arb_doc_record(), b in arb_doc_record()) {
            let mut ab = a.clone(); ab.merge(&b);
            let mut ba = b.clone(); ba.merge(&a);
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn associativity(a in arb_doc_record(), b in arb_doc_record(), c in arb_doc_record()) {
            let mut ab_c = a.clone(); ab_c.merge(&b); ab_c.merge(&c);
            let mut a_bc = a.clone();
            let mut bc = b.clone(); bc.merge(&c);
            a_bc.merge(&bc);
            prop_assert_eq!(ab_c, a_bc);
        }

        #[test]
        fn idempotency(a in arb_doc_record()) {
            let mut aa = a.clone(); aa.merge(&a);
            prop_assert_eq!(a, aa);
        }

        #[test]
        fn monotonic_growth(a in arb_doc_record(), b in arb_doc_record()) {
            let mut merged = a.clone(); merged.merge(&b);
            prop_assert!(a.lattice_le(&merged));
            prop_assert!(b.lattice_le(&merged));
        }
    }

    #[test]
    fn test_alive_when_never_deleted() {
        let r = DocRecord {
            document_id: TEST_UUID,
            path: "foo".into(),
            registered_at: 10,
            renamed_at: 0,
            deleted_at: 0,
        };
        assert!(r.is_alive());
    }

    #[test]
    fn test_dead_when_deleted_after_register() {
        let r = DocRecord {
            document_id: TEST_UUID,
            path: "foo".into(),
            registered_at: 5,
            renamed_at: 0,
            deleted_at: 10,
        };
        assert!(!r.is_alive());
    }

    #[test]
    fn test_revived_by_rename_after_delete() {
        let r = DocRecord {
            document_id: TEST_UUID,
            path: "bar".into(),
            registered_at: 5,
            renamed_at: 15,
            deleted_at: 10,
        };
        assert!(r.is_alive());
    }

    #[test]
    fn test_revived_by_reregister_after_delete() {
        let r = DocRecord {
            document_id: TEST_UUID,
            path: "foo".into(),
            registered_at: 15,
            renamed_at: 0,
            deleted_at: 10,
        };
        assert!(r.is_alive());
    }

    #[test]
    fn test_merge_path_from_later_rename() {
        let mut a = DocRecord {
            document_id: TEST_UUID,
            path: "original".into(),
            registered_at: 5,
            renamed_at: 0,
            deleted_at: 0,
        };
        let b = DocRecord {
            document_id: TEST_UUID,
            path: "renamed".into(),
            registered_at: 3,
            renamed_at: 10,
            deleted_at: 0,
        };
        a.merge(&b);
        assert_eq!(a.path, "renamed");
    }

    #[test]
    fn test_merge_path_from_later_registration() {
        let mut a = DocRecord {
            document_id: TEST_UUID,
            path: "old".into(),
            registered_at: 3,
            renamed_at: 0,
            deleted_at: 0,
        };
        let b = DocRecord {
            document_id: TEST_UUID,
            path: "new".into(),
            registered_at: 10,
            renamed_at: 0,
            deleted_at: 0,
        };
        a.merge(&b);
        assert_eq!(a.path, "new");
    }
}
