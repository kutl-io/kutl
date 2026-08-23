//! The projection contract, run against the OSS sqlite substrate.
//!
//! The commercial crate runs the SAME assertions against Postgres. That pairing
//! is the point: the two substrates diverged silently for months because every
//! Rust suite in this repo runs sqlite, whose fold-backed rebuild quietly
//! repaired defects that Postgres — having no rebuild — simply carried. One set
//! of questions, asked of both, is the only thing that makes "they agree" a
//! claim rather than an assumption.
//!
//! See `kutl_relay::relay::conformance` for what is asserted and why.

use std::sync::Arc;

use kutl_relay::change_backend::SignalProjection;
use kutl_relay::change_sqlite::SqliteChangeBackend;
use kutl_relay::relay::conformance::assert_projection_contract;

#[tokio::test]
async fn sqlite_satisfies_the_projection_contract() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("open in-memory sqlite");
    // The registry backend owns the `registry` table's DDL; the change
    // backend's `get_changes` pages that table for the document half of the
    // feed, so the contract's feed assertions need it to exist — exactly as
    // it does in production, where both backends share the pool.
    kutl_relay::registry_store::SqliteBackend::new(pool.clone())
        .await
        .expect("create the registry table");
    let backend = SqliteChangeBackend::new(pool)
        .await
        .expect("build sqlite change backend");
    let records = tempfile::TempDir::new().expect("scratch dir for the record log");

    // sqlite has no foreign keys on these, so fresh UUIDs are fine; Postgres
    // passes ids it has seeded rows for.
    assert_projection_contract(
        Arc::new(backend) as Arc<dyn SignalProjection>,
        records.path(),
        &uuid::Uuid::new_v4().to_string(),
        &uuid::Uuid::new_v4().to_string(),
    )
    .await;
}
