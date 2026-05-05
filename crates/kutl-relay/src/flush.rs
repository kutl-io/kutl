//! Background flush task — periodically persists dirty documents to storage backends.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::blob_backend::BlobBackend;
use crate::content_backend::ContentBackend;
use crate::quota_backend::QuotaBackend;
use crate::registry_store::{RegistryBackend, Witness};
use crate::relay::{FlushContent, FlushEntry, RelayCommand};

/// Default flush interval.
const FLUSH_INTERVAL: Duration = Duration::from_millis(1500);

/// Channel capacity for flush entries from the relay actor.
///
/// Limits the number of dirty documents sent from a single `FlushDirty`
/// cycle. If more than 256 documents are dirty, the remainder is deferred
/// to the next flush interval (the relay uses `try_send` and skips on full).
const FLUSH_CHANNEL_CAPACITY: usize = 256;

/// Run the flush loop. Sends `FlushDirty` to the relay on each tick,
/// receives encoded documents, and writes them to the backends.
///
/// `quota_backend`, when `Some`, reconciles authoritative post-write
/// `storage_bytes` after each successful flush via `apply_storage_delta`
/// (RFD 0063). The OSS relay passes `None`.
///
/// `registry_backend`, when `Some` and witness support is wired up,
/// receives a durability-witness update after each successful
/// `content_backend.save()` (RFD 0066). Witness updates happen *before*
/// `FlushCompleted` is sent back to the actor — a failed witness write
/// skips `FlushCompleted` entirely so the slot stays dirty and the next
/// flush cycle retries both the backend save (idempotent) and the
/// witness write. The OSS relay passes `None`, which keeps witness
/// updates as a no-op on the trait's default impl.
///
/// Exits when `relay_tx` is dropped (relay shut down).
pub async fn run_flush_task(
    relay_tx: mpsc::Sender<RelayCommand>,
    content_backend: Arc<dyn ContentBackend>,
    blob_backend: Arc<dyn BlobBackend>,
    quota_backend: Option<Arc<dyn QuotaBackend>>,
    registry_backend: Option<Arc<dyn RegistryBackend>>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        let (entry_tx, mut entry_rx) = mpsc::channel::<FlushEntry>(FLUSH_CHANNEL_CAPACITY);

        if relay_tx
            .send(RelayCommand::FlushDirty { reply: entry_tx })
            .await
            .is_err()
        {
            info!("relay shut down, flush task exiting");
            return;
        }

        let mut flushed = 0u64;
        while let Some(entry) = entry_rx.recv().await {
            let result = match &entry.content {
                FlushContent::Text(data) => {
                    content_backend
                        .save(&entry.space_id, &entry.doc_id, data)
                        .await
                }
                FlushContent::Blob(blob) => {
                    blob_backend
                        .save(&entry.space_id, &entry.doc_id, blob)
                        .await
                }
            };
            match result {
                Ok(()) => {
                    // RFD 0063: record the authoritative post-save size. The
                    // flush task is single-threaded, so there's no same-doc
                    // parallelism here; delta math stays consistent. On
                    // retry exhaustion we log and continue — the data is
                    // durably persisted and the monthly reconcile job will
                    // correct any counter drift.
                    if let Some(ref qb) = quota_backend
                        && let Err(e) = crate::relay::apply_delta_with_retry(
                            qb.as_ref(),
                            &entry.space_id,
                            &entry.doc_id,
                            entry.size_bytes,
                        )
                        .await
                    {
                        error!(
                            space_id = %entry.space_id,
                            doc_id = %entry.doc_id,
                            error = %e,
                            "quota delta failed after retries — reconcile will correct"
                        );
                    }
                    // RFD 0066: record the durable persistence witness
                    // *before* confirming the flush to the actor. If the
                    // witness write fails, skip FlushCompleted — the slot
                    // stays dirty and the next flush cycle retries both
                    // the save (idempotent on the backend) and the
                    // witness write. Better to re-flush than to tell the
                    // actor "durably persisted" when the durable projection
                    // didn't land; that would leave `persisted_version` lagging
                    // across a restart, and a subsequent backend load miss
                    // would misclassify as Empty instead of Inconsistent.
                    if let Some(ref rb) = registry_backend {
                        let witness = Witness {
                            persisted_version: entry.flushed_up_to,
                            persisted_at: kutl_core::env::now_ms(),
                        };
                        if let Err(e) = rb.update_witness(&entry.space_id, &entry.doc_id, &witness)
                        {
                            error!(
                                space_id = %entry.space_id,
                                doc_id = %entry.doc_id,
                                flushed_up_to = entry.flushed_up_to,
                                error = %e,
                                "witness update failed — slot stays dirty, next cycle retries"
                            );
                            flushed += 1;
                            continue;
                        }
                    }
                    // Confirm to the relay actor that this snapshot was
                    // persisted. The actor advances flushed_counter so
                    // the slot is no longer dirty (unless a new edit
                    // arrived since the snapshot was taken).
                    let _ = relay_tx
                        .send(RelayCommand::FlushCompleted {
                            space_id: entry.space_id,
                            doc_id: entry.doc_id,
                            flushed_up_to: entry.flushed_up_to,
                        })
                        .await;
                }
                Err(e) => {
                    // Slot stays dirty — next flush cycle will retry.
                    error!(
                        space_id = %entry.space_id,
                        doc_id = %entry.doc_id,
                        error = %e,
                        "failed to flush document to backend"
                    );
                }
            }
            flushed += 1;
        }
        if flushed > 0 {
            debug!(count = flushed, "flush cycle complete");
        }
    }
}

/// Start the flush task if backends are provided. Returns the join handle.
///
/// Returns `None` if no content backend is provided (no persistence needed).
///
/// `quota_backend` is forwarded to the flush loop for post-write reconciliation
/// (RFD 0063). Pass `None` when quota enforcement is disabled (OSS relay).
///
/// `registry_backend`, when `Some`, forwards the handle the flush task
/// uses for the durability-witness write (RFD 0066). The OSS relay passes
/// `None`; kutlhub-relay passes the same `Arc<PostgresRegistryBackend>`
/// already threaded into the app.
pub fn spawn_flush_task(
    relay_tx: mpsc::Sender<RelayCommand>,
    content_backend: Option<Arc<dyn ContentBackend>>,
    blob_backend: Option<Arc<dyn BlobBackend>>,
    quota_backend: Option<Arc<dyn QuotaBackend>>,
    registry_backend: Option<Arc<dyn RegistryBackend>>,
) -> Option<tokio::task::JoinHandle<()>> {
    let content = content_backend?;
    let blob = blob_backend.unwrap_or_else(|| Arc::new(NoOpBlobBackend));
    Some(tokio::spawn(run_flush_task(
        relay_tx,
        content,
        blob,
        quota_backend,
        registry_backend,
        FLUSH_INTERVAL,
    )))
}

/// No-op blob backend used when only content persistence is configured.
struct NoOpBlobBackend;

#[async_trait::async_trait]
impl BlobBackend for NoOpBlobBackend {
    async fn load(
        &self,
        _space_id: &str,
        _doc_id: &str,
    ) -> anyhow::Result<Option<crate::blob_backend::BlobRecord>> {
        Ok(None)
    }

    async fn save(
        &self,
        _space_id: &str,
        _doc_id: &str,
        _blob: &crate::blob_backend::BlobRecord,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete(&self, _space_id: &str, _doc_id: &str) -> anyhow::Result<()> {
        Ok(())
    }
}
