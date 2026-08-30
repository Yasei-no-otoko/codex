use std::path::Path;
use std::sync::Arc;

use codex_protocol::protocol::HistoryPosition;
use codex_rollout::ReverseJsonlScanner;
use codex_rollout::ScanOutcome;
use serde::Deserialize;

use super::LocalThreadStore;
use super::live_writer;
use super::model_context;
use super::thread_rollout_resolver;
use crate::ForkBoundary;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// Stable JSONL envelope used solely to validate a physical history boundary.
///
/// The nested payload is deliberately opaque because persisted rollout schemas evolve independently
/// from the record framing required to retain a safe complete-line cutoff.
#[derive(Deserialize)]
struct RolloutEnvelopeBoundary {
    timestamp: String,
    #[serde(rename = "type")]
    item_type: String,
    payload: serde_json::Map<String, serde_json::Value>,
}

pub(super) async fn prepare(
    store: &LocalThreadStore,
    params: PrepareForkParams,
) -> ThreadStoreResult<PreparedFork> {
    let PrepareForkParams { thread_id, boundary } = params;
    if !matches!(boundary, ForkBoundary::Latest) {
        return Err(ThreadStoreError::Unsupported {
            operation: "legacy fork boundary",
        });
    }

    // The lifecycle lease prevents destructive local operations until the child metadata has been
    // persisted. The byte cutoff remains authoritative for cross-process parent appends.
    let source_reservation = store.live_writer_locks.reserve_lifecycle(thread_id).await;
    match live_writer::persist_thread(store, thread_id).await {
        Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
        Err(err) => return Err(err),
    }
    match live_writer::flush_thread(store, thread_id).await {
        Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
        Err(err) => return Err(err),
    }
    let source = thread_rollout_resolver::resolve_current_including_archived(store, thread_id)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    if source.path.extension().is_some_and(|extension| extension == "zst") {
        return Err(ThreadStoreError::Unsupported {
            operation: "compressed legacy reference fork",
        });
    }
    let end_byte_offset = last_complete_rollout_envelope_offset(source.path.as_path()).await?;
    if end_byte_offset == 0 {
        return Err(ThreadStoreError::Unsupported {
            operation: "legacy reference fork without complete rollout envelope",
        });
    }
    let history_base = HistoryPosition {
        thread_id,
        end_ordinal_exclusive: 0,
        end_byte_offset,
    };
    let model_context = Arc::new(
        model_context::load_legacy_fork_context(source.path, end_byte_offset).await?,
    );
    Ok(PreparedFork::new(
        thread_id,
        Some(history_base),
        model_context,
        source_reservation,
    ))
}

async fn last_complete_rollout_envelope_offset(path: &Path) -> ThreadStoreResult<u64> {
    let file_len = tokio::fs::metadata(path)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to stat source rollout {}: {err}", path.display()),
        })?
        .len();
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(path.as_path()).map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to open source rollout {}: {err}", path.display()),
        })?;
        let mut scanner = ReverseJsonlScanner::new_at(file, file_len).map_err(|err| {
            ThreadStoreError::Internal {
                message: format!("failed to scan source rollout {}: {err}", path.display()),
            }
        })?;
        loop {
            match scanner
                .scan_next::<RolloutEnvelopeBoundary>()
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to scan source rollout {}: {err}", path.display()),
                })?
            {
                Some(ScanOutcome::Parsed(envelope))
                    if scanner.last_record_terminated_by_newline() == Some(true) => {
                        let _ = (envelope.timestamp, envelope.item_type, envelope.payload);
                        return scanner.last_record_end_offset().ok_or_else(|| {
                            ThreadStoreError::Internal {
                                message: format!("source rollout scanner lost offset {}", path.display()),
                            }
                        });
                    }
                Some(ScanOutcome::Parsed(_)) | Some(ScanOutcome::Rejected(_)) => {}
                None => return Ok(0),
            }
        }
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join source rollout scan: {err}"),
    })?
}

#[cfg(test)]
#[path = "legacy_fork_tests.rs"]
mod tests;
