use std::path::Path;
use std::sync::Arc;

use codex_protocol::protocol::HistoryPosition;
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
        let bytes = std::fs::read(path.as_path()).map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to read source rollout {}: {err}", path.display()),
        })?;
        let capped_len = usize::try_from(file_len).map_err(|err| ThreadStoreError::Internal {
            message: format!("invalid source rollout length {}: {err}", path.display()),
        })?;
        let complete_end = bytes[..capped_len]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        if complete_end == 0 {
            return Ok(0);
        }
        let start = bytes[..complete_end.saturating_sub(1)]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        let envelope = serde_json::from_slice::<RolloutEnvelopeBoundary>(&bytes[start..complete_end - 1])
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to parse source rollout envelope {}: {err}", path.display()),
            })?;
        let _ = (envelope.timestamp, envelope.item_type, envelope.payload);
        u64::try_from(complete_end).map_err(|err| ThreadStoreError::Internal {
            message: format!("source rollout cutoff overflow {}: {err}", path.display()),
        })
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join source rollout scan: {err}"),
    })?
}

#[cfg(test)]
#[path = "legacy_fork_tests.rs"]
mod tests;
