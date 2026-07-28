use std::path::Path;
use std::sync::Arc;

use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ReverseJsonlScanner;
use codex_rollout::ScanOutcome;
use serde::Deserialize;

use super::LocalThreadStore;
use super::live_writer;
use super::model_context;
use super::read_thread;
use crate::ForkBoundary;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// The stable, outer shape of a persisted rollout line.
///
/// The nested payload intentionally remains an opaque JSON value. Rollouts can outlive the
/// binary that wrote them, so a historical or newer item schema must not make an otherwise
/// complete rollout envelope unusable as a byte-boundary marker.
#[derive(Deserialize)]
struct RolloutEnvelopeBoundary {
    #[serde(rename = "timestamp")]
    _timestamp: String,
    #[serde(rename = "type")]
    _item_type: String,
    #[serde(rename = "payload")]
    _payload: serde_json::Map<String, serde_json::Value>,
}

/// Prepare a latest legacy fork without copying the source rollout.
pub(super) async fn prepare(
    store: &LocalThreadStore,
    params: PrepareForkParams,
    source_guards: super::ForkSourceGuards,
) -> ThreadStoreResult<PreparedFork> {
    let PrepareForkParams {
        thread_id,
        boundary,
    } = params;
    if !matches!(boundary, ForkBoundary::Latest) {
        return Err(ThreadStoreError::Unsupported {
            operation: "legacy fork boundary",
        });
    }

    let super::ForkSourceGuards {
        lifecycle: source_reservation,
        filesystem: source_writer_lock,
    } = source_guards;
    // Retain the shared lock through preparation so compression and destructive operations cannot
    // rename or remove the source pathname. The complete-newline cutoff remains authoritative for
    // appenders outside this store/process that do not participate in the coordinator.
    // Keep the reservation through the durability barrier, cutoff sample, and model-context
    // scan. Another process can still append to the file, so the cutoff itself must be an
    // immutable complete-record boundary rather than merely the process-local lock boundary.
    let lineage_store = store.clone();
    let (lineage, flushed_file_len, source_reservation, source_writer_lock, ancestor_writer_guards) =
        tokio::spawn(async move {
            match live_writer::persist_thread(&lineage_store, thread_id).await {
                Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
                Err(err) => return Err(err),
            }
            match live_writer::flush_thread(&lineage_store, thread_id).await {
                Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
                Err(err) => return Err(err),
            }
            // Freeze the flushed length before lineage resolution can yield to another process. The
            // cutoff scan below is bounded by this value and still backs up to a complete newline.
            let source_path = read_thread::resolve_rollout_path(
                &lineage_store,
                thread_id,
                /*include_archived*/ true,
            )
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
            let source_path = super::helpers::scoped_rollout_path(
                lineage_store.config.codex_home.clone(),
                source_path.as_path(),
                "Codex home",
            )?;
            let source_path =
                codex_rollout::materialize_rollout_for_reference(source_path.as_path())
                    .await
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!("failed to materialize source rollout: {err}"),
                    })?;
            let source_live_writer_guard = lineage_store.live_writer_locks.lock(thread_id).await;
            let flushed_file_len = tokio::fs::metadata(source_path.as_path())
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!(
                        "failed to stat source rollout {}: {err}",
                        source_path.display()
                    ),
                })?
                .len();
            drop(source_live_writer_guard);
            let (lineage, ancestor_writer_guards) = lineage_store
                .resolve_rollout_lineage_for_reference_locked_with_source_guard(
                    thread_id,
                    source_writer_lock.clone(),
                )
                .await?;
            if lineage.history_mode() != ThreadHistoryMode::Legacy {
                return Err(ThreadStoreError::Unsupported {
                    operation: "legacy latest fork",
                });
            }
            Ok::<_, ThreadStoreError>((
                lineage,
                flushed_file_len,
                source_reservation,
                source_writer_lock,
                ancestor_writer_guards,
            ))
        })
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to resolve legacy fork lineage: {err}"),
        })??;

    let source_segment = lineage
        .segments()
        .last()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork lineage has no source segment".to_string(),
        })?;
    let end_byte_offset = last_complete_rollout_envelope_offset_at_or_before(
        source_segment.rollout_path.as_path(),
        flushed_file_len,
    )
    .await?;
    let history_base = HistoryPosition {
        thread_id,
        // Legacy JSONL has no ordinal stream. The byte cutoff is authoritative.
        end_ordinal_exclusive: 0,
        end_byte_offset,
    };
    let model_context = Arc::new(model_context::load_for_fork(lineage, Some(history_base)).await?);
    drop(ancestor_writer_guards);
    Ok(PreparedFork::new(
        thread_id,
        Some(history_base),
        model_context,
        (source_reservation, source_writer_lock),
    ))
}

/// Return the offset immediately after the last complete rollout envelope at or before the
/// flushed file length.
///
/// Rollout writers are process-local, so another app-server can leave a partial JSONL record at
/// the tail while this process is taking a fork snapshot. Reading only through this boundary
/// keeps the reference immutable and excludes that in-flight record. The nested payload is kept
/// opaque so schema evolution does not invalidate an otherwise complete historical record.
pub(super) async fn last_complete_rollout_envelope_offset_at_or_before(
    path: &Path,
    max_file_len: u64,
) -> ThreadStoreResult<u64> {
    let file_len = tokio::fs::metadata(path)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to stat source rollout {}: {err}", path.display()),
        })?
        .len();
    if max_file_len > file_len {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "source rollout {} shrank from flushed length {} to {}",
                path.display(),
                max_file_len,
                file_len
            ),
        });
    }
    let file_len = max_file_len;
    if file_len == 0 {
        return Ok(0);
    }

    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file =
            std::fs::File::open(path.as_path()).map_err(|err| ThreadStoreError::Internal {
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
                })? {
                Some(ScanOutcome::Parsed(_))
                    if scanner.last_record_terminated_by_newline() == Some(true) =>
                {
                    return scanner.last_record_end_offset().ok_or_else(|| {
                        ThreadStoreError::Internal {
                            message: format!(
                                "source rollout scanner lost record offset {}",
                                path.display()
                            ),
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
