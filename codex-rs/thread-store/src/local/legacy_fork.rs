use std::path::Path;
use std::sync::Arc;

use codex_protocol::protocol::HistoryPosition;

use super::helpers::managed_rollout_path;
use super::legacy_envelope;
use super::live_writer;
use super::model_context;
use super::thread_rollout_resolver;
use super::LocalThreadStore;
use crate::ForkBoundary;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

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
    // Read enough metadata to classify an unsafe source before accepting its pathname. A root
    // legacy thread may still take the existing physical-copy fallback; a reference child must
    // never do so, because that would silently lose its inherited prefix.
    let source_meta = match codex_rollout::read_session_meta_line(source.path.as_path()).await {
        Ok(meta) => meta,
        Err(_) => {
            return Err(unsafe_source_error(
                false,
                "unreadable legacy reference source",
            ));
        }
    };
    let reference_child = source_meta.meta.history_base.is_some();
    let source_path = match managed_rollout_path(
        store.config.codex_home.as_path(),
        source.path.as_path(),
        thread_id,
    ) {
        Ok(path) => path,
        Err(_) => {
            return Err(unsafe_source_error(
                reference_child,
                "unmanaged legacy reference source",
            ));
        }
    };
    if source_meta.meta.id != thread_id {
        return Err(unsafe_source_error(reference_child, "mismatched legacy reference fork"));
    }
    let end_byte_offset = last_complete_rollout_envelope_offset(source_path.as_path()).await?;
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
    let model_context = if reference_child {
        // A legacy child carries an immutable prefix. Resolve every ancestor under the same
        // managed-path rules before creating another reference; accepting only the leaf would
        // make a later copy fallback drop that prefix.
        let lineage = store.resolve_rollout_lineage_for_reference(thread_id).await?;
        Arc::new(model_context::load_for_fork(lineage, Some(history_base)).await?)
    } else {
        Arc::new(model_context::load_legacy_fork_context(source_path, end_byte_offset).await?)
    };
    Ok(PreparedFork::new(
        thread_id,
        Some(history_base),
        model_context,
        source_reservation,
    ))
}

fn unsafe_source_error(reference_child: bool, operation: &'static str) -> ThreadStoreError {
    if reference_child {
        ThreadStoreError::InvalidRequest { message: format!("invalid reference-backed legacy source: {operation}") }
    } else {
        ThreadStoreError::Unsupported { operation }
    }
}

async fn last_complete_rollout_envelope_offset(path: &Path) -> ThreadStoreResult<u64> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        legacy_envelope::last_complete_rollout_envelope_offset(path.as_path()).map_err(|err| {
            ThreadStoreError::Internal {
                message: format!("failed to scan source rollout {}: {err}", path.display()),
            }
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
