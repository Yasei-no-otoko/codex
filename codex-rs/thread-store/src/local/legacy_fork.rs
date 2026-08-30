use std::path::Path;
use std::sync::Arc;

use codex_protocol::protocol::HistoryPosition;

use super::LocalThreadStore;
use super::helpers::managed_rollout_path;
use super::legacy_envelope;
use super::live_writer;
use super::model_context;
use super::thread_rollout_resolver::ResolvedThreadRollout;
use crate::ForkBoundary;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(super) async fn prepare(
    store: &LocalThreadStore,
    params: PrepareForkParams,
    source: ResolvedThreadRollout,
    source_guards: super::ForkSourceGuards,
) -> ThreadStoreResult<PreparedFork> {
    let PrepareForkParams {
        thread_id,
        boundary,
        legacy_source_rollout_path,
    } = params;
    if !matches!(boundary, ForkBoundary::Latest) {
        return Err(ThreadStoreError::Unsupported {
            operation: "legacy fork boundary",
        });
    }

    // Retain both barriers through child durability. The byte cutoff remains authoritative for
    // external parent appends, while the filesystem guard blocks archive/delete/compression.
    let super::ForkSourceGuards {
        lifecycle: source_reservation,
        filesystem: source_filesystem_guard,
    } = source_guards;
    if legacy_source_rollout_path.is_none() {
        match live_writer::persist_thread(store, thread_id).await {
            Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
            Err(err) => return Err(err),
        }
        match live_writer::flush_thread(store, thread_id).await {
            Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
            Err(err) => return Err(err),
        }
    }
    // Read enough metadata to classify an unsafe source before accepting its pathname. A root
    // legacy thread may still take the existing physical-copy fallback; a reference child must
    // never do so, because that would silently lose its inherited prefix.
    let source_meta = match codex_rollout::read_session_meta_line(source.path.as_path()).await {
        Ok(meta) => meta,
        // Without the header we cannot prove this is a standalone root. Fail closed rather than
        // allow the app-server's physical-copy fallback to discard a possible inherited prefix.
        Err(_) => {
            return Err(unsafe_source_error(
                true,
                "unreadable legacy reference source",
            ));
        }
    };
    let reference_child = source_meta.meta.history_base.is_some();
    if source
        .path
        .extension()
        .is_some_and(|extension| extension == "zst")
    {
        return Err(unsafe_source_error(
            reference_child,
            "compressed legacy reference fork",
        ));
    }
    let source_path = match managed_rollout_path(
        store.config.codex_home.as_path(),
        source.path.as_path(),
        source.rollout_id,
    ) {
        Ok(path) => path,
        Err(_) => {
            return Err(unsafe_source_error(
                reference_child,
                "unmanaged legacy reference source",
            ));
        }
    };
    if codex_rollout::rollout_thread_id_from_path(source.path.as_path()) != Some(thread_id)
        || source_meta.meta.id != thread_id
    {
        return Err(unsafe_source_error(
            reference_child,
            "mismatched legacy reference fork",
        ));
    }
    let end_byte_offset = last_complete_rollout_envelope_offset(source_path.as_path()).await?;
    if end_byte_offset == 0 {
        return Err(unsafe_source_error(
            reference_child,
            "legacy reference fork without complete rollout envelope",
        ));
    }
    let history_base = HistoryPosition {
        thread_id: source.rollout_id,
        end_ordinal_exclusive: 0,
        end_byte_offset,
    };
    let mut ancestor_guards = Vec::new();
    let model_context = if reference_child {
        // A legacy child carries an immutable prefix. Resolve every ancestor under the same
        // managed-path rules before creating another reference; accepting only the leaf would
        // make a later copy fallback drop that prefix.
        let (lineage, guards) = store
            .resolve_rollout_lineage_for_reference_from_source_locked_with_source_guard(
                thread_id,
                source.rollout_id,
                source_path.clone(),
                source_filesystem_guard.clone(),
            )
            .await?;
        ancestor_guards = guards;
        Arc::new(model_context::load_for_fork(lineage, Some(history_base)).await?)
    } else {
        Arc::new(model_context::load_legacy_fork_context(source_path, end_byte_offset).await?)
    };
    Ok(PreparedFork::new(
        thread_id,
        Some(history_base),
        model_context,
        (source_reservation, source_filesystem_guard, ancestor_guards),
    ))
}

fn unsafe_source_error(reference_child: bool, operation: &'static str) -> ThreadStoreError {
    if reference_child {
        ThreadStoreError::InvalidRequest {
            message: format!("invalid reference-backed legacy source: {operation}"),
        }
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
