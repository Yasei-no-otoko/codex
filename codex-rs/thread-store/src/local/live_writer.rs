use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutRecorder;
use codex_rollout::RolloutRecorderParams;
use codex_rollout::persisted_rollout_items;
use tracing::warn;

use super::LocalThreadStore;
use super::create_thread;
use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::ResumeThreadParams;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::types::canonical_history_mode_from_rollout_items;

const ROLLOUT_SIZE_BYTES_METRIC: &str = "codex.rollout.size_bytes";

pub(super) async fn create_thread(
    store: &LocalThreadStore,
    params: CreateThreadParams,
) -> ThreadStoreResult<()> {
    let thread_id = params.thread_id;
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    let history_mode = params.history_mode;
    store.ensure_live_recorder_absent(thread_id).await?;
    // Keep the shared filesystem lock for the entire recorder lifetime.  This applies to legacy
    // rollouts as well as paginated ones: maintenance must not rename or remove the pathname
    // while the recorder still owns an open append file descriptor.
    let writer_lock = Some(store.writer_lock_coordinator.acquire(thread_id)?);
    let recorder = create_thread::create_thread(store, params).await?;
    store
        .insert_live_recorder(thread_id, recorder, history_mode, writer_lock)
        .await
}

pub(super) async fn resume_thread(
    store: &LocalThreadStore,
    params: ResumeThreadParams,
) -> ThreadStoreResult<()> {
    let live_writer_guard = store.live_writer_locks.lock(params.thread_id).await;
    store.ensure_live_recorder_absent(params.thread_id).await?;
    // Acquire before resolving the rollout path or reading history.  A second process may be
    // archiving/compressing/deleting the same legacy pathname, so no recorder initialization may
    // observe a path that is moved immediately before it opens its append handle.
    let writer_lock = Some(store.writer_lock_coordinator.acquire(params.thread_id)?);
    // Resolve the resume path exactly once under the pre-held filesystem guard. Keep its
    // canonical header alongside the path so mode and reference detection use one snapshot.
    let resolved_rollout_path = if let Some(path) = params.rollout_path.as_ref() {
        Some(super::read_thread::resolve_requested_rollout_path(store, path.clone()).await?)
    } else {
        super::read_thread::resolve_rollout_path(store, params.thread_id, params.include_archived)
            .await?
            .ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: format!("no rollout found for thread id {}", params.thread_id),
            })
            .map(Some)?
    };
    let resolved_source_meta = if let Some(path) = resolved_rollout_path.as_ref() {
        Some(
            codex_rollout::read_session_meta_line(path.as_path())
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to read session metadata {}: {err}", path.display()),
                })?,
        )
    } else {
        None
    };
    if let Some(meta) = resolved_source_meta.as_ref()
        && meta.meta.id != params.thread_id
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout {} belongs to thread {}, not {}",
                resolved_rollout_path.as_ref().map_or_else(
                    || "<unknown>".to_string(),
                    |path| path.display().to_string()
                ),
                meta.meta.id,
                params.thread_id
            ),
        });
    }
    let history_mode = if let Some(history) = params.history.as_deref() {
        canonical_history_mode_from_rollout_items(history)
    } else {
        resolved_source_meta
            .as_ref()
            .map(|meta| meta.meta.history_mode)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!("missing resolved rollout metadata for {}", params.thread_id),
            })?
    };
    let rollout_path = {
        let path = resolved_rollout_path.ok_or_else(|| ThreadStoreError::Internal {
            message: format!("thread {} does not have a rollout path", params.thread_id),
        })?;
        let has_reference_base = resolved_source_meta
            .as_ref()
            .is_some_and(|meta| meta.meta.history_base.is_some());
        if has_reference_base {
            let thread =
                super::read_thread::read_thread_by_rollout_path_with_preheld_source_guards(
                    store,
                    path.clone(),
                    params.thread_id,
                    params.include_archived,
                    /*include_history*/
                    params.history.is_none() && history_mode != ThreadHistoryMode::Paginated,
                    &live_writer_guard,
                    writer_lock
                        .clone()
                        .ok_or_else(|| ThreadStoreError::Internal {
                            message: format!("missing writer lock for thread {}", params.thread_id),
                        })?,
                )
                .await?;
            thread
                .rollout_path
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!("thread {} does not have a rollout path", params.thread_id),
                })?
        } else {
            let thread = super::read_thread::read_thread_by_rollout_path(
                store,
                path.clone(),
                params.include_archived,
                /*include_history*/
                params.history.is_none() && history_mode != ThreadHistoryMode::Paginated,
            )
            .await?;
            thread
                .rollout_path
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!("thread {} does not have a rollout path", params.thread_id),
                })?
        }
    };
    let cwd = params
        .metadata
        .cwd
        .clone()
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: "local thread store requires a cwd".to_string(),
        })?;
    let config = RolloutConfig {
        codex_home: store.config.codex_home.clone(),
        sqlite: store.config.sqlite.clone(),
        cwd,
        model_provider_id: params.metadata.model_provider.clone(),
        generate_memories: matches!(params.metadata.memory_mode, ThreadMemoryMode::Enabled),
    };
    let recorder = RolloutRecorder::new(&config, RolloutRecorderParams::resume(rollout_path))
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to resume local thread recorder: {err}"),
        })?;
    store
        .insert_live_recorder(params.thread_id, recorder, history_mode, writer_lock)
        .await
}

#[tracing::instrument(
    level = "trace",
    skip_all,
    fields(item_count = params.items.len())
)]
pub(super) async fn append_items(
    store: &LocalThreadStore,
    params: AppendThreadItemsParams,
) -> ThreadStoreResult<()> {
    write_and_project(
        store,
        params.thread_id,
        RolloutWriteOp::AppendItems(params.items),
    )
    .await
}

pub(super) async fn persist_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    write_and_project(store, thread_id, RolloutWriteOp::Persist).await
}

pub(super) async fn flush_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    write_and_project(store, thread_id, RolloutWriteOp::Flush).await
}

pub(super) async fn shutdown_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    let (recorder, history_mode) = live_writer_parts(store, thread_id).await?;
    let rollout_path = recorder.rollout_path().to_path_buf();
    if matches!(history_mode, ThreadHistoryMode::Legacy) {
        recorder.shutdown().await.map_err(thread_store_io_error)?;
    } else {
        recorder.shutdown().await.map_err(thread_store_io_error)?;
        if let Err(err) = super::thread_history_materialization::materialize_to_sqlite(
            store,
            thread_id,
            rollout_path.as_path(),
        )
        .await
        {
            warn!("failed to project durable rollout during shutdown for {thread_id}: {err}");
        }
    }
    sync_materialized_rollout_path(store, thread_id, rollout_path.as_path()).await?;
    if let Some(metrics) = codex_otel::global()
        && let Ok(metadata) = tokio::fs::metadata(rollout_path).await
    {
        let size_bytes = i64::try_from(metadata.len()).unwrap_or(i64::MAX);
        let _ = metrics.histogram(ROLLOUT_SIZE_BYTES_METRIC, size_bytes, &[]);
    }
    store.live_recorders.lock().await.remove(&thread_id);
    Ok(())
}

pub(super) async fn discard_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    store
        .live_recorders
        .lock()
        .await
        .remove(&thread_id)
        .map(|_| ())
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })
}

pub(super) async fn rollout_path(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<PathBuf> {
    Ok(store
        .live_recorders
        .lock()
        .await
        .get(&thread_id)
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?
        .recorder
        .rollout_path()
        .to_path_buf())
}

async fn sync_materialized_rollout_path(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &std::path::Path,
) -> ThreadStoreResult<()> {
    if codex_rollout::existing_rollout_path(rollout_path)
        .await
        .is_none()
    {
        return Ok(());
    }
    let Some(state_db) = store.state_db().await else {
        return Ok(());
    };
    let result: ThreadStoreResult<()> = async {
        let Some(mut metadata) =
            state_db
                .get_thread(thread_id)
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to read thread metadata for {thread_id}: {err}"),
                })?
        else {
            return Ok(());
        };
        if metadata.rollout_path != rollout_path {
            metadata.rollout_path = rollout_path.to_path_buf();
            state_db
                .upsert_thread(&metadata)
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to update thread metadata for {thread_id}: {err}"),
                })?;
        }
        Ok(())
    }
    .await;
    if let Err(err) = result {
        warn!("failed to sync materialized rollout path for thread {thread_id}: {err}");
    }
    Ok(())
}

fn thread_store_io_error(err: std::io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

/// The rollout writer has three distinct lifecycle moments:
/// - `AppendItems` is normal turn/event persistence and adds new rollout records.
/// - `Persist` makes the thread durable before any turn items exist; locally this can write the
///   initial `SessionMeta`.
/// - `Flush` writes any rollout records already queued in the recorder and ensures they are
///   durably persisted.
///
/// Each can advance the rollout JSONL file on disk, so we need to make sure we materialize the
/// new data into the SQLite history tables (turns and items) as necessary.
enum RolloutWriteOp {
    AppendItems(Vec<RolloutItem>),
    Persist,
    Flush,
}

async fn live_writer_parts(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<(RolloutRecorder, ThreadHistoryMode)> {
    let live_recorders = store.live_recorders.lock().await;
    let entry = live_recorders
        .get(&thread_id)
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    Ok((entry.recorder.clone(), entry.history_mode))
}

async fn write_and_project(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    write_op: RolloutWriteOp,
) -> ThreadStoreResult<()> {
    // Every live write should have a recorder: create/resume installs one, while
    // shutdown/discard/delete removes it. Keep the lookup defensive so late writes fail after
    // teardown.
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    let (recorder, history_mode) = live_writer_parts(store, thread_id).await?;
    let sync_rollout_path = matches!(&write_op, RolloutWriteOp::Persist | RolloutWriteOp::Flush);
    let write_op = match write_op {
        RolloutWriteOp::AppendItems(items) => {
            let items = persisted_rollout_items(items.as_slice(), history_mode);
            if items.is_empty() {
                return Ok(());
            }
            RolloutWriteOp::AppendItems(items)
        }
        RolloutWriteOp::Persist => RolloutWriteOp::Persist,
        RolloutWriteOp::Flush => RolloutWriteOp::Flush,
    };
    if matches!(history_mode, ThreadHistoryMode::Legacy) {
        durable_write(&recorder, write_op).await?;
    } else {
        let rollout_path = recorder.rollout_path();
        // SQLite is a rebuildable view. The flush barrier must win before projection starts so it
        // can lag JSONL after failure, but can never get ahead of canonical history.
        durable_write(&recorder, write_op).await?;
        if let Err(err) = super::thread_history_materialization::materialize_to_sqlite(
            store,
            thread_id,
            rollout_path,
        )
        .await
        {
            warn!("failed to project durable rollout for {thread_id}: {err}");
        }
    }
    if sync_rollout_path {
        sync_materialized_rollout_path(store, thread_id, recorder.rollout_path()).await?;
    }
    Ok(())
}

async fn durable_write(recorder: &RolloutRecorder, write: RolloutWriteOp) -> ThreadStoreResult<()> {
    match write {
        RolloutWriteOp::AppendItems(items) => {
            recorder
                .record_canonical_items(items.as_slice())
                .await
                .map_err(thread_store_io_error)?;
            recorder.flush().await.map_err(thread_store_io_error)
        }
        RolloutWriteOp::Persist => recorder.persist().await.map_err(thread_store_io_error),
        RolloutWriteOp::Flush => recorder.flush().await.map_err(thread_store_io_error),
    }
}
