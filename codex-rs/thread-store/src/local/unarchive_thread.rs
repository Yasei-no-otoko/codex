use super::LocalThreadStore;
use super::helpers::matching_rollout_file_name;
use super::helpers::scoped_rollout_path;
use super::helpers::touch_modified_time;
use crate::ArchiveThreadParams;
use crate::StoredThread;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use codex_rollout::find_archived_thread_path_by_id_str;
use codex_rollout::rollout_date_parts;

pub(super) async fn unarchive_thread(
    store: &LocalThreadStore,
    params: ArchiveThreadParams,
) -> ThreadStoreResult<StoredThread> {
    let thread_id = params.thread_id;
    let state_db_ctx = store.state_db().await;
    let _lifecycle_guard = store.live_writer_locks.lock_lifecycle(thread_id).await;
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    if store
        .live_recorders
        .lock()
        .await
        .get(&thread_id)
        .is_some_and(|entry| entry.writer_lock.is_some())
    {
        return Err(ThreadStoreError::Conflict {
            message: format!("thread {thread_id} already has an active writer"),
        });
    }
    let mut writer_guards = store.acquire_writer_locks(&[thread_id]).await?;
    let _topology_guard = store.writer_lock_coordinator.acquire_topology()?;
    let archived_path = find_archived_thread_path_by_id_str(
        store.config.codex_home.as_path(),
        &thread_id.to_string(),
        state_db_ctx.as_deref(),
    )
    .await
    .map_err(|err| ThreadStoreError::InvalidRequest {
        message: format!("failed to locate archived thread id {thread_id}: {err}"),
    })?
    .ok_or_else(|| ThreadStoreError::InvalidRequest {
        message: format!("no archived rollout found for thread id {thread_id}"),
    })?;

    let canonical_archived_path = scoped_rollout_path(
        store
            .config
            .codex_home
            .join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR),
        archived_path.as_path(),
        "archived",
    )?;
    let file_name = matching_rollout_file_name(
        canonical_archived_path.as_path(),
        thread_id,
        archived_path.as_path(),
    )?;
    let Some((year, month, day)) = rollout_date_parts(&file_name) else {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout path `{}` missing filename timestamp",
                archived_path.display()
            ),
        });
    };

    let dest_dir = store
        .config
        .codex_home
        .join(codex_rollout::SESSIONS_SUBDIR)
        .join(year)
        .join(month)
        .join(day);
    std::fs::create_dir_all(&dest_dir).map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to unarchive thread: {err}"),
    })?;
    let restored_path = dest_dir.join(&file_name);
    std::fs::rename(&canonical_archived_path, &restored_path).map_err(|err| {
        ThreadStoreError::Internal {
            message: format!("failed to unarchive thread: {err}"),
        }
    })?;
    touch_modified_time(restored_path.as_path()).map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to update unarchived thread timestamp: {err}"),
    })?;

    if let Some(ctx) = state_db_ctx.as_ref() {
        let _ = ctx
            .mark_unarchived(thread_id, restored_path.as_path())
            .await;
    }

    let source_writer_lock = writer_guards
        .pop()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!("missing writer lock for unarchiving thread {thread_id}"),
        })?;

    super::read_thread::read_thread_by_rollout_path_with_preheld_source_guards(
        store,
        restored_path,
        thread_id,
        false,
        false,
        &_live_writer_guard,
        source_writer_lock,
    )
    .await
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use codex_protocol::ThreadId;
    use codex_protocol::protocol::HistoryPosition;
    use codex_protocol::protocol::SessionSource;
    use codex_protocol::protocol::ThreadHistoryMode;
    use codex_utils_absolute_path::test_support::PathExt;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::ResumeThreadParams;
    use crate::ThreadPersistenceMetadata;
    use crate::ThreadStore;
    use crate::local::LocalThreadStore;
    use crate::local::test_support::set_history_base_in_session_file;
    use crate::local::test_support::test_config;
    use crate::local::test_support::write_archived_session_file;
    use crate::local::test_support::write_session_file;
    use crate::local::test_support::write_session_file_with_fork;
    use codex_protocol::protocol::ThreadMemoryMode;

    #[tokio::test]
    async fn unarchive_thread_restores_rollout_and_returns_updated_thread() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = Uuid::from_u128(203);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let archived_path = write_archived_session_file(home.path(), "2025-01-03T13-00-00", uuid)
            .expect("archived session file");

        let thread = store
            .unarchive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("unarchive thread");

        assert!(!archived_path.exists());
        let restored_path = home
            .path()
            .join("sessions/2025/01/03")
            .join(archived_path.file_name().expect("file name"));
        assert!(restored_path.exists());
        assert_eq!(thread.thread_id, thread_id);
        assert_eq!(thread.rollout_path, Some(restored_path));
        assert_eq!(thread.archived_at, None);
        assert_eq!(thread.preview, "Archived user message");
        assert_eq!(
            thread.first_user_message.as_deref(),
            Some("Archived user message")
        );
    }

    #[tokio::test]
    async fn unarchive_thread_updates_sqlite_metadata_when_present() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let uuid = Uuid::from_u128(204);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let archived_path = write_archived_session_file(home.path(), "2025-01-03T13-00-00", uuid)
            .expect("archived session file");
        let runtime = codex_state::StateRuntime::init(
            codex_state::SqliteConfig::new_for_testing(home.path().abs()),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = LocalThreadStore::new(config.clone(), Some(runtime.clone()));
        runtime
            .mark_backfill_complete(/*last_watermark*/ None)
            .await
            .expect("backfill should be complete");
        let mut builder = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            archived_path.clone(),
            Utc::now(),
            SessionSource::Cli,
        );
        builder.model_provider = Some(config.default_model_provider_id.clone());
        builder.cwd = home.path().to_path_buf();
        builder.cli_version = Some("test_version".to_string());
        let mut metadata = builder.build(config.default_model_provider_id.as_str());
        metadata.archived_at = Some(metadata.updated_at);
        metadata.is_pinned = true;
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("state db upsert should succeed");

        let unarchived = store
            .unarchive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("unarchive thread");
        assert!(unarchived.is_pinned);

        let restored_path = home
            .path()
            .join("sessions/2025/01/03")
            .join(archived_path.file_name().expect("file name"));
        let updated = runtime
            .get_thread(thread_id)
            .await
            .expect("state db read should succeed")
            .expect("thread metadata should exist");
        assert_eq!(updated.rollout_path, restored_path);
        assert_eq!(updated.archived_at, None);
        assert_eq!(updated.recency_at, metadata.recency_at);
        assert!(updated.is_pinned);
    }

    #[tokio::test]
    async fn unarchive_empty_reference_child_releases_guards_before_summary_read() {
        let home = TempDir::new().expect("temp dir");
        let parent_uuid = Uuid::from_u128(214);
        let child_uuid = Uuid::from_u128(215);
        let parent_id = ThreadId::from_string(&parent_uuid.to_string()).expect("parent id");
        let child_id = ThreadId::from_string(&child_uuid.to_string()).expect("child id");
        let parent_path = write_session_file(home.path(), "2025-01-03T13-10-00", parent_uuid)
            .expect("parent session file");
        let child_path = write_session_file_with_fork(
            home.path(),
            home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR),
            "2025-01-03T13-11-00",
            child_uuid,
            "",
            Some("test-provider"),
            Some(parent_uuid),
            ThreadHistoryMode::Legacy,
        )
        .expect("archived child session file");
        set_history_base_in_session_file(
            &child_path,
            &HistoryPosition {
                thread_id: parent_id,
                end_ordinal_exclusive: 0,
                end_byte_offset: std::fs::metadata(&parent_path)
                    .expect("parent metadata")
                    .len(),
            },
        )
        .expect("set child history base");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.unarchive_thread(ArchiveThreadParams {
                thread_id: child_id,
            }),
        )
        .await
        .expect("unarchive summary read must not deadlock")
        .expect("unarchive reference child");
        assert_eq!(result.thread_id, child_id);
        assert_eq!(result.preview, "Hello from user");
        assert!(!child_path.exists());
    }

    #[tokio::test]
    async fn unarchive_rejects_active_archived_legacy_writer_until_shutdown() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let uuid = Uuid::from_u128(205);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let archived_path = write_archived_session_file(home.path(), "2025-01-03T13-00-00", uuid)
            .expect("archived session file");
        let store = LocalThreadStore::new(config, /*state_db*/ None);

        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(archived_path.clone()),
                history: None,
                include_archived: true,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(home.path().to_path_buf()),
                    model_provider: "test-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            })
            .await
            .expect("resume archived legacy writer");
        let error = store
            .unarchive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect_err("unarchive must not move an active legacy rollout");
        assert!(matches!(error, ThreadStoreError::Conflict { .. }));
        assert!(archived_path.exists());

        store
            .shutdown_thread(thread_id)
            .await
            .expect("shutdown archived legacy writer");
        let restored = store
            .unarchive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("unarchive after shutdown");
        assert_eq!(restored.thread_id, thread_id);
        assert!(restored.archived_at.is_none());
    }
}
