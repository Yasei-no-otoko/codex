mod archive_thread;
mod create_thread;
mod delete_thread;
mod helpers;
mod legacy_fork;
mod list_threads;
mod live_writer;
mod model_context;
mod paginated_fork;
mod read_thread;
mod reference_attachment;
// This lands before the reader PRs that consume the shared lineage resolver.
#[allow(dead_code)]
mod rollout_lineage;
mod search_threads;
mod thread_history;
mod thread_history_materialization;
mod unarchive_thread;
mod update_thread_metadata;
mod writer_lock;

pub use reference_attachment::write_reference_logical_attachment_from_items;

#[cfg(test)]
#[path = "reference_attachment_tests.rs"]
mod reference_attachment_tests;
#[cfg(test)]
mod test_support;

use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutRecorder;
use codex_rollout::StateDbHandle;
use codex_state::SqliteConfig;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;
use tokio::sync::OwnedMutexGuard;
use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::OwnedRwLockWriteGuard;
use tokio::sync::RwLock;

use crate::AppendThreadItemsParams;
use crate::ArchiveThreadParams;
use crate::ArchiveThreadsParams;
use crate::CreateThreadParams;
use crate::DeleteThreadParams;
use crate::DeleteThreadsParams;
use crate::ItemPage;
use crate::ListItemsParams;
use crate::ListThreadsParams;
use crate::ListTurnsParams;
use crate::LoadThreadHistoryParams;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::ReadThreadByRolloutPathParams;
use crate::ReadThreadParams;
use crate::ResumeThreadParams;
use crate::SearchThreadOccurrencesParams;
use crate::SearchThreadsParams;
use crate::StoredModelContext;
use crate::StoredThread;
use crate::StoredThreadHistory;
use crate::ThreadOccurrenceSearchPage;
use crate::ThreadPage;
use crate::ThreadSearchPage;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreFuture;
use crate::ThreadStoreResult;
use crate::TurnPage;
use crate::UpdateThreadMetadataParams;
use crate::WriteReferenceLogicalAttachmentOutcome;
use crate::WriteReferenceLogicalAttachmentParams;
use crate::local::writer_lock::WriterLockCoordinator;
use crate::local::writer_lock::WriterLockGuard;

/// Local filesystem/SQLite-backed implementation of [`ThreadStore`].
///
/// Local storage has two compatibility surfaces. Rollout JSONL files are the
/// durable replay format and remain readable without SQLite, including older
/// files that encode metadata in `SessionMeta` items and name-index entries.
/// The SQLite state DB, when available, is the queryable metadata index used by
/// list/read paths for fast lookup.
///
/// Live appends still write canonical JSONL history, but append-derived
/// metadata is observed above the store and applied through
/// [`ThreadStore::update_thread_metadata`]. This implementation applies that
/// patch literally to SQLite while keeping the JSONL/name-index compatibility
/// behavior needed for SQLite-less reads, repair, and old local rollout files.
#[derive(Clone)]
pub struct LocalThreadStore {
    pub(super) config: LocalThreadStoreConfig,
    live_recorders: Arc<Mutex<HashMap<ThreadId, LiveRecorderEntry>>>,
    live_writer_locks: Arc<LiveWriterLocks>,
    writer_lock_coordinator: Arc<WriterLockCoordinator>,
    state_db: Option<StateDbHandle>,
    thread_history_db: Arc<OnceCell<sqlx::SqlitePool>>,
}

struct LiveRecorderEntry {
    recorder: RolloutRecorder,
    // Local rollout files are materialized lazily, but metadata updates can arrive before the
    // canonical SessionMeta is durable. Retain the mode captured when live persistence was opened
    // so missing SQLite rows can still be seeded.
    history_mode: ThreadHistoryMode,
    writer_lock: Option<WriterLockGuard>,
}

#[derive(Default)]
struct LiveWriterLocks {
    // Keep per-thread locks after a writer goes idle. Removing one while another caller is about
    // to acquire it could let two operations for the same thread run at once.
    by_thread: Mutex<HashMap<ThreadId, Arc<ThreadCoordination>>>,
}

#[derive(Default)]
struct ThreadCoordination {
    // Serialize writes and capture consistent fork snapshots.
    writer: Arc<Mutex<()>>,
    // Forks hold a shared lease until their child reference is durable; deletion, archive, and
    // unarchive require exclusive access. Keeping this separate from `writer` lets the source
    // accept writes during child initialization, including MCP startup that can take 30 seconds.
    // Operations that need both locks must acquire `lifecycle` before `writer`.
    lifecycle: Arc<RwLock<()>>,
}

/// Locks reserved before inspecting a fork source's rollout metadata.
///
/// Mode detection and header reads must happen under the same lifecycle/filesystem barrier that
/// the selected preparation path keeps until the child metadata is durable. Otherwise a concurrent
/// archive/compression rename can make the initial read fail and accidentally select a different
/// fork implementation.
pub(super) struct ForkSourceGuards {
    pub(super) lifecycle: OwnedRwLockReadGuard<()>,
    pub(super) filesystem: WriterLockGuard,
}

impl LiveWriterLocks {
    async fn coordination(&self, thread_id: ThreadId) -> Arc<ThreadCoordination> {
        self.by_thread
            .lock()
            .await
            .entry(thread_id)
            .or_default()
            .clone()
    }

    async fn lock(&self, thread_id: ThreadId) -> OwnedMutexGuard<()> {
        self.coordination(thread_id)
            .await
            .writer
            .clone()
            .lock_owned()
            .await
    }

    async fn reserve_lifecycle(&self, thread_id: ThreadId) -> OwnedRwLockReadGuard<()> {
        self.coordination(thread_id)
            .await
            .lifecycle
            .clone()
            .read_owned()
            .await
    }

    async fn lock_lifecycle(&self, thread_id: ThreadId) -> OwnedRwLockWriteGuard<()> {
        self.coordination(thread_id)
            .await
            .lifecycle
            .clone()
            .write_owned()
            .await
    }
}

/// Process-scoped configuration for local thread storage.
///
/// This describes where local storage lives. New-thread rollout metadata such
/// as cwd, provider, and memory mode is supplied when live persistence is opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalThreadStoreConfig {
    pub codex_home: PathBuf,
    pub sqlite: SqliteConfig,
    /// Provider used only when older local metadata does not contain one.
    pub default_model_provider_id: String,
}

impl LocalThreadStoreConfig {
    pub fn from_config(config: &impl codex_rollout::RolloutConfigView) -> Self {
        Self {
            codex_home: config.codex_home().to_path_buf(),
            sqlite: config.sqlite_config().clone(),
            default_model_provider_id: config.model_provider_id().to_string(),
        }
    }
}

impl std::fmt::Debug for LocalThreadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalThreadStore")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LocalThreadStore {
    /// Create a local store using an already initialized state DB handle.
    pub fn new(config: LocalThreadStoreConfig, state_db: Option<StateDbHandle>) -> Self {
        let writer_lock_coordinator = Arc::new(WriterLockCoordinator::new(&config.codex_home));
        Self {
            config,
            live_recorders: Arc::new(Mutex::new(HashMap::new())),
            live_writer_locks: Arc::new(LiveWriterLocks::default()),
            writer_lock_coordinator,
            state_db,
            thread_history_db: Arc::new(OnceCell::new()),
        }
    }

    /// Return the state DB handle used by local rollout writers.
    pub async fn state_db(&self) -> Option<StateDbHandle> {
        self.state_db.clone()
    }

    async fn thread_history_db(&self) -> ThreadStoreResult<&sqlx::SqlitePool> {
        self.thread_history_db
            .get_or_try_init(|| async {
                codex_state::open_thread_history_db(&self.config.sqlite).await
            })
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to open thread history database: {err}"),
            })
    }

    /// Read a local rollout-backed thread by path.
    pub async fn read_thread_by_rollout_path(
        &self,
        rollout_path: PathBuf,
        include_archived: bool,
        include_history: bool,
    ) -> ThreadStoreResult<StoredThread> {
        read_thread::read_thread_by_rollout_path(
            self,
            rollout_path,
            include_archived,
            include_history,
        )
        .await
    }

    /// Return the live local rollout path for legacy local-only code paths.
    pub async fn live_rollout_path(&self, thread_id: ThreadId) -> ThreadStoreResult<PathBuf> {
        live_writer::rollout_path(self, thread_id).await
    }

    pub(super) async fn ensure_live_recorder_absent(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<()> {
        if self.live_recorders.lock().await.contains_key(&thread_id) {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("thread {thread_id} already has a live local writer"),
            });
        }
        Ok(())
    }

    async fn existing_writer_lock(&self, thread_id: ThreadId) -> Option<WriterLockGuard> {
        self.live_recorders
            .lock()
            .await
            .get(&thread_id)
            .and_then(|entry| entry.writer_lock.clone())
    }

    async fn acquire_fork_source_guards(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<ForkSourceGuards> {
        let lifecycle = self.live_writer_locks.reserve_lifecycle(thread_id).await;
        let filesystem = if let Some(guard) = self.existing_writer_lock(thread_id).await {
            guard
        } else {
            self.writer_lock_coordinator.acquire(thread_id)?
        };
        Ok(ForkSourceGuards {
            lifecycle,
            filesystem,
        })
    }

    async fn acquire_writer_locks(
        &self,
        thread_ids: &[ThreadId],
    ) -> ThreadStoreResult<Vec<WriterLockGuard>> {
        let mut writer_locks = Vec::new();
        for &thread_id in thread_ids {
            if self
                .live_recorders
                .lock()
                .await
                .get(&thread_id)
                .is_some_and(|entry| entry.writer_lock.is_some())
            {
                continue;
            }

            // The filesystem lock protects all cross-process destructive operations, including
            // legacy reference preparation. Missing rollouts and damaged headers must
            // conservatively try the lock as well.
            writer_locks.push(self.writer_lock_coordinator.acquire(thread_id)?);
        }
        Ok(writer_locks)
    }

    async fn insert_live_recorder(
        &self,
        thread_id: ThreadId,
        recorder: RolloutRecorder,
        history_mode: ThreadHistoryMode,
        writer_lock: Option<WriterLockGuard>,
    ) -> ThreadStoreResult<()> {
        match self.live_recorders.lock().await.entry(thread_id) {
            Entry::Occupied(entry) => Err(ThreadStoreError::InvalidRequest {
                message: format!("thread {} already has a live local writer", entry.key()),
            }),
            Entry::Vacant(entry) => {
                entry.insert(LiveRecorderEntry {
                    recorder,
                    history_mode,
                    writer_lock,
                });
                Ok(())
            }
        }
    }

    async fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreResult<StoredThreadHistory> {
        if let Ok(rollout_path) = live_writer::rollout_path(self, params.thread_id).await {
            if !params.include_archived
                && helpers::rollout_path_is_archived(
                    self.config.codex_home.as_path(),
                    rollout_path.as_path(),
                )
            {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("thread {} is archived", params.thread_id),
                });
            }
            return read_thread::read_thread_by_rollout_path(
                self,
                rollout_path,
                /*include_archived*/ true,
                /*include_history*/ true,
            )
            .await?
            .history
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!("failed to load history for thread {}", params.thread_id),
            });
        }

        read_thread::read_thread(
            self,
            ReadThreadParams {
                thread_id: params.thread_id,
                include_archived: params.include_archived,
                include_history: true,
            },
        )
        .await?
        .history
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!("failed to load history for thread {}", params.thread_id),
        })
    }

    async fn read_thread_by_rollout_path_params(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreResult<StoredThread> {
        read_thread::read_thread_by_rollout_path(
            self,
            params.rollout_path,
            params.include_archived,
            params.include_history,
        )
        .await
    }

    /// Lists projection-backed turns without enabling app-server routing yet.
    pub async fn list_turns(&self, params: ListTurnsParams) -> ThreadStoreResult<TurnPage> {
        thread_history::list_turns(self, params).await
    }

    /// Lists projection-backed items without enabling app-server routing yet.
    pub async fn list_items(&self, params: ListItemsParams) -> ThreadStoreResult<ItemPage> {
        thread_history::list_items(self, params).await
    }

    /// Searches projection-backed visible messages within one paginated thread.
    pub async fn search_thread_occurrences(
        &self,
        params: SearchThreadOccurrencesParams,
    ) -> ThreadStoreResult<ThreadOccurrenceSearchPage> {
        thread_history::search_thread_occurrences(self, params).await
    }
}

impl ThreadStore for LocalThreadStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::create_thread(self, params).await })
    }

    fn resume_thread(&self, params: ResumeThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::resume_thread(self, params).await })
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::append_items(self, params).await })
    }

    fn persist_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::persist_thread(self, thread_id).await })
    }

    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::flush_thread(self, thread_id).await })
    }

    fn shutdown_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::shutdown_thread(self, thread_id).await })
    }

    fn discard_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::discard_thread(self, thread_id).await })
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        Box::pin(LocalThreadStore::load_history(self, params))
    }

    fn write_reference_logical_attachment(
        &self,
        params: WriteReferenceLogicalAttachmentParams,
    ) -> ThreadStoreFuture<'_, WriteReferenceLogicalAttachmentOutcome> {
        Box::pin(async move {
            reference_attachment::write_reference_logical_attachment(self, params).await
        })
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        Box::pin(async move { model_context::load_latest_model_context(self, params).await })
    }

    fn prepare_fork(&self, params: PrepareForkParams) -> ThreadStoreFuture<'_, PreparedFork> {
        Box::pin(async move {
            let source_guards = self.acquire_fork_source_guards(params.thread_id).await?;
            match live_writer::persist_thread(self, params.thread_id).await {
                Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
                Err(err) => return Err(err),
            }
            let Some(source_path) =
                read_thread::resolve_rollout_path(self, params.thread_id, true).await?
            else {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            };
            let source_meta = codex_rollout::read_session_meta_line(source_path.as_path())
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!(
                        "failed to read fork source metadata {}: {err}",
                        source_path.display()
                    ),
                })?;
            if matches!(&params.boundary, crate::ForkBoundary::Latest)
                && source_meta.meta.history_mode == ThreadHistoryMode::Legacy
            {
                // Pathless latest forks of supported external legacy roots must retain the
                // historical copied-history fallback in app-server. Reference preparation
                // requires a managed source so that lineage cannot splice arbitrary files.
                if !read_thread::rollout_path_is_managed(self, source_path.as_path()).await {
                    if source_meta.meta.history_base.is_some() {
                        return Err(ThreadStoreError::InvalidRequest {
                            message: format!(
                                "reference rollout for thread {} must resolve to its managed Codex home path",
                                params.thread_id
                            ),
                        });
                    }
                    return Err(ThreadStoreError::Unsupported {
                        operation: "external legacy latest fork",
                    });
                }
                return legacy_fork::prepare(self, params, source_guards).await;
            }
            if source_meta.meta.history_mode != ThreadHistoryMode::Paginated {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!(
                        "fork source {} does not use paginated history",
                        params.thread_id
                    ),
                });
            }
            paginated_fork::prepare(self, params, source_guards).await
        })
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move { read_thread::read_thread(self, params).await })
    }

    fn read_thread_by_rollout_path(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(LocalThreadStore::read_thread_by_rollout_path_params(
            self, params,
        ))
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        Box::pin(async move { list_threads::list_threads(self, params).await })
    }

    fn supports_paginated_history_lists(&self) -> bool {
        true
    }

    fn list_turns(&self, params: ListTurnsParams) -> ThreadStoreFuture<'_, TurnPage> {
        Box::pin(LocalThreadStore::list_turns(self, params))
    }

    fn list_items(&self, params: ListItemsParams) -> ThreadStoreFuture<'_, ItemPage> {
        Box::pin(LocalThreadStore::list_items(self, params))
    }

    fn search_threads(
        &self,
        params: SearchThreadsParams,
    ) -> ThreadStoreFuture<'_, ThreadSearchPage> {
        Box::pin(async move { search_threads::search_threads(self, params).await })
    }

    fn search_thread_occurrences(
        &self,
        params: SearchThreadOccurrencesParams,
    ) -> ThreadStoreFuture<'_, ThreadOccurrenceSearchPage> {
        Box::pin(LocalThreadStore::search_thread_occurrences(self, params))
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move { update_thread_metadata::update_thread_metadata(self, params).await })
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            archive_thread::archive_threads(
                self,
                ArchiveThreadsParams {
                    thread_ids: vec![params.thread_id],
                    writer_lock_thread_ids: Vec::new(),
                },
            )
            .await
            .map(|_| ())
        })
    }

    fn archive_threads(
        &self,
        params: ArchiveThreadsParams,
    ) -> ThreadStoreFuture<'_, Vec<ThreadId>> {
        Box::pin(async move { archive_thread::archive_threads(self, params).await })
    }

    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move { unarchive_thread::unarchive_thread(self, params).await })
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { delete_thread::delete_thread(self, params).await })
    }

    fn delete_threads(&self, params: DeleteThreadsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { delete_thread::delete_threads(self, params).await })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use codex_protocol::ThreadId;
    use codex_protocol::config_types::ReasoningSummary;
    use codex_protocol::items::TurnItem;
    use codex_protocol::items::UserMessageItem;
    use codex_protocol::models::BaseInstructions;
    use codex_protocol::models::FunctionCallOutputPayload;
    use codex_protocol::models::MessagePhase;
    use codex_protocol::models::ResponseItem;
    use codex_protocol::protocol::AgentMessageEvent;
    use codex_protocol::protocol::AskForApproval;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::protocol::HistoryPosition;
    use codex_protocol::protocol::ItemCompletedEvent;
    use codex_protocol::protocol::RolloutItem;
    use codex_protocol::protocol::SandboxPolicy;
    use codex_protocol::protocol::SessionSource;
    use codex_protocol::protocol::ThreadHistoryMode;
    use codex_protocol::protocol::ThreadMemoryMode;
    use codex_protocol::protocol::TurnCompleteEvent;
    use codex_protocol::protocol::TurnContextItem;
    use codex_protocol::protocol::TurnStartedEvent;
    use codex_protocol::protocol::UserMessageEvent;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::ForkBoundary;
    use crate::LiveThread;
    use crate::ThreadMetadataPatch;
    use crate::ThreadPersistenceMetadata;
    use crate::local::test_support::compress_session_file;
    use crate::local::test_support::set_history_base_in_session_file;
    use crate::local::test_support::test_config;
    use crate::local::test_support::write_archived_session_file;
    use crate::local::test_support::write_session_file;
    use crate::local::test_support::write_session_file_with_fork;
    use crate::local::test_support::write_session_file_with_history_mode;

    #[tokio::test]
    async fn live_writer_lifecycle_writes_and_closes() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();

        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create live thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("load rollout path");

        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("first live write")],
            })
            .await
            .expect("append live item");
        store
            .persist_thread(thread_id)
            .await
            .expect("persist live thread");
        store
            .flush_thread(thread_id)
            .await
            .expect("flush live thread");

        assert_rollout_contains_message(rollout_path.as_path(), "first live write").await;

        store
            .shutdown_thread(thread_id)
            .await
            .expect("shutdown live thread");
        let err = store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("write after shutdown")],
            })
            .await
            .expect_err("shutdown should remove the live thread writer");
        assert!(
            matches!(err, ThreadStoreError::ThreadNotFound { thread_id: missing } if missing == thread_id)
        );
    }

    #[tokio::test]
    async fn legacy_live_writer_blocks_cross_store_maintenance_until_reopened() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let thread_id = ThreadId::default();
        let store = LocalThreadStore::new(config.clone(), /*state_db*/ None);
        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create legacy live thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("before maintenance")],
            })
            .await
            .expect("append before maintenance");
        store
            .flush_thread(thread_id)
            .await
            .expect("flush before maintenance");

        let maintenance_store = LocalThreadStore::new(config.clone(), /*state_db*/ None);
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("load active rollout path");
        let resume_error = maintenance_store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect_err("resume must not race an active legacy writer");
        assert!(matches!(resume_error, ThreadStoreError::Conflict { .. }));
        let archive_error = maintenance_store
            .archive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect_err("archive must not move an active legacy rollout");
        assert!(matches!(archive_error, ThreadStoreError::Conflict { .. }));
        let delete_error = maintenance_store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect_err("delete must not remove an active legacy rollout");
        assert!(matches!(delete_error, ThreadStoreError::Conflict { .. }));

        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("after maintenance conflict")],
            })
            .await
            .expect("append after maintenance conflict");
        store
            .flush_thread(thread_id)
            .await
            .expect("flush after maintenance conflict");
        store
            .shutdown_thread(thread_id)
            .await
            .expect("close active legacy writer");

        let reopened_store = LocalThreadStore::new(config, /*state_db*/ None);
        let history = reopened_store
            .read_thread(ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: true,
            })
            .await
            .expect("reopen legacy rollout");
        let items = history.history.expect("reopened history").items;
        assert!(items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event))
                    if event.message == "before maintenance"
            )
        }));
        assert!(items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event))
                    if event.message == "after maintenance conflict"
            )
        }));
    }

    #[tokio::test]
    async fn cold_lineage_scan_blocks_same_store_maintenance() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let thread_id =
            ThreadId::from_string(&Uuid::from_u128(902).to_string()).expect("thread id");
        let rollout_path = write_session_file(
            home.path(),
            "2025-01-03T00-00-00.000Z",
            Uuid::from_u128(902),
        )
        .expect("write cold source");
        let store = LocalThreadStore::new(config, /*state_db*/ None);

        let (_lineage, writer_guards) = store
            .resolve_rollout_lineage_for_reference_locked(thread_id)
            .await
            .expect("resolve cold lineage while holding filesystem guard");

        let archive_error = store
            .archive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect_err("archive must conflict with a same-store lineage scan");
        assert!(matches!(archive_error, ThreadStoreError::Conflict { .. }));
        let delete_error = store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect_err("delete must conflict with a same-store lineage scan");
        assert!(matches!(delete_error, ThreadStoreError::Conflict { .. }));
        let unarchive_error = store
            .unarchive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect_err("unarchive must conflict before path mutation");
        assert!(matches!(unarchive_error, ThreadStoreError::Conflict { .. }));
        assert!(
            rollout_path.exists(),
            "maintenance must not mutate the source"
        );

        drop(writer_guards);
        store
            .archive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("archive succeeds after lineage guards are released");
    }

    #[tokio::test]
    async fn unmaterialized_root_preview_patch_does_not_require_rollout_metadata() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
            .await
            .expect("create lazy root");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("load lazy rollout path");
        assert!(!rollout_path.exists());

        let updated = live_thread
            .update_metadata(
                ThreadMetadataPatch {
                    preview: Some("inherited preview".to_string()),
                    first_user_message: Some("inherited first message".to_string()),
                    ..Default::default()
                },
                /*include_archived*/ true,
            )
            .await
            .expect("preview-only patch should work before rollout materialization");
        assert_eq!(updated.preview, "inherited preview");
        assert_eq!(
            updated.first_user_message.as_deref(),
            Some("inherited first message")
        );

        live_thread
            .append_items(&[user_message_item("first child message")])
            .await
            .expect("append first child message");
        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("read metadata")
            .expect("metadata row");
        assert_eq!(metadata.preview.as_deref(), Some("inherited preview"));
        assert_eq!(
            metadata.first_user_message.as_deref(),
            Some("inherited first message")
        );
    }

    #[tokio::test]
    async fn latest_fork_dispatches_lazy_sources_by_persisted_history_mode() {
        for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
            let home = TempDir::new().expect("temp dir");
            let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
            let thread_id = ThreadId::default();
            let mut params = create_thread_params(thread_id);
            params.history_mode = history_mode;
            store
                .create_thread(params)
                .await
                .expect("create lazy source");
            let prepared = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                store.prepare_fork(PrepareForkParams {
                    thread_id,
                    boundary: ForkBoundary::Latest,
                }),
            )
            .await
            .expect("latest fork should not deadlock")
            .expect("latest fork should dispatch after persistence");

            match history_mode {
                ThreadHistoryMode::Legacy => {
                    let history_base = prepared.history_base.expect("legacy reference cutoff");
                    assert_eq!(history_base.thread_id, thread_id);
                    assert_eq!(history_base.end_ordinal_exclusive, 0);
                    assert!(history_base.end_byte_offset > 0);
                }
                ThreadHistoryMode::Paginated => {
                    assert_eq!(prepared.history_base, None);
                }
            }
            drop(prepared);
            store
                .shutdown_thread(thread_id)
                .await
                .expect("shutdown lazy source");
        }
    }

    #[tokio::test]
    async fn latest_legacy_fork_survives_concurrent_archive_during_mode_detection() {
        let home = TempDir::new().expect("home temp dir");
        let source_uuid = uuid::Uuid::from_u128(903);
        let source_id = ThreadId::from_string(&source_uuid.to_string()).expect("source id");
        write_session_file(home.path(), "2025-01-03T00-01-00.000Z", source_uuid)
            .expect("write legacy source");
        let config = test_config(home.path());
        let store = LocalThreadStore::new(config.clone(), /*state_db*/ None);
        let maintenance_store = LocalThreadStore::new(config, /*state_db*/ None);
        let maintenance_race_store = maintenance_store.clone();

        let prepare = tokio::spawn(async move {
            store
                .prepare_fork(PrepareForkParams {
                    thread_id: source_id,
                    boundary: ForkBoundary::Latest,
                })
                .await
        });
        let archive = tokio::spawn(async move {
            maintenance_race_store
                .archive_thread(ArchiveThreadParams {
                    thread_id: source_id,
                })
                .await
        });

        let prepared = tokio::time::timeout(std::time::Duration::from_secs(5), prepare)
            .await
            .expect("fork/archive race must not deadlock")
            .expect("fork task should not panic")
            .expect("legacy source must not be dispatched as paginated");
        let history_base = prepared
            .history_base
            .expect("legacy latest fork should retain a byte cutoff");
        assert_eq!(history_base.thread_id, source_id);
        assert_eq!(history_base.end_ordinal_exclusive, 0);
        drop(prepared);

        let archive_result = tokio::time::timeout(std::time::Duration::from_secs(5), archive)
            .await
            .expect("archive must finish after fork releases its source guard")
            .expect("archive task should not panic");
        match archive_result {
            Ok(()) => {}
            Err(ThreadStoreError::Conflict { .. }) => {
                maintenance_store
                    .archive_thread(ArchiveThreadParams {
                        thread_id: source_id,
                    })
                    .await
                    .expect("archive retry should succeed after fork preparation");
            }
            Err(error) => panic!("unexpected archive result: {error:?}"),
        }
    }

    #[tokio::test]
    async fn raw_append_items_does_not_update_sqlite_metadata() {
        // This pins the ThreadStore contract: raw appends are history-only. Callers that need
        // metadata updates must use LiveThread or call update_thread_metadata explicitly.
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = LocalThreadStore::new(config, Some(runtime.clone()));
        let thread_id = ThreadId::default();

        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create live thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("raw append")],
            })
            .await
            .expect("append raw item");
        store.flush_thread(thread_id).await.expect("flush thread");

        assert_eq!(
            runtime
                .get_thread(thread_id)
                .await
                .expect("sqlite metadata read"),
            None
        );
    }

    #[tokio::test]
    async fn live_thread_observes_appended_items_into_sqlite_metadata() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
            .await
            .expect("create live thread");

        live_thread
            .append_items(&[user_message_item("observed append")])
            .await
            .expect("append observed item");
        live_thread.flush().await.expect("flush thread");

        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(
            metadata.first_user_message.as_deref(),
            Some("observed append")
        );
        assert_eq!(metadata.preview.as_deref(), Some("observed append"));
        assert_eq!(metadata.title, "observed append");
    }

    #[tokio::test]
    async fn paginated_resume_prefers_explicit_rollout_path_over_stale_sqlite_path() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let uuid = uuid::Uuid::from_u128(228);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-00",
            uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("paginated session file");
        let stale_rollout_path = home.path().join("stale-rollout.jsonl");
        tokio::fs::write(&stale_rollout_path, "malformed session metadata\n")
            .await
            .expect("write stale rollout");
        let mut builder = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            stale_rollout_path,
            chrono::Utc::now(),
            SessionSource::Cli,
        );
        builder.history_mode = ThreadHistoryMode::Paginated;
        builder.cwd = home.path().to_path_buf();
        let mut metadata = builder.build("test-provider");
        metadata.preview = Some("original user message".to_string());
        metadata.first_user_message = Some("original user message".to_string());
        metadata.title = "original user message".to_string();
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("update stale sqlite rollout path");

        let resumed = LiveThread::resume(
            store,
            ThreadHistoryMode::Paginated,
            ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path.clone()),
                history: Some(Arc::new(vec![user_message_item("bounded suffix")])),
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(home.path().to_path_buf()),
                    model_provider: "test-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            },
        )
        .await
        .expect("resume paginated thread from its requested rollout");
        assert_eq!(
            resumed.local_rollout_path().await.expect("live rollout"),
            Some(rollout_path)
        );
        resumed.shutdown().await.expect("shutdown resumed writer");

        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(
            (
                metadata.preview.as_deref(),
                metadata.title.as_str(),
                metadata.first_user_message.as_deref(),
            ),
            (
                Some("original user message"),
                "original user message",
                Some("original user message"),
            )
        );
    }

    #[tokio::test]
    async fn live_thread_does_not_derive_metadata_from_inherited_items() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let mut params = create_thread_params(thread_id);
        params.history_mode = ThreadHistoryMode::Paginated;
        let cwd = std::env::current_dir().expect("current directory");
        let turn_context = |model: &str, approval_policy| {
            RolloutItem::TurnContext(TurnContextItem {
                turn_id: Some("turn-1".to_string()),
                cwd: serde_json::from_value(serde_json::json!(cwd)).expect("absolute cwd"),
                workspace_roots: None,
                current_date: None,
                timezone: None,
                approval_policy,
                approvals_reviewer: None,
                sandbox_policy: SandboxPolicy::DangerFullAccess,
                permission_profile: None,
                network: None,
                file_system_sandbox_policy: None,
                model: model.to_string(),
                comp_hash: None,
                personality: None,
                collaboration_mode: None,
                multi_agent_version: None,
                multi_agent_mode: None,
                realtime_active: None,
                effort: None,
                summary: ReasoningSummary::Auto,
            })
        };

        let live_thread = LiveThread::create_with_inherited_model_context(
            store,
            params,
            &[turn_context("parent-model", AskForApproval::Never)],
        )
        .await
        .expect("create live thread with inherited context");
        live_thread.persist().await.expect("persist thread");
        let inherited_metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(inherited_metadata.model, None);

        live_thread
            .append_items(&[turn_context("child-model", AskForApproval::OnRequest)])
            .await
            .expect("append child context");
        let child_metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(child_metadata.model.as_deref(), Some("child-model"));
        assert_eq!(child_metadata.approval_mode, "on-request");
    }

    #[tokio::test]
    async fn live_thread_output_advances_updated_at_but_not_recency_at() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store, create_thread_params(thread_id))
            .await
            .expect("create live thread");

        live_thread
            .append_items(&[user_message_item("start thread")])
            .await
            .expect("append initial user message");
        live_thread.flush().await.expect("flush thread");
        let before_turn_start = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");

        live_thread
            .append_items(&[RolloutItem::EventMsg(EventMsg::TurnStarted(
                TurnStartedEvent {
                    turn_id: "turn-1".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                },
            ))])
            .await
            .expect("append turn start");
        live_thread.flush().await.expect("flush thread");
        let after_turn_start = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert!(after_turn_start.recency_at > before_turn_start.recency_at);

        live_thread
            .append_items(&[
                RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                    message: "commentary".to_string(),
                    phase: Some(MessagePhase::Commentary),
                    memory_citation: None,
                })),
                RolloutItem::ResponseItem(ResponseItem::FunctionCallOutput {
                    id: None,
                    call_id: "call-1".to_string(),
                    output: FunctionCallOutputPayload::from_text("tool output".to_string()),
                    internal_chat_message_metadata_passthrough: None,
                }),
                RolloutItem::EventMsg(EventMsg::TokenCount(
                    codex_protocol::protocol::TokenCountEvent {
                        info: None,
                        rate_limits: None,
                    },
                )),
                RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "turn-1".to_string(),
                    started_at: None,
                    last_agent_message: None,
                    error: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                })),
            ])
            .await
            .expect("append post-start items");
        live_thread.flush().await.expect("flush thread");
        let completed = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");

        assert!(completed.updated_at > after_turn_start.updated_at);
        assert_eq!(completed.recency_at, after_turn_start.recency_at);
    }

    #[tokio::test]
    async fn live_thread_shutdown_does_not_materialize_empty_thread_metadata() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
            .await
            .expect("create live thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("live rollout path");

        live_thread.shutdown().await.expect("shutdown thread");

        assert!(
            !tokio::fs::try_exists(rollout_path.as_path())
                .await
                .expect("rollout path should be checkable")
        );
        assert_eq!(
            runtime
                .get_thread(thread_id)
                .await
                .expect("sqlite metadata read"),
            None
        );
    }

    #[tokio::test]
    async fn live_thread_memory_mode_update_before_rollout_materializes_keeps_history_mode() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
            .await
            .expect("create live thread");

        live_thread
            .update_memory_mode(ThreadMemoryMode::Disabled, /*include_archived*/ false)
            .await
            .expect("update memory mode");

        assert_eq!(
            runtime
                .get_thread(thread_id)
                .await
                .expect("sqlite metadata read")
                .expect("sqlite metadata")
                .history_mode,
            ThreadHistoryMode::Legacy
        );
        assert_eq!(
            runtime
                .get_thread_memory_mode(thread_id)
                .await
                .expect("thread memory mode should be readable")
                .as_deref(),
            Some("disabled")
        );
    }

    #[tokio::test]
    async fn live_thread_shutdown_with_buffered_items_materializes_before_metadata_read() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
            .await
            .expect("create live thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("live rollout path");

        live_thread
            .append_items(&[RolloutItem::EventMsg(EventMsg::TokenCount(
                codex_protocol::protocol::TokenCountEvent {
                    info: None,
                    rate_limits: None,
                },
            ))])
            .await
            .expect("append metadata-only item");
        live_thread.shutdown().await.expect("shutdown thread");

        assert!(
            tokio::fs::try_exists(rollout_path.as_path())
                .await
                .expect("rollout path should be checkable")
        );
        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(metadata.rollout_path, rollout_path);
    }

    #[tokio::test]
    async fn live_thread_resume_loads_history_before_observing_metadata() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let uuid = uuid::Uuid::from_u128(401);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path =
            write_session_file(home.path(), "2025-01-03T17-00-00", uuid).expect("session file");
        let live_thread = LiveThread::resume(
            store,
            ThreadHistoryMode::Legacy,
            ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(home.path().to_path_buf()),
                    model_provider: "different-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            },
        )
        .await
        .expect("resume live thread");

        live_thread
            .append_items(&[user_message_item("new live append")])
            .await
            .expect("append after resume");

        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(
            metadata.created_at.to_rfc3339(),
            "2025-01-03T17:00:00+00:00"
        );
        assert_eq!(metadata.model_provider, "test-provider");
        assert_eq!(
            metadata.first_user_message.as_deref(),
            Some("Hello from user")
        );
    }

    #[tokio::test]
    async fn live_thread_resume_loads_history_from_explicit_external_rollout_path() {
        let home = TempDir::new().expect("temp dir");
        let external_home = TempDir::new().expect("external temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let uuid = uuid::Uuid::from_u128(402);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file(external_home.path(), "2025-01-03T17-30-00", uuid)
            .expect("external session file");
        let live_thread = LiveThread::resume(
            store,
            ThreadHistoryMode::Legacy,
            ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(home.path().to_path_buf()),
                    model_provider: "different-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            },
        )
        .await
        .expect("resume external live thread");

        live_thread
            .append_items(&[user_message_item("new external append")])
            .await
            .expect("append after external resume");

        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(
            metadata.created_at.to_rfc3339(),
            "2025-01-03T17:30:00+00:00"
        );
        assert_eq!(metadata.model_provider, "test-provider");
        assert_eq!(
            metadata.first_user_message.as_deref(),
            Some("Hello from user")
        );
    }

    #[tokio::test]
    async fn create_thread_rejects_missing_cwd() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
            let thread_id = ThreadId::default();
            let mut params = create_thread_params(thread_id);
            params.history_mode = history_mode;
            params.metadata.cwd = None;

            let err = store
                .create_thread(params)
                .await
                .expect_err("local thread store should require cwd");

            assert!(matches!(
                err,
                ThreadStoreError::InvalidRequest { message }
                    if message == "local thread store requires a cwd"
            ));

            let mut valid_params = create_thread_params(thread_id);
            valid_params.history_mode = history_mode;
            store
                .create_thread(valid_params)
                .await
                .expect("failed initialization should release writer ownership");
        }
    }

    #[tokio::test]
    async fn discard_thread_drops_unmaterialized_live_writer() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
            let thread_id = ThreadId::default();
            let mut params = create_thread_params(thread_id);
            params.history_mode = history_mode;

            store
                .create_thread(params)
                .await
                .expect("create live thread");
            let rollout_path = store
                .live_rollout_path(thread_id)
                .await
                .expect("load rollout path");
            assert!(!rollout_path.exists());

            let lock_path = home
                .path()
                .join("thread-writer-locks")
                .join(format!("{thread_id}.lock"));
            assert!(lock_path.exists());
            store
                .discard_thread(thread_id)
                .await
                .expect("discard live thread");

            assert!(!rollout_path.exists());
            assert!(!lock_path.exists());
            let err = store
                .append_items(AppendThreadItemsParams {
                    thread_id,
                    items: vec![user_message_item("write after discard")],
                })
                .await
                .expect_err("discard should remove the live thread writer");
            assert!(
                matches!(err, ThreadStoreError::ThreadNotFound { thread_id: missing } if missing == thread_id)
            );
        }
    }

    #[tokio::test]
    async fn resume_thread_reopens_live_writer_and_appends() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let thread_id = ThreadId::default();

        let first_store = LocalThreadStore::new(config.clone(), /*state_db*/ None);
        first_store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create initial thread");
        first_store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("before resume")],
            })
            .await
            .expect("append initial item");
        first_store
            .persist_thread(thread_id)
            .await
            .expect("persist initial thread");
        first_store
            .flush_thread(thread_id)
            .await
            .expect("flush initial thread");
        let rollout_path = first_store
            .live_rollout_path(thread_id)
            .await
            .expect("load rollout path");
        first_store
            .shutdown_thread(thread_id)
            .await
            .expect("shutdown initial writer");

        let resumed_store = LocalThreadStore::new(config, /*state_db*/ None);
        resumed_store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: None,
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume live thread");
        resumed_store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("after resume")],
            })
            .await
            .expect("append resumed item");
        resumed_store
            .flush_thread(thread_id)
            .await
            .expect("flush resumed thread");

        assert_rollout_contains_message(rollout_path.as_path(), "before resume").await;
        assert_rollout_contains_message(rollout_path.as_path(), "after resume").await;
    }

    #[tokio::test]
    async fn pathless_resume_with_supplied_history_resolves_rollout_path() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let thread_id =
            ThreadId::from_string(&Uuid::from_u128(413).to_string()).expect("thread id");
        let first_store = LocalThreadStore::new(config.clone(), /*state_db*/ None);
        first_store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create thread");
        first_store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("resume history")],
            })
            .await
            .expect("append thread");
        first_store
            .flush_thread(thread_id)
            .await
            .expect("flush thread");
        first_store
            .shutdown_thread(thread_id)
            .await
            .expect("shutdown thread");

        let resumed_store = LocalThreadStore::new(config, /*state_db*/ None);
        resumed_store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: None,
                history: Some(Arc::new(Vec::new())),
                include_archived: false,
                metadata: thread_metadata(),
            })
            .await
            .expect("pathless resume with supplied history");
        resumed_store
            .shutdown_thread(thread_id)
            .await
            .expect("shutdown resumed thread");
    }

    #[tokio::test]
    async fn pathless_cold_reference_resume_materializes_compressed_lineage_and_preserves_history()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = TempDir::new()?;
        let config = test_config(home.path());
        let parent_uuid = Uuid::from_u128(414);
        let child_uuid = Uuid::from_u128(415);
        let parent_id = ThreadId::from_string(&parent_uuid.to_string())?;
        let child_id = ThreadId::from_string(&child_uuid.to_string())?;
        let parent_path = write_session_file_with_fork(
            home.path(),
            home.path().join("sessions/2025/01/03"),
            "2025-01-04T12-00-00",
            parent_uuid,
            "compressed parent message",
            Some("test-provider"),
            None,
            ThreadHistoryMode::Legacy,
        )?;
        let parent_cutoff = std::fs::metadata(&parent_path)?.len();
        let child_path = write_session_file_with_fork(
            home.path(),
            home.path().join("sessions/2025/01/03"),
            "2025-01-04T12-01-00",
            child_uuid,
            "compressed child message",
            Some("test-provider"),
            Some(parent_uuid),
            ThreadHistoryMode::Legacy,
        )?;
        set_history_base_in_session_file(
            &child_path,
            &HistoryPosition {
                thread_id: parent_id,
                end_ordinal_exclusive: 0,
                end_byte_offset: parent_cutoff,
            },
        )?;
        let parent_compressed = compress_session_file(&parent_path)?;
        let child_compressed = compress_session_file(&child_path)?;
        let store = LocalThreadStore::new(config, /*state_db*/ None);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.resume_thread(ResumeThreadParams {
                thread_id: child_id,
                rollout_path: None,
                history: None,
                include_archived: false,
                metadata: thread_metadata(),
            }),
        )
        .await
        .expect("cold reference resume must not deadlock")?;

        let parent_plain = codex_rollout::plain_rollout_path(parent_compressed.as_path());
        let child_plain = codex_rollout::plain_rollout_path(child_compressed.as_path());
        assert!(parent_plain.exists());
        assert!(child_plain.exists());
        let history = store
            .load_history(LoadThreadHistoryParams {
                thread_id: child_id,
                include_archived: false,
            })
            .await?;
        let messages = history
            .items
            .iter()
            .filter_map(|item| match item {
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) => Some(event.message.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            messages,
            vec!["compressed parent message", "compressed child message"]
        );
        store.shutdown_thread(child_id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn reference_resume_guard_matrix_handles_path_and_history_variants()
    -> Result<(), Box<dyn std::error::Error>> {
        for (explicit_path, supplied_history) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let home = TempDir::new()?;
            let config = test_config(home.path());
            let parent_uuid = Uuid::from_u128(416 + u128::from(explicit_path) * 4);
            let child_uuid = Uuid::from_u128(417 + u128::from(explicit_path) * 4);
            let parent_id = ThreadId::from_string(&parent_uuid.to_string())?;
            let child_id = ThreadId::from_string(&child_uuid.to_string())?;
            let parent_path = write_session_file_with_fork(
                home.path(),
                home.path().join("sessions/2025/01/03"),
                "2025-01-04T12-10-00",
                parent_uuid,
                "matrix parent",
                Some("test-provider"),
                None,
                ThreadHistoryMode::Legacy,
            )?;
            let parent_cutoff = std::fs::metadata(&parent_path)?.len();
            let child_path = write_session_file_with_fork(
                home.path(),
                home.path().join("sessions/2025/01/03"),
                "2025-01-04T12-11-00",
                child_uuid,
                "matrix child",
                Some("test-provider"),
                Some(parent_uuid),
                ThreadHistoryMode::Legacy,
            )?;
            set_history_base_in_session_file(
                &child_path,
                &HistoryPosition {
                    thread_id: parent_id,
                    end_ordinal_exclusive: 0,
                    end_byte_offset: parent_cutoff,
                },
            )?;
            let child_compressed = compress_session_file(&child_path)?;
            compress_session_file(&parent_path)?;

            let store = LocalThreadStore::new(config, /*state_db*/ None);
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                store.resume_thread(ResumeThreadParams {
                    thread_id: child_id,
                    rollout_path: explicit_path.then_some(child_compressed),
                    history: supplied_history.then(|| Arc::new(Vec::new())),
                    include_archived: false,
                    metadata: thread_metadata(),
                }),
            )
            .await
            .expect("reference resume matrix must not deadlock")?;
            let history = store
                .load_history(LoadThreadHistoryParams {
                    thread_id: child_id,
                    include_archived: false,
                })
                .await?;
            let messages = history
                .items
                .iter()
                .filter_map(|item| match item {
                    RolloutItem::EventMsg(EventMsg::UserMessage(event)) => {
                        Some(event.message.as_str())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(messages, vec!["matrix parent", "matrix child"]);
            store.shutdown_thread(child_id).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn explicit_empty_reference_resume_avoids_summary_lineage_reentry_plain_and_zstd()
    -> Result<(), Box<dyn std::error::Error>> {
        for (compressed, offset) in [(false, 0_u128), (true, 2_u128)] {
            let home = TempDir::new()?;
            let config = test_config(home.path());
            let parent_uuid = Uuid::from_u128(430 + offset);
            let child_uuid = Uuid::from_u128(431 + offset);
            let parent_id = ThreadId::from_string(&parent_uuid.to_string())?;
            let child_id = ThreadId::from_string(&child_uuid.to_string())?;
            let parent_path = write_session_file_with_fork(
                home.path(),
                home.path().join("sessions/2025/01/03"),
                "2025-01-04T12-30-00",
                parent_uuid,
                "empty fixture parent",
                Some("test-provider"),
                None,
                ThreadHistoryMode::Legacy,
            )?;
            let parent_cutoff = std::fs::metadata(&parent_path)?.len();
            let child_path = write_session_file_with_fork(
                home.path(),
                home.path().join("sessions/2025/01/03"),
                "2025-01-04T12-31-00",
                child_uuid,
                "",
                Some("test-provider"),
                Some(parent_uuid),
                ThreadHistoryMode::Legacy,
            )?;
            set_history_base_in_session_file(
                &child_path,
                &HistoryPosition {
                    thread_id: parent_id,
                    end_ordinal_exclusive: 0,
                    end_byte_offset: parent_cutoff,
                },
            )?;

            let explicit_path = if compressed {
                let child_compressed = compress_session_file(&child_path)?;
                compress_session_file(&parent_path)?;
                child_compressed
            } else {
                child_path
            };
            let store = LocalThreadStore::new(config, /*state_db*/ None);
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                store.resume_thread(ResumeThreadParams {
                    thread_id: child_id,
                    rollout_path: Some(explicit_path),
                    history: None,
                    include_archived: false,
                    metadata: thread_metadata(),
                }),
            )
            .await
            .expect("empty explicit reference resume must not reenter summary lineage")?;

            let history = store
                .load_history(LoadThreadHistoryParams {
                    thread_id: child_id,
                    include_archived: false,
                })
                .await?;
            assert!(history.items.iter().any(|item| {
                matches!(
                    item,
                    RolloutItem::EventMsg(EventMsg::UserMessage(event))
                        if event.message == "empty fixture parent"
                )
            }));
            store.shutdown_thread(child_id).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn explicit_empty_reference_resume_historyless_avoids_summary_lineage_reentry()
    -> Result<(), Box<dyn std::error::Error>> {
        for compressed in [false, true] {
            let home = TempDir::new()?;
            let config = test_config(home.path());
            let parent_uuid = Uuid::from_u128(432);
            let child_uuid = Uuid::from_u128(433);
            let parent_id = ThreadId::from_string(&parent_uuid.to_string())?;
            let child_id = ThreadId::from_string(&child_uuid.to_string())?;
            let parent_path = write_session_file_with_fork(
                home.path(),
                home.path().join("sessions/2025/01/03"),
                "2025-01-04T12-32-00",
                parent_uuid,
                "",
                Some("test-provider"),
                None,
                ThreadHistoryMode::Legacy,
            )?;
            let parent_cutoff = std::fs::metadata(&parent_path)?.len();
            let child_path = write_session_file_with_fork(
                home.path(),
                home.path().join("sessions/2025/01/03"),
                "2025-01-04T12-33-00",
                child_uuid,
                "",
                Some("test-provider"),
                Some(parent_uuid),
                ThreadHistoryMode::Legacy,
            )?;
            set_history_base_in_session_file(
                &child_path,
                &HistoryPosition {
                    thread_id: parent_id,
                    end_ordinal_exclusive: 0,
                    end_byte_offset: parent_cutoff,
                },
            )?;

            let explicit_path = if compressed {
                let child_compressed = compress_session_file(&child_path)?;
                compress_session_file(&parent_path)?;
                child_compressed
            } else {
                child_path
            };
            let store = LocalThreadStore::new(config, /*state_db*/ None);
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                store.resume_thread(ResumeThreadParams {
                    thread_id: child_id,
                    rollout_path: Some(explicit_path),
                    history: Some(Arc::new(Vec::new())),
                    include_archived: false,
                    metadata: thread_metadata(),
                }),
            )
            .await
            .expect(
                "empty explicit reference historyless resume must not reenter summary lineage",
            )?;

            let history = store
                .load_history(LoadThreadHistoryParams {
                    thread_id: child_id,
                    include_archived: false,
                })
                .await?;
            let messages = history
                .items
                .iter()
                .filter_map(|item| match item {
                    RolloutItem::EventMsg(EventMsg::UserMessage(event)) => {
                        Some(event.message.as_str())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(messages, vec!["", ""]);
            store.shutdown_thread(child_id).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn explicit_resume_rejects_mismatched_rollout_id_before_preheld_lineage() {
        for supplied_history in [false, true] {
            let home = TempDir::new().expect("temp dir");
            let expected_id =
                ThreadId::from_string(&Uuid::from_u128(421).to_string()).expect("expected id");
            let other_uuid = Uuid::from_u128(422);
            let other_path = write_session_file(home.path(), "2025-01-04T12-20-00", other_uuid)
                .expect("other rollout");
            let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                store.resume_thread(ResumeThreadParams {
                    thread_id: expected_id,
                    rollout_path: Some(other_path),
                    history: supplied_history.then(|| Arc::new(Vec::new())),
                    include_archived: false,
                    metadata: thread_metadata(),
                }),
            )
            .await
            .expect("mismatched explicit resume must not deadlock");
            assert!(matches!(
                result,
                Err(ThreadStoreError::InvalidRequest { .. })
            ));
        }
    }

    #[tokio::test]
    async fn create_thread_rejects_duplicate_live_writer() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();

        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create live thread");

        let err = store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect_err("duplicate live writer should fail");

        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));
        assert!(err.to_string().contains("already has a live local writer"));
    }

    #[tokio::test]
    async fn resume_thread_rejects_duplicate_live_writer() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();

        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create live thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("live rollout path");
        let err = store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect_err("duplicate live resume should fail");
        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));
        assert!(err.to_string().contains("already has a live local writer"));
    }

    #[tokio::test]
    async fn resume_thread_rejects_missing_cwd() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(407);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path =
            write_session_file(home.path(), "2025-01-04T11-30-00", uuid).expect("session file");
        let err = store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: true,
                metadata: ThreadPersistenceMetadata {
                    cwd: None,
                    model_provider: "test-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            })
            .await
            .expect_err("missing cwd should fail");

        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));
        assert!(err.to_string().contains("requires a cwd"));
    }

    #[tokio::test]
    async fn load_history_uses_live_writer_rollout_path() {
        let home = TempDir::new().expect("temp dir");
        let external_home = TempDir::new().expect("external temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(404);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file(external_home.path(), "2025-01-04T10-00-00", uuid)
            .expect("external session file");

        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume live thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("external history item")],
            })
            .await
            .expect("append live item");
        store
            .flush_thread(thread_id)
            .await
            .expect("flush live thread");

        let history = store
            .load_history(LoadThreadHistoryParams {
                thread_id,
                include_archived: false,
            })
            .await
            .expect("load external live history");

        assert!(history.items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == "external history item"
            )
        }));
    }

    #[tokio::test]
    async fn read_thread_uses_live_writer_rollout_path_for_external_resume() {
        let home = TempDir::new().expect("temp dir");
        let external_home = TempDir::new().expect("external temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(406);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file(external_home.path(), "2025-01-04T11-00-00", uuid)
            .expect("external session file");

        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path.clone()),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume live thread");

        let thread = store
            .read_thread(ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: true,
            })
            .await
            .expect("read external live thread");

        assert_eq!(thread.rollout_path, Some(rollout_path));
        assert!(thread.history.expect("history").items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == "Hello from user"
            )
        }));

        let error = store
            .prepare_fork(PrepareForkParams {
                thread_id,
                boundary: crate::ForkBoundary::Latest,
            })
            .await
            .expect_err("external latest forks should use the copied-history fallback");
        assert!(matches!(error, ThreadStoreError::Unsupported { .. }));
    }

    #[tokio::test]
    async fn load_history_uses_live_writer_rollout_path_for_archived_source() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(405);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_archived_session_file(home.path(), "2025-01-04T10-30-00", uuid)
            .expect("archived session file");

        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume live archived thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("archived live history item")],
            })
            .await
            .expect("append live item");
        store
            .flush_thread(thread_id)
            .await
            .expect("flush live thread");

        let err = store
            .read_thread(ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: false,
            })
            .await
            .expect_err("active-only read should reject archived live thread");
        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));

        let err = store
            .load_history(LoadThreadHistoryParams {
                thread_id,
                include_archived: false,
            })
            .await
            .expect_err("active-only history should reject archived live thread");
        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));
        assert!(err.to_string().contains("archived"));

        let history = store
            .load_history(LoadThreadHistoryParams {
                thread_id,
                include_archived: true,
            })
            .await
            .expect("load archived live history");

        assert!(history.items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == "archived live history item"
            )
        }));
    }

    #[tokio::test]
    async fn read_thread_by_rollout_path_includes_history() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();

        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("path read")],
            })
            .await
            .expect("append item");
        store.flush_thread(thread_id).await.expect("flush thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("load rollout path");

        let thread = store
            .read_thread_by_rollout_path(
                rollout_path,
                /*include_archived*/ true,
                /*include_history*/ true,
            )
            .await
            .expect("read thread by rollout path");

        assert_eq!(thread.thread_id, thread_id);
        assert_eq!(thread.history_mode, ThreadHistoryMode::Legacy);
        assert_eq!(
            thread
                .history
                .as_ref()
                .expect("history")
                .items
                .iter()
                .filter(|item| matches!(item, RolloutItem::EventMsg(EventMsg::UserMessage(_))))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn paginated_threads_allow_metadata_reads_and_resume_but_reject_legacy_history_paths() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(408);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-04T12-00-00",
            uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("session file");

        let thread = store
            .read_thread(ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: false,
            })
            .await
            .expect("metadata read");
        assert_eq!(thread.history_mode, ThreadHistoryMode::Paginated);
        assert!(thread.history.is_none());

        let thread = store
            .read_thread_by_rollout_path(
                rollout_path.clone(),
                /*include_archived*/ true,
                /*include_history*/ false,
            )
            .await
            .expect("metadata path read");
        assert_eq!(thread.history_mode, ThreadHistoryMode::Paginated);
        assert!(thread.history.is_none());

        assert_paginated_threads_unsupported(
            store
                .read_thread(ReadThreadParams {
                    thread_id,
                    include_archived: false,
                    include_history: true,
                })
                .await
                .expect_err("full history read should fail"),
        );
        assert_paginated_threads_unsupported(
            store
                .read_thread_by_rollout_path(
                    rollout_path.clone(),
                    /*include_archived*/ true,
                    /*include_history*/ true,
                )
                .await
                .expect_err("full history path read should fail"),
        );
        assert_paginated_threads_unsupported(
            store
                .load_history(LoadThreadHistoryParams {
                    thread_id,
                    include_archived: false,
                })
                .await
                .expect_err("history load should fail"),
        );
        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: false,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume should succeed");
    }

    #[tokio::test]
    async fn paginated_live_appends_use_paginated_history_mode() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();
        let mut create_params = create_thread_params(thread_id);
        create_params.history_mode = ThreadHistoryMode::Paginated;
        store
            .create_thread(create_params)
            .await
            .expect("create paginated thread");
        let paginated_item = RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
            thread_id,
            turn_id: "turn-1".to_string(),
            item: TurnItem::UserMessage(UserMessageItem {
                id: "item-1".to_string(),
                client_id: None,
                content: Vec::new(),
            }),
            started_at_ms: Some(0),
            completed_at_ms: 1,
        }));
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![
                    user_message_item("legacy event should not persist"),
                    paginated_item,
                ],
            })
            .await
            .expect("append paginated item");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("paginated rollout path");
        let (items, _, _) = RolloutRecorder::load_rollout_items(rollout_path.as_path())
            .await
            .expect("load paginated rollout");
        assert!(items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
                    if event.turn_id == "turn-1"
            )
        }));
        assert!(!items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event))
                    if event.message == "legacy event should not persist"
            )
        }));
    }

    fn create_thread_params(thread_id: ThreadId) -> CreateThreadParams {
        CreateThreadParams {
            session_id: thread_id.into(),
            thread_id,
            extra_config: None,
            forked_from_id: None,
            parent_thread_id: None,
            source: SessionSource::Exec,
            thread_source: None,
            originator: "test_originator".to_string(),
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: None,
            history_mode: ThreadHistoryMode::Legacy,
            history_base: None,
            preview: None,
            first_user_message: None,
            subagent_history_start_ordinal: None,
            initial_window_id: uuid::Uuid::now_v7().to_string(),
            metadata: thread_metadata(),
        }
    }

    fn assert_paginated_threads_unsupported(err: ThreadStoreError) {
        assert!(matches!(
            err,
            ThreadStoreError::Unsupported {
                operation: "paginated_threads"
            }
        ));
    }

    fn thread_metadata() -> ThreadPersistenceMetadata {
        ThreadPersistenceMetadata {
            cwd: Some(std::env::current_dir().expect("cwd")),
            model_provider: "test-provider".to_string(),
            memory_mode: ThreadMemoryMode::Enabled,
        }
    }

    fn user_message_item(message: &str) -> RolloutItem {
        RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            client_id: None,
            message: message.to_string(),
            images: None,
            local_images: Vec::new(),
            text_elements: Vec::new(),
            ..Default::default()
        }))
    }

    async fn assert_rollout_contains_message(path: &std::path::Path, expected: &str) {
        let (items, _, _) = RolloutRecorder::load_rollout_items(path)
            .await
            .expect("load rollout items");
        assert!(items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == expected
            )
        }));
    }
}
