//! Thread-store error mapping for rollout's shared cross-process writer locks.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use codex_protocol::ThreadId;

use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(test)]
pub(super) const WRITER_LOCK_DIR: &str = codex_rollout::THREAD_WRITER_LOCK_DIR;
#[cfg(test)]
pub(super) const COORDINATION_LOCK_FILE: &str = codex_rollout::THREAD_WRITER_COORDINATION_LOCK_FILE;

pub(super) type WriterLockGuard = codex_rollout::ThreadWriterLockGuard;
pub(super) type TopologyLockGuard = codex_rollout::ThreadWriterTopologyLockGuard;

#[derive(Hash, PartialEq, Eq)]
struct SourceLockKey {
    codex_home: PathBuf,
    thread_id: ThreadId,
}

static SOURCE_LOCKS: OnceLock<
    Mutex<HashMap<SourceLockKey, codex_rollout::ThreadWriterLockWeakGuard>>,
> = OnceLock::new();

fn source_locks() -> &'static Mutex<HashMap<SourceLockKey, codex_rollout::ThreadWriterLockWeakGuard>>
{
    SOURCE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) struct WriterLockCoordinator {
    inner: Arc<codex_rollout::ThreadWriterLockCoordinator>,
    codex_home: PathBuf,
}

impl WriterLockCoordinator {
    pub(super) fn new(codex_home: &std::path::Path) -> Self {
        Self {
            inner: Arc::new(codex_rollout::ThreadWriterLockCoordinator::new(codex_home)),
            codex_home: codex_home.to_path_buf(),
        }
    }

    pub(super) fn acquire(
        self: &Arc<Self>,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<WriterLockGuard> {
        self.inner.acquire(thread_id).map_err(|err| {
            if err.kind() == std::io::ErrorKind::WouldBlock {
                ThreadStoreError::Conflict {
                    message: err.to_string(),
                }
            } else {
                ThreadStoreError::Internal {
                    message: format!(
                        "failed to acquire thread writer lock for thread {thread_id}: {err}"
                    ),
                }
            }
        })
    }

    /// Reuse an active in-process source lease, or acquire a new cross-process source lease.
    ///
    /// Fork/read preparation may safely share this guard. Writer lifecycle and maintenance callers
    /// must continue to use [`Self::acquire`] for exclusive ownership.
    pub(super) fn acquire_source(
        self: &Arc<Self>,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<WriterLockGuard> {
        let key = SourceLockKey {
            codex_home: self.codex_home.clone(),
            thread_id,
        };
        let mut source_locks = source_locks()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        source_locks.retain(|_, guard| guard.upgrade().is_some());
        if let Some(guard) = source_locks
            .get(&key)
            .and_then(codex_rollout::ThreadWriterLockWeakGuard::upgrade)
        {
            return Ok(guard);
        }
        let guard = self.acquire(thread_id)?;
        source_locks.insert(key, guard.downgrade());
        Ok(guard)
    }

    /// Acquire an exclusive live-writer guard and publish it atomically for in-process readers.
    ///
    /// This follows the same registry-before-filesystem order as [`Self::acquire_source`], so a
    /// fork from a different [`super::LocalThreadStore`] cannot see an unregistered live writer.
    pub(super) fn acquire_registered_source_owner(
        self: &Arc<Self>,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<WriterLockGuard> {
        let key = SourceLockKey {
            codex_home: self.codex_home.clone(),
            thread_id,
        };
        let mut source_locks = source_locks()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        source_locks.retain(|_, guard| guard.upgrade().is_some());
        if source_locks
            .get(&key)
            .and_then(codex_rollout::ThreadWriterLockWeakGuard::upgrade)
            .is_some()
        {
            return Err(ThreadStoreError::Conflict {
                message: format!("thread {thread_id} already has an active source lease"),
            });
        }
        let guard = self.acquire(thread_id)?;
        source_locks.insert(key, guard.downgrade());
        Ok(guard)
    }

    pub(super) fn acquire_topology(&self) -> ThreadStoreResult<TopologyLockGuard> {
        self.inner.acquire_topology().map_err(|err| {
            if err.kind() == std::io::ErrorKind::WouldBlock {
                ThreadStoreError::Conflict {
                    message: err.to_string(),
                }
            } else {
                ThreadStoreError::Internal {
                    message: format!("failed to acquire rollout topology lock: {err}"),
                }
            }
        })
    }
}

#[cfg(test)]
#[path = "writer_lock_tests.rs"]
mod tests;
