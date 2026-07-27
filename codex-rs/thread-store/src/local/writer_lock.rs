use std::sync::Arc;

use codex_protocol::ThreadId;
use codex_rollout::ThreadWriterLockCoordinator;
use codex_rollout::ThreadWriterLockGuard;
use codex_rollout::ThreadWriterTopologyLockGuard;

use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(test)]
pub(super) const WRITER_LOCK_DIR: &str = codex_rollout::THREAD_WRITER_LOCK_DIR;
#[cfg(test)]
pub(super) const COORDINATION_LOCK_FILE: &str = codex_rollout::THREAD_WRITER_COORDINATION_LOCK_FILE;
#[cfg(test)]
pub(super) const TOPOLOGY_LOCK_FILE: &str = codex_rollout::THREAD_WRITER_TOPOLOGY_LOCK_FILE;
pub(super) type WriterLockGuard = ThreadWriterLockGuard;
pub(super) type TopologyLockGuard = ThreadWriterTopologyLockGuard;

pub(super) struct WriterLockCoordinator {
    inner: Arc<ThreadWriterLockCoordinator>,
}

impl WriterLockCoordinator {
    pub(super) fn new(codex_home: &std::path::Path) -> Self {
        Self {
            inner: Arc::new(ThreadWriterLockCoordinator::new(codex_home)),
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
                    message: format!("failed to acquire thread writer lock for thread {thread_id}: {err}"),
                }
            }
        })
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
