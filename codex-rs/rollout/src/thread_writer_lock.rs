//! Cross-process per-thread writer coordination shared by rollout maintenance and thread store.
//!
//! The lock file name and cleanup protocol live in this lower-level crate so compression,
//! archive/delete, and reference preparation cannot silently drift apart.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_protocol::ThreadId;
use tracing::warn;

pub const THREAD_WRITER_LOCK_DIR: &str = "thread-writer-locks";
pub const THREAD_WRITER_COORDINATION_LOCK_FILE: &str = ".coordination.lock";
/// Global topology barrier for reference-index scans and sessions/archive renames.
pub const THREAD_WRITER_TOPOLOGY_LOCK_FILE: &str = ".reference-topology.lock";

/// Coordinator for the per-thread lock files under a Codex home.
pub struct ThreadWriterLockCoordinator {
    directory: PathBuf,
    cleanup_attempted: AtomicBool,
}

/// An owned cross-process lock for one thread.
#[derive(Clone)]
pub struct ThreadWriterLockGuard {
    inner: Arc<ThreadWriterLockGuardInner>,
}

/// A cross-process barrier for operations that change rollout topology.
///
/// This is deliberately separate from the short-lived lock-file coordination lock. The latter is
/// acquired by writer-guard drop and must never be held across a reference-index scan or rename.
/// Callers acquire lifecycle/local/thread writer guards first, then try this barrier immediately
/// before the scan or rename so a waiting operation does not occupy the global barrier.
pub struct ThreadWriterTopologyLockGuard {
    _file: File,
}

impl std::fmt::Debug for ThreadWriterTopologyLockGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadWriterTopologyLockGuard")
            .finish_non_exhaustive()
    }
}

struct ThreadWriterLockGuardInner {
    coordinator: Arc<ThreadWriterLockCoordinator>,
    path: PathBuf,
    file: Option<File>,
}

impl std::fmt::Debug for ThreadWriterLockGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadWriterLockGuard")
            .field("path", &self.inner.path)
            .finish_non_exhaustive()
    }
}

impl ThreadWriterLockCoordinator {
    pub fn new(codex_home: &Path) -> Self {
        Self {
            directory: codex_home.join(THREAD_WRITER_LOCK_DIR),
            cleanup_attempted: AtomicBool::new(false),
        }
    }

    pub fn acquire(self: &Arc<Self>, thread_id: ThreadId) -> io::Result<ThreadWriterLockGuard> {
        let coordination_lock = self.lock_coordination()?;
        if !self.cleanup_attempted.swap(true, Ordering::Relaxed)
            && let Err(err) = self.remove_stale_thread_locks()
        {
            warn!("failed to clean up stale thread writer locks: {err}");
        }

        let path = self.directory.join(format!("{thread_id}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("thread {thread_id} already has an active writer"),
                ));
            }
            Err(std::fs::TryLockError::Error(err)) => return Err(err),
        }

        let inner = Arc::new(ThreadWriterLockGuardInner {
            coordinator: Arc::clone(self),
            path,
            file: Some(file),
        });
        drop(coordination_lock);
        Ok(ThreadWriterLockGuard { inner })
    }

    /// Try to acquire the cross-process topology barrier without reusing the short coordination
    /// lock. Callers hold it only across reference-index scans and topology-changing renames.
    pub fn acquire_topology(&self) -> io::Result<ThreadWriterTopologyLockGuard> {
        fs::create_dir_all(&self.directory)?;
        let path = self.directory.join(THREAD_WRITER_TOPOLOGY_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        match file.try_lock() {
            Ok(()) => Ok(ThreadWriterTopologyLockGuard { _file: file }),
            Err(std::fs::TryLockError::WouldBlock) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "rollout topology is busy",
            )),
            Err(std::fs::TryLockError::Error(err)) => Err(err),
        }
    }

    fn lock_coordination(&self) -> io::Result<File> {
        fs::create_dir_all(&self.directory)?;
        let path = self.directory.join(THREAD_WRITER_COORDINATION_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.lock()?;
        Ok(file)
    }

    fn remove_stale_thread_locks(&self) -> io::Result<()> {
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(thread_id) = file_name.strip_suffix(".lock") else {
                continue;
            };
            if ThreadId::from_string(thread_id).is_err() {
                continue;
            }

            let path = entry.path();
            let file = match OpenOptions::new().read(true).write(true).open(&path) {
                Ok(file) => file,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => {
                    warn!(
                        "failed to inspect thread writer lock {}: {err}",
                        path.display()
                    );
                    continue;
                }
            };
            match file.try_lock() {
                Ok(()) => {
                    drop(file);
                    if let Err(err) = fs::remove_file(&path)
                        && err.kind() != io::ErrorKind::NotFound
                    {
                        warn!(
                            "failed to remove stale thread writer lock {}: {err}",
                            path.display()
                        );
                    }
                }
                Err(std::fs::TryLockError::WouldBlock) => {}
                Err(std::fs::TryLockError::Error(err)) => {
                    warn!(
                        "failed to inspect thread writer lock {}: {err}",
                        path.display()
                    );
                }
            }
        }
        Ok(())
    }
}

impl Drop for ThreadWriterLockGuardInner {
    fn drop(&mut self) {
        let coordination_lock = match self.coordinator.lock_coordination() {
            Ok(lock) => lock,
            Err(err) => {
                warn!("failed to coordinate thread writer lock cleanup: {err}");
                drop(self.file.take());
                return;
            }
        };

        // Close the writer lock before deleting it so cleanup also works on Windows.
        drop(self.file.take());
        if let Err(err) = fs::remove_file(&self.path)
            && err.kind() != io::ErrorKind::NotFound
        {
            warn!(
                "failed to remove thread writer lock {}: {err}",
                self.path.display()
            );
        }
        drop(coordination_lock);
    }
}

#[cfg(test)]
mod tests {
    use super::ThreadWriterLockCoordinator;
    use codex_protocol::ThreadId;
    use tempfile::TempDir;
    use uuid::Uuid;

    #[test]
    fn same_coordinator_operations_conflict_in_both_directions() -> std::io::Result<()> {
        let home = TempDir::new()?;
        let thread_id = ThreadId::from_string(&Uuid::from_u128(901).to_string())
            .map_err(std::io::Error::other)?;
        let coordinator = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
        let normal_guard = coordinator.acquire(thread_id)?;
        let maintenance_attempt = match coordinator.acquire(thread_id) {
            Ok(_) => {
                return Err(std::io::Error::other(
                    "maintenance acquired a lock held by a normal operation",
                ));
            }
            Err(error) => error,
        };
        assert_eq!(maintenance_attempt.kind(), std::io::ErrorKind::WouldBlock);
        let other = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
        let error = match other.acquire(thread_id) {
            Ok(_) => {
                return Err(std::io::Error::other(
                    "distinct coordinator acquired a held lock",
                ));
            }
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        drop(normal_guard);

        let maintenance_guard = other.acquire(thread_id)?;
        let normal_attempt = match coordinator.acquire(thread_id) {
            Ok(_) => {
                return Err(std::io::Error::other(
                    "normal operation acquired a lock held by maintenance",
                ));
            }
            Err(error) => error,
        };
        assert_eq!(normal_attempt.kind(), std::io::ErrorKind::WouldBlock);
        drop(maintenance_guard);
        assert!(coordinator.acquire(thread_id).is_ok());
        Ok(())
    }
}
