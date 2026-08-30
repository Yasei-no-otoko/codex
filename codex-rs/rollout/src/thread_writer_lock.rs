//! Cross-process per-thread writer coordination shared by rollout maintenance and thread store.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_protocol::ThreadId;
use tracing::warn;

/// Directory below Codex home containing cross-process rollout writer locks.
pub const THREAD_WRITER_LOCK_DIR: &str = "thread-writer-locks";
/// Short-lived mutex used when creating or removing a per-thread lock file.
pub const THREAD_WRITER_COORDINATION_LOCK_FILE: &str = ".coordination.lock";
/// Global barrier for reference-index scans and topology-changing rollout moves.
pub const THREAD_WRITER_TOPOLOGY_LOCK_FILE: &str = ".reference-topology.lock";

/// Coordinates cloneable per-thread locks and the global rollout topology barrier.
pub struct ThreadWriterLockCoordinator {
    directory: PathBuf,
    cleanup_attempted: AtomicBool,
}

/// A cloneable cross-process lock for a single thread's rollout representation.
#[derive(Clone)]
pub struct ThreadWriterLockGuard {
    inner: Arc<ThreadWriterLockGuardInner>,
}

/// A non-owning handle that can reuse an in-process thread writer lock while it remains held.
#[derive(Clone)]
pub struct ThreadWriterLockWeakGuard {
    inner: Weak<ThreadWriterLockGuardInner>,
}

/// A cross-process barrier for scans and renames that change rollout topology.
pub struct ThreadWriterTopologyLockGuard {
    _file: File,
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

impl ThreadWriterLockGuard {
    /// Downgrade this guard so callers can share an existing lock without extending its lifetime.
    pub fn downgrade(&self) -> ThreadWriterLockWeakGuard {
        ThreadWriterLockWeakGuard {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl ThreadWriterLockWeakGuard {
    /// Return a clone of the active guard, if its last owner has not released the lock.
    pub fn upgrade(&self) -> Option<ThreadWriterLockGuard> {
        self.inner
            .upgrade()
            .map(|inner| ThreadWriterLockGuard { inner })
    }
}

impl std::fmt::Debug for ThreadWriterTopologyLockGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadWriterTopologyLockGuard")
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
        drop(coordination_lock);
        Ok(ThreadWriterLockGuard {
            inner: Arc::new(ThreadWriterLockGuardInner {
                coordinator: Arc::clone(self),
                path,
                file: Some(file),
            }),
        })
    }

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
#[path = "thread_writer_lock_tests.rs"]
mod tests;
