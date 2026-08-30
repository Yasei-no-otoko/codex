use std::fs;
use std::sync::Arc;

use codex_protocol::ThreadId;
use tempfile::TempDir;

use super::COORDINATION_LOCK_FILE;
use super::WRITER_LOCK_DIR;
use super::WriterLockCoordinator;
use crate::ThreadStoreError;

#[test]
fn writer_locks_reject_competing_owners_and_release_their_files() {
    let home = TempDir::new().expect("temp dir");
    let primary = Arc::new(WriterLockCoordinator::new(home.path()));
    let secondary = Arc::new(WriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::default();
    let other_thread_id = ThreadId::default();

    let owner = primary.acquire(thread_id).expect("acquire writer lock");
    let lock_path = home
        .path()
        .join(WRITER_LOCK_DIR)
        .join(format!("{thread_id}.lock"));
    assert!(lock_path.exists());

    let err = match secondary.acquire(thread_id) {
        Ok(_) => panic!("competing owner should fail"),
        Err(err) => err,
    };
    assert!(matches!(err, ThreadStoreError::Conflict { .. }));
    let other_owner = secondary
        .acquire(other_thread_id)
        .expect("other thread should acquire its own lock");

    drop(owner);
    assert!(!lock_path.exists());
    let next_owner = secondary
        .acquire(thread_id)
        .expect("released thread should accept another owner");
    drop(next_owner);
    drop(other_owner);

    let entries = fs::read_dir(home.path().join(WRITER_LOCK_DIR))
        .expect("read lock directory")
        .map(|entry| entry.expect("lock directory entry").file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, vec![COORDINATION_LOCK_FILE]);
}

#[test]
fn first_acquisition_removes_stale_locks_without_removing_active_locks() {
    let home = TempDir::new().expect("temp dir");
    let primary = Arc::new(WriterLockCoordinator::new(home.path()));
    let active_thread_id = ThreadId::default();
    let active_owner = primary
        .acquire(active_thread_id)
        .expect("acquire active writer lock");

    let stale_thread_id = ThreadId::default();
    let stale_path = home
        .path()
        .join(WRITER_LOCK_DIR)
        .join(format!("{stale_thread_id}.lock"));
    fs::File::create(&stale_path).expect("create stale writer lock");

    let secondary = Arc::new(WriterLockCoordinator::new(home.path()));
    let secondary_owner = secondary
        .acquire(ThreadId::default())
        .expect("acquire writer lock after cleanup");

    assert!(!stale_path.exists());
    let err = match secondary.acquire(active_thread_id) {
        Ok(_) => panic!("active writer should survive cleanup"),
        Err(err) => err,
    };
    assert!(matches!(err, ThreadStoreError::Conflict { .. }));

    drop(secondary_owner);
    drop(active_owner);
}

#[test]
fn source_leases_are_shared_but_live_writer_owners_remain_exclusive() {
    let home = TempDir::new().expect("temp dir");
    let primary = Arc::new(WriterLockCoordinator::new(home.path()));
    let secondary = Arc::new(WriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::default();

    let owner = primary
        .acquire_registered_source_owner(thread_id)
        .expect("acquire live writer owner");
    let reader = secondary
        .acquire_source(thread_id)
        .expect("share the active source lease");
    let err = secondary
        .acquire_registered_source_owner(thread_id)
        .expect_err("source reader must block a competing live writer owner");
    assert!(matches!(err, ThreadStoreError::Conflict { .. }));

    drop(reader);
    drop(owner);
    secondary
        .acquire_registered_source_owner(thread_id)
        .expect("released source lease should accept a new writer owner");
}

#[test]
fn source_leases_are_scoped_to_codex_home() {
    let first_home = TempDir::new().expect("first temp dir");
    let second_home = TempDir::new().expect("second temp dir");
    let thread_id = ThreadId::default();

    let first = Arc::new(WriterLockCoordinator::new(first_home.path()));
    let second = Arc::new(WriterLockCoordinator::new(second_home.path()));
    let _first_owner = first
        .acquire_registered_source_owner(thread_id)
        .expect("acquire first home writer owner");
    let _second_owner = second
        .acquire_registered_source_owner(thread_id)
        .expect("same thread id in a different Codex home is independent");
}
