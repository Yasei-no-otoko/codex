use codex_protocol::ThreadId;
use tempfile::TempDir;
use uuid::Uuid;

use super::ThreadWriterLockCoordinator;

#[test]
fn shared_writer_locks_conflict_across_independent_coordinators() -> std::io::Result<()> {
    let home = TempDir::new()?;
    let thread_id =
        ThreadId::from_string(&Uuid::from_u128(901).to_string()).map_err(std::io::Error::other)?;
    let first = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
    let second = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
    let held = first.acquire(thread_id)?;
    let err = second
        .acquire(thread_id)
        .expect_err("second coordinator must observe the held lock");
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    drop(held);
    assert!(second.acquire(thread_id).is_ok());
    Ok(())
}

#[test]
fn cloned_guard_retains_the_cross_process_lock_until_last_owner_drops() -> std::io::Result<()> {
    let home = TempDir::new()?;
    let thread_id =
        ThreadId::from_string(&Uuid::from_u128(902).to_string()).map_err(std::io::Error::other)?;
    let first = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
    let second = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
    let original = first.acquire(thread_id)?;
    let retained = original.clone();
    drop(original);
    let err = second
        .acquire(thread_id)
        .expect_err("clone must retain the lock after the original owner drops");
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    drop(retained);
    assert!(second.acquire(thread_id).is_ok());
    Ok(())
}

#[test]
fn topology_barrier_serializes_reference_scans_and_renames() -> std::io::Result<()> {
    let home = TempDir::new()?;
    let first = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
    let second = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
    let held = first.acquire_topology()?;
    let err = second
        .acquire_topology()
        .expect_err("topology operations must not overlap");
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    drop(held);
    assert!(second.acquire_topology().is_ok());
    Ok(())
}

#[test]
fn per_thread_source_guards_allow_independent_preparations() -> std::io::Result<()> {
    let home = TempDir::new()?;
    let first_thread =
        ThreadId::from_string(&Uuid::from_u128(903).to_string()).map_err(std::io::Error::other)?;
    let second_thread =
        ThreadId::from_string(&Uuid::from_u128(904).to_string()).map_err(std::io::Error::other)?;
    let first = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
    let second = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
    let _first_prepared = first.acquire(first_thread)?;
    let _second_prepared = second.acquire(second_thread)?;
    let maintenance = std::sync::Arc::new(ThreadWriterLockCoordinator::new(home.path()));
    assert_eq!(
        maintenance
            .acquire(first_thread)
            .expect_err("first source guard must block maintenance")
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(
        maintenance
            .acquire(second_thread)
            .expect_err("second source guard must block maintenance")
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    Ok(())
}
