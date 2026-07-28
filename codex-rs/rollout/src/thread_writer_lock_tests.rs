use super::ThreadWriterLockCoordinator;
use codex_protocol::ThreadId;
use tempfile::TempDir;
use uuid::Uuid;

#[test]
fn same_coordinator_operations_conflict_in_both_directions() -> std::io::Result<()> {
    let home = TempDir::new()?;
    let thread_id =
        ThreadId::from_string(&Uuid::from_u128(901).to_string()).map_err(std::io::Error::other)?;
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
