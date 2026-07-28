use super::LocalThreadStore;
use super::prepare;
use crate::ForkBoundary;
use crate::PrepareForkParams;
use crate::ThreadStoreError;
use crate::local::test_support::test_config;
use crate::local::test_support::write_session_file_with_history_mode;
use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn prepare_rejects_legacy_lineage_before_projection() {
    let home = TempDir::new().expect("temp dir");
    let source_uuid = Uuid::from_u128(434);
    let source_id = ThreadId::from_string(&source_uuid.to_string()).expect("source id");
    write_session_file_with_history_mode(
        home.path(),
        "2025-01-04T12-35-00",
        source_uuid,
        ThreadHistoryMode::Legacy,
    )
    .expect("legacy source rollout");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let source_guards = store
        .acquire_fork_source_guards(source_id)
        .await
        .expect("source guards");
    let error = prepare(
        &store,
        PrepareForkParams {
            thread_id: source_id,
            boundary: ForkBoundary::Latest,
        },
        source_guards,
    )
    .await
    .expect_err("paginated preparation must reject legacy source");
    assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
    assert!(error.to_string().contains("paginated history"));
}
