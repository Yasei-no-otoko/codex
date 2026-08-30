use std::fs;

use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;
use tempfile::tempdir;

use super::last_complete_rollout_envelope_offset;
use super::unsafe_source_error;
use super::super::helpers::managed_rollout_path;
use crate::ThreadStoreError;

#[tokio::test]
async fn cutoff_accepts_schema_evolved_payload_envelope() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("legacy.jsonl");
    let complete = b"{\"timestamp\":\"2026-08-30T00:00:00Z\",\"type\":\"token_count\",\"payload\":{\"future_schema\":{\"nested\":true}}}\n";
    fs::write(&path, complete).expect("write fixture");

    assert_eq!(
        last_complete_rollout_envelope_offset(path.as_path())
            .await
            .expect("complete outer envelope"),
        u64::try_from(complete.len()).expect("fixture length")
    );
}

#[tokio::test]
async fn cutoff_excludes_incomplete_tail() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("legacy.jsonl");
    let complete = b"{\"timestamp\":\"2026-08-30T00:00:00Z\",\"type\":\"event_msg\",\"payload\":{}}\n";
    let partial = b"{\"timestamp\":\"2026-08-30T00:00:01Z\",\"type\":\"event_msg\"";
    let mut contents = complete.to_vec();
    contents.extend_from_slice(partial);
    fs::write(&path, contents).expect("write fixture");

    assert_eq!(
        last_complete_rollout_envelope_offset(path.as_path())
            .await
            .expect("complete prefix"),
        u64::try_from(complete.len()).expect("fixture length")
    );
}

#[test]
fn root_unsafe_source_allows_copy_fallback_but_reference_child_fails_closed() {
    assert!(matches!(
        unsafe_source_error(false, "unmanaged legacy reference source"),
        ThreadStoreError::Unsupported { .. }
    ));
    assert!(matches!(
        unsafe_source_error(true, "unmanaged legacy reference source"),
        ThreadStoreError::InvalidRequest { .. }
    ));
}

#[test]
fn compressed_reference_child_cannot_fall_back_to_a_physical_suffix_copy() {
    assert!(matches!(
        unsafe_source_error(false, "compressed legacy reference fork"),
        ThreadStoreError::Unsupported { .. }
    ));
    assert!(matches!(
        unsafe_source_error(true, "compressed legacy reference fork"),
        ThreadStoreError::InvalidRequest { .. }
    ));
}

#[test]
fn managed_source_requires_session_tree_and_canonical_filename_id() {
    let home = tempdir().expect("temporary Codex home");
    let thread_id = ThreadId::default();
    let sessions = home.path().join("sessions/2026/08/30");
    fs::create_dir_all(sessions.as_path()).expect("create sessions directory");
    fs::create_dir_all(home.path().join("archived_sessions")).expect("create archive directory");

    let external = tempdir().expect("external directory");
    let external_path = external
        .path()
        .join(format!("rollout-2026-08-30T00-00-00-{thread_id}.jsonl"));
    fs::write(&external_path, b"{}").expect("write external fixture");
    assert!(managed_rollout_path(home.path(), external_path.as_path(), thread_id).is_err());

    let mismatched_name = sessions.join("rollout-2026-08-30T00-00-00-not-a-thread.jsonl");
    fs::write(&mismatched_name, b"{}").expect("write renamed fixture");
    assert!(managed_rollout_path(home.path(), mismatched_name.as_path(), thread_id).is_err());
}
