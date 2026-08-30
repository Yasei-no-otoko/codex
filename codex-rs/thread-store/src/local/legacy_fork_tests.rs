use std::fs;

use pretty_assertions::assert_eq;
use tempfile::tempdir;

use super::last_complete_rollout_envelope_offset;

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
