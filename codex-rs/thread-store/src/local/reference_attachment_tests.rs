use std::fs;
use std::fs::OpenOptions;
use std::io::Write;

use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::UserMessageEvent;
use pretty_assertions::assert_eq;
use tempfile::NamedTempFile;
use tempfile::TempDir;
use uuid::Uuid;

use super::LocalThreadStore;
use super::reference_attachment::write_reference_logical_attachment_from_items;
use super::test_support::compress_session_file;
use super::test_support::set_history_base_in_session_file;
use super::test_support::test_config;
use super::test_support::write_session_file;
use super::test_support::write_session_file_with_fork;
use crate::ThreadStore;
use crate::WriteReferenceLogicalAttachmentOutcome;
use crate::WriteReferenceLogicalAttachmentParams;

#[test]
fn loaded_logical_attachment_keeps_head_and_tail_complete_lines() {
    let output = NamedTempFile::new().expect("create output");
    let make_message = |message: &str| {
        RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: message.to_string(),
            ..Default::default()
        }))
    };
    let items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta::default(),
            git: None,
        }),
        make_message(&format!("parent-head-marker:{}", "p".repeat(500))),
        make_message(&format!("middle:{}", "m".repeat(900))),
        make_message(&format!("child-tail-marker:{}", "c".repeat(500))),
    ];

    let outcome =
        write_reference_logical_attachment_from_items(output.path().to_path_buf(), items, 2_048)
            .expect("write loaded logical attachment");
    assert_eq!(
        outcome,
        WriteReferenceLogicalAttachmentOutcome::Written { truncated: true }
    );
    let bytes = fs::read(output.path()).expect("read output");
    assert!(bytes.len() <= 2_048);
    assert!(bytes.ends_with(b"\n"));
    let text = String::from_utf8(bytes).expect("JSONL output");
    assert!(text.contains("parent-head-marker"));
    assert!(text.contains("child-tail-marker"));
    let first: serde_json::Value = serde_json::from_str(
        text.lines()
            .next()
            .expect("SessionMeta line should be present"),
    )
    .expect("valid SessionMeta JSON");
    assert!(first["payload"]["history_base"].is_null());
}

#[test]
fn loaded_logical_attachment_rejects_oversized_session_meta() {
    let output = NamedTempFile::new().expect("create output");
    let mut meta = SessionMeta::default();
    meta.preview = Some("x".repeat(2_000));
    let err = write_reference_logical_attachment_from_items(
        output.path().to_path_buf(),
        vec![RolloutItem::SessionMeta(SessionMetaLine {
            meta,
            git: None,
        })],
        1_024,
    )
    .expect_err("oversized SessionMeta must fail closed");
    assert!(
        err.to_string()
            .contains("SessionMeta exceeds attachment head budget")
    );
}

#[tokio::test]
async fn cold_reference_attachment_reads_compressed_ancestor_without_materializing()
-> Result<(), Box<dyn std::error::Error>> {
    let home = TempDir::new()?;
    let parent_uuid = Uuid::from_u128(0x101);
    let child_uuid = Uuid::from_u128(0x102);
    let parent_id = ThreadId::from_string(&parent_uuid.to_string())?;
    let child_id = ThreadId::from_string(&child_uuid.to_string())?;
    let parent_path = write_session_file(home.path(), "2025-01-03T12-00-00", parent_uuid)?;
    let parent_end = fs::metadata(&parent_path)?.len();
    let child_path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T12-01-00",
        child_uuid,
        "child-tail-marker",
        Some("test-provider"),
        Some(parent_uuid),
        codex_protocol::protocol::ThreadHistoryMode::Legacy,
    )?;
    set_history_base_in_session_file(
        &child_path,
        &HistoryPosition {
            thread_id: parent_id,
            end_ordinal_exclusive: 0,
            end_byte_offset: parent_end,
        },
    )?;
    let compressed_parent = compress_session_file(&parent_path)?;
    let store = LocalThreadStore::new(test_config(home.path()), None);
    let output = NamedTempFile::new()?;
    let outcome = store
        .write_reference_logical_attachment(WriteReferenceLogicalAttachmentParams {
            thread_id: child_id,
            include_archived: true,
            output_path: output.path().to_path_buf(),
            max_bytes: 4 * 1024 * 1024,
        })
        .await?;
    assert_eq!(
        outcome,
        WriteReferenceLogicalAttachmentOutcome::Written { truncated: false }
    );
    let text = fs::read_to_string(output.path())?;
    assert!(text.contains("Hello from user"));
    assert!(text.contains("child-tail-marker"));
    assert!(
        compressed_parent.exists(),
        "read-only attachment must retain zstd"
    );
    assert!(
        !parent_path.exists(),
        "read-only attachment must not materialize plain JSONL"
    );
    Ok(())
}

#[tokio::test]
async fn cold_paginated_reference_attachment_reads_frozen_lineage_without_materializing()
-> Result<(), Box<dyn std::error::Error>> {
    let home = TempDir::new()?;
    let parent_uuid = Uuid::from_u128(0x201);
    let child_uuid = Uuid::from_u128(0x202);
    let parent_id = ThreadId::from_string(&parent_uuid.to_string())?;
    let child_id = ThreadId::from_string(&child_uuid.to_string())?;
    let day_dir = home.path().join("sessions/2025/01/03");
    let parent_path = super::test_support::write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T12-02-00",
        parent_uuid,
        codex_protocol::protocol::ThreadHistoryMode::Paginated,
    )?;
    append_paginated_message(&parent_path, "parent-frozen-marker", 1)?;
    let parent_cutoff = fs::metadata(&parent_path)?.len();
    append_paginated_message(&parent_path, "parent-after-fork-marker", 2)?;

    let child_path = write_session_file_with_fork(
        home.path(),
        day_dir,
        "2025-01-03T12-02-01",
        child_uuid,
        "unused",
        Some("test-provider"),
        Some(parent_uuid),
        codex_protocol::protocol::ThreadHistoryMode::Paginated,
    )?;
    append_paginated_message(&child_path, "child-delta-marker", 3)?;
    set_history_base_in_session_file(
        &child_path,
        &HistoryPosition {
            thread_id: parent_id,
            end_ordinal_exclusive: 2,
            end_byte_offset: parent_cutoff,
        },
    )?;
    let compressed_parent = compress_session_file(&parent_path)?;

    let store = LocalThreadStore::new(test_config(home.path()), None);
    let output = NamedTempFile::new()?;
    let outcome = store
        .write_reference_logical_attachment(WriteReferenceLogicalAttachmentParams {
            thread_id: child_id,
            include_archived: true,
            output_path: output.path().to_path_buf(),
            max_bytes: 4 * 1024 * 1024,
        })
        .await?;
    assert_eq!(
        outcome,
        WriteReferenceLogicalAttachmentOutcome::Written { truncated: false }
    );
    let text = fs::read_to_string(output.path())?;
    assert!(text.contains("parent-frozen-marker"));
    assert!(text.contains("child-delta-marker"));
    assert!(!text.contains("parent-after-fork-marker"));
    assert!(compressed_parent.exists());
    assert!(!parent_path.exists());
    Ok(())
}

fn append_paginated_message(
    path: &std::path::Path,
    message: &str,
    ordinal: u64,
) -> std::io::Result<()> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    let line = codex_protocol::protocol::RolloutLine {
        timestamp: "2025-01-03T12:02:02Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: message.to_string(),
            ..Default::default()
        })),
    };
    writeln!(
        file,
        "{}",
        serde_json::to_string(&line).expect("serialize paginated line")
    )
}
