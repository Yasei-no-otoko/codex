use std::fs;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_rollout::RolloutLine;
use tempfile::NamedTempFile;

use super::reference_attachment::AsyncAttachmentWriter;
use super::reference_attachment::envelope_kind;
use super::reference_attachment::stream_segment;
use super::rollout_lineage::RolloutLineageSegment;

fn envelope(kind: &str, payload: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "timestamp": "2026-08-30T00:00:00Z",
        "type": kind,
        "payload": payload,
    }))
    .expect("serialize envelope")
}

fn join_lines(lines: impl IntoIterator<Item = Vec<u8>>) -> Vec<u8> {
    let mut output = Vec::new();
    for line in lines {
        output.extend_from_slice(line.as_slice());
        output.push(b'\n');
    }
    output
}

#[tokio::test]
async fn bounded_output_over_four_megabytes_keeps_complete_lines() {
    let output = NamedTempFile::new().expect("create output");
    let mut writer = AsyncAttachmentWriter::new(output.path().to_path_buf(), 4 * 1024 * 1024)
        .await
        .expect("create bounded writer");
    writer
        .push_header_line(&envelope("session_meta", serde_json::json!({})))
        .await
        .expect("write header");
    writer.push_line(b"head-marker").await.expect("write head");
    let oversized = format!("padding:{}", "x".repeat(4 * 1024 * 1024));
    writer
        .push_line(oversized.as_bytes())
        .await
        .expect("write oversized line");
    writer.push_line(b"tail-marker").await.expect("write tail");
    assert!(writer.finish().await.expect("finish"));

    let bytes = fs::read(output.path()).expect("read output");
    assert!(bytes.len() <= 4 * 1024 * 1024);
    assert!(bytes.ends_with(b"\n"));
    assert!(
        bytes
            .windows(b"head-marker".len())
            .any(|w| w == b"head-marker")
    );
    assert!(
        bytes
            .windows(b"tail-marker".len())
            .any(|w| w == b"tail-marker")
    );
    assert!(!bytes.windows(b"padding:".len()).any(|w| w == b"padding:"));
}

#[tokio::test]
async fn oversized_single_line_is_drained_without_unbounded_reader_allocation() {
    let source = NamedTempFile::new().expect("create source");
    let oversized = format!("{}\n", "x".repeat(4 * 1024 * 1024));
    fs::write(source.path(), [oversized.as_bytes(), b"tail\n"].concat()).expect("write source");
    let mut reader = codex_rollout::open_rollout_raw_line_reader(source.path())
        .await
        .expect("open raw reader");
    let oversized_record = reader
        .next_raw_line_limited(1024)
        .await
        .expect("read oversized line");
    assert!(matches!(
        oversized_record,
        Some(codex_rollout::RawRolloutLine::Oversized {
            byte_count: _,
            terminated: true,
        })
    ));
    assert_eq!(
        reader.next_raw_line_limited(1024).await.expect("read tail"),
        Some(codex_rollout::RawRolloutLine::Complete(b"tail\n".to_vec()))
    );
}

#[tokio::test]
async fn raw_reader_allows_exact_payload_limit_plus_lf_for_plain_and_zstd() {
    const PAYLOAD_LIMIT: usize = 16 * 1024 * 1024;
    for compressed in [false, true] {
        let source = NamedTempFile::new().expect("create source");
        let mut exact = vec![b'x'; PAYLOAD_LIMIT];
        exact.push(b'\n');
        let path = if compressed {
            let path = source.path().with_extension("jsonl.zst");
            fs::write(
                &path,
                zstd::stream::encode_all(exact.as_slice(), 3).expect("compress exact record"),
            )
            .expect("write compressed exact record");
            path
        } else {
            fs::write(source.path(), exact.as_slice()).expect("write exact record");
            source.path().to_path_buf()
        };
        let mut reader = codex_rollout::open_rollout_raw_line_reader(&path)
            .await
            .expect("open exact record");
        assert!(matches!(
            reader
                .next_raw_line_limited(PAYLOAD_LIMIT + 1)
                .await
                .expect("read exact record"),
            Some(codex_rollout::RawRolloutLine::Complete(line)) if line.len() == PAYLOAD_LIMIT + 1
        ));

        let mut over = vec![b'x'; PAYLOAD_LIMIT + 1];
        over.push(b'\n');
        let over_path = if compressed {
            let path = source.path().with_file_name("over.jsonl.zst");
            fs::write(
                &path,
                zstd::stream::encode_all(over.as_slice(), 3).expect("compress over-limit record"),
            )
            .expect("write compressed over-limit record");
            path
        } else {
            let path = source.path().with_file_name("over.jsonl");
            fs::write(&path, over.as_slice()).expect("write over-limit record");
            path
        };
        let mut reader = codex_rollout::open_rollout_raw_line_reader(&over_path)
            .await
            .expect("open over-limit record");
        assert!(matches!(
            reader
                .next_raw_line_limited(PAYLOAD_LIMIT + 1)
                .await
                .expect("read over-limit record"),
            Some(codex_rollout::RawRolloutLine::Oversized {
                byte_count: _,
                terminated: true,
            })
        ));
    }
}

#[tokio::test]
async fn oversized_record_at_ancestor_cutoff_cannot_leak_post_cutoff_secret() {
    let source = NamedTempFile::new().expect("create source");
    let prefix = envelope("event_msg", serde_json::json!({"message": "small-prefix"}));
    let oversized = format!("{}\n", "x".repeat(4 * 1024 * 1024));
    let secret = envelope(
        "event_msg",
        serde_json::json!({"message": "post-cutoff-secret"}),
    );
    let mut bytes = Vec::new();
    bytes.extend_from_slice(prefix.as_slice());
    bytes.push(b'\n');
    bytes.extend_from_slice(oversized.as_bytes());
    bytes.extend_from_slice(secret.as_slice());
    bytes.push(b'\n');
    fs::write(source.path(), bytes).expect("write source");
    let cutoff = prefix.len() as u64 + 1 + oversized.len() as u64;
    let output = NamedTempFile::new().expect("create output");
    let mut writer = AsyncAttachmentWriter::new(output.path().to_path_buf(), 4 * 1024 * 1024)
        .await
        .expect("create writer");
    stream_segment(
        &RolloutLineageSegment {
            rollout_id: ThreadId::default(),
            rollout_path: source.path().to_path_buf(),
            start_ordinal: 0,
            end: Some(HistoryPosition {
                thread_id: ThreadId::default(),
                end_ordinal_exclusive: 3,
                end_byte_offset: cutoff,
            }),
        },
        &mut writer,
    )
    .await
    .expect("stream bounded ancestor");
    assert!(writer.finish().await.expect("finish"));
    let text = fs::read_to_string(output.path()).expect("read output");
    assert!(text.contains("small-prefix"));
    assert!(!text.contains("post-cutoff-secret"));
}

#[tokio::test]
async fn oversized_record_after_cutoff_does_not_mark_attachment_truncated() {
    let source = NamedTempFile::new().expect("create source");
    let prefix = envelope("event_msg", serde_json::json!({"message": "before-cutoff"}));
    let oversized = format!("{}\n", "x".repeat(4 * 1024 * 1024));
    let secret = envelope("event_msg", serde_json::json!({"message": "after-cutoff"}));
    let mut bytes = prefix.clone();
    bytes.push(b'\n');
    bytes.extend_from_slice(oversized.as_bytes());
    bytes.extend_from_slice(secret.as_slice());
    bytes.push(b'\n');
    fs::write(source.path(), bytes).expect("write source");
    let output = NamedTempFile::new().expect("create output");
    let mut writer = AsyncAttachmentWriter::new(output.path().to_path_buf(), 4 * 1024 * 1024)
        .await
        .expect("create writer");
    stream_segment(
        &RolloutLineageSegment {
            rollout_id: ThreadId::default(),
            rollout_path: source.path().to_path_buf(),
            start_ordinal: 0,
            end: Some(HistoryPosition {
                thread_id: ThreadId::default(),
                end_ordinal_exclusive: 1,
                end_byte_offset: prefix.len() as u64 + 1,
            }),
        },
        &mut writer,
    )
    .await
    .expect("stream before cutoff");
    assert!(!writer.finish().await.expect("finish"));
    let text = fs::read_to_string(output.path()).expect("read output");
    assert!(text.contains("before-cutoff"));
    assert!(!text.contains("after-cutoff"));
}

#[tokio::test]
async fn zstd_rollout_streams_logical_lines_without_plain_materialization() {
    let source = NamedTempFile::new().expect("create source");
    let compressed_path = source.path().with_extension("jsonl.zst");
    let lines = join_lines([
        envelope("session_meta", serde_json::json!({})),
        envelope(
            "event_msg",
            serde_json::json!({"message": "compressed-marker"}),
        ),
    ]);
    let compressed = zstd::stream::encode_all(lines.as_slice(), 3).expect("compress rollout");
    fs::write(&compressed_path, compressed).expect("write compressed rollout");
    let output = NamedTempFile::new().expect("create output");
    let mut writer = AsyncAttachmentWriter::new(output.path().to_path_buf(), 4096)
        .await
        .expect("create writer");
    stream_segment(
        &RolloutLineageSegment {
            rollout_id: ThreadId::default(),
            rollout_path: compressed_path.clone(),
            start_ordinal: 0,
            end: None,
        },
        &mut writer,
    )
    .await
    .expect("stream compressed rollout");
    assert!(!writer.finish().await.expect("finish"));
    let text = fs::read_to_string(output.path()).expect("read output");
    assert!(text.contains("compressed-marker"));
    assert!(compressed_path.exists());
    assert!(!compressed_path.with_extension("jsonl").exists());
}

#[tokio::test]
async fn valid_json_without_lf_is_not_written_as_a_child_tail() {
    let source = NamedTempFile::new().expect("create source");
    let tail = envelope(
        "event_msg",
        serde_json::json!({"message": "partial-child-tail"}),
    );
    fs::write(source.path(), tail).expect("write partial child");
    let output = NamedTempFile::new().expect("create output");
    let mut writer = AsyncAttachmentWriter::new(output.path().to_path_buf(), 4096)
        .await
        .expect("create writer");
    stream_segment(
        &RolloutLineageSegment {
            rollout_id: ThreadId::default(),
            rollout_path: source.path().to_path_buf(),
            start_ordinal: 0,
            end: None,
        },
        &mut writer,
    )
    .await
    .expect("stream partial child");
    writer.finish().await.expect("finish");
    assert!(
        fs::read_to_string(output.path())
            .expect("read output")
            .is_empty()
    );
}

#[tokio::test]
async fn ancestor_cutoff_accepts_only_newline_terminated_records() {
    let source = NamedTempFile::new().expect("create source");
    let partial = envelope(
        "event_msg",
        serde_json::json!({"message": "partial-ancestor"}),
    );
    let cutoff = partial.len() as u64;
    fs::write(source.path(), partial).expect("write partial ancestor");
    let output = NamedTempFile::new().expect("create output");
    let mut writer = AsyncAttachmentWriter::new(output.path().to_path_buf(), 4096)
        .await
        .expect("create writer");
    stream_segment(
        &RolloutLineageSegment {
            rollout_id: ThreadId::default(),
            rollout_path: source.path().to_path_buf(),
            start_ordinal: 0,
            end: Some(HistoryPosition {
                thread_id: ThreadId::default(),
                end_ordinal_exclusive: 2,
                end_byte_offset: cutoff,
            }),
        },
        &mut writer,
    )
    .await
    .expect("stream partial ancestor");
    writer.finish().await.expect("finish");
    assert!(
        fs::read_to_string(output.path())
            .expect("read output")
            .is_empty()
    );
}

#[tokio::test]
async fn prefix_and_child_delta_are_streamed_as_complete_lines() {
    let parent = NamedTempFile::new().expect("create parent");
    let child = NamedTempFile::new().expect("create child");
    let parent_bytes = join_lines([
        envelope("session_meta", serde_json::json!({})),
        envelope("event_msg", serde_json::json!({"message": "prefix-marker"})),
    ]);
    fs::write(parent.path(), parent_bytes).expect("write parent");
    fs::write(
        child.path(),
        join_lines([
            envelope("session_meta", serde_json::json!({})),
            envelope(
                "event_msg",
                serde_json::json!({"message": "child-delta-marker"}),
            ),
        ]),
    )
    .expect("write child");

    let output = NamedTempFile::new().expect("create output");
    let mut writer = AsyncAttachmentWriter::new(output.path().to_path_buf(), 4096)
        .await
        .expect("create writer");
    stream_segment(
        &RolloutLineageSegment {
            rollout_id: ThreadId::default(),
            rollout_path: parent.path().to_path_buf(),
            start_ordinal: 0,
            end: Some(HistoryPosition {
                thread_id: ThreadId::default(),
                end_ordinal_exclusive: 2,
                end_byte_offset: fs::metadata(parent.path()).expect("parent metadata").len(),
            }),
        },
        &mut writer,
    )
    .await
    .expect("stream prefix");
    stream_segment(
        &RolloutLineageSegment {
            rollout_id: ThreadId::default(),
            rollout_path: child.path().to_path_buf(),
            start_ordinal: 0,
            end: None,
        },
        &mut writer,
    )
    .await
    .expect("stream child delta");
    writer.finish().await.expect("finish");

    let text = fs::read_to_string(output.path()).expect("read output");
    assert!(text.contains("prefix-marker"));
    assert!(text.contains("child-delta-marker"));
    assert!(text.lines().all(|line| !line.is_empty()));
}

#[tokio::test]
async fn paginated_reference_stream_preserves_ordinal_lines() {
    let source = NamedTempFile::new().expect("create source");
    let lines = join_lines([
        serde_json::to_vec(&RolloutLine {
            timestamp: "2026-08-30T00:00:00Z".to_string(),
            ordinal: Some(1),
            item: codex_rollout::RolloutItem::EventMsg(
                codex_protocol::protocol::EventMsg::ShutdownComplete,
            ),
        })
        .expect("serialize ordinal line"),
        envelope(
            "event_msg",
            serde_json::json!({"message": "paginated-marker"}),
        ),
    ]);
    fs::write(source.path(), lines).expect("write source");
    let output = NamedTempFile::new().expect("create output");
    let mut writer = AsyncAttachmentWriter::new(output.path().to_path_buf(), 4096)
        .await
        .expect("create writer");
    stream_segment(
        &RolloutLineageSegment {
            rollout_id: ThreadId::default(),
            rollout_path: source.path().to_path_buf(),
            start_ordinal: 1,
            end: None,
        },
        &mut writer,
    )
    .await
    .expect("stream paginated source");
    writer.finish().await.expect("finish");
    let text = fs::read_to_string(output.path()).expect("read output");
    assert!(text.contains("paginated-marker"));
    assert!(
        text.lines()
            .all(|line| line.ends_with('}') || line.ends_with(']'))
    );
}

#[test]
fn numeric_legacy_fork_metadata_is_a_valid_non_reference_envelope() {
    let line = envelope(
        "session_meta",
        serde_json::json!({"subagent_history_start_ordinal": 7}),
    );
    let info = envelope_kind(&line).expect("session metadata envelope");
    assert_eq!(info.kind, "session_meta");
    assert!(!info.ghost_snapshot);
}

#[tokio::test]
async fn legacy_ghost_snapshot_is_omitted_without_payload_materialization() {
    let source = NamedTempFile::new().expect("create source");
    let lines = join_lines([
        envelope(
            "response_item",
            serde_json::json!({"type": "ghost_snapshot"}),
        ),
        envelope(
            "event_msg",
            serde_json::json!({"message": "retained-marker"}),
        ),
    ]);
    fs::write(source.path(), lines).expect("write source");
    let output = NamedTempFile::new().expect("create output");
    let mut writer = AsyncAttachmentWriter::new(output.path().to_path_buf(), 4096)
        .await
        .expect("create writer");
    stream_segment(
        &RolloutLineageSegment {
            rollout_id: ThreadId::default(),
            rollout_path: source.path().to_path_buf(),
            start_ordinal: 0,
            end: None,
        },
        &mut writer,
    )
    .await
    .expect("stream source");
    writer.finish().await.expect("finish");
    let text = fs::read_to_string(output.path()).expect("read output");
    assert!(text.contains("retained-marker"));
    assert!(!text.contains("ghost_snapshot"));
}
