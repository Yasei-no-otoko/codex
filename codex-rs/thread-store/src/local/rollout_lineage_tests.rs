use std::fs;
use std::io::Write;
use std::path::Path;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use super::RolloutLineageSegment;

#[tokio::test]
async fn resolves_nested_lineage_with_empty_intermediate_segments() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let middle = ThreadId::default();
    let child = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 6,
    );
    let root_end = history_position(root_path.as_path(), root, /*end_ordinal_exclusive*/ 4);
    let middle_path = write_rollout(home.path(), middle, Some(root_end), /*next_ordinal*/ 1);
    let middle_end = history_position(
        middle_path.as_path(),
        middle,
        /*end_ordinal_exclusive*/ 5,
    );
    let child_path = write_rollout(
        home.path(),
        child,
        Some(middle_end),
        /*next_ordinal*/ 3,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve nested lineage");

    assert_eq!(
        lineage.segments,
        vec![
            RolloutLineageSegment {
                thread_id: root,
                rollout_path: root_path.clone(),
                start_ordinal: 1,
                end: Some(root_end),
            },
            RolloutLineageSegment {
                thread_id: middle,
                rollout_path: middle_path.clone(),
                start_ordinal: 5,
                end: Some(middle_end),
            },
            RolloutLineageSegment {
                thread_id: child,
                rollout_path: child_path,
                start_ordinal: 6,
                end: None,
            },
        ]
    );
}

#[tokio::test]
async fn resolves_archived_ancestors() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let child = ThreadId::default();
    let root_path = write_rollout_under(
        home.path().join("archived_sessions"),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 3,
    );
    write_rollout(
        home.path(),
        child,
        Some(history_position(
            root_path.as_path(),
            root,
            /*end_ordinal_exclusive*/ 3,
        )),
        /*next_ordinal*/ 2,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve archived ancestor");

    assert_eq!(lineage.segments[0].rollout_path, root_path);
}

#[tokio::test]
async fn resolves_lineage_at_explicit_history_position() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let child = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 6,
    );
    let root_end = history_position(root_path.as_path(), root, /*end_ordinal_exclusive*/ 4);
    let child_path = write_rollout(home.path(), child, Some(root_end), /*next_ordinal*/ 4);
    let end = history_position(
        child_path.as_path(),
        child,
        /*end_ordinal_exclusive*/ 6,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve child lineage")
        .truncate_at(end)
        .await
        .expect("resolve explicit position");

    assert_eq!(
        lineage.segments,
        vec![
            RolloutLineageSegment {
                thread_id: root,
                rollout_path: root_path.clone(),
                start_ordinal: 1,
                end: Some(root_end),
            },
            RolloutLineageSegment {
                thread_id: child,
                rollout_path: child_path.clone(),
                start_ordinal: 5,
                end: Some(end),
            },
        ]
    );
}

#[tokio::test]
async fn rejects_missing_cycles_and_out_of_bounds_offsets() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let missing_parent = ThreadId::default();
    let missing_child = ThreadId::default();
    write_rollout(
        home.path(),
        missing_child,
        Some(unchecked_history_position(
            missing_parent,
            /*end_ordinal_exclusive*/ 1,
        )),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(&store, missing_child, "missing source rollout").await;

    let cycle_a = ThreadId::default();
    let cycle_b = ThreadId::default();
    let cycle_a_path = write_rollout(
        home.path(),
        cycle_a,
        /*history_base*/ None,
        /*next_ordinal*/ 2,
    );
    let cycle_b_path = write_rollout(
        home.path(),
        cycle_b,
        /*history_base*/ None,
        /*next_ordinal*/ 2,
    );
    for _ in 0..3 {
        let cycle_a_len = fs::metadata(cycle_a_path.as_path())
            .expect("cycle A metadata")
            .len();
        let cycle_b_len = fs::metadata(cycle_b_path.as_path())
            .expect("cycle B metadata")
            .len();
        write_rollout(
            home.path(),
            cycle_a,
            Some(HistoryPosition {
                thread_id: cycle_b,
                end_ordinal_exclusive: 1,
                end_byte_offset: cycle_b_len,
            }),
            /*next_ordinal*/ 2,
        );
        write_rollout(
            home.path(),
            cycle_b,
            Some(HistoryPosition {
                thread_id: cycle_a,
                end_ordinal_exclusive: 1,
                end_byte_offset: cycle_a_len,
            }),
            /*next_ordinal*/ 2,
        );
    }
    assert_invalid_lineage(&store, cycle_a, "cycle detected").await;

    let root = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 2,
    );
    let err = super::validate_cutoff_bounds(
        root,
        root_path.as_path(),
        &HistoryPosition {
            thread_id: root,
            end_ordinal_exclusive: 2,
            end_byte_offset: fs::metadata(root_path.as_path())
                .expect("root metadata")
                .len()
                + 1,
        },
        ThreadHistoryMode::Paginated,
        false,
    )
    .await
    .expect_err("cutoff past the source rollout should be rejected");
    assert!(
        err.to_string()
            .contains("cutoff byte offset is past the source rollout"),
        "{err}"
    );
}

#[tokio::test]
async fn paginated_cutoff_accepts_rejected_and_blank_newline_tail() {
    let home = TempDir::new().expect("temp dir");
    let root = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 3,
    );
    fs::OpenOptions::new()
        .append(true)
        .open(root_path.as_path())
        .expect("open root rollout")
        .write_all(b"{\"rejected\":true}\n\n")
        .expect("append rejected tail");
    let cutoff = fs::metadata(root_path.as_path())
        .expect("root metadata")
        .len();

    super::validate_cutoff_bounds(
        root,
        root_path.as_path(),
        &HistoryPosition {
            thread_id: root,
            end_ordinal_exclusive: 3,
            end_byte_offset: cutoff,
        },
        ThreadHistoryMode::Paginated,
        false,
    )
    .await
    .expect("paginated physical cutoff should accept rejected tail");
}

#[tokio::test]
async fn legacy_cutoff_rejects_newline_terminated_malformed_tail() {
    let home = TempDir::new().expect("temp dir");
    let root = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 2,
    );
    fs::OpenOptions::new()
        .append(true)
        .open(root_path.as_path())
        .expect("open root rollout")
        .write_all(b"{\"malformed\":true}\n")
        .expect("append malformed tail");
    let cutoff = fs::metadata(root_path.as_path())
        .expect("root metadata")
        .len();

    let err = super::validate_cutoff_bounds(
        root,
        root_path.as_path(),
        &HistoryPosition {
            thread_id: root,
            end_ordinal_exclusive: 0,
            end_byte_offset: cutoff,
        },
        ThreadHistoryMode::Legacy,
        false,
    )
    .await
    .expect_err("legacy cutoff must reject malformed tail");
    assert!(
        err.to_string().contains("complete rollout envelope"),
        "{err}"
    );
}

#[tokio::test]
async fn legacy_cutoff_accepts_unknown_nested_payload_schema() {
    let home = TempDir::new().expect("temp dir");
    let root = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 2,
    );
    fs::OpenOptions::new()
        .append(true)
        .open(root_path.as_path())
        .expect("open root rollout")
        .write_all(
            br#"{"timestamp":"2026-07-16T00:00:01.000Z","type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"primary":{"used_percent":{"future_shape":[1,2,3]}}}}}"#,
        )
        .expect("append schema-evolved envelope");
    fs::OpenOptions::new()
        .append(true)
        .open(root_path.as_path())
        .expect("reopen root rollout")
        .write_all(b"\n")
        .expect("terminate schema-evolved envelope");
    let cutoff = fs::metadata(root_path.as_path())
        .expect("root metadata")
        .len();

    super::validate_cutoff_bounds(
        root,
        root_path.as_path(),
        &HistoryPosition {
            thread_id: root,
            end_ordinal_exclusive: 0,
            end_byte_offset: cutoff,
        },
        ThreadHistoryMode::Legacy,
        false,
    )
    .await
    .expect("legacy cutoff should accept a complete envelope with an unknown payload schema");
}

#[tokio::test]
async fn compressed_legacy_cutoff_accepts_complete_envelope_without_materializing() {
    let home = TempDir::new().expect("temp dir");
    let root = ThreadId::default();
    let plain_path = home
        .path()
        .join("sessions/2026/07/16")
        .join(format!("rollout-2026-07-16T00-00-00-{root}.jsonl"));
    fs::create_dir_all(plain_path.parent().expect("rollout parent")).expect("create rollout dir");
    let bytes = br#"{"timestamp":"2026-07-16T00:00:00.000Z","type":"session_meta","payload":{}}
{"timestamp":"2026-07-16T00:00:01.000Z","type":"future_event","payload":{"new_shape":[1,2,3]}}
"#;
    fs::write(&plain_path, bytes).expect("write legacy rollout");
    let compressed_path = plain_path.with_extension("jsonl.zst");
    let mut output = fs::File::create(&compressed_path).expect("create compressed rollout");
    let mut input = fs::File::open(&plain_path).expect("open legacy rollout");
    zstd::stream::copy_encode(&mut input, &mut output, 3).expect("compress legacy rollout");
    fs::remove_file(&plain_path).expect("remove plain rollout");

    super::validate_cutoff_bounds(
        root,
        compressed_path.as_path(),
        &HistoryPosition {
            thread_id: root,
            end_ordinal_exclusive: 0,
            end_byte_offset: bytes.len() as u64,
        },
        ThreadHistoryMode::Legacy,
        false,
    )
    .await
    .expect("compressed legacy envelope boundary should validate");
    assert!(
        compressed_path.exists(),
        "validation must not materialize zstd"
    );
    assert!(
        !plain_path.exists(),
        "validation must not create a plain sibling"
    );
}

async fn assert_invalid_lineage(store: &LocalThreadStore, thread_id: ThreadId, detail: &str) {
    let err = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect_err("lineage should be invalid");
    assert!(err.to_string().contains(detail), "{err}");
}

fn write_rollout(
    home: &Path,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
    next_ordinal: u64,
) -> std::path::PathBuf {
    write_rollout_under(
        home.join("sessions/2026/07/16"),
        thread_id,
        history_base,
        next_ordinal,
    )
}

fn write_rollout_under(
    directory: std::path::PathBuf,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
    next_ordinal: u64,
) -> std::path::PathBuf {
    fs::create_dir_all(directory.as_path()).expect("create rollout directory");
    let path = directory.join(format!("rollout-2026-07-16T00-00-00-{thread_id}.jsonl"));
    let initial_ordinal = history_base.map_or(0, |base| base.end_ordinal_exclusive);
    let mut lines = vec![rollout_line(
        initial_ordinal,
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                history_mode: ThreadHistoryMode::Paginated,
                history_base,
                ..SessionMeta::default()
            },
            git: None,
        }),
    )];
    for offset in 1..next_ordinal {
        let ordinal = initial_ordinal
            .checked_add(offset)
            .expect("fixture ordinal");
        lines.push(rollout_line(
            ordinal,
            RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
        ));
    }
    fs::write(path.as_path(), format!("{}\n", lines.join("\n"))).expect("write rollout");
    path
}

fn rollout_line(ordinal: u64, item: RolloutItem) -> String {
    serde_json::to_string(&RolloutLine {
        timestamp: "2026-07-16T00:00:00.000Z".to_string(),
        ordinal: Some(ordinal),
        item,
    })
    .expect("serialize rollout line")
}

fn history_position(
    path: &Path,
    thread_id: ThreadId,
    end_ordinal_exclusive: u64,
) -> HistoryPosition {
    HistoryPosition {
        thread_id,
        end_ordinal_exclusive,
        end_byte_offset: rollout_end_byte_offset(path, end_ordinal_exclusive),
    }
}

fn rollout_end_byte_offset(path: &Path, end_ordinal_exclusive: u64) -> u64 {
    let bytes = fs::read(path).expect("read rollout");
    let end_byte_offset = bytes
        .split_inclusive(|byte| *byte == b'\n')
        .take_while(|line| {
            serde_json::from_slice::<RolloutLine>(line)
                .expect("parse rollout fixture")
                .ordinal
                .expect("paginated rollout ordinal")
                < end_ordinal_exclusive
        })
        .map(<[u8]>::len)
        .sum::<usize>();
    u64::try_from(end_byte_offset).expect("rollout byte offset fits u64")
}

fn unchecked_history_position(thread_id: ThreadId, end_ordinal_exclusive: u64) -> HistoryPosition {
    HistoryPosition {
        thread_id,
        end_ordinal_exclusive,
        end_byte_offset: 0,
    }
}
