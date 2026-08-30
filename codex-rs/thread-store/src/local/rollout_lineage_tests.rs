use std::fs;
use std::path::Path;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use super::RolloutLineageSegment;
use crate::ArchiveThreadParams;
use crate::DeleteThreadParams;
use crate::ThreadStore;

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
                rollout_id: root,
                rollout_path: root_path.clone(),
                start_ordinal: 1,
                end: Some(root_end),
            },
            RolloutLineageSegment {
                rollout_id: middle,
                rollout_path: middle_path.clone(),
                start_ordinal: 5,
                end: Some(middle_end),
            },
            RolloutLineageSegment {
                rollout_id: child,
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
                rollout_id: root,
                rollout_path: root_path.clone(),
                start_ordinal: 1,
                end: Some(root_end),
            },
            RolloutLineageSegment {
                rollout_id: child,
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
    write_rollout(
        home.path(),
        cycle_a,
        Some(unchecked_history_position(
            cycle_b, /*end_ordinal_exclusive*/ 1,
        )),
        /*next_ordinal*/ 2,
    );
    write_rollout(
        home.path(),
        cycle_b,
        Some(unchecked_history_position(
            cycle_a, /*end_ordinal_exclusive*/ 1,
        )),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(&store, cycle_a, "cycle detected").await;

    let root = ThreadId::default();
    let invalid_child = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 2,
    );
    write_rollout(
        home.path(),
        invalid_child,
        Some(HistoryPosition {
            thread_id: root,
            end_ordinal_exclusive: 2,
            end_byte_offset: fs::metadata(root_path).expect("root metadata").len() + 1,
        }),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(
        &store,
        invalid_child,
        "cutoff byte offset is past the source rollout",
    )
    .await;
}

#[tokio::test]
async fn reference_lineage_rejects_mismatched_leaf_and_ancestor_metadata() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let root = ThreadId::default();
    let child = ThreadId::default();
    let unrelated = ThreadId::default();
    let root_path = write_rollout_with_meta_id(home.path(), root, unrelated, None);
    write_rollout(
        home.path(),
        child,
        Some(history_position(
            root_path.as_path(),
            root,
            /*end_ordinal_exclusive*/ 1,
        )),
        /*next_ordinal*/ 2,
    );
    assert_invalid_reference_lineage(&store, child, "belongs to another thread").await;

    let mismatched_leaf = ThreadId::default();
    write_rollout_with_meta_id(home.path(), mismatched_leaf, unrelated, None);
    assert_invalid_reference_lineage(&store, mismatched_leaf, "belongs to another thread").await;
}

#[tokio::test]
async fn locked_reference_lineage_retains_ancestor_guard_across_stores() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let store = LocalThreadStore::new(config.clone(), /*state_db*/ None);
    let maintenance_store = LocalThreadStore::new(config, /*state_db*/ None);
    let root_logical_thread_id = ThreadId::default();
    let root_rollout_id = ThreadId::default();
    let child = ThreadId::default();
    let root_path = write_reverted_rollout(
        home.path(),
        root_logical_thread_id,
        root_rollout_id,
        None,
        /*next_ordinal*/ 2,
    );
    write_rollout(
        home.path(),
        child,
        Some(history_position(
            root_path.as_path(),
            root_rollout_id,
            /*end_ordinal_exclusive*/ 2,
        )),
        /*next_ordinal*/ 2,
    );

    let source_guard = store
        .writer_lock_coordinator
        .acquire(child)
        .expect("hold source writer guard");
    let (lineage, ancestor_guards) = store
        .resolve_rollout_lineage_for_reference_locked_with_source_guard(child, source_guard)
        .await
        .expect("resolve locked lineage");
    assert_eq!(lineage.segments().len(), 2);
    assert_eq!(ancestor_guards.len(), 1);
    let err = maintenance_store
        .writer_lock_coordinator
        .acquire(root_logical_thread_id)
        .expect_err("ancestor guard must block another store's maintenance");
    assert!(matches!(err, crate::ThreadStoreError::Conflict { .. }));
    // The immutable rollout ID intentionally remains unlocked: it is a lineage address, not the
    // mutation identity. Archive/delete/revert/compression all use the stable logical ID.
    assert!(
        maintenance_store
            .writer_lock_coordinator
            .acquire(root_rollout_id)
            .is_ok()
    );
    let archive_err = maintenance_store
        .archive_thread(ArchiveThreadParams {
            thread_id: root_logical_thread_id,
        })
        .await
        .expect_err("ancestor guard must prevent an archive move");
    assert!(matches!(
        archive_err,
        crate::ThreadStoreError::Conflict { .. }
    ));
    let delete_err = maintenance_store
        .delete_thread(DeleteThreadParams {
            thread_id: root_logical_thread_id,
        })
        .await
        .expect_err("ancestor guard must prevent deletion");
    assert!(matches!(
        delete_err,
        crate::ThreadStoreError::Conflict { .. }
    ));
    drop(ancestor_guards);
    assert!(
        maintenance_store
            .writer_lock_coordinator
            .acquire(root_logical_thread_id)
            .is_ok()
    );
}

#[tokio::test]
async fn locked_reference_ancestor_guard_blocks_unarchive_of_reverted_rollout() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let store = LocalThreadStore::new(config.clone(), /*state_db*/ None);
    let maintenance_store = LocalThreadStore::new(config, /*state_db*/ None);
    let ancestor_thread_id = ThreadId::default();
    let ancestor_rollout_id = ThreadId::default();
    let child = ThreadId::default();
    let active_path = write_reverted_rollout(
        home.path(),
        ancestor_thread_id,
        ancestor_rollout_id,
        None,
        /*next_ordinal*/ 2,
    );
    let archived_dir = home.path().join("archived_sessions");
    fs::create_dir_all(&archived_dir).expect("create archived directory");
    let archived_path = archived_dir.join(active_path.file_name().expect("rollout file name"));
    fs::rename(&active_path, &archived_path).expect("archive ancestor fixture");
    write_rollout(
        home.path(),
        child,
        Some(history_position(
            archived_path.as_path(),
            ancestor_rollout_id,
            /*end_ordinal_exclusive*/ 2,
        )),
        /*next_ordinal*/ 2,
    );

    let source_guard = store
        .writer_lock_coordinator
        .acquire(child)
        .expect("hold child source guard");
    let (_lineage, ancestor_guards) = store
        .resolve_rollout_lineage_for_reference_locked_with_source_guard(child, source_guard)
        .await
        .expect("resolve locked archived lineage");
    let err = maintenance_store
        .unarchive_thread(ArchiveThreadParams {
            thread_id: ancestor_thread_id,
        })
        .await
        .expect_err("ancestor guard must prevent unarchive move");
    assert!(matches!(err, crate::ThreadStoreError::Conflict { .. }));
    drop(ancestor_guards);
}

#[tokio::test]
async fn resolves_reverted_rollout_with_distinct_logical_and_immutable_ids() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let logical_thread = ThreadId::default();
    let replacement_rollout = ThreadId::default();
    let root_path = write_rollout(home.path(), root, None, /*next_ordinal*/ 2);
    write_reverted_rollout(
        home.path(),
        logical_thread,
        replacement_rollout,
        Some(history_position(
            root_path.as_path(),
            root,
            /*end_ordinal_exclusive*/ 2,
        )),
        /*next_ordinal*/ 3,
    );

    let lineage = store
        .resolve_rollout_lineage(logical_thread)
        .await
        .expect("resolve reverted rollout lineage");
    assert_eq!(lineage.segments()[0].rollout_id(), root);
    assert_eq!(lineage.segments()[1].rollout_id(), replacement_rollout);
}

#[tokio::test]
async fn rejects_reverted_filename_with_mismatched_logical_thread_id() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let child = ThreadId::default();
    let unrelated = ThreadId::default();
    let root_path = write_reverted_rollout(home.path(), unrelated, root, None, 2);
    write_rollout(
        home.path(),
        child,
        Some(history_position(
            root_path.as_path(),
            root,
            /*end_ordinal_exclusive*/ 2,
        )),
        /*next_ordinal*/ 2,
    );
    assert_invalid_reference_lineage(&store, child, "filename or metadata belongs").await;
}

async fn assert_invalid_lineage(store: &LocalThreadStore, thread_id: ThreadId, detail: &str) {
    let err = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect_err("lineage should be invalid");
    assert!(err.to_string().contains(detail), "{err}");
}

async fn assert_invalid_reference_lineage(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    detail: &str,
) {
    let err = store
        .resolve_rollout_lineage_for_reference(thread_id)
        .await
        .expect_err("reference lineage should be invalid");
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

fn write_rollout_with_meta_id(
    home: &Path,
    path_thread_id: ThreadId,
    meta_thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
) -> std::path::PathBuf {
    let directory = home.join("sessions/2026/07/16");
    fs::create_dir_all(directory.as_path()).expect("create rollout directory");
    let path = directory.join(format!(
        "rollout-2026-07-16T00-00-00-{path_thread_id}.jsonl"
    ));
    let initial_ordinal = history_base.map_or(0, |base| base.end_ordinal_exclusive);
    let line = rollout_line(
        initial_ordinal,
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: meta_thread_id.into(),
                id: meta_thread_id,
                history_mode: ThreadHistoryMode::Paginated,
                history_base,
                ..SessionMeta::default()
            },
            git: None,
        }),
    );
    fs::write(path.as_path(), format!("{line}\n")).expect("write rollout");
    path
}

fn write_reverted_rollout(
    home: &Path,
    logical_thread_id: ThreadId,
    rollout_id: ThreadId,
    history_base: Option<HistoryPosition>,
    next_ordinal: u64,
) -> std::path::PathBuf {
    let directory = home.join("sessions/2026/07/16");
    fs::create_dir_all(directory.as_path()).expect("create rollout directory");
    let path = directory.join(format!(
        "rollout-2026-07-16T00-00-00-{logical_thread_id}_{rollout_id}.jsonl"
    ));
    let initial_ordinal = history_base.map_or(0, |base| base.end_ordinal_exclusive);
    let mut lines = vec![rollout_line(
        initial_ordinal,
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: logical_thread_id.into(),
                id: logical_thread_id,
                history_mode: ThreadHistoryMode::Paginated,
                history_base,
                ..SessionMeta::default()
            },
            git: None,
        }),
    )];
    for offset in 1..next_ordinal {
        lines.push(rollout_line(
            initial_ordinal
                .checked_add(offset)
                .expect("fixture ordinal"),
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
