use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::UserMessageEvent;
use pretty_assertions::assert_eq;
use std::fs;

use tempfile::TempDir;
use tempfile::tempdir;
use uuid::Uuid;

use super::last_complete_rollout_envelope_offset_at_or_before;
use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::DeleteThreadParams;
use crate::ForkBoundary;
use crate::LoadThreadHistoryParams;
use crate::PrepareForkParams;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::local::LocalThreadStore;
use crate::local::test_support::compress_session_file;
use crate::local::test_support::set_history_base_in_session_file;
use crate::local::test_support::test_config;
use crate::local::test_support::write_session_file_with_fork;
use codex_state::ThreadMetadataBuilder;

fn valid_rollout_line(label: &str) -> Vec<u8> {
    serde_json::to_vec(&RolloutLine {
        timestamp: "2025-01-03T12:00:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: label.to_string(),
            ..Default::default()
        })),
    })
    .expect("serialize valid rollout line")
}

#[tokio::test]
async fn freezes_before_trailing_partial_record() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("rollout.jsonl");
    let mut contents = valid_rollout_line("complete");
    contents.push(b'\n');
    contents.extend_from_slice(b"{\"partial\":");
    fs::write(&path, &contents).expect("write rollout");

    let cutoff = last_complete_rollout_envelope_offset_at_or_before(
        &path,
        fs::metadata(&path).expect("rollout metadata").len(),
    )
    .await
    .expect("cutoff should resolve");
    assert_eq!(cutoff, valid_rollout_line("complete").len() as u64 + 1);
}

#[tokio::test]
async fn freezes_before_newline_terminated_malformed_record() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("rollout.jsonl");
    let mut contents = valid_rollout_line("complete");
    contents.push(b'\n');
    contents.extend_from_slice(b"{\"malformed\":true}\n");
    fs::write(&path, &contents).expect("write rollout");

    let cutoff = last_complete_rollout_envelope_offset_at_or_before(
        &path,
        fs::metadata(&path).expect("rollout metadata").len(),
    )
    .await
    .expect("cutoff should resolve");
    assert_eq!(cutoff, valid_rollout_line("complete").len() as u64 + 1);
}

#[tokio::test]
async fn accepts_complete_envelope_with_unknown_nested_payload_schema() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("rollout.jsonl");
    let mut contents = valid_rollout_line("complete");
    contents.push(b'\n');
    contents.extend_from_slice(
        br#"{"timestamp":"2025-01-03T12:00:01Z","type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"primary":{"used_percent":{"future_shape":[1,2,3]}}}}}"#,
    );
    contents.push(b'\n');
    fs::write(&path, &contents).expect("write rollout");

    let cutoff = last_complete_rollout_envelope_offset_at_or_before(
        &path,
        fs::metadata(&path).expect("rollout metadata").len(),
    )
    .await
    .expect("cutoff should accept an unknown nested payload schema");
    assert_eq!(cutoff, contents.len() as u64);
}

#[tokio::test]
async fn rejects_non_object_envelope_payloads() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("rollout.jsonl");
    let complete = valid_rollout_line("complete");
    for payload in ["null", "[]"] {
        let mut contents = complete.clone();
        contents.push(b'\n');
        contents.extend_from_slice(
            format!(
                r#"{{"timestamp":"2025-01-03T12:00:01Z","type":"event_msg","payload":{payload}}}"#
            )
            .as_bytes(),
        );
        contents.push(b'\n');
        fs::write(&path, &contents).expect("write rollout");

        let cutoff = last_complete_rollout_envelope_offset_at_or_before(
            &path,
            fs::metadata(&path).expect("rollout metadata").len(),
        )
        .await
        .expect("cutoff should resolve");
        assert_eq!(cutoff, complete.len() as u64 + 1);
    }
}

#[tokio::test]
async fn keeps_all_complete_records_when_tail_is_complete() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("rollout.jsonl");
    let mut contents = valid_rollout_line("first");
    contents.push(b'\n');
    contents.extend_from_slice(valid_rollout_line("second").as_slice());
    contents.push(b'\n');
    fs::write(&path, &contents).expect("write rollout");

    let cutoff = last_complete_rollout_envelope_offset_at_or_before(
        &path,
        fs::metadata(&path).expect("rollout metadata").len(),
    )
    .await
    .expect("cutoff should resolve");
    assert_eq!(cutoff, contents.len() as u64);
}

#[tokio::test]
async fn legacy_latest_prepare_freezes_cutoff_and_logical_history()
-> Result<(), Box<dyn std::error::Error>> {
    let home = TempDir::new()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let source_uuid = Uuid::from_u128(701);
    let source_id = ThreadId::from_string(&source_uuid.to_string())?;
    let source_path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T12-00-00",
        source_uuid,
        "source before fork",
        Some("test-provider"),
        None,
        ThreadHistoryMode::Legacy,
    )?;
    let source_cutoff = fs::metadata(&source_path)?.len();

    let prepared = store
        .prepare_fork(PrepareForkParams {
            thread_id: source_id,
            boundary: ForkBoundary::Latest,
        })
        .await
        .expect("prepare legacy reference fork");
    let history_base = prepared.history_base.expect("legacy history base");
    assert_eq!(history_base.thread_id, source_id);
    assert_eq!(history_base.end_ordinal_exclusive, 0);
    assert_eq!(history_base.end_byte_offset, source_cutoff);
    assert!(
        !prepared
            .model_context
            .iter()
            .filter_map(user_message_text)
            .any(|text| text == "source after fork")
    );
    // The prepared fork owns the source filesystem lock until the child metadata is durable.
    // Release it here to model the post-durability read path.
    drop(prepared);

    append_user_message(&source_path, "source after fork").await?;
    let child_uuid = Uuid::from_u128(702);
    let child_id = ThreadId::from_string(&child_uuid.to_string())?;
    let child_path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T12-01-00",
        child_uuid,
        "child local turn",
        Some("test-provider"),
        Some(source_uuid),
        ThreadHistoryMode::Legacy,
    )?;
    set_history_base(&child_path, history_base)?;

    let child = store
        .read_thread(crate::ReadThreadParams {
            thread_id: child_id,
            include_archived: false,
            include_history: true,
        })
        .await?;
    let history = child.history.expect("logical child history");
    assert_eq!(
        history
            .items
            .iter()
            .filter_map(user_message_text)
            .collect::<Vec<_>>(),
        vec!["source before fork", "child local turn"]
    );
    assert!(matches!(
        history.items.first(),
        Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == child_id
    ));
    assert_eq!(
        history
            .items
            .iter()
            .filter(|item| matches!(item, RolloutItem::SessionMeta(_)))
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn legacy_latest_rejects_external_compressed_reference_without_materializing()
-> Result<(), Box<dyn std::error::Error>> {
    let home = TempDir::new()?;
    let external = TempDir::new()?;
    let config = test_config(home.path());
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let child_uuid = Uuid::from_u128(709);
    let child_id = ThreadId::from_string(&child_uuid.to_string())?;
    let source_id = ThreadId::from_string(&Uuid::from_u128(710).to_string())?;
    let plain_path = write_session_file_with_fork(
        external.path(),
        external.path().join("sessions/2025/01/03"),
        "2025-01-03T12-09-00",
        child_uuid,
        "external child",
        Some("test-provider"),
        Some(Uuid::from_u128(710)),
        ThreadHistoryMode::Legacy,
    )?;
    set_history_base_in_session_file(
        &plain_path,
        &HistoryPosition {
            thread_id: source_id,
            end_ordinal_exclusive: 0,
            end_byte_offset: 1,
        },
    )?;
    let compressed_path = compress_session_file(&plain_path)?;
    let compressed_before = fs::read(&compressed_path)?;

    let mut builder =
        ThreadMetadataBuilder::new(child_id, plain_path.clone(), Utc::now(), SessionSource::Cli);
    builder.history_mode = ThreadHistoryMode::Legacy;
    builder.model_provider = Some(config.default_model_provider_id.clone());
    builder.cwd = home.path().to_path_buf();
    let mut metadata = builder.build(config.default_model_provider_id.as_str());
    metadata.preview = Some("external child".to_string());
    runtime.upsert_thread(&metadata).await?;

    let store = LocalThreadStore::new(config, Some(runtime));
    let error = store
        .prepare_fork(PrepareForkParams {
            thread_id: child_id,
            boundary: ForkBoundary::Latest,
        })
        .await
        .expect_err("external reference must not be materialized for a fork");
    assert!(matches!(
        error,
        ThreadStoreError::ThreadNotFound { .. } | ThreadStoreError::InvalidRequest { .. }
    ));
    assert_eq!(fs::read(&compressed_path)?, compressed_before);
    assert!(!plain_path.exists());
    Ok(())
}

#[tokio::test]
async fn external_legacy_root_uses_copied_history_fallback_for_pathless_latest_fork()
-> Result<(), Box<dyn std::error::Error>> {
    let home = TempDir::new()?;
    let external = TempDir::new()?;
    let config = test_config(home.path());
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let source_uuid = Uuid::from_u128(711);
    let source_id = ThreadId::from_string(&source_uuid.to_string())?;
    let source_path = write_session_file_with_fork(
        external.path(),
        external.path().join("sessions/2025/01/03"),
        "2025-01-03T12-11-00",
        source_uuid,
        "external legacy root",
        Some("test-provider"),
        None,
        ThreadHistoryMode::Legacy,
    )?;
    let mut builder = ThreadMetadataBuilder::new(
        source_id,
        source_path.clone(),
        Utc::now(),
        SessionSource::Cli,
    );
    builder.history_mode = ThreadHistoryMode::Legacy;
    builder.model_provider = Some(config.default_model_provider_id.clone());
    builder.cwd = home.path().to_path_buf();
    runtime
        .upsert_thread(&builder.build(config.default_model_provider_id.as_str()))
        .await?;

    let store = LocalThreadStore::new(config, Some(runtime));
    let error = store
        .prepare_fork(PrepareForkParams {
            thread_id: source_id,
            boundary: ForkBoundary::Latest,
        })
        .await
        .expect_err("external legacy root must not enter reference preparation");
    assert!(matches!(error, ThreadStoreError::Unsupported { .. }));
    assert!(source_path.exists());
    Ok(())
}

#[tokio::test]
async fn active_legacy_prepare_reuses_recorder_lock_and_freezes_cutoff()
-> Result<(), Box<dyn std::error::Error>> {
    let home = TempDir::new()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(708);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    store
        .create_thread(CreateThreadParams {
            session_id: thread_id.into(),
            thread_id,
            extra_config: None,
            forked_from_id: None,
            parent_thread_id: None,
            source: SessionSource::Cli,
            thread_source: None,
            originator: "test-originator".to_string(),
            base_instructions: Default::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: None,
            history_mode: ThreadHistoryMode::Legacy,
            history_base: None,
            preview: None,
            first_user_message: None,
            subagent_history_start_ordinal: None,
            initial_window_id: uuid::Uuid::now_v7().to_string(),
            metadata: crate::ThreadPersistenceMetadata {
                cwd: Some(home.path().to_path_buf()),
                model_provider: "test-provider".to_string(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        })
        .await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![RolloutItem::EventMsg(EventMsg::UserMessage(
                UserMessageEvent {
                    message: "active source before fork".to_string(),
                    ..Default::default()
                },
            ))],
        })
        .await?;
    store.flush_thread(thread_id).await?;
    let source_path = store.live_rollout_path(thread_id).await?;
    let source_cutoff = fs::metadata(&source_path)?.len();
    let prepared = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.prepare_fork(PrepareForkParams {
            thread_id,
            boundary: ForkBoundary::Latest,
        }),
    )
    .await??;
    let history_base = prepared.history_base.expect("active legacy history base");
    assert_eq!(history_base.end_byte_offset, source_cutoff);
    assert!(
        prepared
            .model_context
            .iter()
            .any(|item| { user_message_text(item) == Some("active source before fork") })
    );
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![RolloutItem::EventMsg(EventMsg::UserMessage(
                UserMessageEvent {
                    message: "active source after fork".to_string(),
                    ..Default::default()
                },
            ))],
        })
        .await?;
    store.flush_thread(thread_id).await?;
    assert!(
        !prepared
            .model_context
            .iter()
            .any(|item| { user_message_text(item) == Some("active source after fork") })
    );
    drop(prepared);
    store.shutdown_thread(thread_id).await?;
    Ok(())
}

#[tokio::test]
async fn legacy_reference_holds_cross_store_delete_lock_and_then_index_protection()
-> Result<(), Box<dyn std::error::Error>> {
    let home = TempDir::new()?;
    let source_uuid = Uuid::from_u128(703);
    let source_id = ThreadId::from_string(&source_uuid.to_string())?;
    let source_path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T12-00-00",
        source_uuid,
        "source",
        Some("test-provider"),
        None,
        ThreadHistoryMode::Legacy,
    )?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let prepared = store
        .prepare_fork(PrepareForkParams {
            thread_id: source_id,
            boundary: ForkBoundary::Latest,
        })
        .await
        .expect("prepare legacy reference fork");
    let other_store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let lock_error = other_store
        .delete_thread(DeleteThreadParams {
            thread_id: source_id,
        })
        .await
        .expect_err("cross-process writer lock should protect preparation");
    assert!(matches!(lock_error, ThreadStoreError::Conflict { .. }));

    let child_uuid = Uuid::from_u128(704);
    let child_path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T12-01-00",
        child_uuid,
        "child",
        Some("test-provider"),
        Some(source_uuid),
        ThreadHistoryMode::Legacy,
    )?;
    set_history_base(
        &child_path,
        HistoryPosition {
            thread_id: source_id,
            end_ordinal_exclusive: 0,
            end_byte_offset: fs::metadata(&source_path)?.len(),
        },
    )?;
    drop(prepared);
    let reference_error = other_store
        .delete_thread(DeleteThreadParams {
            thread_id: source_id,
        })
        .await
        .expect_err("durable legacy reference should protect source");
    assert!(
        reference_error
            .to_string()
            .contains("forked history still references")
    );
    Ok(())
}

#[tokio::test]
async fn nested_legacy_reference_replay_and_model_context_are_cutoff_aware()
-> Result<(), Box<dyn std::error::Error>> {
    let home = TempDir::new()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root_uuid = Uuid::from_u128(705);
    let middle_uuid = Uuid::from_u128(706);
    let child_uuid = Uuid::from_u128(707);
    let root_id = ThreadId::from_string(&root_uuid.to_string())?;
    let middle_id = ThreadId::from_string(&middle_uuid.to_string())?;
    let child_id = ThreadId::from_string(&child_uuid.to_string())?;
    let root_path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T12-00-00",
        root_uuid,
        "root message",
        Some("test-provider"),
        None,
        ThreadHistoryMode::Legacy,
    )?;
    let root_cutoff = fs::metadata(&root_path)?.len();
    let middle_path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T12-01-00",
        middle_uuid,
        "middle message",
        Some("test-provider"),
        Some(root_uuid),
        ThreadHistoryMode::Legacy,
    )?;
    set_history_base(
        &middle_path,
        HistoryPosition {
            thread_id: root_id,
            end_ordinal_exclusive: 0,
            end_byte_offset: root_cutoff,
        },
    )?;
    let middle_cutoff = fs::metadata(&middle_path)?.len();
    let child_path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T12-02-00",
        child_uuid,
        "child message",
        Some("test-provider"),
        Some(middle_uuid),
        ThreadHistoryMode::Legacy,
    )?;
    set_history_base(
        &child_path,
        HistoryPosition {
            thread_id: middle_id,
            end_ordinal_exclusive: 0,
            end_byte_offset: middle_cutoff,
        },
    )?;
    append_user_message(&root_path, "root after fork").await?;
    append_user_message(&middle_path, "middle after fork").await?;
    compress_rollout(&root_path)?;
    compress_rollout(&child_path)?;
    assert!(
        !root_path.exists(),
        "compressed ancestor should replace plain file"
    );

    let thread = store
        .read_thread(crate::ReadThreadParams {
            thread_id: child_id,
            include_archived: false,
            include_history: true,
        })
        .await?;
    let history = thread.history.expect("nested logical history");
    assert_eq!(
        history
            .items
            .iter()
            .filter_map(user_message_text)
            .collect::<Vec<_>>(),
        vec!["root message", "middle message", "child message"]
    );
    let model_context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id: child_id,
            include_archived: false,
        })
        .await?;
    assert!(
        model_context
            .items
            .iter()
            .filter_map(user_message_text)
            .any(|text| text == "root message")
    );
    assert!(
        !model_context
            .items
            .iter()
            .filter_map(user_message_text)
            .any(|text| text == "middle after fork")
    );
    Ok(())
}

async fn append_user_message(path: &Path, message: &str) -> Result<(), Box<dyn std::error::Error>> {
    let line = serde_json::to_string(&codex_protocol::protocol::RolloutLine {
        timestamp: "2025-01-03T12:03:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: message.to_string(),
            ..Default::default()
        })),
    })?;
    let mut file = OpenOptions::new().append(true).open(path)?;
    writeln!(file, "{line}")?;
    Ok(())
}

fn set_history_base(
    path: &Path,
    history_base: HistoryPosition,
) -> Result<(), Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(path)?;
    let mut lines = contents.lines();
    let mut first: serde_json::Value = serde_json::from_str(lines.next().expect("meta line"))?;
    first["payload"]["history_base"] = serde_json::to_value(history_base)?;
    let mut rewritten = serde_json::to_string(&first)?;
    for line in lines {
        rewritten.push('\n');
        rewritten.push_str(line);
    }
    rewritten.push('\n');
    fs::write(path, rewritten)?;
    Ok(())
}

fn user_message_text(item: &RolloutItem) -> Option<&str> {
    match item {
        RolloutItem::EventMsg(EventMsg::UserMessage(user)) => Some(user.message.as_str()),
        _ => None,
    }
}

fn compress_rollout(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let compressed_path = path.with_extension("jsonl.zst");
    let input = fs::File::open(path)?;
    let output = fs::File::create(compressed_path)?;
    let mut encoder = zstd::stream::write::Encoder::new(output, 3)?;
    let mut input = std::io::BufReader::new(input);
    std::io::copy(&mut input, &mut encoder)?;
    encoder.finish()?;
    fs::remove_file(path)?;
    Ok(())
}
