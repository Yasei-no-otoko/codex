use anyhow::Result;
use codex_core::CodexThread;
use codex_core::config::AgentRoleConfig;
use codex_features::Feature;
use codex_protocol::ThreadId;
use codex_protocol::models::PermissionProfile;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::ThreadHistoryMode;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call_with_namespace;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_once_match;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep;

const COLLABORATION_NAMESPACE: &str = "collaboration";
const SPAWN_CALL_ID: &str = "spawn-worker";
const FOLLOWUP_CALL_ID: &str = "followup-worker";
const INITIAL_PROMPT: &str = "spawn a durable worker";
const INITIAL_TASK: &str = "inspect the repository";
const FOLLOWUP_PROMPT: &str = "continue the durable worker";
const FOLLOWUP_TASK: &str = "inspect the tests too";
const FIRST_HISTORY_PROMPT: &str = "record the first legacy context";
const FIRST_HISTORY_REPLY: &str = "first legacy context recorded";
const FIRST_HISTORY_SUMMARY: &str = "FIRST_LEGACY_SUMMARY";
const SECOND_HISTORY_PROMPT: &str = "record the second legacy context";
const SECOND_HISTORY_REPLY: &str = "second legacy context recorded";
const SECOND_HISTORY_SUMMARY: &str = "SECOND_LEGACY_SUMMARY";
const ROLE_NAME: &str = "durable_worker";
const ROLE_MODEL: &str = "gpt-5.4";
const ROLE_MODEL_PROVIDER_ID: &str = "mock";
const ROLE_DEVELOPER_INSTRUCTIONS: &str = "Keep the durable worker role configuration.";
const SUBAGENT_DEVELOPER_INSTRUCTIONS: &str = "Use the default durable worker instructions.";

fn decoded_body(request: &wiremock::Request) -> Option<Vec<u8>> {
    let is_zstd = request
        .headers
        .get("content-encoding")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|entry| entry.trim().eq_ignore_ascii_case("zstd"))
        });
    if is_zstd {
        zstd::stream::decode_all(std::io::Cursor::new(&request.body)).ok()
    } else {
        Some(request.body.clone())
    }
}

fn body_contains(request: &wiremock::Request, text: &str) -> bool {
    decoded_body(request)
        .and_then(|body| String::from_utf8(body).ok())
        .is_some_and(|body| body.contains(text))
}

fn request_has_input_type(request: &wiremock::Request, input_type: &str) -> bool {
    decoded_body(request)
        .and_then(|body| serde_json::from_slice::<Value>(&body).ok())
        .and_then(|body| body.get("input").and_then(Value::as_array).cloned())
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("type").and_then(Value::as_str) == Some(input_type))
        })
}

fn enable_multi_agent_v2(config: &mut codex_core::config::Config) {
    config
        .features
        .enable(Feature::Collab)
        .expect("test config should allow feature update");
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config.multi_agent_v2.subagent_developer_instructions =
        Some(SUBAGENT_DEVELOPER_INSTRUCTIONS.to_string());
}

fn enable_multi_agent_v2_with_local_compaction(config: &mut codex_core::config::Config) {
    enable_multi_agent_v2(config);
    config.model_provider.name = "OpenAI-compatible test provider".to_string();
}

fn configure_multi_agent_v2_with_role(
    config: &mut codex_core::config::Config,
    model_provider_base_url: &str,
) {
    enable_multi_agent_v2(config);
    let role_path = config.codex_home.join("durable-worker-role.toml");
    std::fs::write(
        &role_path,
        format!(
            "model = \"{ROLE_MODEL}\"\nmodel_reasoning_effort = \"high\"\ndeveloper_instructions = \"{ROLE_DEVELOPER_INSTRUCTIONS}\"\nsandbox_mode = \"read-only\"\nmodel_provider = \"mock\"\n\n[model_providers.mock]\nname = \"mock\"\nbase_url = \"{model_provider_base_url}\"\nenv_key = \"PATH\"\nwire_api = \"responses\"\n"
        ),
    )
    .expect("write durable worker role config");
    config.agent_roles.insert(
        ROLE_NAME.to_string(),
        AgentRoleConfig {
            description: Some("Durable worker role".to_string()),
            config_file: Some(role_path.to_path_buf()),
            nickname_candidates: None,
        },
    );
}

async fn wait_for_spawned_worker(
    test: &TestCodex,
    root_thread_id: ThreadId,
) -> Result<(ThreadId, Arc<CodexThread>)> {
    let discovery_deadline = Instant::now() + Duration::from_secs(10);
    let thread_id = loop {
        if let Some(thread_id) = test
            .thread_manager
            .list_thread_ids()
            .await
            .into_iter()
            .find(|thread_id| *thread_id != root_thread_id)
        {
            break thread_id;
        }
        if Instant::now() >= discovery_deadline {
            anyhow::bail!("timed out waiting for spawned worker");
        }
        sleep(Duration::from_millis(10)).await;
    };
    let thread = test.thread_manager.get_thread(thread_id).await?;
    let completion_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if matches!(thread.agent_status().await, AgentStatus::Completed(_)) {
            return Ok((thread_id, thread));
        }
        if Instant::now() >= completion_deadline {
            anyhow::bail!("timed out waiting for worker completion");
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn submit_compact(thread: &Arc<CodexThread>) -> Result<()> {
    let submission_id = thread.submit(Op::Compact).await?;
    let completion = async {
        loop {
            let event = thread.next_event().await?;
            if event.id != submission_id {
                continue;
            }
            match event.msg {
                EventMsg::TurnComplete(_) => break,
                EventMsg::Error(error) => {
                    anyhow::bail!("compaction failed: {}", error.message);
                }
                _ => {}
            }
        }
        Ok::<(), anyhow::Error>(())
    };
    tokio::time::timeout(Duration::from_secs(10), completion)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "timed out waiting for compaction completion for submission {submission_id:?}"
            )
        })??;
    Ok(())
}

async fn mount_message_response(
    server: &wiremock::MockServer,
    request_text: &'static str,
    id: &'static str,
    message: &'static str,
) {
    let response_id = format!("resp-{id}");
    let message_id = format!("msg-{id}");
    mount_sse_once_match(
        server,
        move |request: &wiremock::Request| body_contains(request, request_text),
        sse(vec![
            ev_response_created(&response_id),
            ev_assistant_message(&message_id, message),
            ev_completed(&response_id),
        ]),
    )
    .await;
}

fn compacted_checkpoints(path: &Path) -> Result<Vec<(String, Option<u64>)>> {
    Ok(fs::read_to_string(path)?
        .lines()
        .map(serde_json::from_str::<RolloutLine>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter_map(|line| match line.item {
            RolloutItem::Compacted(item) => Some((item.message, item.window_number)),
            _ => None,
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_history_spawn_from_compacted_legacy_parent_is_bounded_and_resumable() -> Result<()> {
    let server = start_mock_server().await;
    mount_message_response(
        &server,
        FIRST_HISTORY_PROMPT,
        "history-1",
        FIRST_HISTORY_REPLY,
    )
    .await;
    mount_message_response(
        &server,
        FIRST_HISTORY_REPLY,
        "compact-1",
        FIRST_HISTORY_SUMMARY,
    )
    .await;
    mount_message_response(
        &server,
        SECOND_HISTORY_PROMPT,
        "history-2",
        SECOND_HISTORY_REPLY,
    )
    .await;
    mount_message_response(
        &server,
        SECOND_HISTORY_REPLY,
        "compact-2",
        SECOND_HISTORY_SUMMARY,
    )
    .await;
    let spawn_args = serde_json::to_string(&json!({
        "message": INITIAL_TASK,
        "task_name": "worker",
        "fork_turns": "all",
    }))?;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, INITIAL_PROMPT),
        sse(vec![
            ev_response_created("resp-bounded-spawn"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                COLLABORATION_NAMESPACE,
                "spawn_agent",
                &spawn_args,
            ),
            ev_completed("resp-bounded-spawn"),
        ]),
    )
    .await;
    let child_request = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            request_has_input_type(request, "agent_message") && body_contains(request, INITIAL_TASK)
        },
        sse(vec![
            ev_response_created("resp-bounded-child"),
            ev_assistant_message("msg-bounded-child", "bounded child complete"),
            ev_completed("resp-bounded-child"),
        ]),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, SPAWN_CALL_ID)
                && !request_has_input_type(request, "agent_message")
        },
        sse(vec![
            ev_response_created("resp-bounded-root-complete"),
            ev_assistant_message("msg-bounded-root-complete", "worker spawned"),
            ev_completed("resp-bounded-root-complete"),
        ]),
    )
    .await;

    let mut builder = test_codex()
        .with_history_mode(ThreadHistoryMode::Legacy)
        .with_config(enable_multi_agent_v2_with_local_compaction);
    let test = builder.build_with_auto_env(&server).await?;
    let root_thread_id = test.session_configured.thread_id;
    test.submit_turn(FIRST_HISTORY_PROMPT).await?;
    submit_compact(&test.codex).await?;
    test.submit_turn(SECOND_HISTORY_PROMPT).await?;
    submit_compact(&test.codex).await?;
    test.submit_turn(INITIAL_PROMPT).await?;

    let (worker_id, worker) = wait_for_spawned_worker(&test, root_thread_id).await?;
    let worker_id = worker_id.to_string();
    let request = child_request
        .requests()
        .into_iter()
        .find(|request| request.header("thread-id").as_deref() == Some(worker_id.as_str()))
        .expect("child inference request");
    assert!(request.body_contains_text(SECOND_HISTORY_SUMMARY));
    assert!(request.body_contains_text(INITIAL_PROMPT));
    assert!(!request.body_contains_text(FIRST_HISTORY_SUMMARY));
    assert!(
        request
            .header("x-codex-window-id")
            .is_some_and(|window_id| window_id.ends_with(":2"))
    );
    worker.flush_rollout().await?;
    let path = worker.rollout_path().expect("child rollout").to_path_buf();
    let checkpoints = compacted_checkpoints(&path)?;
    assert_eq!(checkpoints.len(), 1);
    assert!(checkpoints[0].0.contains(SECOND_HISTORY_SUMMARY));
    assert_eq!(checkpoints[0].1, Some(2));

    let home = test.home.clone();
    drop(worker);
    drop(test);
    let resumed_request = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, FOLLOWUP_PROMPT),
        sse(vec![
            ev_response_created("resp-bounded-resume"),
            ev_assistant_message("msg-bounded-resume", "resume complete"),
            ev_completed("resp-bounded-resume"),
        ]),
    )
    .await;
    let mut resume_builder = test_codex()
        .with_history_mode(ThreadHistoryMode::Legacy)
        .with_config(enable_multi_agent_v2_with_local_compaction);
    let resumed = resume_builder.resume(&server, home, path).await?;
    resumed.submit_turn(FOLLOWUP_PROMPT).await?;
    let request = resumed_request.single_request();
    assert!(request.body_contains_text(SECOND_HISTORY_SUMMARY));
    assert!(!request.body_contains_text(FIRST_HISTORY_SUMMARY));
    assert!(
        request
            .header("x-codex-window-id")
            .is_some_and(|window_id| window_id.ends_with(":2"))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_root_resume_restores_agent_identity_and_role_on_followup() -> Result<()> {
    let server = start_mock_server().await;
    let spawn_args = serde_json::to_string(&json!({
        "message": INITIAL_TASK,
        "task_name": "worker",
        "agent_type": ROLE_NAME,
        "fork_turns": "none",
    }))?;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, INITIAL_PROMPT),
        sse(vec![
            ev_response_created("resp-spawn-1"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                COLLABORATION_NAMESPACE,
                "spawn_agent",
                &spawn_args,
            ),
            ev_completed("resp-spawn-1"),
        ]),
    )
    .await;
    let initial_child_request = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            request_has_input_type(request, "agent_message") && body_contains(request, INITIAL_TASK)
        },
        sse(vec![
            ev_response_created("resp-worker-1"),
            ev_assistant_message("msg-worker-1", "initial task complete"),
            ev_completed("resp-worker-1"),
        ]),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, SPAWN_CALL_ID)
                && !request_has_input_type(request, "agent_message")
        },
        sse(vec![
            ev_response_created("resp-spawn-2"),
            ev_assistant_message("msg-spawn-2", "worker spawned"),
            ev_completed("resp-spawn-2"),
        ]),
    )
    .await;

    let initial_model_provider_base_url = format!("{}/v1", server.uri());
    let mut initial_builder = test_codex().with_config(move |config| {
        configure_multi_agent_v2_with_role(config, &initial_model_provider_base_url);
    });
    let initial = initial_builder.build_with_auto_env(&server).await?;
    let root_thread_id = initial.session_configured.thread_id;
    let home = initial.home.clone();
    let rollout_path = initial
        .codex
        .rollout_path()
        .expect("root rollout path")
        .to_path_buf();
    initial.submit_turn(INITIAL_PROMPT).await?;

    let (worker_thread_id, worker_thread) =
        wait_for_spawned_worker(&initial, root_thread_id).await?;
    assert!(initial_child_request.requests().iter().any(|request| {
        request.body_contains_text(INITIAL_TASK)
            && request.body_contains_text(ROLE_DEVELOPER_INSTRUCTIONS)
            && !request.body_contains_text(SUBAGENT_DEVELOPER_INSTRUCTIONS)
    }));
    let initial_worker_config = worker_thread.config_snapshot().await;
    let initial_worker_role_config = (
        initial_worker_config.model,
        initial_worker_config.model_provider_id,
        initial_worker_config.reasoning_effort,
        initial_worker_config.permission_profile,
    );
    assert_eq!(
        initial_worker_role_config,
        (
            ROLE_MODEL.to_string(),
            ROLE_MODEL_PROVIDER_ID.to_string(),
            Some(ReasoningEffort::High),
            PermissionProfile::Disabled,
        )
    );
    worker_thread.flush_rollout().await?;
    initial.codex.flush_rollout().await?;
    drop(worker_thread);
    drop(initial);

    let followup_args = serde_json::to_string(&json!({
        "target": "worker",
        "message": FOLLOWUP_TASK,
    }))?;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, FOLLOWUP_PROMPT),
        sse(vec![
            ev_response_created("resp-followup-1"),
            ev_function_call_with_namespace(
                FOLLOWUP_CALL_ID,
                COLLABORATION_NAMESPACE,
                "followup_task",
                &followup_args,
            ),
            ev_completed("resp-followup-1"),
        ]),
    )
    .await;
    let followup_child_request = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            request_has_input_type(request, "agent_message")
                && body_contains(request, FOLLOWUP_TASK)
        },
        sse(vec![
            ev_response_created("resp-worker-2"),
            ev_assistant_message("msg-worker-2", "follow-up complete"),
            ev_completed("resp-worker-2"),
        ]),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, FOLLOWUP_CALL_ID)
                && !request_has_input_type(request, "agent_message")
        },
        sse(vec![
            ev_response_created("resp-followup-2"),
            ev_assistant_message("msg-followup-2", "follow-up sent"),
            ev_completed("resp-followup-2"),
        ]),
    )
    .await;

    let resumed_model_provider_base_url = format!("{}/v1", server.uri());
    let mut resume_builder = test_codex().with_config(move |config| {
        configure_multi_agent_v2_with_role(config, &resumed_model_provider_base_url);
    });
    let resumed = resume_builder.resume(&server, home, rollout_path).await?;
    assert_eq!(
        resumed.thread_manager.list_thread_ids().await,
        vec![root_thread_id]
    );
    assert!(
        resumed
            .thread_manager
            .get_thread(worker_thread_id)
            .await
            .is_err()
    );

    resumed.submit_turn(FOLLOWUP_PROMPT).await?;

    let (reloaded_worker_id, reloaded_worker) =
        wait_for_spawned_worker(&resumed, root_thread_id).await?;
    assert_eq!(reloaded_worker_id, worker_thread_id);
    assert!(followup_child_request.requests().iter().any(|request| {
        request.body_contains_text(FOLLOWUP_TASK)
            && request.body_contains_text(ROLE_DEVELOPER_INSTRUCTIONS)
            && !request.body_contains_text(SUBAGENT_DEVELOPER_INSTRUCTIONS)
    }));
    let reloaded_worker_config = reloaded_worker.config_snapshot().await;
    let reloaded_worker_role_config = (
        reloaded_worker_config.model,
        reloaded_worker_config.model_provider_id,
        reloaded_worker_config.reasoning_effort,
        reloaded_worker_config.permission_profile,
    );
    assert_eq!(reloaded_worker_role_config, initial_worker_role_config);

    Ok(())
}
