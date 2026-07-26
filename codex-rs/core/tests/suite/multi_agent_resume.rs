use anyhow::Result;
use codex_core::CodexThread;
use codex_core::TurnInputRequest;
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
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::assert_parent_turn;
use core_test_support::responses::assert_root_turn;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call_with_namespace;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_once_match;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
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
const NESTED_CALL_ID: &str = "spawn-grandchild";
const QUEUE_CALL_ID: &str = "queue-worker-message";
const FOLLOWUP_CALL_ID: &str = "followup-worker";
const SIBLING_SPAWN_CALL_ID: &str = "spawn-survivor";
const SIBLING_FOLLOWUP_CALL_ID: &str = "followup-survivor";
const INTERRUPT_CALL_ID: &str = "interrupt-worker";
const INITIAL_PROMPT: &str = "spawn a durable worker";
const INITIAL_TASK: &str = "inspect the repository";
const NESTED_TASK: &str = "inspect the nested repository";
const QUEUE_PROMPT: &str = "queue context for the durable worker";
const QUEUED_MESSAGE: &str = "queue-only context from an earlier parent turn";
const FOLLOWUP_PROMPT: &str = "continue the durable worker";
const FOLLOWUP_TASK: &str = "inspect the tests too";
const SIBLING_PROMPT: &str = "spawn a second durable worker";
const SIBLING_TASK: &str = "inspect the release lifecycle";
const SIBLING_FOLLOWUP_PROMPT: &str = "continue the surviving worker";
const SIBLING_FOLLOWUP_TASK: &str = "verify the surviving worker";
const INTERRUPT_PROMPT: &str = "release the interrupted worker";
const SIBLING_NAME: &str = "survivor";
const FIRST_HISTORY_PROMPT: &str = "record the first legacy context";
const FIRST_HISTORY_REPLY: &str = "first legacy context recorded";
const FIRST_HISTORY_SUMMARY: &str = "FIRST_LEGACY_SUMMARY";
const SECOND_HISTORY_PROMPT: &str = "record the second legacy context";
const SECOND_HISTORY_REPLY: &str = "second legacy context recorded";
const SECOND_HISTORY_SUMMARY: &str = "SECOND_LEGACY_SUMMARY";
const ROLE_NAME: &str = "durable_worker";
const ROLE_MODEL: &str = "gpt-5.6-sol";
const ROLE_MODEL_PROVIDER_ID: &str = "openai";
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

fn request_has_model(request: &wiremock::Request, model: &str) -> bool {
    decoded_body(request)
        .and_then(|body| serde_json::from_slice::<Value>(&body).ok())
        .is_some_and(|body| body.get("model").and_then(Value::as_str) == Some(model))
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

async fn mount_root_collaboration_call(
    server: &wiremock::MockServer,
    prompt: &'static str,
    call_id: &'static str,
    tool_name: &'static str,
    arguments: &str,
) {
    let first_response_id = format!("resp-{call_id}-1");
    mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            body_contains(request, prompt) && !request_has_model(request, ROLE_MODEL)
        },
        sse(vec![
            ev_response_created(&first_response_id),
            ev_function_call_with_namespace(call_id, COLLABORATION_NAMESPACE, tool_name, arguments),
            ev_completed(&first_response_id),
        ]),
    )
    .await;

    let second_response_id = format!("resp-{call_id}-2");
    let message_id = format!("msg-{call_id}-2");
    mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            body_contains(request, call_id) && !request_has_model(request, ROLE_MODEL)
        },
        sse(vec![
            ev_response_created(&second_response_id),
            ev_assistant_message(&message_id, "collaboration completed"),
            ev_completed(&second_response_id),
        ]),
    )
    .await;
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
    config.multi_agent_v2.subagent_developer_instructions =
        Some(SUBAGENT_DEVELOPER_INSTRUCTIONS.to_string());
    config.multi_agent_v2.max_concurrent_threads_per_session = 3;
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
    let deadline = Instant::now() + Duration::from_secs(10);
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
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for spawned worker");
        }
        sleep(Duration::from_millis(10)).await;
    };
    let thread = test.thread_manager.get_thread(thread_id).await?;
    loop {
        if matches!(thread.agent_status().await, AgentStatus::Completed(_)) {
            return Ok((thread_id, thread));
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for worker completion");
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn submit_compact(thread: &Arc<CodexThread>) -> Result<()> {
    let submission_id = thread.submit(Op::Compact).await?;
    loop {
        let event = thread.next_event().await?;
        if event.id == submission_id && matches!(event.msg, EventMsg::TurnComplete(_)) {
            break;
        }
    }
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
            request_has_model(request, ROLE_MODEL)
                && request_has_input_type(request, "agent_message")
                && body_contains(request, INITIAL_TASK)
        },
        sse(vec![
            ev_response_created("resp-worker-1"),
            ev_function_call_with_namespace(
                NESTED_CALL_ID,
                COLLABORATION_NAMESPACE,
                "spawn_agent",
                r#"{"message":"inspect the nested repository","task_name":"grandchild","fork_turns":"none"}"#,
            ),
            ev_completed("resp-worker-1"),
        ]),
    )
    .await;
    let nested_mock = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, NESTED_TASK)
                && request_has_input_type(request, "agent_message")
                && !body_contains(request, NESTED_CALL_ID)
        },
        sse(vec![ev_completed("resp-parent-turn-assistant")]),
    )
    .await;
    for (text, is_subagent) in [(NESTED_CALL_ID, true), (QUEUE_CALL_ID, false)] {
        mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                body_contains(request, text)
                    && request_has_input_type(request, "agent_message") == is_subagent
            },
            sse(vec![ev_completed("resp-parent-turn-assistant")]),
        )
        .await;
    }
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, SPAWN_CALL_ID) && !request_has_model(request, ROLE_MODEL)
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
    initial
        .codex
        .start_or_steer_turn(
            TurnInputRequest::user_input(vec![UserInput::Text {
                text: INITIAL_PROMPT.to_string(),
                text_elements: Vec::new(),
            }])
            .with_thread_settings(ThreadSettingsOverrides {
                permission_profile: Some(PermissionProfile::Disabled),
                ..Default::default()
            }),
        )
        .await?;
    wait_for_event(&initial.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    let deadline = Instant::now() + Duration::from_secs(2);
    let worker_thread_id = loop {
        if let Some(thread_id) = initial_child_request
            .requests()
            .into_iter()
            .find_map(|request| {
                let body = request.body_json();
                if body["client_metadata"]["x-codex-parent-thread-id"] != json!(root_thread_id) {
                    return None;
                }
                body["client_metadata"]["thread_id"]
                    .as_str()
                    .and_then(|thread_id| codex_protocol::ThreadId::from_string(thread_id).ok())
            })
        {
            break thread_id;
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for spawned worker");
        }
        sleep(Duration::from_millis(10)).await;
    };
    let worker_thread = initial.thread_manager.get_thread(worker_thread_id).await?;
    wait_for_event(worker_thread.as_ref(), |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert!(initial_child_request.requests().iter().any(|request| {
        request.body_contains_text(INITIAL_TASK)
            && request.body_contains_text(ROLE_DEVELOPER_INSTRUCTIONS)
            && request.body_contains_text("<permission_profile type=\"disabled\">")
            && !request.body_contains_text(SUBAGENT_DEVELOPER_INSTRUCTIONS)
    }));
    assert_eq!(
        worker_thread.config().await.model_provider,
        initial.codex.config().await.model_provider,
        "roles must inherit the parent's complete model provider",
    );
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

    let sibling_spawn_args = serde_json::to_string(&json!({
        "message": SIBLING_TASK,
        "task_name": SIBLING_NAME,
        "agent_type": ROLE_NAME,
        "fork_turns": "none",
    }))?;
    mount_root_collaboration_call(
        &server,
        SIBLING_PROMPT,
        SIBLING_SPAWN_CALL_ID,
        "spawn_agent",
        &sibling_spawn_args,
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            request_has_model(request, ROLE_MODEL)
                && request_has_input_type(request, "agent_message")
                && body_contains(request, SIBLING_TASK)
        },
        sse(vec![
            ev_response_created("resp-survivor-1"),
            ev_assistant_message("msg-survivor-1", "initial survivor task complete"),
            ev_completed("resp-survivor-1"),
        ]),
    )
    .await;
    initial.submit_turn(SIBLING_PROMPT).await?;

    let grandchild = nested_mock.last_request().expect("grandchild").body_json();
    let nested_id = &grandchild["client_metadata"]["thread_id"];
    let sibling_thread_id = initial
        .thread_manager
        .list_thread_ids()
        .await
        .into_iter()
        .find(|id| ![root_thread_id, worker_thread_id].contains(id) && &json!(id) != nested_id)
        .ok_or_else(|| anyhow::anyhow!("spawned sibling should be registered"))?;
    let sibling_thread = initial.thread_manager.get_thread(sibling_thread_id).await?;
    wait_for_event(sibling_thread.as_ref(), |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    sibling_thread.flush_rollout().await?;
    worker_thread.flush_rollout().await?;
    initial.codex.flush_rollout().await?;
    sibling_thread.shutdown_and_wait().await?;
    worker_thread.shutdown_and_wait().await?;
    drop(sibling_thread);
    drop(worker_thread);

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
            request_has_model(request, ROLE_MODEL)
                && request_has_input_type(request, "agent_message")
                && body_contains(request, FOLLOWUP_TASK)
                && body_contains(request, QUEUED_MESSAGE)
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
            body_contains(request, FOLLOWUP_CALL_ID) && !request_has_model(request, ROLE_MODEL)
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
    let resumed = resume_builder.restart(&server, &initial).await?;
    drop(initial);
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
    assert!(
        resumed
            .thread_manager
            .get_thread(sibling_thread_id)
            .await
            .is_err()
    );

    let redirected_server = start_mock_server().await;
    let redirected_base_url = format!("{}/v1", redirected_server.uri());
    std::fs::write(
        resumed.config.codex_home.join("durable-worker-role.toml"),
        format!(
            r#"model = "{ROLE_MODEL}"
model_reasoning_effort = "high"
developer_instructions = "{ROLE_DEVELOPER_INSTRUCTIONS}"
model_provider = "{ROLE_MODEL_PROVIDER_ID}"
openai_base_url = "{redirected_base_url}"
"#
        ),
    )?;

    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, QUEUE_PROMPT),
        sse(vec![
            ev_response_created("resp-queue"),
            ev_function_call_with_namespace(
                QUEUE_CALL_ID,
                COLLABORATION_NAMESPACE,
                "send_message",
                r#"{"target":"worker","message":"queue-only context from an earlier parent turn"}"#,
            ),
            ev_completed("resp-queue"),
        ]),
    )
    .await;
    resumed.submit_turn(QUEUE_PROMPT).await?;

    let reloaded_worker = resumed
        .thread_manager
        .get_thread(worker_thread_id)
        .await
        .expect("queued message should lazily reload the original worker");
    assert_eq!(
        reloaded_worker.config().await.model_provider,
        resumed.codex.config().await.model_provider,
        "cold reload must preserve the parent's complete model provider",
    );
    resumed.submit_turn(FOLLOWUP_PROMPT).await?;
    wait_for_event(reloaded_worker.as_ref(), |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert!(followup_child_request.requests().iter().any(|request| {
        request.body_contains_text(FOLLOWUP_TASK)
            && request.body_contains_text(ROLE_DEVELOPER_INSTRUCTIONS)
            && request.body_contains_text("<permission_profile type=\"disabled\">")
            && !request.body_contains_text(SUBAGENT_DEVELOPER_INSTRUCTIONS)
    }));
    let requests = server
        .received_requests()
        .await
        .expect("captured response requests");
    assert!(!followup_child_request.requests().iter().any(|request| {
        request.body_json()["client_metadata"]["thread_id"] == json!(worker_thread_id)
            && request.body_contains_text(QUEUED_MESSAGE)
            && !request.body_contains_text(FOLLOWUP_TASK)
    }));
    let body_for = |text: &str, thread: codex_protocol::ThreadId| {
        requests
            .iter()
            .find_map(|request| {
                let body: Value = serde_json::from_slice(&decoded_body(request)?).ok()?;
                (body_contains(request, text)
                    && body["client_metadata"]["thread_id"] == json!(thread))
                .then_some(body)
            })
            .expect("matching model request for expected thread")
    };
    let initial_root = body_for(INITIAL_PROMPT, root_thread_id);
    let queue_root = body_for(QUEUE_PROMPT, root_thread_id);
    let followup_root = body_for(FOLLOWUP_PROMPT, root_thread_id);
    let initial_child = body_for(INITIAL_TASK, worker_thread_id);
    let followup_child = body_for(FOLLOWUP_TASK, worker_thread_id);
    let initial_parent = initial_root["client_metadata"]["turn_id"]
        .as_str()
        .expect("initial parent turn");
    let queue_parent = queue_root["client_metadata"]["turn_id"]
        .as_str()
        .expect("queue-only parent turn");
    let followup_parent = followup_root["client_metadata"]["turn_id"]
        .as_str()
        .expect("follow-up parent turn");
    assert_ne!(followup_parent, initial_parent);
    assert_ne!(followup_parent, queue_parent);
    let nested_parent = initial_child["client_metadata"]["turn_id"]
        .as_str()
        .expect("nested worker parent turn");
    for (body, parent_thread, parent_turn) in [
        (&initial_root, None, None),
        (&queue_root, None, None),
        (&followup_root, None, None),
        (&initial_child, Some(root_thread_id), Some(initial_parent)),
        (&followup_child, Some(root_thread_id), Some(followup_parent)),
        (&grandchild, Some(worker_thread_id), Some(nested_parent)),
    ] {
        if let Some(parent_thread) = parent_thread {
            assert_eq!(
                body["client_metadata"]["x-codex-parent-thread-id"],
                json!(parent_thread)
            );
        }
        assert_parent_turn(body, parent_turn)?;
    }
    for (body, root_turn) in [
        (&initial_root, initial_parent),
        (&queue_root, queue_parent),
        (&followup_root, followup_parent),
        (&initial_child, initial_parent),
        (&followup_child, followup_parent),
        (&grandchild, initial_parent),
    ] {
        assert_root_turn(body, Some(root_turn))?;
    }
    let reloaded_worker_config = reloaded_worker.config_snapshot().await;
    let reloaded_worker_role_config = (
        reloaded_worker_config.model,
        reloaded_worker_config.model_provider_id,
        reloaded_worker_config.reasoning_effort,
        reloaded_worker_config.permission_profile,
    );
    assert_eq!(reloaded_worker_role_config, initial_worker_role_config);

    reloaded_worker.shutdown_and_wait().await?;
    assert!(
        resumed
            .thread_manager
            .get_thread(worker_thread_id)
            .await
            .is_ok()
    );

    let interrupt_args = serde_json::to_string(&json!({
        "target": "worker",
    }))?;
    mount_root_collaboration_call(
        &server,
        INTERRUPT_PROMPT,
        INTERRUPT_CALL_ID,
        "interrupt_agent",
        &interrupt_args,
    )
    .await;
    resumed.submit_turn(INTERRUPT_PROMPT).await?;
    assert!(
        resumed
            .thread_manager
            .get_thread(worker_thread_id)
            .await
            .is_err()
    );

    let sibling_followup_args = serde_json::to_string(&json!({
        "target": SIBLING_NAME,
        "message": SIBLING_FOLLOWUP_TASK,
    }))?;
    mount_root_collaboration_call(
        &server,
        SIBLING_FOLLOWUP_PROMPT,
        SIBLING_FOLLOWUP_CALL_ID,
        "followup_task",
        &sibling_followup_args,
    )
    .await;
    let sibling_followup_request = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            request_has_model(request, ROLE_MODEL)
                && request_has_input_type(request, "agent_message")
                && body_contains(request, SIBLING_FOLLOWUP_TASK)
        },
        sse(vec![
            ev_response_created("resp-survivor-2"),
            ev_assistant_message("msg-survivor-2", "survivor follow-up complete"),
            ev_completed("resp-survivor-2"),
        ]),
    )
    .await;
    resumed.submit_turn(SIBLING_FOLLOWUP_PROMPT).await?;

    let surviving_sibling = resumed
        .thread_manager
        .get_thread(sibling_thread_id)
        .await
        .expect("follow-up should reload the surviving sibling");
    wait_for_event(surviving_sibling.as_ref(), |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert!(sibling_followup_request.requests().iter().any(|request| {
        request.body_contains_text(SIBLING_FOLLOWUP_TASK)
            && request.body_contains_text(ROLE_DEVELOPER_INSTRUCTIONS)
    }));
    assert!(
        redirected_server
            .received_requests()
            .await
            .expect("captured redirected-provider requests")
            .is_empty(),
        "a changed role must not redirect resumed model requests",
    );

    Ok(())
}
