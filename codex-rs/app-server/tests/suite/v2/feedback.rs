use anyhow::Result;
use app_test_support::TestAppServer;
use app_test_support::create_fake_rollout;
use app_test_support::rollout_path;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::FeedbackUploadParams;
use codex_app_server_protocol::FeedbackUploadResponse;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::SessionMetaLine;
use codex_rollout::read_session_meta_line;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;

const FEEDBACK_DSN_ENV_VAR: &str = "CODEX_FEEDBACK_DSN";

#[tokio::test]
async fn feedback_upload_json_rpc_includes_logical_reference_attachment() -> Result<()> {
    let sentry = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&sentry)
        .await;

    let codex_home = TempDir::new()?;
    fs::write(
        codex_home.path().join("config.toml"),
        "[feedback]\nenabled = true\n",
    )?;
    let (parent_id, child_id) = write_reference_rollouts(codex_home.path()).await?;
    let sentry_dsn = sentry
        .uri()
        .strip_prefix("http://")
        .map(|authority| format!("http://public@{authority}/1"))
        .expect("wiremock URI should use http");

    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .with_env_overrides(&[(FEEDBACK_DSN_ENV_VAR, Some(sentry_dsn.as_str()))])
        .build_initialized()
        .await?;

    let response: FeedbackUploadResponse = app_server
        .request(|request_id| ClientRequest::FeedbackUpload {
            request_id,
            params: FeedbackUploadParams {
                classification: "bug".to_string(),
                reason: Some("logical reference feedback regression".to_string()),
                thread_id: Some(child_id.to_string()),
                include_logs: true,
                extra_log_files: None,
                tags: None,
            },
        })
        .await?;
    assert_eq!(response.thread_id, child_id.to_string());

    let envelope = wait_for_feedback_envelope(&sentry).await?;
    let envelope_text = String::from_utf8_lossy(&envelope);
    assert!(
        envelope_text.contains("rollout-history-"),
        "feedback envelope should include the logical rollout attachment filename: {envelope_text}"
    );
    assert!(
        envelope_text.contains(&parent_id.to_string()),
        "feedback envelope should include the inherited parent session metadata"
    );
    assert!(
        envelope_text.contains("parent feedback marker"),
        "feedback envelope should include the inherited parent record"
    );
    assert!(
        envelope_text.contains("child feedback marker"),
        "feedback envelope should include the child delta"
    );

    app_server.shutdown_gracefully().await?;
    Ok(())
}

async fn write_reference_rollouts(codex_home: &Path) -> Result<(ThreadId, ThreadId)> {
    let filename_timestamp = "2025-01-06T12-00-00";
    let timestamp = "2025-01-06T12:00:00Z";
    let parent_id = ThreadId::from_string(&create_fake_rollout(
        codex_home,
        filename_timestamp,
        timestamp,
        "parent feedback marker",
        Some("mock_provider"),
        None,
    )?)?;
    let parent_path = rollout_path(codex_home, filename_timestamp, &parent_id.to_string());
    let parent_len = fs::metadata(&parent_path)?.len();
    let child_id = ThreadId::new();
    let mut child_meta = read_session_meta_line(&parent_path).await?.meta;
    child_meta.id = child_id;
    child_meta.session_id = child_id.into();
    child_meta.forked_from_id = Some(parent_id);
    child_meta.history_base = Some(HistoryPosition {
        thread_id: parent_id,
        end_ordinal_exclusive: 0,
        end_byte_offset: parent_len,
    });
    let child_meta_line = RolloutLine {
        timestamp: timestamp.to_string(),
        ordinal: None,
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: child_meta,
            git: None,
        }),
    };
    let child_response_line = json!({
        "timestamp": timestamp,
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "child feedback marker"}]
        }
    });
    let child_path = rollout_path(codex_home, filename_timestamp, &child_id.to_string());
    fs::write(
        child_path,
        format!(
            "{}\n{}\n",
            serde_json::to_string(&child_meta_line)?,
            serde_json::to_string(&child_response_line)?,
        ),
    )?;
    Ok((parent_id, child_id))
}

async fn wait_for_feedback_envelope(server: &MockServer) -> Result<Vec<u8>> {
    for _ in 0..200 {
        if let Some(requests) = server.received_requests().await
            && let Some(request) = requests.into_iter().next()
        {
            return Ok(request.body);
        }
        sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("timed out waiting for the feedback Sentry envelope")
}
