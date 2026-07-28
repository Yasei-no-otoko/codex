use super::*;
use codex_models_manager::model_info::model_info_from_slug;
use codex_utils_output_truncation::approx_token_count;
use tempfile::tempdir;

#[test]
fn build_stage_one_input_message_truncates_rollout_using_model_context_window() {
    let input = format!("{}{}{}", "a".repeat(700_000), "middle", "z".repeat(700_000));
    let mut model_info = model_info_from_slug("gpt-5.3-codex");
    model_info.context_window = Some(123_000);
    let model_rollout_token_limit = usize::try_from(
        ((123_000_i64 * model_info.effective_context_window_percent) / 100)
            * crate::stage_one::CONTEXT_WINDOW_PERCENT
            / 100,
    )
    .unwrap();
    let message = build_stage_one_input_message(
        &model_info,
        Path::new("/tmp/rollout.jsonl"),
        Path::new("/tmp"),
        &input,
    )
    .unwrap();

    assert!(model_rollout_token_limit > crate::stage_one::MAX_INPUT_ITEM_TOKENS);
    assert!(message.contains("tokens truncated"));
    assert!(message.contains('a'));
    assert!(message.contains('z'));
    assert!(approx_token_count(&message) <= crate::stage_one::MAX_INPUT_ITEM_TOKENS);
}

#[test]
fn build_stage_one_input_message_uses_default_limit_when_model_context_window_missing() {
    let input = format!("{}{}{}", "a".repeat(700_000), "middle", "z".repeat(700_000));
    let mut model_info = model_info_from_slug("gpt-5.3-codex");
    model_info.context_window = None;
    model_info.max_context_window = None;
    let message = build_stage_one_input_message(
        &model_info,
        Path::new("/tmp/rollout.jsonl"),
        Path::new("/tmp"),
        &input,
    )
    .unwrap();

    assert!(message.contains("tokens truncated"));
    assert!(approx_token_count(&message) <= crate::stage_one::MAX_INPUT_ITEM_TOKENS);
}

#[test]
fn build_stage_one_input_message_caps_large_inherited_rollout_to_one_item_limit() {
    let input = format!(
        "{}\nchild delta tail",
        "inherited parent prefix ".repeat(200_000)
    );
    let mut model_info = model_info_from_slug("gpt-5.3-codex");
    model_info.context_window = Some(1_000_000);

    let message = build_stage_one_input_message(
        &model_info,
        Path::new("/tmp/rollout.jsonl"),
        Path::new("/tmp"),
        &input,
    )
    .expect("stage-one input should render");

    assert!(message.contains("tokens truncated"));
    assert!(message.contains("child delta tail"));
    assert!(approx_token_count(&message) <= crate::stage_one::MAX_INPUT_ITEM_TOKENS);
}

#[test]
fn build_consolidation_prompt_points_to_workspace_diff_and_extension_tree() {
    let temp = tempdir().unwrap();
    let memory_root = temp.path().join("memories");
    let memory_extensions_root = memory_root.join("extensions");
    std::fs::create_dir_all(&memory_extensions_root).unwrap();

    let prompt = build_consolidation_prompt(&memory_root);

    assert!(prompt.contains("Memory workspace diff:"));
    assert!(prompt.contains("phase2_workspace_diff.md"));
    assert!(prompt.contains(&format!(
        "Memory extensions (under {}/):",
        memory_extensions_root.display()
    )));
    assert!(prompt.contains("workspace diff shows deleted extension resource files"));
}
