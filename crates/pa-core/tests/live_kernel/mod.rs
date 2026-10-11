//! Shared harness for the live-kernel session tests: one scripted turn whose
//! only tool call is an `ipython` cell, run through a real session engine and
//! kernel. Each test binary carries exactly one live-kernel test, because the
//! env overrides are process-global.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use pa_agent::scripted::{tool_call_turn_steps, ScriptedProvider, ScriptedTurn};
use pa_core::session::manager::SessionManager;
use pa_core::session_engine::engine::{create_session, SessionEngineConfig};
use pa_core::session_engine::PromptOptions;
use serde_json::{json, Value};

/// The kernel Python with prime-agent-runtime installed. Skipped (with a note)
/// on machines without one; `PA_CORE_KERNEL_PYTHON` points at an explicit one.
pub fn kernel_python() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("PA_CORE_KERNEL_PYTHON") {
        let explicit = PathBuf::from(explicit);
        assert!(
            explicit.exists(),
            "PA_CORE_KERNEL_PYTHON {} not found",
            explicit.display()
        );
        return Some(explicit);
    }
    let candidate = PathBuf::from(std::env::var("HOME").map_or_else(
        |_| "/home/ubuntu/.prime/agent/kernel-venv/bin/python".to_string(),
        |home| format!("{home}/.prime/agent/kernel-venv/bin/python"),
    ));
    if candidate.exists() {
        return Some(candidate);
    }
    eprintln!(
        "kernel python {} not found; skipping live kernel test",
        candidate.display()
    );
    None
}

/// Scoped process-env overrides: applied on construction, restored on drop.
pub struct EnvOverride {
    saved: Vec<(String, Option<String>)>,
}

impl EnvOverride {
    pub fn apply(pairs: &[(&str, Option<String>)]) -> Self {
        let saved = pairs
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        for (key, value) in pairs {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        EnvOverride { saved }
    }
}

impl Drop for EnvOverride {
    fn drop(&mut self) {
        for (key, value) in &self.saved {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

/// Where the session under test keeps its transcript.
pub enum SessionStore<'a> {
    /// Persisted under this session dir; the session gets an artifact dir.
    Persisted { session_dir: &'a Path },
    /// In memory only (`--no-session`); no artifact dir.
    Ephemeral,
}

pub struct CellRun {
    /// The session's artifact dir (`<session-dir parent>/session-artifacts/<id>`),
    /// `None` for an ephemeral session.
    pub artifact_dir: Option<PathBuf>,
    /// The `details` of the cell's tool result.
    pub details: Value,
}

/// Run `code` as the only tool call of one scripted turn.
pub async fn run_cell(
    agent_dir: &Path,
    cwd: &Path,
    store: SessionStore<'_>,
    rlm_depth: Option<u32>,
    code: &str,
) -> CellRun {
    let mut session = SessionManager::in_memory(cwd);
    match store {
        SessionStore::Persisted { session_dir } => {
            std::fs::create_dir_all(session_dir).expect("session dir");
            session.materialize_session_file(Some(session_dir.to_path_buf()));
        }
        SessionStore::Ephemeral => {}
    }
    let artifact_dir = session.get_session_artifact_dir();

    let model: pa_agent::types::Model = serde_json::from_value(json!({
        "id": "faux-1", "name": "Faux", "api": "test", "provider": "faux",
        "baseUrl": "http://localhost", "reasoning": false,
        "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
        "contextWindow": 200_000, "maxTokens": 4_000
    }))
    .expect("faux loop model");
    let provider = std::sync::Arc::new(ScriptedProvider::new(model.clone()));
    provider.push_turn(ScriptedTurn::Events(tool_call_turn_steps(
        &model,
        Some("running the cell"),
        vec![("call-1", "ipython", json!({ "code": code }))],
    )));
    provider.push_text_turn("done");

    let engine = create_session(SessionEngineConfig {
        on_late_sent_agent_message: None,
        semantic_edges: None,
        cron_store: None,
        queued_steering_probe: None,
        image_model_router: None,
        steering_mode: None,
        follow_up_mode: None,
        cwd: cwd.to_path_buf(),
        agent_dir: agent_dir.to_path_buf(),
        model: Some(model),
        thinking_level: None,
        stream_fn: Some(provider.stream_fn()),
        tools: Vec::new(),
        custom_system_prompt: None,
        prompt_guidelines: Vec::new(),
        generic_mcp_servers: Vec::new(),
        allow_recursion: None,
        session_manager: Some(session),
        extra_host_handlers: None,
        conversation_log_path: None,
        additional_skill_paths: Vec::new(),
        additional_prompt_paths: Vec::new(),
        extra_builtin_skill_overrides: Vec::new(),
        rlm_subagent_host: None,
        rlm_depth,
        telemetry: None,
        model_info: None,
        mcp_manager: None,
        prewarm_ipython_kernel: None,
        on_background_work_settled: None,
        queued_goal_context_purge: None,
    })
    .await
    .expect("create_session");
    engine
        .prompt("run the cell", PromptOptions::default())
        .await
        .expect("prompt");
    let entries =
        serde_json::to_value(engine.session.entries().await).expect("session entries json");
    let details = find_tool_result(&entries, "call-1")
        .and_then(|result| result.get("details").cloned())
        .unwrap_or_else(|| panic!("no tool result for call-1 in {entries}"));
    CellRun {
        artifact_dir,
        details,
    }
}

fn find_tool_result<'a>(value: &'a Value, tool_call_id: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            if map.get("role").and_then(Value::as_str) == Some("toolResult")
                && map.get("toolCallId").and_then(Value::as_str) == Some(tool_call_id)
            {
                return Some(value);
            }
            map.values()
                .find_map(|child| find_tool_result(child, tool_call_id))
        }
        Value::Array(items) => items
            .iter()
            .find_map(|child| find_tool_result(child, tool_call_id)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => None,
    }
}
