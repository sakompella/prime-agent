// Pedantic-gate dispositions as src/lib.rs.
#![allow(clippy::large_futures)]

//! Verifier: a session's kernel resolves default `rlm.harness` local state to
//! the session's own artifact dir, the `<artifact-dir>/harness` the host reads
//! for the harness digest (v0.9.8 parity). Covers a root session and an RLM
//! child laid out the way the daemon writes children
//! (`session-artifacts/<parent>/sub-<id>`).
#![cfg(unix)]

mod live_kernel;

use std::path::Path;

use serde_json::{json, Value};

const HARNESS_ENV_KEYS: [&str; 3] = [
    "RLM_GLOBAL_HARNESS_STATE_DIR",
    "RLM_HARNESS_STATE_DIR",
    "RLM_SESSION_DIR",
];

/// Writes a local memory through the default `rlm.harness` proxy and prints
/// the kernel's harness env plus the write outcome as one JSON line.
fn harness_cell() -> String {
    format!(
        "import json, os\nreceipt = {{\"env\": {{key: os.environ.get(key) for key in {HARNESS_ENV_KEYS:?}}}}}\ntry:\n    rlm.harness.create_memory(\"slice1 local memory\", \"local harness works\", id=\"slice1_local\")\n    receipt[\"write\"] = \"ok\"\nexcept Exception as error:\n    receipt[\"write\"] = str(error)\nprint(json.dumps(receipt))",
    )
}

fn receipt(details: &Value) -> Value {
    let stdout = details["stdout"]
        .as_str()
        .expect("the cell prints its receipt");
    serde_json::from_str(stdout.trim()).expect("receipt json")
}

fn expected_receipt(agent_dir: &Path, artifact_dir: &Path) -> Value {
    json!({
        "env": {
            "RLM_GLOBAL_HARNESS_STATE_DIR": agent_dir.join("harness"),
            "RLM_HARNESS_STATE_DIR": artifact_dir.join("harness"),
            "RLM_SESSION_DIR": artifact_dir,
        },
        "write": "ok",
    })
}

fn stored_local_memory_ids(artifact_dir: &Path) -> Vec<String> {
    let path = artifact_dir.join("harness").join("harness_state.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("no local harness state at {}", path.display()));
    let state: Value = serde_json::from_str(&raw).expect("harness state json");
    state["entries"]["memory"]
        .as_object()
        .map(|memories| memories.keys().cloned().collect())
        .unwrap_or_default()
}

#[tokio::test]
async fn kernel_local_harness_resolves_to_the_session_artifact_dir() {
    let Some(kernel_python) = live_kernel::kernel_python() else {
        return;
    };
    let dir = tempfile::tempdir().expect("temp dir");
    let agent_dir = dir.path().join("agent");
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).expect("project dir");

    // No harness binding may leak in from the test's own env: a run inside a
    // Prime kernel carries its own RLM_* values.
    let mut overrides = vec![
        (
            "PRIME_AGENT_KERNEL_PYTHON",
            Some(kernel_python.display().to_string()),
        ),
        ("PRIME_AGENT_CODING_AGENT_DIR", None),
        ("PRIME_API_KEY", None),
        ("RLM_DEPTH", None),
    ];
    overrides.extend(HARNESS_ENV_KEYS.iter().map(|key| (*key, None)));
    let _env = live_kernel::EnvOverride::apply(&overrides);

    let root = live_kernel::run_cell(
        &agent_dir,
        &cwd,
        &agent_dir.join("sessions"),
        None,
        &harness_cell(),
    )
    .await;
    assert_eq!(
        receipt(&root.details),
        expected_receipt(&agent_dir, &root.artifact_dir)
    );
    assert_eq!(
        root.artifact_dir.parent(),
        Some(agent_dir.join("session-artifacts").as_path()),
        "root artifacts live at <agent-dir>/session-artifacts/<session>"
    );
    assert_eq!(
        stored_local_memory_ids(&root.artifact_dir),
        vec!["slice1_local".to_string()]
    );

    let parent_id = root
        .artifact_dir
        .file_name()
        .expect("the artifact dir names its session")
        .to_owned();
    let parent_artifacts = agent_dir.join("session-artifacts").join(&parent_id);
    let child = live_kernel::run_cell(
        &agent_dir,
        &cwd,
        &parent_artifacts.join("sub-0000abcd"),
        Some(1),
        &harness_cell(),
    )
    .await;
    assert_eq!(
        receipt(&child.details),
        expected_receipt(&agent_dir, &child.artifact_dir)
    );
    assert_eq!(
        child.artifact_dir.parent(),
        Some(parent_artifacts.join("session-artifacts").as_path()),
        "child artifacts live beside the child dirs, as in v0.9.8"
    );
    assert_eq!(
        stored_local_memory_ids(&child.artifact_dir),
        vec!["slice1_local".to_string()]
    );
}
