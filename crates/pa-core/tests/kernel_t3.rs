// Pedantic-gate dispositions as src/lib.rs.
#![allow(clippy::large_futures)]

//! Verifier: a session's kernel binds the `t3` module only when the process
//! carries T3 Code's MCP env, and a bound `t3` that cannot reach T3 raises
//! `T3Unavailable` instead of breaking the kernel.
#![cfg(unix)]

mod live_kernel;

use live_kernel::SessionStore;
use serde_json::{json, Value};

const T3_CELL: &str = "import json\nreceipt = {\"bound\": \"t3\" in globals()}\nif receipt[\"bound\"]:\n    try:\n        await t3.project_list()\n        receipt[\"call\"] = \"returned\"\n    except Exception as error:\n        receipt[\"call\"] = type(error).__name__\nprint(json.dumps(receipt))";

fn receipt(details: &Value) -> Value {
    let stdout = details["stdout"]
        .as_str()
        .unwrap_or_else(|| panic!("the cell prints no receipt: {details}"));
    serde_json::from_str(stdout.trim()).expect("receipt json")
}

#[tokio::test]
async fn kernel_binds_t3_only_in_t3_sessions() {
    let Some(kernel_python) = live_kernel::kernel_python() else {
        return;
    };
    let dir = tempfile::tempdir().expect("temp dir");
    let agent_dir = dir.path().join("agent");
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).expect("project dir");
    let closed_port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("free port")
        .port();
    let session_env = |url: Option<String>, token: Option<String>| {
        live_kernel::EnvOverride::apply(&[
            (
                "PRIME_AGENT_KERNEL_PYTHON",
                Some(kernel_python.display().to_string()),
            ),
            ("PRIME_AGENT_CODING_AGENT_DIR", None),
            ("PRIME_API_KEY", None),
            ("RLM_DEPTH", None),
            ("T3_MCP_URL", url),
            ("T3_MCP_BEARER_TOKEN", token),
        ])
    };

    let standalone = {
        let _env = session_env(None, None);
        live_kernel::run_cell(&agent_dir, &cwd, SessionStore::Ephemeral, None, T3_CELL).await
    };
    assert_eq!(receipt(&standalone.details), json!({ "bound": false }));

    let unreachable_t3 = {
        let _env = session_env(
            Some(format!("http://127.0.0.1:{closed_port}/mcp")),
            Some("test-token".to_string()),
        );
        live_kernel::run_cell(&agent_dir, &cwd, SessionStore::Ephemeral, None, T3_CELL).await
    };
    assert_eq!(
        receipt(&unreachable_t3.details),
        json!({ "bound": true, "call": "T3Unavailable" })
    );
}
