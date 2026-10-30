//! `apply_session_env`: the supervisor hands a live worker the T3
//! connection of a client that reused it. A worker the daemon started on
//! its own (a boot or message wake) runs without any; this gives its next
//! kernel, its later children and its running kernel the client's.

use std::collections::BTreeMap;

use pa_core::T3Code;
use pa_types::daemon::launch_env;
use serde_json::{json, Value};

use super::Worker;
use crate::protocol::{response_failure, response_success, DaemonResponse};

const T3_CODE_CONNECTED_CUSTOM_TYPE: &str = "t3_code_connected";

fn t3_code(env: &BTreeMap<String, String>) -> T3Code {
    T3Code::from_env(|key| env.get(key).cloned())
}

/// The next-turn row that tells the model `t3` works now: its system
/// prompt, built without T3, never said so, and earlier turns may have
/// recorded that the connection was missing.
fn t3_code_connected_notice() -> Value {
    let content = format!(
        "[t3-code-connected]\n\nT3 Code reconnected this session, so its Python kernel has T3 Code's connection again. Earlier results that said it was missing no longer hold.\n\n{}",
        pa_core::prompts::T3_CODE_SECTION
    );
    json!({
        "role": "custom",
        "customType": T3_CODE_CONNECTED_CUSTOM_TYPE,
        "content": content,
        "display": true,
        "timestamp": crate::util::now_ms(),
    })
}

impl Worker {
    /// `apply_session_env { env }`: make `env`'s session-scoped vars this
    /// worker's own. The running kernel's cell is queued in the background,
    /// behind any running cell, so the reusing client's open never waits on it.
    pub(crate) fn handle_apply_session_env(&self, payload: &Value) -> DaemonResponse {
        if let Err(response) = self.require_created("apply_session_env") {
            return response;
        }
        let Some(env) = payload.get("env").and_then(Value::as_object) else {
            return response_failure(
                None,
                "apply_session_env",
                "apply_session_env requires an env object",
                None,
            );
        };
        let env: BTreeMap<String, String> = env
            .iter()
            .filter(|(key, _)| launch_env::is_session_scoped(key))
            .filter_map(|(key, value)| Some((key.clone(), value.as_str()?.to_string())))
            .collect();
        let current = launch_env::session_scoped_env(&launch_env::collect_process_launch_env());
        if env == current {
            return response_success(None, "apply_session_env", Some(json!({ "changed": false })));
        }
        launch_env::replace_session_env(&env);
        if t3_code(&env) == T3Code::Present && t3_code(&current) == T3Code::Absent {
            self.core
                .lock()
                .unwrap()
                .pending_next_turn
                .push(t3_code_connected_notice());
        }
        if let Some(engine) = self.agent_engine.clone() {
            tokio::spawn(async move {
                if let Err(error) = engine.rebind_kernel_t3_env(&env).await {
                    eprintln!("pa-daemon: the kernel kept its old T3 connection: {error:#}");
                }
            });
        }
        response_success(None, "apply_session_env", Some(json!({ "changed": true })))
    }
}
