// large_futures: stack futures on hot paths by design. too_many_lines:
// style gate only.
#![allow(clippy::large_futures, clippy::too_many_lines)]

//! `--mode rpc` hosts its session in a daemon worker: the daemon-only
//! surfaces (RLM children, agent messages, heartbeats) work over the rpc
//! stdio protocol, driven by scripted faux sessions against a sandboxed
//! supervisor.

mod rpc_support;

use serde_json::{json, Value};

use rpc_support::{first_text, kernel_python, rpc_command, RpcChild};

/// The kernel cell that spawns one scripted RLM child.
const SPAWN_CELL: &str =
    "handle = await rlm.spawn(\"run the lane task\", name=\"kid\")\nprint(handle.rlm_child_id)";

fn ipython_call(code: &str) -> Value {
    json!({ "content": [
        { "type": "toolCall", "name": "ipython", "arguments": { "code": code } },
    ] })
}

/// The parent's turns: the spawn cell, its wrap-up, then one answer per wake.
fn spawn_parent_script(wake_replies: &[&str]) -> Value {
    let mut responses = vec![
        ipython_call(SPAWN_CELL),
        json!({ "text": "spawn turn done" }),
    ];
    responses.extend(wake_replies.iter().map(|reply| json!({ "text": reply })));
    json!({ "engine": "faux", "responses": responses })
}

fn is_assistant_end_with(frame: &Value, text: &str) -> bool {
    frame["type"] == "message_end"
        && frame["message"]["role"] == "assistant"
        && first_text(&frame["message"]) == text
}

#[test]
fn rpc_spawned_child_streams_and_its_reply_reaches_the_parent() {
    let Some(kernel_python) = kernel_python() else {
        return;
    };
    let mut client = RpcChild::spawn_with_children(
        &["--mode", "rpc"],
        &spawn_parent_script(&["notice seen"]),
        &json!({ "responses": ["kid done"] }),
        &kernel_python,
    );
    let response = client.request(&json!({ "type": "prompt", "message": "spawn the kid" }));
    assert_eq!(response["success"], true, "{response}");
    let (settled, _) = client.wait_frame(|frame| {
        frame["type"] == "rlm_child_update" && frame["child"]["status"] == "done"
    });
    assert_eq!(
        settled["child"]["answerPreview"], "kid done",
        "the child's final reply rides its terminal update: {settled}"
    );
    let (_, before) = client.wait_frame(|frame| is_assistant_end_with(frame, "notice seen"));
    let notice = before
        .iter()
        .find(|frame| {
            frame["type"] == "message_start"
                && frame["message"]["customType"] == "rlm_child_terminal_notice"
        })
        .expect("the child's terminal notice wakes the parent");
    assert!(
        notice["message"].to_string().contains("kid done"),
        "the notice carries the child's reply: {notice}"
    );
}

#[test]
fn rpc_child_agent_message_wakes_the_parent() {
    let Some(kernel_python) = kernel_python() else {
        return;
    };
    let child_script = json!({ "engine": "faux", "responses": [
        ipython_call("await agent_message.send('kid hello', receiver_role='parent')"),
        { "text": "kid done" },
    ] });
    let mut client = RpcChild::spawn_with_children(
        &["--mode", "rpc"],
        &spawn_parent_script(&["parent woke", "parent woke again"]),
        &child_script,
        &kernel_python,
    );
    let response = client.request(&json!({ "type": "prompt", "message": "spawn the kid" }));
    assert_eq!(response["success"], true, "{response}");
    let (delivered, _) = client.wait_frame(|frame| {
        frame["type"] == "message_start" && frame["message"]["customType"] == "agent_message"
    });
    assert_eq!(
        (
            &delivered["message"]["content"],
            &delivered["message"]["details"]["message"],
            &delivered["message"]["details"]["fromRelationship"],
        ),
        (
            &json!("[agent-message from child:kid]\n\nkid hello"),
            &json!("kid hello"),
            &json!("child"),
        ),
        "the delivered row carries the child's message: {delivered}"
    );
    client.wait_frame(|frame| is_assistant_end_with(frame, "parent woke"));
}

#[test]
fn rpc_heartbeat_fires_without_a_client_prompt() {
    let mut client = RpcChild::spawn(
        &["--mode", "rpc"],
        &json!({ "engine": "faux", "responses": ["beat ack"] }),
    );
    let set = client.request(&json!({
        "type": "set_heartbeat",
        "schedule": "every 10s",
        "prompt": "check in",
    }));
    assert_eq!(set["success"], true, "{set}");
    let job_id = set["data"]["heartbeat"]["id"].clone();
    assert!(job_id.is_string(), "the set answers the heartbeat: {set}");
    let listed = client.request(&json!({ "type": "list_heartbeats" }));
    let listed_ids: Vec<Value> = listed["data"]["heartbeats"]
        .as_array()
        .expect("the heartbeat listing")
        .iter()
        .map(|heartbeat| heartbeat["job"]["id"].clone())
        .collect();
    assert_eq!(listed_ids, vec![job_id], "{listed}");
    let (prompt, _) = client.wait_frame(|frame| {
        frame["type"] == "message_start" && frame["message"]["customType"] == "heartbeat_prompt"
    });
    assert!(
        prompt["message"].to_string().contains("check in"),
        "the heartbeat prompt row: {prompt}"
    );
    client.wait_frame(|frame| is_assistant_end_with(frame, "beat ack"));
    client.wait_event("agent_end");
}

/// An abort mid-stream ends the turn; the session settles idle.
#[test]
fn rpc_abort_ends_the_running_turn() {
    let mut client = RpcChild::spawn(
        &["--mode", "rpc"],
        &json!({
            "engine": "faux",
            "tokensPerSecond": 2,
            "responses": ["a reply that streams far slower than the test waits for it"],
        }),
    );
    let response = client.request(&json!({ "type": "prompt", "message": "go" }));
    assert_eq!(response["success"], true, "{response}");
    client.wait_frame(|frame| {
        frame["type"] == "message_update" && frame["message"]["role"] == "assistant"
    });
    let (aborted, end) = client.request_and_wait(&json!({ "type": "abort" }), |frame| {
        frame["type"] == "agent_end"
    });
    assert_eq!(aborted["success"], true, "{aborted}");
    let last = end["messages"]
        .as_array()
        .and_then(|messages| messages.last())
        .expect("the run's messages");
    assert_eq!(last["stopReason"], "aborted", "{end}");
    let state = client.request(&json!({ "type": "get_state" }));
    assert_eq!(state["data"]["isStreaming"], false, "{state}");
}

/// `observe` streams a running child session: its history answers the
/// command, its live events follow as `observed_session_event` frames.
#[test]
fn rpc_observe_streams_a_child_session() {
    let Some(kernel_python) = kernel_python() else {
        return;
    };
    // The child's first turn holds in a cell until the test creates the
    // gate file, so it is provably running while the test observes it.
    let gate = std::cell::OnceCell::new();
    let mut client = RpcChild::spawn_configured(
        &["--mode", "rpc"],
        &spawn_parent_script(&["notice seen"]),
        |home, command| {
            let gate_path = home.join("release-child");
            let hold = format!(
                "import asyncio, os\nwhile not os.path.exists({:?}):\n    await asyncio.sleep(0.05)",
                gate_path.display().to_string()
            );
            let child_script = json!({ "engine": "faux", "responses": [
                ipython_call(&hold),
                { "text": "kid done" },
            ] });
            let child_script_path = home.join("child-script.json");
            std::fs::write(&child_script_path, child_script.to_string()).unwrap();
            command
                .env("PRIME_AGENT_FAUX_CHILD_SCRIPT", &child_script_path)
                .env("PRIME_AGENT_KERNEL_PYTHON", &kernel_python);
            gate.set(gate_path).unwrap();
        },
    );
    let response = client.request(&json!({ "type": "prompt", "message": "spawn the kid" }));
    assert_eq!(response["success"], true, "{response}");
    let (running, _) = client.wait_frame(|frame| {
        frame["type"] == "rlm_child_update"
            && frame["child"]["status"] == "running"
            && frame["child"]["activeSessionId"].is_string()
    });
    let child_session = running["child"]["activeSessionId"].clone();
    let id = client.command(&json!({ "type": "observe", "activeSessionId": child_session }));
    let (observed, before) = client.wait_response(&id);
    assert_eq!(observed["success"], true, "{observed}");
    assert!(
        observed["data"]["messages"].is_array(),
        "the observe answers the child's history: {observed}"
    );
    assert!(
        before
            .iter()
            .all(|frame| frame["type"] != "observed_session_event"),
        "no observed frame precedes the observe response: {before:?}"
    );
    std::fs::write(gate.get().expect("the gate path"), "").unwrap();
    let (end, _) = client.wait_frame(|frame| {
        frame["type"] == "observed_session_event"
            && frame["activeSessionId"] == child_session
            && frame["event"]["type"] == "agent_end"
    });
    let reply = end["event"]["messages"]
        .as_array()
        .and_then(|messages| messages.last())
        .expect("the child's run messages");
    assert_eq!(first_text(reply), "kid done", "{end}");
    let again = client.request(&json!({ "type": "observe", "activeSessionId": child_session }));
    assert_eq!(
        again["success"], true,
        "a repeated observe answers again: {again}"
    );
    let unobserved =
        client.request(&json!({ "type": "unobserve", "activeSessionId": child_session }));
    assert_eq!(
        unobserved,
        json!({ "id": unobserved["id"], "type": "response", "command": "unobserve", "success": true })
    );
}

/// A session that ran the turns "first turn" and "second turn".
fn two_turn_session() -> RpcChild {
    let mut client = RpcChild::spawn(
        &["--mode", "rpc"],
        &json!({ "engine": "faux", "responses": ["one", "two"] }),
    );
    for message in ["first turn", "second turn"] {
        let response = client.request(&json!({ "type": "prompt", "message": message }));
        assert_eq!(response["success"], true, "{response}");
        client.wait_event("agent_end");
    }
    client
}

/// Roll the session back to before its second prompt, as T3 does.
fn roll_back_the_second_turn(client: &mut RpcChild) -> Value {
    let forks = client.request(&json!({ "type": "get_fork_messages" }));
    let second_turn = forks["data"]["messages"][1]["entryId"].clone();
    assert!(second_turn.is_string(), "{forks}");
    client.request(&json!({ "type": "navigate_tree", "targetId": second_turn }))
}

/// The user and assistant texts `get_messages` answers.
fn conversation_texts(client: &mut RpcChild) -> Vec<String> {
    let messages = client.request(&json!({ "type": "get_messages" }));
    messages["data"]["messages"]
        .as_array()
        .unwrap_or_else(|| panic!("the messages: {messages}"))
        .iter()
        .filter(|message| matches!(message["role"].as_str(), Some("user" | "assistant")))
        .map(|message| first_text(message).to_string())
        .collect()
}

/// The texts of the user entries `get_fork_messages` answers.
fn fork_message_texts(client: &mut RpcChild) -> Vec<String> {
    let forks = client.request(&json!({ "type": "get_fork_messages" }));
    forks["data"]["messages"]
        .as_array()
        .unwrap_or_else(|| panic!("the fork messages: {forks}"))
        .iter()
        .map(|message| message["text"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// `navigate_tree` moves the leaf in place: the branch shortens, the file stays.
#[test]
fn rpc_navigate_tree_rolls_the_session_back_in_place() {
    let mut client = two_turn_session();
    let before = client.request(&json!({ "type": "get_state" }));
    let navigated = roll_back_the_second_turn(&mut client);
    assert_eq!(
        navigated["data"],
        json!({ "cancelled": false, "editorText": "second turn" }),
        "{navigated}"
    );
    assert_eq!(conversation_texts(&mut client), vec!["first turn", "one"]);
    let after = client.request(&json!({ "type": "get_state" }));
    assert_eq!(after["data"]["sessionFile"], before["data"]["sessionFile"]);
}

/// T3 baselines its turns on `get_fork_messages`: after a rollback it lists
/// only the active branch's user entries, not the abandoned one.
#[test]
fn rpc_get_fork_messages_lists_only_the_active_branch() {
    let mut client = two_turn_session();
    let navigated = roll_back_the_second_turn(&mut client);
    assert_eq!(navigated["success"], true, "{navigated}");
    assert_eq!(fork_message_texts(&mut client), vec!["first turn"]);
}

/// The faux turns of a session whose kernel reports its `T3_PROBE`.
fn t3_probe_script() -> Value {
    json!({ "engine": "faux", "responses": [
        ipython_call("import os\nprint(repr(os.environ.get('T3_PROBE')))"),
        { "text": "probed" },
    ] })
}

/// Prompt a [`t3_probe_script`] session; returns what its kernel printed.
fn kernel_t3_probe(client: &mut RpcChild) -> String {
    let response = client.request(&json!({ "type": "prompt", "message": "probe" }));
    assert_eq!(response["success"], true, "{response}");
    let (result, _) = client.wait_frame(|frame| {
        frame["type"] == "message_end" && frame["message"]["role"] == "toolResult"
    });
    first_text(&result["message"]).trim().to_string()
}

/// The supervisor inherits the env of the client that started it; a
/// session's kernel must still see only its own client's `T3_*` env.
#[test]
fn rpc_session_kernel_sees_only_its_clients_t3_env() {
    if kernel_python().is_none() {
        return;
    }
    let mut starter =
        RpcChild::spawn_configured(&["--mode", "rpc"], &t3_probe_script(), |_, command| {
            command.env("T3_PROBE", "stale");
        });
    let state = starter.request(&json!({ "type": "get_state" }));
    assert_eq!(state["success"], true, "the daemon is up: {state}");
    let mut fresh = starter.join(
        "fresh-script.json",
        &["--mode", "rpc"],
        &t3_probe_script(),
        |_, command| {
            command.env("T3_PROBE", "fresh");
        },
    );
    let mut bare = starter.join(
        "bare-script.json",
        &["--mode", "rpc"],
        &t3_probe_script(),
        |_, command| {
            command.env_remove("T3_PROBE");
        },
    );
    assert_eq!(
        (kernel_t3_probe(&mut fresh), kernel_t3_probe(&mut bare)),
        ("'fresh'".to_string(), "None".to_string())
    );
}

/// An RLM child is a worker of its own: its kernel must see its parent's
/// `T3_*` env, not the supervisor's.
#[test]
fn rpc_spawned_child_kernel_sees_its_parents_t3_env() {
    let Some(kernel_python) = kernel_python() else {
        return;
    };
    let mut starter =
        RpcChild::spawn_configured(&["--mode", "rpc"], &t3_probe_script(), |_, command| {
            command.env("T3_PROBE", "stale");
        });
    let state = starter.request(&json!({ "type": "get_state" }));
    assert_eq!(state["success"], true, "the daemon is up: {state}");
    let child_script = json!({ "engine": "faux", "responses": [
        ipython_call(
            "import os\nawait agent_message.send(repr(os.environ.get('T3_PROBE')), receiver_role='parent')",
        ),
        { "text": "kid done" },
    ] });
    let mut parent = starter.join(
        "parent-script.json",
        &["--mode", "rpc"],
        &spawn_parent_script(&["parent woke", "parent woke again"]),
        |home, command| {
            let child_script_path = home.join("child-script.json");
            std::fs::write(&child_script_path, child_script.to_string()).unwrap();
            command
                .env("PRIME_AGENT_FAUX_CHILD_SCRIPT", &child_script_path)
                .env("PRIME_AGENT_KERNEL_PYTHON", &kernel_python)
                .env("T3_PROBE", "fresh");
        },
    );
    let response = parent.request(&json!({ "type": "prompt", "message": "spawn the kid" }));
    assert_eq!(response["success"], true, "{response}");
    let (delivered, _) = parent.wait_frame(|frame| {
        frame["type"] == "message_start" && frame["message"]["customType"] == "agent_message"
    });
    assert_eq!(
        delivered["message"]["details"]["message"], "'fresh'",
        "{delivered}"
    );
}

/// A faux reply that streams for far longer than the lifecycle tests wait.
fn slow_stream_script() -> Value {
    json!({
        "engine": "faux",
        "tokensPerSecond": 2,
        "responses": ["a reply that streams far slower than the test waits for it, word after word after word"],
    })
}

/// Start a streaming turn, close stdin, and return once the stream has
/// outlived the EOF: the EOF settle is then waiting on the running turn.
fn spawn_settling_after_eof() -> RpcChild {
    let mut client = RpcChild::spawn(&["--mode", "rpc"], &slow_stream_script());
    let response = client.request(&json!({ "type": "prompt", "message": "go" }));
    assert_eq!(response["success"], true, "{response}");
    let assistant_update = |frame: &Value| {
        frame["type"] == "message_update" && frame["message"]["role"] == "assistant"
    };
    client.wait_frame(assistant_update);
    client.close_stdin();
    client.wait_frame(assistant_update);
    client
}

/// A signal still ends the process while the EOF settle waits on a streaming turn.
#[cfg(unix)]
#[test]
fn rpc_sigterm_during_the_eof_settle_exits_143() {
    let mut client = spawn_settling_after_eof();
    let sent = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(client.child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(sent.success(), "the SIGTERM dispatch succeeded");
    assert_eq!(client.wait_closed().code(), Some(143));
}

/// Losing the daemon while the EOF settle waits is a failure, never a clean exit.
#[test]
fn rpc_daemon_loss_during_the_eof_settle_exits_1() {
    let mut client = spawn_settling_after_eof();
    rpc_support::shutdown_sandboxed_daemon(&client.socket);
    assert_eq!(client.wait_closed().code(), Some(1));
    let stderr = client.stderr_text();
    assert!(
        stderr.starts_with("Error: "),
        "the loss is reported: {stderr}"
    );
}

/// The bound session is never observed, so unobserving its id cannot
/// detach the main stream.
#[test]
fn rpc_observe_refuses_the_bound_session() {
    let mut client = RpcChild::spawn(
        &["--mode", "rpc"],
        &json!({ "engine": "faux", "responses": ["still streaming"] }),
    );
    let state = client.request(&json!({ "type": "get_state" }));
    let bound = state["data"]["activeSessionId"].clone();
    assert!(bound.is_string(), "{state}");
    let observed = client.request(&json!({ "type": "observe", "activeSessionId": bound }));
    assert_eq!(
        observed["error"],
        format!(
            "Cannot observe the session this connection is bound to: {}",
            bound.as_str().unwrap()
        ),
        "{observed}"
    );
    let unobserved = client.request(&json!({ "type": "unobserve", "activeSessionId": bound }));
    assert_eq!(unobserved["success"], true, "{unobserved}");
    let response = client.request(&json!({ "type": "prompt", "message": "go" }));
    assert_eq!(response["success"], true, "{response}");
    client.wait_frame(|frame| is_assistant_end_with(frame, "still streaming"));
}

/// `--extension` fails before any daemon work: no supervisor starts.
#[test]
fn rpc_rejects_extensions_before_any_daemon_work() {
    let home = tempfile::TempDir::new().unwrap();
    let socket = home.path().join("daemon.sock");
    let output = rpc_command(
        home.path(),
        &socket,
        &["--mode", "rpc", "--extension", "t3.ts"],
    )
    .stdin(std::process::Stdio::null())
    .output()
    .expect("binary present");
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "Error: Unknown option: --extension\n"
    );
    assert!(!socket.exists(), "no daemon was started");
}
