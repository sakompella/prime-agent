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
    let aborted = client.request(&json!({ "type": "abort" }));
    assert_eq!(aborted["success"], true, "{aborted}");
    let (end, _) = client.wait_frame(|frame| frame["type"] == "agent_end");
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
    let mut client = RpcChild::spawn_with_children(
        &["--mode", "rpc"],
        &spawn_parent_script(&["notice seen"]),
        &json!({ "responses": [{ "text": "kid done", "delayMs": 4_000 }] }),
        &kernel_python,
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
