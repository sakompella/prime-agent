// large_futures: stack-resident futures on hot paths by design.
// too_many_lines: style gate, not correctness.
#![allow(clippy::large_futures, clippy::too_many_lines)]

//! End-to-end RPC-mode verification: the real binary serves the TS
//! `modes/rpc` JSONL command surface over stdio on a daemon session (a
//! sandboxed supervisor hosting a scripted faux worker): protocol contract,
//! core command set, and the stdin-close lifecycle.

mod rpc_support;

use serde_json::{json, Value};

use rpc_support::{first_text, RpcChild};

/// The worker's faux provider session answering `steps` in order.
fn turn_script(steps: &Value) -> Value {
    json!({ "engine": "faux", "responses": steps })
}

#[test]
fn rpc_get_state_answers_the_fresh_session() {
    let mut client = RpcChild::spawn(
        &["--mode", "rpc", "--no-session"],
        &turn_script(&json!(["unused"])),
    );
    let response = client.request(&json!({ "type": "get_state" }));
    assert_eq!(response["success"], true, "the response: {response}");
    let data = &response["data"];
    assert_eq!(data["isStreaming"], false);
    assert_eq!(data["isCompacting"], false);
    assert!(
        data["model"]["id"].is_string(),
        "the resolved model rides the state"
    );
    assert!(data["thinkingLevel"].is_string());
    assert_eq!(data["messageCount"], 0);
    assert_eq!(data["sessionActions"]["queuedCount"], 0);
    assert_eq!(data["sessionActions"]["steering"], json!([]));
    assert_eq!(data["sessionActions"]["followUps"], json!([]));
    assert_eq!(data["goal"]["active"], false);
    assert_eq!(
        data["sessionFile"], "",
        "--no-session keeps the session in memory (the daemon reports no file): {data}"
    );
    assert!(data["sessionId"].is_string());
}

/// The prompt response precedes the turn's stream events (TS `promptResponsePending` buffering).
#[test]
fn rpc_prompt_streams_events_after_the_response() {
    let mut client = RpcChild::spawn(
        &["--mode", "rpc", "--no-session"],
        &turn_script(&json!(["first reply"])),
    );
    let id = client.command(&json!({ "type": "prompt", "message": "hi" }));
    let (response, before) = client.wait_response(&id);
    assert_eq!(response["success"], true, "the response: {response}");
    assert!(
        response.get("data").is_none(),
        "prompt answers without a data key (TS success(id, command))"
    );
    assert!(
        before.is_empty(),
        "the prompt response precedes every turn event: {before:?}"
    );
    client.wait_frame(|frame| {
        frame["type"] == "message_start" && frame["message"]["role"] == "assistant"
    });
    let end = client.wait_event("agent_end");
    assert!(
        end["messages"]
            .as_array()
            .is_some_and(|messages| !messages.is_empty()),
        "agent_end carries the run's messages"
    );
}

#[test]
fn rpc_parse_and_unknown_command_errors() {
    let mut client = RpcChild::spawn(
        &["--mode", "rpc", "--no-session"],
        &turn_script(&json!(["unused"])),
    );
    // No id to match on, so match the command name.
    client.send(&json!("not an object"));
    let (parse_error, _) =
        client.wait_frame(|frame| frame["type"] == "response" && frame["command"] == "parse");
    assert_eq!(
        parse_error,
        json!({
            "type": "response",
            "command": "parse",
            "success": false,
            "error": "Invalid command: expected an object with a string type",
        })
    );
    client.send(&json!({ "id": "t-unknown", "type": "definitely_not_a_command" }));
    let (unknown, _) = client.wait_frame(|frame| frame["command"] == "definitely_not_a_command");
    assert_eq!(
        unknown,
        json!({
            "type": "response",
            "command": "definitely_not_a_command",
            "success": false,
            "error": "Unknown command: definitely_not_a_command",
        }),
        "an unknown command answers without the id (TS parity)"
    );
}

/// Steer and follow-up queue behind a streaming turn and show in the
/// state's queue projection; abort ends the turn.
#[test]
fn rpc_steer_and_follow_up_queue_then_abort() {
    let script = json!({
        "engine": "faux",
        "tokensPerSecond": 2,
        "responses": [
            "a reply that streams far slower than the test waits for it",
            "steer answer",
            "follow-up answer",
        ],
    });
    let mut client = RpcChild::spawn(&["--mode", "rpc", "--no-session"], &script);
    let response = client.request(&json!({ "type": "prompt", "message": "go" }));
    assert_eq!(response["success"], true);
    client.wait_frame(|frame| {
        frame["type"] == "message_update" && frame["message"]["role"] == "assistant"
    });
    let steer = client.request(&json!({ "type": "steer", "message": "steer this" }));
    assert_eq!(steer["success"], true, "steer queues: {steer}");
    let follow_up = client.request(&json!({ "type": "follow_up", "message": "fu this" }));
    assert_eq!(follow_up["success"], true, "follow_up queues: {follow_up}");
    let state = client.request(&json!({ "type": "get_state" }));
    assert_eq!(
        state["data"]["isStreaming"], true,
        "the turn is still running past the prompt response: {state}"
    );
    assert_eq!(
        state["data"]["sessionActions"]["queuedCount"], 2,
        "both rows queued while the turn streams: {state}"
    );
    assert_eq!(
        state["data"]["sessionActions"]["steering"],
        json!(["steer this"]),
        "the queue projection carries the queued previews: {state}"
    );
    let (aborted, _) = client.request_and_wait(&json!({ "type": "abort" }), |frame| {
        frame["type"] == "agent_end"
    });
    assert_eq!(aborted["success"], true, "{aborted}");
}

#[test]
fn rpc_thinking_level_set_and_cycle() {
    let script = json!({ "engine": "faux", "responses": ["unused"], "reasoning": true });
    let mut client = RpcChild::spawn(&["--mode", "rpc", "--no-session"], &script);
    let response = client.request(&json!({ "type": "set_thinking_level", "level": "high" }));
    assert_eq!(response["success"], true, "the response: {response}");
    assert!(
        response.get("data").is_none(),
        "set_thinking_level answers without data"
    );
    let state = client.request(&json!({ "type": "get_state" }));
    assert_eq!(state["data"]["thinkingLevel"], "high");
    let cycled = client.request(&json!({ "type": "cycle_thinking_level" }));
    assert_eq!(cycled["success"], true);
    assert!(
        cycled["data"]["level"].is_string(),
        "the cycle answers the next level: {cycled}"
    );
    let invalid = client.request(&json!({ "type": "set_thinking_level", "level": "bogus" }));
    assert_eq!(invalid["success"], false, "{invalid}");
}

#[test]
fn rpc_compact_answers_the_skip_error() {
    let mut client = RpcChild::spawn(
        &["--mode", "rpc", "--no-session"],
        &turn_script(&json!(["unused"])),
    );
    let response = client.request(&json!({ "type": "compact" }));
    assert_eq!(
        response,
        json!({
            "id": response["id"],
            "type": "response",
            "command": "compact",
            "success": false,
            "error": "Session is too short to compact — try again once it grows",
        })
    );
}

/// A compaction that runs streams its start and end frames to the rpc
/// client ahead of the response carrying the summary.
#[test]
fn rpc_compact_streams_its_frames_and_answers_the_summary() {
    let mut client = RpcChild::spawn_configured(
        &["--mode", "rpc", "--no-session"],
        &turn_script(&json!([
            "turn one reply",
            "turn two reply",
            "the summary",
            "the summary"
        ])),
        |home, _| {
            let agent_dir = home.join(".prime/agent");
            std::fs::create_dir_all(&agent_dir).unwrap();
            std::fs::write(
                agent_dir.join("settings.json"),
                json!({ "compaction": { "enabled": true, "keepRecentTokens": 10 } }).to_string(),
            )
            .unwrap();
        },
    );
    for message in [
        format!("turn one {}", "x".repeat(8_000)),
        "turn two".to_string(),
    ] {
        let response = client.request(&json!({ "type": "prompt", "message": message }));
        assert_eq!(response["success"], true, "{response}");
        client.wait_event("agent_end");
    }
    let id = client.command(&json!({ "type": "compact" }));
    let (response, before) = client.wait_response(&id);
    assert_eq!(response["success"], true, "{response}");
    assert!(
        response["data"]["summary"]
            .as_str()
            .is_some_and(|summary| summary.contains("the summary")),
        "{response}"
    );
    let boundaries: Vec<&Value> = before
        .iter()
        .filter(|frame| {
            matches!(
                frame["type"].as_str(),
                Some("compaction_start" | "compaction_end")
            )
        })
        .collect();
    let [start, end] = boundaries.as_slice() else {
        panic!("one start and one end frame precede the response: {before:?}");
    };
    assert_eq!(start["type"], "compaction_start", "{before:?}");
    assert_eq!(end["type"], "compaction_end", "{before:?}");
    assert_eq!(
        end["result"], response["data"],
        "the end frame carries the answered result"
    );
}

/// The daemon-only surfaces answer over the daemon session instead of the
/// in-process daemon-mode errors.
#[test]
fn rpc_daemon_surfaces_answer_over_the_daemon_session() {
    let mut client = RpcChild::spawn(&["--mode", "rpc"], &turn_script(&json!(["unused"])));
    let bash = client.request(&json!({ "type": "bash", "command": "printf hi" }));
    assert_eq!(bash["success"], true, "{bash}");
    assert_eq!(bash["data"]["output"], "hi", "{bash}");
    let schedule =
        client.request(&json!({ "type": "add_schedule", "schedule": "every 5m", "prompt": "hi" }));
    assert_eq!(schedule["success"], true, "{schedule}");
    let job_id = schedule["data"]["job"]["id"].clone();
    let jobs = client.request(&json!({ "type": "list_schedules" }));
    let listed: Vec<Value> = jobs["data"]["jobs"]
        .as_array()
        .expect("the job listing")
        .iter()
        .map(|job| job["id"].clone())
        .collect();
    assert_eq!(listed, vec![job_id.clone()], "{jobs}");
    let cancelled = client.request(&json!({ "type": "cancel_schedule", "jobId": job_id }));
    assert_eq!(cancelled["success"], true, "{cancelled}");
    let get_heartbeat = client.request(&json!({ "type": "get_heartbeat" }));
    assert_eq!(get_heartbeat["data"], json!({ "heartbeat": null }));
    let observe = client.request(&json!({ "type": "observe", "activeSessionId": "nope" }));
    assert_eq!(observe["error"], "Unknown active session: nope");
    let unobserve = client.request(&json!({ "type": "unobserve", "activeSessionId": "nope" }));
    assert_eq!(unobserve["success"], true, "{unobserve}");
    assert!(unobserve.get("data").is_none(), "{unobserve}");
}

#[test]
fn rpc_set_session_name_round_trip() {
    let mut client = RpcChild::spawn(
        &["--mode", "rpc", "--no-session"],
        &turn_script(&json!(["unused"])),
    );
    let id = client.command(&json!({ "type": "set_session_name", "name": "  my session  " }));
    let (response, before) = client.wait_response(&id);
    assert_eq!(response["success"], true, "the response: {response}");
    assert!(response.get("data").is_none(), "{response}");
    let changed = before
        .iter()
        .find(|frame| frame["type"] == "session_info_changed")
        .expect("the changed event precedes the response");
    assert_eq!(changed["name"], "my session");
    let state = client.request(&json!({ "type": "get_state" }));
    assert_eq!(state["data"]["sessionName"], "my session");
    let empty = client.request(&json!({ "type": "set_session_name", "name": "   " }));
    assert_eq!(empty["error"], "Session name cannot be empty");
}

/// `fork` branches at the entry's parent leaf and moves the connection onto the fork's file.
#[test]
fn rpc_fork_messages_and_fork_swap() {
    let script = turn_script(&json!(["one", "two"]));
    let mut client = RpcChild::spawn(&["--mode", "rpc"], &script);
    for message in ["first turn", "second turn"] {
        let response = client.request(&json!({ "type": "prompt", "message": message }));
        assert_eq!(response["success"], true, "the response: {response}");
        client.wait_event("agent_end");
    }
    let forks = client.request(&json!({ "type": "get_fork_messages" }));
    let messages = forks["data"]["messages"].as_array().cloned().unwrap();
    assert_eq!(
        messages
            .iter()
            .map(|message| message["text"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["first turn", "second turn"],
        "the user rows in file order"
    );
    assert!(
        messages
            .iter()
            .all(|message| message["entryId"].as_str().is_some_and(|id| !id.is_empty())),
        "each row carries its entry id"
    );
    let before = client.request(&json!({ "type": "get_state" }));
    let first_entry_id = messages[0]["entryId"].as_str().unwrap().to_string();
    let fork = client.request(&json!({ "type": "fork", "entryId": first_entry_id }));
    assert_eq!(
        fork["data"],
        json!({ "text": "first turn", "cancelled": false }),
        "the fork response: {fork}"
    );
    let after = client.request(&json!({ "type": "get_state" }));
    assert_ne!(
        before["data"]["sessionFile"], after["data"]["sessionFile"],
        "the fork is a new session file"
    );
    let clone = client.request(&json!({ "type": "clone" }));
    assert_eq!(
        clone["data"],
        json!({ "cancelled": false }),
        "the clone response: {clone}"
    );
}

#[test]
fn rpc_new_session_swaps_the_session() {
    let script = turn_script(&json!(["one", "unused"]));
    let mut client = RpcChild::spawn(&["--mode", "rpc"], &script);
    let response = client.request(&json!({ "type": "prompt", "message": "hi" }));
    assert_eq!(response["success"], true);
    client.wait_event("agent_end");
    let before = client.request(&json!({ "type": "get_state" }));
    let fresh = client.request(&json!({ "type": "new_session" }));
    assert_eq!(fresh["data"], json!({ "cancelled": false }), "{fresh}");
    let after = client.request(&json!({ "type": "get_state" }));
    assert_ne!(
        before["data"]["sessionId"], after["data"]["sessionId"],
        "new_session adopts a fresh session"
    );
    assert_eq!(after["data"]["messageCount"], 0);
}

/// `switch_session` reopens a saved session in place: T3 resumes a thread this way.
#[test]
fn rpc_switch_session_resumes_a_saved_session() {
    let script = turn_script(&json!(["one", "unused"]));
    let mut client = RpcChild::spawn(&["--mode", "rpc"], &script);
    let response = client.request(&json!({ "type": "prompt", "message": "remember me" }));
    assert_eq!(response["success"], true);
    client.wait_event("agent_end");
    let saved = client.request(&json!({ "type": "get_state" }));
    let fresh = client.request(&json!({ "type": "new_session" }));
    assert_eq!(fresh["success"], true, "{fresh}");
    let switched = client.request(&json!({
        "type": "switch_session",
        "sessionPath": saved["data"]["sessionFile"],
    }));
    assert_eq!(
        switched["data"],
        json!({ "cancelled": false }),
        "{switched}"
    );
    let state = client.request(&json!({ "type": "get_state" }));
    assert_eq!(state["data"]["sessionFile"], saved["data"]["sessionFile"]);
    let messages = client.request(&json!({ "type": "get_messages" }));
    let texts: Vec<&str> = messages["data"]["messages"]
        .as_array()
        .expect("the messages")
        .iter()
        .filter(|message| matches!(message["role"].as_str(), Some("user" | "assistant")))
        .map(first_text)
        .collect();
    assert_eq!(texts, vec!["remember me", "one"], "{messages}");
}

#[test]
fn rpc_get_available_models_lists_the_catalog() {
    let mut client = RpcChild::spawn_configured(
        &["--mode", "rpc", "--no-session"],
        &turn_script(&json!(["unused"])),
        |home, _| {
            let agent_dir = home.join(".prime/agent");
            std::fs::create_dir_all(&agent_dir).unwrap();
            std::fs::write(
                agent_dir.join("models.json"),
                json!({
                    "providers": {
                        "faux": {
                            "api": "faux",
                            "baseUrl": "http://localhost:0",
                            "apiKey": "sk-faux",
                            "models": [{
                                "id": "faux-1",
                                "name": "Faux Model",
                                "contextWindow": 100_000,
                                "maxTokens": 4_096,
                            }],
                        }
                    }
                })
                .to_string(),
            )
            .unwrap();
        },
    );
    let response = client.request(&json!({ "type": "get_available_models" }));
    assert_eq!(response["success"], true, "the response: {response}");
    let models = response["data"]["models"].as_array().cloned().unwrap();
    assert!(
        models.iter().any(|model| model["id"] == "faux-1"),
        "the faux model is available: {models:?}"
    );
}

#[test]
fn rpc_eof_settles_and_exits_zero() {
    let script = json!({
        "engine": "faux",
        "responses": [
            { "text": "slow", "delayMs": 300 },
            { "text": "after" },
        ],
    });
    let mut client = RpcChild::spawn(&["--mode", "rpc", "--no-session"], &script);
    let response = client.request(&json!({ "type": "prompt", "message": "go" }));
    assert_eq!(response["success"], true);
    client.wait_event("message_start");
    client.close_stdin();
    let exit = client.wait_exit();
    assert!(
        exit.success(),
        "stdin close settles the turn and exits 0 (status {exit})"
    );
}

/// SIGTERM exits 143 while a turn runs.
#[cfg(unix)]
#[test]
fn rpc_sigterm_exits_143() {
    let script =
        json!({ "engine": "faux", "responses": [ { "text": "slow", "delayMs": 30_000 } ] });
    let mut client = RpcChild::spawn(&["--mode", "rpc", "--no-session"], &script);
    let response = client.request(&json!({ "type": "prompt", "message": "go" }));
    assert_eq!(response["success"], true);
    client.wait_event("agent_start");
    let sent = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(client.child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(sent.success(), "the SIGTERM dispatch succeeded");
    assert_eq!(client.wait_exit().code(), Some(143), "SIGTERM exits 143");
}

#[test]
fn rpc_get_session_stats_answers_the_ts_shape() {
    let script = turn_script(&json!(["one"]));
    let mut client = RpcChild::spawn(&["--mode", "rpc"], &script);
    let response = client.request(&json!({ "type": "prompt", "message": "hi" }));
    assert_eq!(response["success"], true);
    client.wait_event("agent_end");
    let stats = client.request(&json!({ "type": "get_session_stats" }));
    assert_eq!(stats["success"], true, "the stats response: {stats}");
    let data = &stats["data"];
    assert_eq!(data["userMessages"], 1);
    assert_eq!(data["assistantMessages"], 1);
    assert!(data["sessionId"].is_string());
    assert!(
        data["sessionFile"].is_string(),
        "persisted session reports its file"
    );
    assert!(data["tokens"].is_object());
}

/// Regression test: `--mode rpc` must never print the missing-subsystem stub.
#[test]
fn rpc_mode_never_prints_the_missing_subsystem_stub() {
    let mut client = RpcChild::spawn(
        &["--mode", "rpc", "--no-session"],
        &turn_script(&json!(["unused"])),
    );
    // Any answered command proves the transport is live (the stub exited 1).
    let response = client.request(&json!({ "type": "get_state" }));
    assert_eq!(response["success"], true);
    client.close_stdin();
    let exit = client.wait_exit();
    assert!(exit.success(), "the mode serves the protocol: {exit}");
}

/// The session files holding a runtime lease right now.
fn leased_session_files(agent_dir: &std::path::Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(agent_dir.join("session-leases")) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| std::fs::read_to_string(entry.path().join("owner.json")).ok())
        .filter_map(|text| serde_json::from_str::<Value>(&text).ok())
        .filter_map(|owner| owner["sessionPath"].as_str().map(str::to_string))
        .collect()
}

/// Canonical-path comparison: host symlinks must not mask the match.
fn is_leased(leased: &[String], session_file: &str) -> bool {
    let session_file = std::path::Path::new(session_file);
    leased.iter().any(|leased| {
        std::fs::canonicalize(leased).ok() == std::fs::canonicalize(session_file).ok()
    })
}

/// A fresh persisted session leases its file while the worker hosts it.
#[test]
fn rpc_fresh_sessions_lease_their_files() {
    let mut client = RpcChild::spawn(&["--mode", "rpc"], &turn_script(&json!(["one"])));
    let response = client.request(&json!({ "type": "prompt", "message": "hi" }));
    assert_eq!(response["success"], true);
    client.wait_event("agent_end");
    let stats = client.request(&json!({ "type": "get_session_stats" }));
    let session_file = stats["data"]["sessionFile"]
        .as_str()
        .expect("the persisted session reports its file")
        .to_string();
    let leased = leased_session_files(&client.agent_dir());
    assert!(
        is_leased(&leased, &session_file),
        "the fresh session's file is runtime-leased while the worker is live (leased: {leased:?})"
    );
    client.close_stdin();
    let exit = client.wait_exit();
    assert!(exit.success(), "eof settles the leased session: {exit}");
}

/// The `--fork` copy is the session the worker hosts.
#[test]
fn rpc_fork_flag_hosts_the_materialized_copy() {
    let mut source = RpcChild::spawn(&["--mode", "rpc"], &turn_script(&json!(["one"])));
    let response = source.request(&json!({ "type": "prompt", "message": "hi" }));
    assert_eq!(response["success"], true);
    source.wait_event("agent_end");
    let stats = source.request(&json!({ "type": "get_session_stats" }));
    let source_file = stats["data"]["sessionFile"]
        .as_str()
        .expect("the source session reports its file")
        .to_string();
    source.close_stdin();
    assert!(source.wait_exit().success(), "the source session settles");

    let mut forked = RpcChild::spawn(
        &["--mode", "rpc", "--fork", &source_file],
        &turn_script(&json!(["hi there"])),
    );
    let response = forked.request(&json!({ "type": "prompt", "message": "again" }));
    assert_eq!(response["success"], true);
    forked.wait_event("agent_end");
    let stats = forked.request(&json!({ "type": "get_session_stats" }));
    let forked_file = stats["data"]["sessionFile"]
        .as_str()
        .expect("the forked session reports its file")
        .to_string();
    assert_ne!(forked_file, source_file, "the fork materialized a new file");
    assert_eq!(
        stats["data"]["userMessages"], 2,
        "the copy kept the source turn: {stats}"
    );
}

/// A failed `switch_session` leaves the live session untouched: its queued work still delivers.
#[test]
fn rpc_failed_switch_keeps_the_session_and_its_queue() {
    let script = json!({ "engine": "faux", "responses": [
        { "text": "first answer", "delayMs": 1_000 },
        "steer answer",
    ]});
    let mut client = RpcChild::spawn(&["--mode", "rpc", "--no-session"], &script);
    let response = client.request(&json!({ "type": "prompt", "message": "go" }));
    assert_eq!(response["success"], true);
    client.wait_event("agent_start");
    let steer = client.request(&json!({ "type": "steer", "message": "steer me" }));
    assert_eq!(steer["success"], true, "the steer queues: {steer}");
    // The crafted header stores a gone cwd, so the switch fails deterministically.
    let bad_file = client.home().join("bad-cwd-session.jsonl");
    std::fs::write(
        &bad_file,
        concat!(
            "{\"type\":\"session\",\"id\":\"bad-cwd\",",
            "\"timestamp\":\"2026-09-26T00:00:00.000Z\",",
            "\"cwd\":\"/definitely/missing/project\"}\n"
        ),
    )
    .expect("write the crafted session");
    let (failed, _) = client.request_and_wait(
        &json!({
            "type": "switch_session",
            "sessionPath": bad_file.to_string_lossy(),
        }),
        |frame| {
            frame["type"] == "message_end"
                && frame["message"]["role"] == "assistant"
                && first_text(&frame["message"]) == "steer answer"
        },
    );
    assert_eq!(failed["success"], false, "{failed}");
    assert!(
        failed["error"]
            .as_str()
            .unwrap_or_default()
            .contains("Stored session working directory does not exist"),
        "the failure is the crafted stale cwd, not an unrelated refusal: {failed}"
    );
}
