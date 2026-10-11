//! The daemon-attached RPC transport: the `modes/rpc` JSONL surface served
//! over a daemon session (TS `runRpcModeWithConnection(DaemonAgentConnection)`).
//!
//! Startup creates and attaches the daemon session the CLI session flags
//! select. Every command maps to daemon commands through [`RpcRoute`]; the
//! bound session's events stream to stdout unwrapped, the observed child
//! sessions' events as `observed_session_event` frames. A prompt's events
//! wait until its response is written. Stdin EOF settles the session and
//! completes it while this client owns it; SIGTERM/SIGHUP exit 143/129; a
//! closed session or a lost daemon exits 1.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use pa_types::daemon::{
    DaemonClosingReason, DaemonCommand, DaemonSessionClosedReason, DaemonSessionLifecycle,
};
use serde_json::{json, Map, Value};
use tokio::io::AsyncBufReadExt;

use super::protocol::{self, ParsedLine, ResponseData, RpcCommand};
use super::LineWriter;
use crate::daemon_link::{DaemonLink, LinkClient, LinkEvent};

pub struct DaemonRpcOptions {
    pub socket_path: PathBuf,
    /// The startup `create` built from the CLI session flags.
    pub create: DaemonCommand,
}

/// `prime-agent --capabilities`: the fork identity T3 Code reads to tell this
/// fork's rpc surface from stock upstream 0.10, which reports the same
/// version. Bump [`T3_FORK_CONTRACT`] when the T3-facing rpc commands or
/// frames change.
#[must_use]
pub fn t3_fork_capabilities(version: &str) -> Value {
    json!({
        "fork": "t3fork",
        "contract": T3_FORK_CONTRACT,
        "version": version,
        "rpc": RPC_COMMANDS,
    })
}

pub const T3_FORK_CONTRACT: u32 = 1;

/// Every command [`RpcRoute::of`] routes; a new route joins this list.
const RPC_COMMANDS: &[&str] = &[
    "prompt",
    "steer",
    "follow_up",
    "abort",
    "new_session",
    "switch_session",
    "get_state",
    "set_model",
    "cycle_model",
    "get_available_models",
    "set_thinking_level",
    "cycle_thinking_level",
    "set_steering_mode",
    "set_follow_up_mode",
    "compact",
    "refine",
    "set_auto_compaction",
    "set_auto_retry",
    "abort_retry",
    "bash",
    "abort_bash",
    "get_session_stats",
    "export_html",
    "fork",
    "navigate_tree",
    "clone",
    "get_fork_messages",
    "get_last_assistant_text",
    "set_session_name",
    "get_messages",
    "get_commands",
    "get_rlm_children",
    "send_message",
    "agent_messages_status",
    "agent_messages_pause",
    "agent_messages_resume",
    "agent_messages_clear",
    "list_schedules",
    "add_schedule",
    "cancel_schedule",
    "list_heartbeats",
    "get_heartbeat",
    "set_heartbeat",
    "update_heartbeat",
    "manage_heartbeat",
    "observe",
    "unobserve",
];

/// How one rpc command reaches the daemon (TS `rpc-mode.js` `handleCommand`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcRoute {
    /// One daemon command carrying the rpc payload's fields.
    Forward {
        daemon_type: &'static str,
        target: Target,
        answer: Answer,
    },
    /// `set_session_name`: the trimmed name; a blank one never reaches the daemon.
    SessionName,
    /// `clone`: fork at the current leaf, keeping it (`position: "at"`).
    Clone,
    /// `get_fork_messages`: the active branch's user messages, the turns a
    /// rollback keeps (v0.9.8 listed the whole session's).
    ForkMessages,
    /// Attach a second session on this link and stream it as observed frames.
    Observe,
    Unobserve,
    Unknown,
}

/// Which session a forwarded command addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// The bound session (`activeSessionId`).
    Bound,
    /// The bound session, promoted out of client ownership by the first
    /// schedule: a schedule must outlive this client (TS
    /// `withOwnedSessionPromotion`).
    BoundPromoting,
    /// The payload's own `activeSessionId` (`manage_heartbeat` manages any session's job).
    Payload,
    /// The payload's `targetActiveSessionId`; the bound session is the sender.
    Sender,
}

/// How a daemon answer becomes the rpc response `data` (TS `rpc-mode.js`
/// re-wraps what the connection method returns).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// No `data` key: the rpc method resolves void.
    Unit,
    /// The daemon `data` as-is.
    Data,
    /// The daemon `data`, or `null` when it carried none.
    DataOrNull,
    /// `{key: data[key]}`, the key omitted when the daemon left it out.
    Field(&'static str),
    /// `{key: data[key]}` with `null` for a missing value.
    FieldOrNull(&'static str),
    /// `{key: data}`.
    Wrapped(&'static str),
    /// `{level}`, or `null` when the cycle reported no level.
    LevelOrNull,
    /// `{text: data.selectedText, cancelled}`.
    ForkSelection,
    /// `{commands}` with each command's name, description, source, and sourceInfo.
    CommandSummaries,
}

impl RpcRoute {
    fn of(command: &str) -> Self {
        let bound = |daemon_type, answer| RpcRoute::Forward {
            daemon_type,
            target: Target::Bound,
            answer,
        };
        match command {
            "prompt" => bound("prompt", Answer::Unit),
            "steer" => bound("steer", Answer::Unit),
            "follow_up" => bound("follow_up", Answer::Unit),
            "abort" => bound("abort", Answer::Unit),
            "new_session" => bound("new_session", Answer::Data),
            "switch_session" => bound("switch_session", Answer::Data),
            // The daemon state is a superset of the TS rpc projection under
            // the same field names, so it passes through.
            "get_state" => bound("get_connection_state", Answer::Data),
            "set_model" => bound("set_model", Answer::Data),
            "cycle_model" => bound("cycle_model", Answer::DataOrNull),
            "get_available_models" => bound("get_available_models", Answer::Field("models")),
            "set_thinking_level" => bound("set_thinking_level", Answer::Unit),
            "cycle_thinking_level" => bound("cycle_thinking_level", Answer::LevelOrNull),
            "set_steering_mode" => bound("set_steering_mode", Answer::Unit),
            "set_follow_up_mode" => bound("set_follow_up_mode", Answer::Unit),
            "compact" => bound("compact", Answer::Data),
            "refine" => bound("refine", Answer::Data),
            "set_auto_compaction" => bound("set_auto_compaction", Answer::Unit),
            "set_auto_retry" => bound("set_auto_retry", Answer::Unit),
            "abort_retry" => bound("abort_retry", Answer::Unit),
            "bash" => bound("execute_bash_and_wait", Answer::Data),
            "abort_bash" => bound("abort_bash", Answer::Unit),
            "get_session_stats" => bound("get_session_stats", Answer::Data),
            "export_html" => bound("export_html", Answer::Field("path")),
            "fork" => bound("fork", Answer::ForkSelection),
            "navigate_tree" => bound("navigate_tree", Answer::Data),
            "clone" => RpcRoute::Clone,
            "get_fork_messages" => RpcRoute::ForkMessages,
            "get_last_assistant_text" => bound("get_last_assistant_text", Answer::Field("text")),
            "set_session_name" => RpcRoute::SessionName,
            "get_messages" => bound("get_messages", Answer::Field("messages")),
            "get_commands" => bound("get_commands", Answer::CommandSummaries),
            "get_rlm_children" => bound("get_rlm_children", Answer::Data),
            "send_message" => RpcRoute::Forward {
                daemon_type: "send_message",
                target: Target::Sender,
                answer: Answer::Data,
            },
            "agent_messages_status" => bound("agent_messages_status", Answer::Data),
            "agent_messages_pause" => bound("agent_messages_pause", Answer::Data),
            "agent_messages_resume" => bound("agent_messages_resume", Answer::Data),
            "agent_messages_clear" => bound("agent_messages_clear", Answer::Wrapped("cleared")),
            "list_schedules" => bound("cron_list", Answer::Field("jobs")),
            "add_schedule" => RpcRoute::Forward {
                daemon_type: "cron_add",
                target: Target::BoundPromoting,
                answer: Answer::Field("job"),
            },
            "cancel_schedule" => bound("cron_cancel", Answer::Field("job")),
            "list_heartbeats" => bound("heartbeats_list", Answer::Field("heartbeats")),
            "get_heartbeat" => bound("heartbeat_get", Answer::FieldOrNull("heartbeat")),
            "set_heartbeat" => RpcRoute::Forward {
                daemon_type: "heartbeat_set",
                target: Target::BoundPromoting,
                answer: Answer::Field("heartbeat"),
            },
            "update_heartbeat" => bound("heartbeat_update", Answer::FieldOrNull("heartbeat")),
            "manage_heartbeat" => RpcRoute::Forward {
                daemon_type: "heartbeat_manage",
                target: Target::Payload,
                answer: Answer::Field("heartbeat"),
            },
            "observe" => RpcRoute::Observe,
            "unobserve" => RpcRoute::Unobserve,
            _ => RpcRoute::Unknown,
        }
    }

    /// The ordering lane: prompts run in stdin order, and so do the
    /// observation commands; every other command runs as it arrives.
    fn lane(self) -> Option<Lane> {
        match self {
            RpcRoute::Forward {
                daemon_type: "prompt",
                ..
            } => Some(Lane::Prompt),
            RpcRoute::Observe | RpcRoute::Unobserve => Some(Lane::Observation),
            RpcRoute::Forward { .. }
            | RpcRoute::SessionName
            | RpcRoute::Clone
            | RpcRoute::ForkMessages
            | RpcRoute::Unknown => None,
        }
    }
}

impl Answer {
    fn shape(self, data: Option<Value>) -> ResponseData {
        let field = |key: &str| data.as_ref().and_then(|data| data.get(key)).cloned();
        let picked = |keys: &[&str], value: &Value| {
            let mut object = Map::new();
            for key in keys {
                if let Some(field) = value.get(*key) {
                    object.insert((*key).to_string(), field.clone());
                }
            }
            Value::Object(object)
        };
        match self {
            Answer::Unit => ResponseData::Absent,
            Answer::Data => data.map_or(ResponseData::Absent, ResponseData::Present),
            Answer::DataOrNull => ResponseData::Present(data.unwrap_or(Value::Null)),
            Answer::Field(key) => {
                ResponseData::Present(data.map_or_else(|| json!({}), |data| picked(&[key], &data)))
            }
            Answer::FieldOrNull(key) => {
                ResponseData::Present(json!({ key: field(key).unwrap_or(Value::Null) }))
            }
            Answer::Wrapped(key) => {
                ResponseData::Present(json!({ key: data.unwrap_or(Value::Null) }))
            }
            Answer::LevelOrNull => ResponseData::Present(
                field("level")
                    .filter(|level| !level.is_null())
                    .map_or(Value::Null, |level| json!({ "level": level })),
            ),
            Answer::ForkSelection => {
                let mut object = Map::new();
                if let Some(text) = field("selectedText") {
                    object.insert("text".to_string(), text);
                }
                object.insert(
                    "cancelled".to_string(),
                    field("cancelled").unwrap_or(Value::Bool(false)),
                );
                ResponseData::Present(Value::Object(object))
            }
            Answer::CommandSummaries => {
                let commands = field("commands")
                    .and_then(|commands| commands.as_array().cloned())
                    .unwrap_or_default()
                    .iter()
                    .map(|command| {
                        picked(&["name", "description", "source", "sourceInfo"], command)
                    })
                    .collect::<Vec<_>>();
                ResponseData::Present(json!({ "commands": commands }))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Lane {
    Prompt,
    Observation,
}

/// What ends the process from outside the stdin loop; it can arrive at any
/// point, including while the EOF settle runs.
enum Interrupt {
    /// SIGTERM (143) or SIGHUP (129).
    Signal { exit_code: i32 },
    /// The bound session closed or the daemon link dropped: exit 1 with the reason.
    Lost(String),
}

/// Why the stdin loop stopped serving.
enum Ending {
    /// Stdin closed: settle the session, release it, exit 0.
    InputClosed,
    Interrupted(Interrupt),
}

/// The signal exit's budget for its own daemon requests (abort, release):
/// a hung daemon must not hold the 143/129 exit.
const SIGNAL_RELEASE_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

/// The daemon session this connection serves.
struct Binding {
    /// Empty until the startup create answers; a `session_binding` rebind moves it.
    active_session_id: String,
    /// `ClientOwned` until a schedule promotes the session: the teardown
    /// completes an owned session and detaches from a resident one.
    lifecycle: DaemonSessionLifecycle,
    /// Set once this client asked the daemon to complete the session: the
    /// session close that follows is expected, not a loss.
    completing: bool,
    /// The supervisor's announced stop, which explains the session close that follows.
    daemon_closing: Option<DaemonClosingReason>,
}

/// The stdout side of the streamed sessions.
#[derive(Default)]
struct Output {
    /// `Some` while a `prompt` response is pending: the session events wait
    /// here and follow the response (TS `promptResponsePending`).
    prompt_buffer: Option<Vec<Value>>,
    /// By the id the client observed; frames keep reporting that id.
    observed: HashMap<String, Observed>,
}

/// One observed session.
struct Observed {
    /// The daemon id the session streams under now. Only frames from this
    /// id reach the observation: a rebind moves it, and the superseded ids'
    /// late frames match nothing.
    current: String,
    state: Observation,
}

/// An observed session, by the state of its `observe` response.
enum Observation {
    /// The response is not written yet: the session's frames wait here.
    Pending {
        frames: Vec<Value>,
        closed: bool,
    },
    Live,
}

impl Output {
    /// The observed id of the session streaming under `current` now.
    fn observed_id_of(&self, current: &str) -> Option<String> {
        self.observed
            .iter()
            .find(|(_, observed)| observed.current == current)
            .map(|(observed_id, _)| observed_id.clone())
    }

    /// Publish an observed session's close and retire it: at once when
    /// live, after the `observe` response when it is still pending.
    fn close_observed(&mut self, writer: &LineWriter, observed_id: &str, error: &str) {
        let frame = json!({
            "type": "observed_session_closed",
            "activeSessionId": observed_id,
            "error": error,
        });
        match self.observed.remove(observed_id) {
            Some(Observed {
                state: Observation::Live,
                ..
            }) => self.emit(writer, frame),
            Some(Observed {
                current,
                state: Observation::Pending { mut frames, .. },
            }) => {
                frames.push(frame);
                self.observed.insert(
                    observed_id.to_string(),
                    Observed {
                        current,
                        state: Observation::Pending {
                            frames,
                            closed: true,
                        },
                    },
                );
            }
            None => {}
        }
    }

    fn emit(&mut self, writer: &LineWriter, frame: Value) {
        match self.prompt_buffer.as_mut() {
            Some(buffer) => buffer.push(frame),
            None => writer.write(frame),
        }
    }
}

struct RpcConnection {
    link: Arc<DaemonLink>,
    writer: LineWriter,
    binding: Mutex<Binding>,
    output: Mutex<Output>,
    /// What ends the process from outside the stdin loop.
    interrupts: tokio::sync::mpsc::UnboundedSender<Interrupt>,
}

impl RpcConnection {
    fn active_session_id(&self) -> String {
        self.binding.lock().unwrap().active_session_id.clone()
    }

    /// Send one daemon command (a JSON object in the daemon wire shape) and
    /// return its `data`. The link close bounds the wait (turn-long commands).
    async fn send(&self, command: Value) -> Result<Option<Value>, String> {
        let command = serde_json::from_value::<DaemonCommand>(command)
            .map_err(|error| format!("Invalid command: {error}"))?;
        let response = self
            .link
            .request_until_close(command)
            .await
            .map_err(|error| format!("{error:#}"))?;
        if response.success {
            Ok(response.data)
        } else {
            Err(response
                .error
                .unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    /// Answer one command, then write the response: a prompt's buffered
    /// events follow its response, and an observed session's frames follow
    /// its `observe` response.
    async fn dispatch(&self, command: RpcCommand, route: RpcRoute) {
        let gated = route.lane() == Some(Lane::Prompt);
        if gated {
            self.output.lock().unwrap().prompt_buffer = Some(Vec::new());
        }
        let response = self.answer(&command, route).await;
        let mut output = self.output.lock().unwrap();
        self.writer.write(response);
        if route == RpcRoute::Observe {
            let target = command.payload["activeSessionId"]
                .as_str()
                .unwrap_or_default();
            if let Some(Observed {
                state: Observation::Pending { frames, closed },
                ..
            }) = output.observed.get_mut(target)
            {
                let (frames, closed) = (std::mem::take(frames), *closed);
                for frame in frames {
                    output.emit(&self.writer, frame);
                }
                if closed {
                    output.observed.remove(target);
                } else if let Some(observed) = output.observed.get_mut(target) {
                    observed.state = Observation::Live;
                }
            }
        }
        if gated {
            for frame in output.prompt_buffer.take().unwrap_or_default() {
                self.writer.write(frame);
            }
        }
    }

    async fn answer(&self, command: &RpcCommand, route: RpcRoute) -> Value {
        let name = command.command.as_str();
        let payload = &command.payload;
        let bound = self.active_session_id();
        let outcome = match route {
            RpcRoute::Forward {
                daemon_type,
                target,
                answer,
            } => {
                let mut object = payload.as_object().cloned().unwrap_or_default();
                object.remove("id");
                object.insert("type".to_string(), json!(daemon_type));
                let promotes = target == Target::BoundPromoting
                    && self.binding.lock().unwrap().lifecycle
                        == DaemonSessionLifecycle::ClientOwned;
                match target {
                    Target::Bound | Target::BoundPromoting => {
                        object.insert("activeSessionId".to_string(), json!(bound));
                    }
                    Target::Payload => {}
                    Target::Sender => {
                        object.insert("fromActiveSessionId".to_string(), json!(bound));
                    }
                }
                if promotes {
                    object.insert("promoteOwnedSession".to_string(), json!(true));
                }
                let sent = self.send(Value::Object(object)).await;
                if promotes && sent.is_ok() {
                    self.binding.lock().unwrap().lifecycle = DaemonSessionLifecycle::Resident;
                }
                sent.map(|data| answer.shape(data))
            }
            RpcRoute::SessionName => {
                let name = payload["name"].as_str().unwrap_or_default().trim();
                if name.is_empty() {
                    Err("Session name cannot be empty".to_string())
                } else {
                    self.send(json!({
                        "type": "set_session_name",
                        "activeSessionId": bound,
                        "name": name,
                    }))
                    .await
                    .map(|_| ResponseData::Absent)
                }
            }
            RpcRoute::ForkMessages => self
                .send(json!({
                    "type": "get_user_messages_for_forking",
                    "activeSessionId": bound,
                    "scope": "activeBranch",
                }))
                .await
                .map(|data| Answer::Field("messages").shape(data)),
            RpcRoute::Clone => {
                let leaf = self
                    .send(json!({ "type": "get_connection_state", "activeSessionId": bound }))
                    .await
                    .map(|state| {
                        state
                            .and_then(|state| state.get("leafId").cloned())
                            .filter(Value::is_string)
                    });
                match leaf {
                    Ok(Some(leaf)) => self
                        .send(json!({
                            "type": "fork",
                            "activeSessionId": bound,
                            "entryId": leaf,
                            "position": "at",
                        }))
                        .await
                        .map(|data| Answer::Field("cancelled").shape(data)),
                    Ok(None) => Err("Cannot clone session: no current entry selected".to_string()),
                    Err(error) => Err(error),
                }
            }
            // The bound session shares the link's one attachment for its id:
            // observing it would let `unobserve` detach the main stream.
            RpcRoute::Observe if payload["activeSessionId"] == bound.as_str() => Err(format!(
                "Cannot observe the session this connection is bound to: {bound}"
            )),
            RpcRoute::Observe => {
                let target = payload["activeSessionId"].as_str().unwrap_or_default();
                let observed = self.output.lock().unwrap().observed.contains_key(target);
                let attached = if observed {
                    Ok(())
                } else {
                    // Registered before the attach: the frames streamed
                    // from the attach on wait for the response.
                    self.output.lock().unwrap().observed.insert(
                        target.to_string(),
                        Observed {
                            current: target.to_string(),
                            state: Observation::Pending {
                                frames: Vec::new(),
                                closed: false,
                            },
                        },
                    );
                    self.send(json!({ "type": "attach", "activeSessionId": target }))
                        .await
                        .map(|_| ())
                        .map_err(|_| format!("Unknown active session: {target}"))
                };
                let messages = match attached {
                    Ok(()) => {
                        let current = self
                            .output
                            .lock()
                            .unwrap()
                            .observed
                            .get(target)
                            .map_or_else(
                                || target.to_string(),
                                |observed| observed.current.clone(),
                            );
                        self.send(json!({ "type": "get_messages", "activeSessionId": current }))
                            .await
                    }
                    Err(error) => Err(error),
                };
                if messages.is_err() && !observed {
                    self.output.lock().unwrap().observed.remove(target);
                    let _ = self
                        .send(json!({ "type": "detach", "activeSessionId": target }))
                        .await;
                }
                messages.map(|data| Answer::Field("messages").shape(data))
            }
            RpcRoute::Unobserve => {
                let target = payload["activeSessionId"].as_str().unwrap_or_default();
                let observed = self.output.lock().unwrap().observed.remove(target);
                if let Some(observed) = observed {
                    let _ = self
                        .send(json!({ "type": "detach", "activeSessionId": observed.current }))
                        .await;
                }
                Ok(ResponseData::Absent)
            }
            // TS answers an unknown type without the id.
            RpcRoute::Unknown => {
                return protocol::error(None, name, &format!("Unknown command: {name}"));
            }
        };
        match outcome {
            Ok(data) => protocol::success(command.id.as_ref(), name, data),
            Err(message) => protocol::error(command.id.as_ref(), name, &message),
        }
    }

    /// Release the bound session: complete it while this client owns it
    /// (the worker stops), detach from it once a schedule promoted it.
    async fn release_session(&self) -> Result<(), String> {
        let (active_session_id, release) = {
            let mut binding = self.binding.lock().unwrap();
            let release = match binding.lifecycle {
                DaemonSessionLifecycle::ClientOwned => {
                    binding.completing = true;
                    "complete_owned_session"
                }
                DaemonSessionLifecycle::Resident => "detach",
            };
            (binding.active_session_id.clone(), release)
        };
        self.send(json!({ "type": release, "activeSessionId": active_session_id }))
            .await
            .map(|_| ())
    }

    /// Exit on an interrupt, wherever the stdin loop or the EOF settle is.
    /// The stdin reader thread never returns on its own, so the exit must
    /// not wait for the runtime to join it.
    async fn exit_on(&self, interrupt: Interrupt) -> ! {
        match interrupt {
            Interrupt::Signal { exit_code } => {
                // An owned session stops with this client: its turn settles
                // aborted first. A promoted one keeps running in the daemon.
                let _ = tokio::time::timeout(SIGNAL_RELEASE_BUDGET, async {
                    let owned = self.binding.lock().unwrap().lifecycle
                        == DaemonSessionLifecycle::ClientOwned;
                    if owned {
                        let _ = self
                            .send(json!({
                                "type": "abort",
                                "activeSessionId": self.active_session_id(),
                            }))
                            .await;
                    }
                    let _ = self.release_session().await;
                })
                .await;
                self.writer.drain_bounded().await;
                std::process::exit(exit_code)
            }
            Interrupt::Lost(reason) => {
                self.writer.drain_bounded().await;
                eprintln!("Error: {reason}");
                std::process::exit(1)
            }
        }
    }

    /// Route one link event to stdout. Returns the interrupt a closed bound session causes.
    fn route_event(self: &Arc<Self>, event: LinkEvent) -> Option<Interrupt> {
        match event {
            LinkEvent::Session {
                active_session_id,
                event,
            } => {
                let bound = self.binding.lock().unwrap().active_session_id == active_session_id;
                let mut output = self.output.lock().unwrap();
                if bound {
                    output.emit(&self.writer, event);
                    return None;
                }
                let observed_id = output.observed_id_of(&active_session_id)?;
                let frame = json!({
                    "type": "observed_session_event",
                    "activeSessionId": observed_id,
                    "event": event,
                });
                match output.observed.get_mut(&observed_id) {
                    Some(Observed {
                        state: Observation::Pending { frames, .. },
                        ..
                    }) => frames.push(frame),
                    Some(Observed {
                        state: Observation::Live,
                        ..
                    }) => output.emit(&self.writer, frame),
                    None => {}
                }
                None
            }
            LinkEvent::SessionClosed {
                active_session_id,
                reason,
            } => {
                let (bound, daemon_closing) = {
                    let binding = self.binding.lock().unwrap();
                    (
                        binding.active_session_id == active_session_id,
                        binding.daemon_closing,
                    )
                };
                // An announced daemon stop closes its sessions as killed:
                // the announcement is the reason the client reads.
                let reason = match daemon_closing {
                    Some(DaemonClosingReason::Shutdown) => DaemonSessionClosedReason::Shutdown,
                    Some(DaemonClosingReason::Update) => DaemonSessionClosedReason::Update,
                    None => reason,
                };
                let error = session_closed_error(reason);
                if bound {
                    let completing = self.binding.lock().unwrap().completing;
                    return (!completing).then(|| Interrupt::Lost(error.to_string()));
                }
                let mut output = self.output.lock().unwrap();
                if let Some(observed_id) = output.observed_id_of(&active_session_id) {
                    output.close_observed(&self.writer, &observed_id, error);
                }
                None
            }
            // A worker replacement moved a session to a new id. The routing
            // follows at once and the link attaches the new id in the
            // background (the supervisor does not move the subscription),
            // like the TUI's rebind. A refused attach ends the stream it was
            // for, unless a newer binding or an unobserve moved on since.
            LinkEvent::SessionBinding {
                previous_active_session_id,
                active_session_id,
            } => {
                let bound = {
                    let mut binding = self.binding.lock().unwrap();
                    let bound = binding.active_session_id == previous_active_session_id;
                    if bound {
                        binding.active_session_id.clone_from(&active_session_id);
                    }
                    bound
                };
                let observed = {
                    let mut output = self.output.lock().unwrap();
                    let observed_id = output.observed_id_of(&previous_active_session_id);
                    if let Some(observed) = observed_id
                        .as_ref()
                        .and_then(|observed_id| output.observed.get_mut(observed_id))
                    {
                        observed.current.clone_from(&active_session_id);
                    }
                    observed_id.is_some()
                };
                if bound || observed {
                    let connection = Arc::clone(self);
                    tokio::spawn(async move {
                        let Err(refusal) = connection
                            .send(json!({ "type": "attach", "activeSessionId": active_session_id }))
                            .await
                        else {
                            return;
                        };
                        let error = format!(
                            "The session moved to {active_session_id}, and attaching it failed: {refusal}"
                        );
                        if connection.active_session_id() == active_session_id {
                            let _ = connection.interrupts.send(Interrupt::Lost(error.clone()));
                        }
                        let mut output = connection.output.lock().unwrap();
                        if let Some(observed_id) = output.observed_id_of(&active_session_id) {
                            output.close_observed(&connection.writer, &observed_id, &error);
                        }
                    });
                }
                None
            }
            LinkEvent::DaemonClosing(reason) => {
                self.binding.lock().unwrap().daemon_closing = Some(reason);
                None
            }
            // No rpc frame: clients re-read `list_heartbeats` after a turn settles.
            LinkEvent::HeartbeatsChanged => None,
        }
    }
}

/// The client-facing explanation of a session close (TS
/// `formatDaemonSessionClosedError`).
fn session_closed_error(reason: DaemonSessionClosedReason) -> &'static str {
    match reason {
        DaemonSessionClosedReason::Killed => "The daemon stopped this agent session. Its transcript remains saved and can be reopened from Agents View.",
        DaemonSessionClosedReason::Shutdown => "The Prime Agent daemon shut down while this window was attached. The session transcript remains saved; restart Prime Agent and reopen it from Agents View.",
        DaemonSessionClosedReason::Completed => "The daemon closed this agent session after it completed. Its transcript remains available from Agents View.",
        DaemonSessionClosedReason::Replaced => "The daemon replaced this agent session with another session. Reopen the current session from Agents View.",
        DaemonSessionClosedReason::Update => "The Prime Agent daemon restarted for an update, but this window did not restore automatically. The session transcript remains saved; restart Prime Agent and reopen it from Agents View.",
    }
}

/// Serve the daemon-attached RPC mode until stdin closes, a signal
/// arrives, or the session is lost. The caller guarantees the socket
/// answers (the composition spawns a supervisor when none is listening).
///
/// # Errors
///
/// Returns an error, before any RPC frame is written, when the daemon
/// socket connect, the startup create, or its attach fails.
///
/// # Panics
///
/// Panics when a connection state lock is poisoned (a holder panicked).
pub async fn run_daemon_attached_rpc_mode(options: DaemonRpcOptions) -> anyhow::Result<i32> {
    let link = Arc::new(DaemonLink::connect(&options.socket_path, LinkClient::Rpc).await?);
    let DaemonCommand::Create { lifecycle, .. } = &options.create else {
        anyhow::bail!("the startup command must create the session");
    };
    // An unspecified lifecycle is resident (the supervisor's reading).
    let lifecycle = lifecycle.unwrap_or(DaemonSessionLifecycle::Resident);
    let (interrupts_tx, mut interrupts) = tokio::sync::mpsc::unbounded_channel::<Interrupt>();
    let connection = Arc::new(RpcConnection {
        link: Arc::clone(&link),
        writer: LineWriter::spawn(),
        binding: Mutex::new(Binding {
            active_session_id: String::new(),
            lifecycle,
            completing: false,
            daemon_closing: None,
        }),
        output: Mutex::new(Output::default()),
        interrupts: interrupts_tx,
    });
    {
        // The link answers requests only while its events are consumed,
        // so the consumer runs before the startup create.
        let connection = Arc::clone(&connection);
        tokio::spawn(async move {
            while let Some(event) = connection.link.next_event().await {
                if let Some(interrupt) = connection.route_event(event) {
                    let _ = connection.interrupts.send(interrupt);
                }
            }
            let _ = connection.interrupts.send(Interrupt::Lost(
                "Lost connection to the Prime Agent daemon.".to_string(),
            ));
        });
    }
    let created = link.request(options.create).await?;
    if !created.success {
        anyhow::bail!(created.error.unwrap_or_else(|| "unknown error".to_string()));
    }
    let active_session_id = created
        .data
        .as_ref()
        .and_then(|summary| summary.get("activeSessionId"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    connection
        .binding
        .lock()
        .unwrap()
        .active_session_id
        .clone_from(&active_session_id);
    let attached = link
        .request(DaemonCommand::Attach {
            id: None,
            active_session_id,
            client_id: None,
            capabilities: None,
            resume_cursor: None,
            telemetry_disabled: None,
            recovery_config: None,
            env: None,
            launch_env: None,
            rest: Map::default(),
        })
        .await;
    match attached {
        Ok(response) if response.success => {}
        failed => {
            let _ = connection.release_session().await;
            anyhow::bail!(match failed {
                Ok(response) => response
                    .error
                    .unwrap_or_else(|| "unknown error".to_string()),
                Err(error) => format!("{error:#}"),
            });
        }
    }
    #[cfg(unix)]
    for (kind, exit_code) in [
        (tokio::signal::unix::SignalKind::terminate(), 143),
        (tokio::signal::unix::SignalKind::hangup(), 129),
    ] {
        let mut signals = tokio::signal::unix::signal(kind)?;
        let interrupts = connection.interrupts.clone();
        tokio::spawn(async move {
            if signals.recv().await.is_some() {
                let _ = interrupts.send(Interrupt::Signal { exit_code });
            }
        });
    }

    let mut handlers = tokio::task::JoinSet::new();
    let mut lane_tails: HashMap<Lane, tokio::sync::oneshot::Receiver<()>> = HashMap::new();
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut input_line = String::new();
    let ending = loop {
        input_line.clear();
        tokio::select! {
            Some(interrupt) = interrupts.recv() => break Ending::Interrupted(interrupt),
            read = stdin.read_line(&mut input_line) => {
                match read {
                    Ok(0) | Err(_) => break Ending::InputClosed,
                    Ok(_) => {}
                }
            }
        }
        if input_line.trim().is_empty() {
            continue;
        }
        match protocol::parse_line(&input_line) {
            ParsedLine::ParseError(response) => connection.writer.write(response),
            ParsedLine::Command(command) => {
                let route = RpcRoute::of(&command.command);
                let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                let previous = route
                    .lane()
                    .and_then(|lane| lane_tails.insert(lane, done_rx));
                let connection = Arc::clone(&connection);
                handlers.spawn(async move {
                    if let Some(previous) = previous {
                        let _ = previous.await;
                    }
                    connection.dispatch(command, route).await;
                    let _ = done_tx.send(());
                });
            }
        }
        while handlers.try_join_next().is_some() {}
    };
    match ending {
        Ending::InputClosed => {
            let settle = async {
                while handlers.join_next().await.is_some() {}
                connection
                    .send(json!({
                        "type": "wait_for_idle",
                        "activeSessionId": connection.active_session_id(),
                    }))
                    .await?;
                connection.release_session().await?;
                connection.writer.drain().await;
                Ok::<(), String>(())
            };
            // Interrupts stay live through the settle. The settle wins a
            // tie: its own request failures already report a loss.
            tokio::select! {
                biased;
                settled = settle => match settled {
                    Ok(()) => Ok(0),
                    Err(reason) => connection.exit_on(Interrupt::Lost(reason)).await,
                },
                Some(interrupt) = interrupts.recv() => connection.exit_on(interrupt).await,
            }
        }
        Ending::Interrupted(interrupt) => connection.exit_on(interrupt).await,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use pa_types::daemon::{DaemonCommandEnvelope, DaemonResponse};

    use super::*;

    #[test]
    fn every_advertised_rpc_command_has_a_route() {
        for command in RPC_COMMANDS {
            assert_ne!(RpcRoute::of(command), RpcRoute::Unknown, "{command}");
        }
    }

    /// A connection bound to `root` over a scripted link, observing each of
    /// `observed` live. A pending prompt response holds every emitted frame
    /// in the prompt buffer, which the tests read as stdout.
    struct Scripted {
        connection: Arc<RpcConnection>,
        commands: tokio::sync::mpsc::UnboundedReceiver<String>,
        pending: crate::daemon_link::PendingResponses,
        interrupts: tokio::sync::mpsc::UnboundedReceiver<Interrupt>,
    }

    impl Scripted {
        fn new(observed: &[&str]) -> Scripted {
            let (writer, commands) = tokio::sync::mpsc::unbounded_channel::<String>();
            let pending = Arc::new(std::sync::Mutex::new(HashMap::new()));
            let (interrupts_tx, interrupts) = tokio::sync::mpsc::unbounded_channel();
            let connection = Arc::new(RpcConnection {
                link: Arc::new(DaemonLink::scripted(writer, Arc::clone(&pending))),
                writer: LineWriter::spawn(),
                binding: Mutex::new(Binding {
                    active_session_id: "root".to_string(),
                    lifecycle: DaemonSessionLifecycle::ClientOwned,
                    completing: false,
                    daemon_closing: None,
                }),
                output: Mutex::new(Output {
                    prompt_buffer: Some(Vec::new()),
                    observed: observed
                        .iter()
                        .map(|id| {
                            (
                                (*id).to_string(),
                                Observed {
                                    current: (*id).to_string(),
                                    state: Observation::Live,
                                },
                            )
                        })
                        .collect(),
                }),
                interrupts: interrupts_tx,
            });
            Scripted {
                connection,
                commands,
                pending,
                interrupts,
            }
        }

        fn bind(&self, previous: &str, current: &str) {
            self.connection.route_event(LinkEvent::SessionBinding {
                previous_active_session_id: previous.to_string(),
                active_session_id: current.to_string(),
            });
        }

        fn event(&self, active_session_id: &str) {
            self.connection.route_event(LinkEvent::Session {
                active_session_id: active_session_id.to_string(),
                event: json!({ "type": "agent_end" }),
            });
        }

        /// The next command line the connection sent, as the daemon reads it.
        async fn next_command(&mut self) -> DaemonCommandEnvelope {
            let line = tokio::time::timeout(Duration::from_secs(5), self.commands.recv())
                .await
                .expect("the connection sends a command")
                .expect("the link writer is open");
            serde_json::from_str(&line).unwrap()
        }

        /// The next attach's session id; its answer is left to the test.
        async fn next_attach(&mut self) -> (String, String) {
            let envelope = self.next_command().await;
            let DaemonCommand::Attach {
                active_session_id, ..
            } = envelope.command
            else {
                panic!("expected an attach, got {:?}", envelope.command);
            };
            (envelope.id, active_session_id)
        }

        fn answer(&self, envelope_id: &str, outcome: Result<(), &str>) {
            let reply = self.pending.lock().unwrap().remove(envelope_id).unwrap();
            reply
                .send(DaemonResponse {
                    id: Some(envelope_id.to_string()),
                    command: "attach".to_string(),
                    success: outcome.is_ok(),
                    data: None,
                    error: outcome.err().map(str::to_string),
                    error_info: None,
                })
                .unwrap();
        }

        fn stdout(&self) -> Vec<Value> {
            self.connection
                .output
                .lock()
                .unwrap()
                .prompt_buffer
                .clone()
                .unwrap_or_default()
        }
    }

    fn observed_end(observed_id: &str) -> Value {
        json!({
            "type": "observed_session_event",
            "activeSessionId": observed_id,
            "event": { "type": "agent_end" },
        })
    }

    /// A worker replacement rebinds the bound session and an observed child:
    /// the connection attaches both new ids and routes their events in their
    /// old roles (the bound stream, the child observed under the id the
    /// client asked for).
    #[tokio::test]
    async fn a_session_binding_attaches_and_routes_the_rebound_sessions() {
        let mut scripted = Scripted::new(&["kid"]);
        scripted.bind("root", "root-2");
        scripted.bind("kid", "kid-2");
        let mut attached = Vec::new();
        for _ in 0..2 {
            let (envelope_id, active_session_id) = scripted.next_attach().await;
            scripted.answer(&envelope_id, Ok(()));
            attached.push(active_session_id);
        }
        attached.sort();
        assert_eq!(attached, vec!["kid-2", "root-2"]);
        scripted.event("root-2");
        scripted.event("kid-2");
        assert_eq!(
            scripted.stdout(),
            vec![json!({ "type": "agent_end" }), observed_end("kid")],
            "the bound stream follows unwrapped, the child under its observed id"
        );
    }

    /// Frames from an observed child's superseded id neither close nor feed
    /// the observation once it streams under the new id.
    #[tokio::test]
    async fn a_superseded_id_cannot_close_the_rebound_observation() {
        let mut scripted = Scripted::new(&["kid"]);
        scripted.bind("kid", "kid-2");
        let (envelope_id, _) = scripted.next_attach().await;
        scripted.answer(&envelope_id, Ok(()));
        scripted.event("kid");
        scripted.connection.route_event(LinkEvent::SessionClosed {
            active_session_id: "kid".to_string(),
            reason: DaemonSessionClosedReason::Replaced,
        });
        scripted.event("kid-2");
        assert_eq!(scripted.stdout(), vec![observed_end("kid")]);
    }

    /// A refused reattach is never swallowed: the bound session's ends the
    /// process as a loss, an observed child's closes its observation. A
    /// newer binding makes the earlier attempt's result stale.
    #[tokio::test]
    async fn a_refused_reattach_ends_the_stream_it_was_for() {
        let mut scripted = Scripted::new(&["kid"]);
        scripted.bind("root", "root-2");
        scripted.bind("root-2", "root-3");
        scripted.bind("kid", "kid-2");
        scripted.bind("kid-2", "kid-3");
        let mut attaches = HashMap::new();
        for _ in 0..4 {
            let (envelope_id, active_session_id) = scripted.next_attach().await;
            attaches.insert(active_session_id, envelope_id);
        }
        for id in ["root-2", "kid-2", "kid-3", "root-3"] {
            scripted.answer(&attaches[id], Err("Session worker is overloaded"));
        }
        let interrupt = tokio::time::timeout(Duration::from_secs(5), scripted.interrupts.recv())
            .await
            .expect("the refusal interrupts the connection");
        let Some(Interrupt::Lost(reason)) = interrupt else {
            panic!("the refused bound reattach is a loss");
        };
        assert_eq!(
            reason,
            "The session moved to root-3, and attaching it failed: Session worker is overloaded"
        );
        assert!(
            scripted.interrupts.try_recv().is_err(),
            "the stale root-2 attempt raised nothing"
        );
        scripted.event("kid-3");
        assert_eq!(
            scripted.stdout(),
            vec![json!({
                "type": "observed_session_closed",
                "activeSessionId": "kid",
                "error": "The session moved to kid-3, and attaching it failed: Session worker is overloaded",
            })],
            "only the current attempt closes the observation, which then stops streaming"
        );
    }
}
