//! A client connection to the supervisor socket, shared by the headless
//! stdio transports (ACP and RPC): JSONL command envelopes out, responses
//! matched by id, and the session-scoped frames handed to the transport in
//! arrival order.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use pa_types::daemon::{
    DaemonClosingReason, DaemonCommand, DaemonCommandEnvelope, DaemonCommandFrameType,
    DaemonOutbound, DaemonProtocolInfo, DaemonResponse, DaemonSessionClosedReason,
    DAEMON_PROTOCOL_NAME, DAEMON_PROTOCOL_VERSION,
};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, Mutex};

/// Response timeout for session-scoped commands.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The requests still waiting for their response, by envelope id.
pub(crate) type PendingResponses =
    Arc<std::sync::Mutex<HashMap<String, oneshot::Sender<DaemonResponse>>>>;

/// Which stdio transport owns the link: it prefixes the envelope ids
/// (`acp-1`) and names the client (`acp:<pid>`), so the supervisor's
/// ownership and logs tell the transports apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkClient {
    Acp,
    Rpc,
}

impl LinkClient {
    fn label(self) -> &'static str {
        match self {
            LinkClient::Acp => "acp",
            LinkClient::Rpc => "rpc",
        }
    }
}

/// One non-response frame from the supervisor.
pub(crate) enum LinkEvent {
    /// A session's streamed event (`session_event`).
    Session {
        active_session_id: String,
        event: Value,
    },
    /// The daemon-global heartbeat-catalog broadcast the supervisor
    /// re-broadcasts to every client (`heartbeats_changed`).
    HeartbeatsChanged,
    /// An attached session ended (`session_closed`).
    SessionClosed {
        active_session_id: String,
        reason: DaemonSessionClosedReason,
    },
    /// A worker replacement rebound an attached session to a new id
    /// (`session_binding`).
    SessionBinding {
        previous_active_session_id: String,
        active_session_id: String,
    },
    /// The supervisor announced its own orderly stop (`daemon_closing`).
    DaemonClosing(DaemonClosingReason),
}

enum LinkFrame {
    Event(LinkEvent),
    Response(DaemonResponse),
}

pub(crate) struct DaemonLink {
    writer: mpsc::UnboundedSender<String>,
    pending: PendingResponses,
    /// Set once the frame channel ends (the socket closed and every
    /// frame the reader read is delivered): every new request fails
    /// fast instead of waiting for a response no one will send.
    closed: Arc<AtomicBool>,
    frames: Mutex<mpsc::UnboundedReceiver<LinkFrame>>,
    protocol_version: u64,
    client: LinkClient,
    next_request_id: AtomicU64,
}

impl DaemonLink {
    pub(crate) async fn connect(socket_path: &Path, client: LinkClient) -> anyhow::Result<Self> {
        let stream = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            pa_types::platform::transport::connect_transport(socket_path),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timed out connecting to the daemon socket"))??;
        let (reader_half, writer_half) = stream.split();
        let (line_tx, mut line_rx) = mpsc::unbounded_channel::<String>();
        let (frame_tx, frame_rx) = mpsc::unbounded_channel::<LinkFrame>();
        let (hello_tx, hello_rx) = oneshot::channel::<Value>();

        tokio::spawn(async move {
            let mut writer = writer_half;
            while let Some(line) = line_rx.recv().await {
                let mut payload = line.into_bytes();
                payload.push(b'\n');
                if writer.write_all(&payload).await.is_err() {
                    break;
                }
            }
            let _ = writer.shutdown().await;
        });
        // The reader only classifies frames; the consumer of `next_event`
        // owns the ordering, so a turn's chunks always precede its boundary
        // frames.
        tokio::spawn(async move {
            let mut reader = BufReader::new(reader_half);
            let mut line = String::new();
            let mut hello_tx = Some(hello_tx);
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
                let Ok(value) = serde_json::from_str::<Value>(line.trim()) else {
                    continue;
                };
                let frame = match value
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                {
                    "daemon_hello" => {
                        if let Some(tx) = hello_tx.take() {
                            let _ = tx.send(value);
                        }
                        continue;
                    }
                    "response" => match serde_json::from_value::<DaemonResponse>(value) {
                        Ok(response) => LinkFrame::Response(response),
                        Err(_) => continue,
                    },
                    "heartbeats_changed" => LinkFrame::Event(LinkEvent::HeartbeatsChanged),
                    // The hot path: the event payload moves out as-is, never
                    // through the typed union's flatten buffering.
                    "session_event" => {
                        let Value::Object(mut frame) = value else {
                            continue;
                        };
                        let Some(Value::String(active_session_id)) =
                            frame.remove("activeSessionId")
                        else {
                            continue;
                        };
                        LinkFrame::Event(LinkEvent::Session {
                            active_session_id,
                            event: frame.remove("event").unwrap_or(Value::Null),
                        })
                    }
                    "session_closed" | "session_binding" | "daemon_closing" => {
                        match serde_json::from_value::<DaemonOutbound>(value) {
                            Ok(DaemonOutbound::SessionClosed {
                                active_session_id,
                                reason,
                                ..
                            }) => LinkFrame::Event(LinkEvent::SessionClosed {
                                active_session_id,
                                reason,
                            }),
                            Ok(DaemonOutbound::SessionBinding {
                                previous_active_session_id,
                                active_session_id,
                                ..
                            }) => LinkFrame::Event(LinkEvent::SessionBinding {
                                previous_active_session_id,
                                active_session_id,
                            }),
                            Ok(DaemonOutbound::DaemonClosing { reason, .. }) => {
                                LinkFrame::Event(LinkEvent::DaemonClosing(reason))
                            }
                            _ => continue,
                        }
                    }
                    _ => continue,
                };
                if frame_tx.send(frame).is_err() {
                    break;
                }
            }
        });

        let hello = tokio::time::timeout(std::time::Duration::from_secs(3), hello_rx)
            .await
            .map_err(|_| anyhow::anyhow!("the daemon did not send its handshake"))?
            .map_err(|_| anyhow::anyhow!("the daemon connection closed before the handshake"))?;
        let protocol = hello
            .get("protocol")
            .cloned()
            .and_then(|p| serde_json::from_value::<DaemonProtocolInfo>(p).ok())
            .unwrap_or(DaemonProtocolInfo {
                name: DAEMON_PROTOCOL_NAME.to_string(),
                version: DAEMON_PROTOCOL_VERSION,
            });
        let version = protocol.version.min(DAEMON_PROTOCOL_VERSION);
        Ok(DaemonLink {
            writer: line_tx,
            pending: Arc::new(std::sync::Mutex::new(HashMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
            frames: Mutex::new(frame_rx),
            protocol_version: version,
            client,
            next_request_id: AtomicU64::new(0),
        })
    }

    /// A link with no socket behind it: the test drives the command lines
    /// `writer` receives and answers through `pending`.
    #[cfg(test)]
    pub(crate) fn scripted(
        writer: mpsc::UnboundedSender<String>,
        pending: PendingResponses,
    ) -> Self {
        let (_frames_tx, frames) = mpsc::unbounded_channel();
        DaemonLink {
            writer,
            pending,
            closed: Arc::new(AtomicBool::new(false)),
            frames: Mutex::new(frames),
            protocol_version: DAEMON_PROTOCOL_VERSION,
            client: LinkClient::Acp,
            next_request_id: AtomicU64::new(0),
        }
    }

    /// The next non-response frame, in arrival order. The responses read
    /// before it resolve their waiting requests first, so a request's
    /// answer never overtakes the events the daemon sent ahead of it.
    /// `None` once the socket closed: every pending and later request fails.
    ///
    /// # Panics
    ///
    /// Panics when the pending-response map lock is poisoned (a holder
    /// panicked while holding it).
    pub(crate) async fn next_event(&self) -> Option<LinkEvent> {
        let mut frames = self.frames.lock().await;
        while let Some(frame) = frames.recv().await {
            match frame {
                LinkFrame::Event(event) => return Some(event),
                LinkFrame::Response(response) => {
                    let id = response.id.clone().unwrap_or_default();
                    if let Some(tx) = self.pending.lock().unwrap().remove(&id) {
                        let _ = tx.send(response);
                    }
                }
            }
        }
        // The reader hit the socket close and every frame it read has been
        // delivered: fail the still-pending requests (dropping a sender
        // answers with the mid-request error) and fail every later request
        // fast. The flag goes up before the clear; `exchange` covers the race.
        self.closed.store(true, Ordering::SeqCst);
        self.pending.lock().unwrap().clear();
        None
    }

    /// Send one command envelope and wait for the matching response
    /// under the session-scoped response timeout.
    pub(crate) async fn request(&self, command: DaemonCommand) -> anyhow::Result<DaemonResponse> {
        self.exchange(command, Some(REQUEST_TIMEOUT)).await
    }

    pub(crate) async fn request_ok(&self, command: DaemonCommand) -> anyhow::Result<()> {
        let response = self.request(command).await?;
        if !response.success {
            anyhow::bail!(response
                .error
                .unwrap_or_else(|| "unknown error".to_string()));
        }
        Ok(())
    }

    /// Send one command envelope with no fixed cap: the response or the
    /// link close ends the wait (turn-long commands — declared
    /// divergence, TS caps them at 24 h; the link-close signal is the
    /// liveness bound here, not a timer).
    pub(crate) async fn request_until_close(
        &self,
        command: DaemonCommand,
    ) -> anyhow::Result<DaemonResponse> {
        self.exchange(command, None).await
    }

    /// One exchange: register the response slot, send the envelope, and
    /// wait for the answer.
    async fn exchange(
        &self,
        command: DaemonCommand,
        timeout: Option<std::time::Duration>,
    ) -> anyhow::Result<DaemonResponse> {
        let label = self.client.label();
        let id = format!(
            "{label}-{}",
            self.next_request_id.fetch_add(1, Ordering::SeqCst) + 1
        );
        let envelope = DaemonCommandEnvelope {
            frame_type: DaemonCommandFrameType::Command,
            id: id.clone(),
            protocol: DaemonProtocolInfo {
                name: DAEMON_PROTOCOL_NAME.to_string(),
                version: self.protocol_version,
            },
            client_id: Some(format!("{label}:{}", std::process::id())),
            command,
        };
        let line = serde_json::to_string(&envelope)?;
        let (tx, rx) = oneshot::channel::<DaemonResponse>();
        self.pending.lock().unwrap().insert(id.clone(), tx);
        // A request that races the close loses either way: the flag
        // fails it here, or the pending clear already dropped its
        // sender. The flag is stored before the clear, so no request
        // parks unnoticed.
        if self.closed.load(Ordering::SeqCst) {
            self.pending.lock().unwrap().remove(&id);
            anyhow::bail!("the daemon connection is closed");
        }
        if self.writer.send(line).is_err() {
            // A closed writer leaves the pending slot behind otherwise; a
            // link that never answers again would grow one entry per
            // request.
            self.pending.lock().unwrap().remove(&id);
            anyhow::bail!("the daemon connection is closed");
        }
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, rx).await.map_err(|_| {
                self.pending.lock().unwrap().remove(&id);
                anyhow::anyhow!("timed out waiting for the daemon response")
            })?,
            None => rx.await,
        }
        .map_err(|_| anyhow::anyhow!("the daemon connection closed mid-request"))
    }
}
