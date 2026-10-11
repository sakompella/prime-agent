//! The daemon-attached RPC child the rpc e2e crates share: the real binary
//! in `--mode rpc` against a sandboxed supervisor on `<home>/daemon.sock`,
//! hosting the scripted faux worker through the `PRIME_AGENT_FAUX_SCRIPT`
//! create-config seam. The drop shuts the supervisor down with the child.

// Each test crate uses a subset of the harness.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

pub const TIMEOUT: Duration = Duration::from_secs(60);

pub struct RpcChild {
    pub child: Child,
    /// `None` once [`RpcChild::close_stdin`] sent EOF.
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    next_id: u64,
    /// The child's HOME and cwd: it must outlive the child.
    home: tempfile::TempDir,
    pub socket: PathBuf,
    stderr: Option<ChildStderr>,
}

impl RpcChild {
    pub fn spawn(args: &[&str], script: &Value) -> RpcChild {
        Self::spawn_configured(args, script, |_, _| {})
    }

    /// Spawn after `configure` seeded the home or extended the command.
    pub fn spawn_configured(
        args: &[&str],
        script: &Value,
        configure: impl FnOnce(&Path, &mut Command),
    ) -> RpcChild {
        let home = tempfile::TempDir::new().unwrap();
        let socket = home.path().join("daemon.sock");
        std::fs::write(home.path().join("worker-script.json"), script.to_string()).unwrap();
        let mut command = rpc_command(home.path(), &socket, args);
        configure(home.path(), &mut command);
        let mut child = command.spawn().expect("binary present");
        let stdin = child.stdin.take().expect("stdin piped");
        let stdout = child.stdout.take().expect("stdout piped");
        let stderr = child.stderr.take().expect("stderr piped");
        let (tx, lines) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        RpcChild {
            child,
            stdin: Some(stdin),
            lines,
            next_id: 0,
            home,
            socket,
            stderr: Some(stderr),
        }
    }

    /// The scripted-children lane: `rlm.spawn` children run `child_script`
    /// (the `PRIME_AGENT_FAUX_CHILD_SCRIPT` seam) and the worker kernel runs
    /// `kernel_python` (the env flows rpc -> supervisor -> worker).
    pub fn spawn_with_children(
        args: &[&str],
        script: &Value,
        child_script: &Value,
        kernel_python: &Path,
    ) -> RpcChild {
        Self::spawn_configured(args, script, |home, command| {
            let child_script_path = home.join("child-script.json");
            std::fs::write(&child_script_path, child_script.to_string()).unwrap();
            command
                .env("PRIME_AGENT_FAUX_CHILD_SCRIPT", &child_script_path)
                .env("PRIME_AGENT_KERNEL_PYTHON", kernel_python);
        })
    }

    pub fn home(&self) -> &Path {
        self.home.path()
    }

    /// The agent dir the sandboxed daemon runs with.
    pub fn agent_dir(&self) -> PathBuf {
        self.home.path().join(".prime/agent")
    }

    pub fn send(&mut self, frame: &Value) {
        let mut line = serde_json::to_string(frame).unwrap();
        line.push('\n');
        let stdin = self.stdin.as_mut().expect("stdin open");
        stdin.write_all(line.as_bytes()).unwrap();
        stdin.flush().unwrap();
    }

    /// Send EOF: the client disconnects.
    pub fn close_stdin(&mut self) {
        self.stdin = None;
    }

    /// Send `command` with a fresh id; returns the id.
    pub fn command(&mut self, command: &Value) -> String {
        self.next_id += 1;
        let id = format!("t-{}", self.next_id);
        let mut frame = command.clone();
        frame["id"] = json!(id);
        self.send(&frame);
        id
    }

    /// Read frames until one matches (the readiness signal, never a timer);
    /// returns it with the frames read before it.
    pub fn wait_frame(
        &mut self,
        mut frame_matches: impl FnMut(&Value) -> bool,
    ) -> (Value, Vec<Value>) {
        let deadline = Instant::now() + TIMEOUT;
        let mut before = Vec::new();
        loop {
            let timeout_left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(timeout_left) {
                Ok(line) => {
                    let frame: Value = serde_json::from_str(&line).expect("valid JSON line");
                    if frame_matches(&frame) {
                        return (frame, before);
                    }
                    before.push(frame);
                }
                Err(RecvTimeoutError::Timeout) => {
                    panic!("timed out waiting for a frame (read: {before:?})")
                }
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("rpc child stdout closed (read: {before:?})")
                }
            }
        }
    }

    /// Read frames until the response to `id`; returns it with the frames before it.
    pub fn wait_response(&mut self, id: &str) -> (Value, Vec<Value>) {
        self.wait_frame(|frame| frame["type"] == "response" && frame["id"] == id)
    }

    /// Read frames until one of `event_type` arrives.
    pub fn wait_event(&mut self, event_type: &str) -> Value {
        self.wait_frame(|frame| frame["type"] == event_type).0
    }

    pub fn request(&mut self, command: &Value) -> Value {
        let id = self.command(command);
        self.wait_response(&id).0
    }

    /// Wait for the child to exit; stdout must close with it.
    pub fn wait_exit(&mut self) -> std::process::ExitStatus {
        self.child.wait().expect("the child exits")
    }

    /// The child's stderr so far (call after it exited).
    pub fn stderr_text(&mut self) -> String {
        let mut text = String::new();
        if let Some(mut stderr) = self.stderr.take() {
            let _ = stderr.read_to_string(&mut text);
        }
        text
    }
}

impl Drop for RpcChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let text = self.stderr_text();
        if !text.is_empty() {
            eprintln!("RPC child stderr: {text}");
        }
        shutdown_sandboxed_daemon(&self.socket);
    }
}

/// The daemon-attached child's command on `home` and `socket`: the
/// supervisor-lost exit runs on a short window (the env flows child ->
/// supervisor -> worker), the worker script is `<home>/worker-script.json`,
/// and the worker kernel reuses an installed interpreter when one exists (a
/// fresh home otherwise provisions its own venv, which a whole-session
/// replacement waits out past the 30 s route budget on a loaded host).
pub fn rpc_command(home: &Path, socket: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_prime-agent"));
    command
        .args(args)
        .arg("--daemon-socket")
        .arg(socket)
        .env("HOME", home)
        .env("PRIME_AGENT_FAUX_SCRIPT", home.join("worker-script.json"))
        .env(
            pa_daemon::worker::WORKER_SUPERVISOR_LOST_EXIT_MS_ENV,
            "15000",
        )
        .current_dir(home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(kernel_python) = kernel_python() {
        command.env("PRIME_AGENT_KERNEL_PYTHON", kernel_python);
    }
    command
}

pub fn shutdown_sandboxed_daemon(socket: &Path) {
    let Ok(mut stream) = pa_types::platform::transport::connect_blocking(socket) else {
        return;
    };
    let frame = format!(
        "{{\"type\":\"command\",\"id\":\"shutdown-test\",\"protocol\":{{\"name\":\"prime-agent.daemon\",\"version\":{}}},\"command\":{{\"type\":\"shutdown\"}}}}\n",
        pa_types::daemon::DAEMON_PROTOCOL_VERSION
    );
    let _ = stream.write_all(frame.as_bytes());
    let _ = stream.flush();
    // The supervisor exits after the shutdown response.
    std::thread::sleep(Duration::from_millis(300));
}

/// The kernel interpreter the RLM lanes need: `PA_E2E_KERNEL_PYTHON`, else
/// the installed kernel venv; `None` skips the lane with a note.
pub fn kernel_python() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("PA_E2E_KERNEL_PYTHON") {
        let explicit = PathBuf::from(explicit);
        assert!(
            explicit.exists(),
            "PA_E2E_KERNEL_PYTHON {} not found",
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
        "kernel python {} not found; skipping the RLM e2e lane",
        candidate.display()
    );
    None
}

/// The text of a message's first text content part.
pub fn first_text(message: &Value) -> &str {
    message["content"]
        .as_array()
        .and_then(|content| content.iter().find(|part| part["type"] == "text"))
        .and_then(|part| part["text"].as_str())
        .or_else(|| message["content"].as_str())
        .unwrap_or_default()
}
