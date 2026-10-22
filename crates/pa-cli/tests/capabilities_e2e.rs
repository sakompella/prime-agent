//! `prime-agent --capabilities`: the fork identity T3 Code reads to tell this
//! fork's rpc contract apart from stock upstream 0.10, which prints the same
//! `--version`.
#![cfg(unix)]

use std::process::Command;

use serde_json::Value;

#[test]
fn capabilities_report_the_fork_and_its_rpc_contract() {
    let home = tempfile::tempdir().expect("temp HOME");
    let output = Command::new(env!("CARGO_BIN_EXE_prime-agent"))
        .arg("--capabilities")
        .env("HOME", home.path())
        .env_remove("PRIME_AGENT_CODING_AGENT_DIR")
        .env_remove("PI_CODING_AGENT_DIR")
        .output()
        .expect("run prime-agent --capabilities");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "exit {:?}, stdout {stdout:?}, stderr {:?}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 1, "one JSON line: {stdout:?}");
    let capabilities: Value = serde_json::from_str(lines[0]).expect("JSON");
    assert_eq!(capabilities["fork"], "t3fork", "{capabilities}");
    assert_eq!(capabilities["contract"], 2, "{capabilities}");
    assert_eq!(
        capabilities["version"],
        env!("CARGO_PKG_VERSION"),
        "{capabilities}"
    );
    let rpc: Vec<&str> = capabilities["rpc"]
        .as_array()
        .expect("rpc command list")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    for command in [
        "prompt",
        "navigate_tree",
        "get_fork_messages",
        "observe",
        "unobserve",
    ] {
        assert!(rpc.contains(&command), "{command} missing from {rpc:?}");
    }
    assert!(
        std::fs::read_dir(home.path())
            .expect("HOME")
            .next()
            .is_none(),
        "--capabilities writes nothing under HOME"
    );
}
