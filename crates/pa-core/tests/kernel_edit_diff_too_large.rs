// Pedantic-gate dispositions as src/lib.rs.
#![allow(clippy::large_futures)]

//! Verifier: an edit whose diff display payload would exceed the runtime's
//! 16 MiB frame cap still reaches `details.diffs`, with both texts clipped to
//! the edit skill's cap, a marker naming the full length, and `truncated`.
#![cfg(unix)]

mod live_kernel;

use serde_json::json;

const SIDE_CHARS: usize = 9_000_000;
const DIFF_TEXT_CAP: usize = 64 * 1024;

fn clipped(fill: &str) -> String {
    format!(
        "{}\n[... diff text truncated: {DIFF_TEXT_CAP} of {SIDE_CHARS} characters shown ...]",
        fill.repeat(DIFF_TEXT_CAP)
    )
}

#[tokio::test]
async fn an_oversized_edit_keeps_a_truncated_diff_record() {
    let Some(kernel_python) = live_kernel::kernel_python() else {
        return;
    };
    let dir = tempfile::tempdir().expect("temp dir");
    let agent_dir = dir.path().join("agent");
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).expect("project dir");
    let target = cwd.join("big.txt");
    std::fs::write(&target, "A".repeat(SIDE_CHARS)).expect("target file");
    let target = std::fs::canonicalize(&target).expect("canonical target");
    let edit_skill_src =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills/edit/src");

    let _env = live_kernel::EnvOverride::apply(&[
        (
            "PRIME_AGENT_KERNEL_PYTHON",
            Some(kernel_python.display().to_string()),
        ),
        ("PRIME_AGENT_CODING_AGENT_DIR", None),
        ("PRIME_API_KEY", None),
        ("RLM_DEPTH", None),
    ]);

    let cell = format!(
        "import sys\nsys.path.insert(0, {skill:?})\nfrom edit import run as edit_file\nawait edit_file({target:?}, 'A' * {SIDE_CHARS}, 'B' * {SIDE_CHARS})",
        skill = edit_skill_src.display().to_string(),
        target = target.display().to_string(),
    );
    let run = live_kernel::run_cell(
        &agent_dir,
        &cwd,
        live_kernel::SessionStore::Ephemeral,
        None,
        &cell,
    )
    .await;

    assert_eq!(
        run.details["status"], "ok",
        "details: {}",
        run.details["error"]
    );
    assert_eq!(
        std::fs::read_to_string(&target).expect("edited file"),
        "B".repeat(SIDE_CHARS)
    );
    let expected = json!([{
        "path": target,
        "oldStr": clipped("A"),
        "newStr": clipped("B"),
        "startLine": 1,
        "truncated": true,
    }]);
    // A plain assert_eq! would print megabytes of diff text on failure.
    let diffs = &run.details["diffs"];
    assert!(
        *diffs == expected,
        "diffs: {} entries, truncated {}, oldStr {} chars",
        diffs.as_array().map_or(0, Vec::len),
        diffs[0]["truncated"],
        diffs[0]["oldStr"].as_str().map_or(0, str::len),
    );
}
