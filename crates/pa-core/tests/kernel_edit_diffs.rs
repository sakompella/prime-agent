// Pedantic-gate dispositions as src/lib.rs.
#![allow(clippy::large_futures)]

//! Verifier: file edits a cell makes through the edit skill reach the
//! `ipython` tool result as `details.diffs` (`{ path, oldStr, newStr,
//! startLine }` per edit, in order), the v0.9.8 shape T3 Code renders as
//! file changes.
#![cfg(unix)]

mod live_kernel;

use serde_json::json;

#[tokio::test]
async fn ipython_result_details_carry_the_cell_edit_diffs() {
    let Some(kernel_python) = live_kernel::kernel_python() else {
        return;
    };
    let dir = tempfile::tempdir().expect("temp dir");
    let agent_dir = dir.path().join("agent");
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).expect("project dir");
    let target = cwd.join("notes.txt");
    std::fs::write(&target, "first\nalpha\nbeta\ngamma\n").expect("target file");
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
        "import sys\nsys.path.insert(0, {skill:?})\nfrom edit import run as edit_file\nawait edit_file({target:?}, \"alpha\", \"ALPHA\")\nawait edit_file({target:?}, \"gamma\\n\", \"\")",
        skill = edit_skill_src.display().to_string(),
        target = target.display().to_string(),
    );
    let run =
        live_kernel::run_cell(&agent_dir, &cwd, &agent_dir.join("sessions"), None, &cell).await;

    assert_eq!(
        std::fs::read_to_string(&target).expect("edited file"),
        "first\nALPHA\nbeta\n"
    );
    assert_eq!(
        run.details["diffs"],
        json!([
            { "path": target, "oldStr": "alpha", "newStr": "ALPHA", "startLine": 2 },
            { "path": target, "oldStr": "gamma\n", "newStr": "", "startLine": 4 },
        ]),
        "details: {}",
        run.details
    );
}
