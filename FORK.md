# Fork ledger

This fork (`sakompella/prime-agent`, branch `fork/main`) starts at upstream tag `v0.10.0`.
Goals: the TUI keeps working, Prime works without T3 Code, T3 integration is native, and approvals are not needed (full access only).

Each line is one change: what it does, why, and when to drop it.

- Kernel harness env: `kernel_provisioner` sets `RLM_GLOBAL_HARNESS_STATE_DIR`, `RLM_SESSION_DIR` and `RLM_HARNESS_STATE_DIR` from the agent dir and session artifact dir, so default local `rlm.harness` CRUD works in root sessions and RLM children (v0.9.8 parity; v0.10.0 failed with "Local harness state requires RLM_HARNESS_STATE_DIR or RLM_SESSION_DIR"). Drop when upstream's kernel env carries these vars and `crates/pa-core/tests/kernel_harness_env.rs` passes without this change.
- Edit diffs: the `ipython` tool result carries `details.diffs` (`{path, oldStr, newStr, startLine?}` per edit-skill change, in cell order) from the kernel's diff display events, the v0.9.8 shape T3 Code renders as file changes; v0.10.0 collected them in the kernel manager and dropped them in `convert_execute_result`. Drop when upstream ipython details include `diffs` and `crates/pa-core/tests/kernel_edit_diffs.rs` passes without this change.
