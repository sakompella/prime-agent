# Fork ledger

This fork (`sakompella/prime-agent`, branch `fork/main`) starts at upstream tag `v0.10.0`.
Goals: the TUI keeps working, Prime works without T3 Code, T3 integration is native, and approvals are not needed (full access only).

Each line is one change: what it does, why, and when to drop it.

- Kernel harness env: `kernel_provisioner` sets `RLM_GLOBAL_HARNESS_STATE_DIR`, `RLM_SESSION_DIR` and `RLM_HARNESS_STATE_DIR` from the agent dir and session artifact dir, so default local `rlm.harness` CRUD works in root sessions and RLM children (v0.9.8 parity; v0.10.0 failed with "Local harness state requires RLM_HARNESS_STATE_DIR or RLM_SESSION_DIR"). Drop when upstream's kernel env carries these vars and `crates/pa-core/tests/kernel_harness_env.rs` passes without this change.
