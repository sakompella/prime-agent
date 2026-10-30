// Pedantic-gate dispositions as src/lib.rs.
#![allow(clippy::large_futures)]

//! Verifier: a running kernel moves onto a session env replaced after it
//! started (a client reusing a live worker brings its own T3 connection)
//! before the next cell, and an aborted move is tried again. Its own binary:
//! the replacement is process-wide.
#![cfg(unix)]

mod live_kernel;

use std::collections::BTreeMap;

use pa_core::kernel::cancellation::AbortSignal;
use pa_core::kernel::provisioner::{IpythonKernelProvisioner, IpythonKernelProvisionerOptions};
use pa_core::kernel::shared::ExecuteOptions;

#[tokio::test]
async fn a_running_kernel_catches_up_on_a_replaced_session_env() {
    let Some(kernel_python) = live_kernel::kernel_python() else {
        return;
    };
    let dir = tempfile::tempdir().expect("temp dir");
    let provisioner = IpythonKernelProvisioner::new(
        dir.path(),
        IpythonKernelProvisionerOptions {
            python: Some(kernel_python),
            ..Default::default()
        },
    );
    let manager = provisioner.ensure(None, None).await.expect("kernel");
    pa_types::daemon::launch_env::replace_session_env(&BTreeMap::from([(
        "T3_MARKER".to_string(),
        "moved".to_string(),
    )]));

    let aborted = AbortSignal::new();
    aborted.abort();
    provisioner
        .sync_session_env(&manager, Some(aborted))
        .await
        .expect_err("an aborted move fails");
    provisioner
        .sync_session_env(&manager, None)
        .await
        .expect("the next move runs");
    let result = manager
        .execute(
            "import os\nprint(os.environ.get('T3_MARKER'))",
            ExecuteOptions::default(),
        )
        .await
        .expect("cell");
    assert_eq!(result.stdout.trim(), "moved");
    provisioner.stop_kernel(None).await;
}
