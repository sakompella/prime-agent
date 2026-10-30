//! The client launch-env wire contract (TS `collectDaemonLaunchEnv`,
//! `daemon-protocol.ts`): the environment a client forwards on a
//! session `create` — its whole process env minus the daemon-internal
//! vars.
//!
//! A session's worker runs as a separate process the supervisor spawns
//! with the daemon's own boot env; without this contract the session
//! would run in whatever environment the daemon booted in (the TS
//! port-parity gap): PATH changes, direnv, proxies, and certificate
//! paths from the terminal that opened the session would be lost.
//!
//! The trust model mirrors the Herdr client-env contract: any client
//! that can reach the user-scoped daemon socket can already run
//! commands through it, so the forwarded env crosses no privilege
//! boundary — but the daemon re-applies the same filter on receipt
//! (the socket peer is untrusted), and the daemon-internal
//! `PRIME_AGENT_INTERNAL_*` vars never ride the wire: a worker's
//! identity is stamped by the supervisor at spawn, never inherited
//! from a client, and a worker forwarding its own env to a spawned
//! child (the TS in-process-parity path) must not leak its internal
//! identity into it.

use std::collections::BTreeMap;

/// The daemon-internal env prefix (`PRIME_AGENT_INTERNAL_*`): the
/// supervisor's worker identity and protocol vars. Neither side of the
/// wire ever forwards them.
pub const INTERNAL_ENV_PREFIX: &str = "PRIME_AGENT_INTERNAL_";

/// Collect the launch env from a process-env-like source: everything
/// except the daemon-internal vars (the client side of the wire
/// contract). The daemon re-applies the same function to a received
/// map (the socket peer is untrusted): a forwarded map never carries
/// more than a client may legitimately forward.
#[must_use]
pub fn collect_launch_env(
    source: impl IntoIterator<Item = (String, String)>,
) -> BTreeMap<String, String> {
    source
        .into_iter()
        .filter(|(key, _)| !key.starts_with(INTERNAL_ENV_PREFIX))
        .collect()
}

/// Collect the launch env from THIS process's environment, with its
/// session-scoped vars as [`replace_session_env`] last set them. Entries
/// that are not valid UTF-8 are skipped: the JSON wire cannot carry them,
/// and `std::env::vars` would panic on them.
#[must_use]
pub fn collect_process_launch_env() -> BTreeMap<String, String> {
    let mut env = collect_launch_env(
        std::env::vars_os()
            .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?))),
    );
    if let Some(replaced) = replaced_session_env().as_ref() {
        env.retain(|key, _| !is_session_scoped(key));
        env.extend(replaced.clone());
    }
    env
}

/// The session-scoped vars that replaced this process's own, once a
/// client reusing a live worker brought a different T3 connection. The
/// process env itself is never written: other threads may read it.
static REPLACED_SESSION_ENV: std::sync::RwLock<Option<BTreeMap<String, String>>> =
    std::sync::RwLock::new(None);

fn replaced_session_env() -> std::sync::RwLockReadGuard<'static, Option<BTreeMap<String, String>>> {
    REPLACED_SESSION_ENV
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Make `env`'s session-scoped vars this process's whole session-scoped
/// set: a var the process env has and `env` lacks reads as unset.
pub fn replace_session_env(env: &BTreeMap<String, String>) {
    *REPLACED_SESSION_ENV
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(session_scoped_env(env));
}

/// One var as this process's session sees it: a session-scoped var from
/// the last [`replace_session_env`], anything else from the process env.
#[must_use]
pub fn session_var(key: &str) -> Option<String> {
    match replaced_session_env().as_ref() {
        Some(replaced) if is_session_scoped(key) => replaced.get(key).cloned(),
        _ => std::env::var(key).ok(),
    }
}

/// The overrides a process spawned with this process's env needs so it
/// sees the session-scoped vars [`session_var`] reports: empty until
/// [`replace_session_env`] runs, then the replaced set plus an empty value
/// for each process-env session var the replacement dropped.
#[must_use]
pub fn session_env_overrides() -> BTreeMap<String, String> {
    let Some(replaced) = replaced_session_env().clone() else {
        return BTreeMap::new();
    };
    let mut overrides: BTreeMap<String, String> = std::env::vars_os()
        .filter_map(|(key, _)| key.into_string().ok())
        .filter(|key| is_session_scoped(key))
        .map(|key| (key, String::new()))
        .collect();
    overrides.extend(replaced);
    overrides
}

/// Whether a var belongs to one session's client rather than the daemon:
/// T3 Code's `T3_*` connection vars (MCP URL, bearer token, tool list,
/// runtime mode). The supervisor's own copies come from whichever client
/// started it, so a worker never inherits them; it gets only its session's.
#[must_use]
pub fn is_session_scoped(key: &str) -> bool {
    key.starts_with("T3_")
}

/// The session-scoped entries of a launch env.
#[must_use]
pub fn session_scoped_env(env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    env.iter()
        .filter(|(key, _)| is_session_scoped(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// After a replacement the session-scoped vars are exactly the
    /// replacement's, for reads, for a spawned child's launch env, and for a
    /// process inheriting this one's env; other vars stay the process's.
    /// The only test in this binary that touches the process-wide state.
    #[test]
    fn a_replaced_session_env_is_the_whole_session_scoped_set() {
        let path = std::env::var("PATH").ok();
        replace_session_env(&BTreeMap::from([
            ("T3_MCP_URL".to_string(), "http://t3".to_string()),
            ("PATH".to_string(), "/not-session-scoped".to_string()),
        ]));
        assert_eq!(session_var("T3_MCP_URL").as_deref(), Some("http://t3"));
        assert_eq!(session_var("T3_MCP_BEARER_TOKEN"), None);
        assert_eq!(session_var("PATH"), path);
        let launch_env = collect_process_launch_env();
        assert_eq!(
            session_scoped_env(&launch_env),
            BTreeMap::from([("T3_MCP_URL".to_string(), "http://t3".to_string())])
        );
        assert_eq!(launch_env.get("PATH"), path.as_ref());
        let overrides = session_env_overrides();
        assert_eq!(overrides.get("T3_MCP_URL").map(String::as_str), Some("http://t3"));
        assert!(overrides.keys().all(|key| is_session_scoped(key)));
        assert!(overrides
            .iter()
            .all(|(key, value)| key == "T3_MCP_URL" || value.is_empty()));
    }

    /// The client side forwards everything except the daemon-internal
    /// vars: no other var is special-cased (the whole point of the
    /// contract — PATH, proxies, credential paths ride along).
    #[test]
    fn the_client_collects_everything_but_the_internal_vars() {
        let env = collect_launch_env([
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("SSL_CERT_FILE".to_string(), "/certs/ca.pem".to_string()),
            ("PA_MARKER".to_string(), "a-1".to_string()),
            (
                "PRIME_AGENT_INTERNAL_DAEMON_WORKER".to_string(),
                "1".to_string(),
            ),
            (
                "PRIME_AGENT_INTERNAL_DAEMON_WORKER_TOKEN".to_string(),
                "secret-token".to_string(),
            ),
            (
                "PRIME_AGENT_INTERNAL_".to_string(),
                "bare-prefix".to_string(),
            ),
        ]);
        assert_eq!(
            env,
            BTreeMap::from([
                ("PATH".to_string(), "/usr/bin".to_string()),
                ("SSL_CERT_FILE".to_string(), "/certs/ca.pem".to_string()),
                ("PA_MARKER".to_string(), "a-1".to_string()),
            ])
        );
    }
}
