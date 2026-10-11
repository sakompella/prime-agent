//! The session-scoped client env: the `T3_*` vars a headless client (rpc,
//! ACP) forwards on its session create so the session's worker, kernel and
//! RLM children see that client's T3 Code connection. The supervisor's own
//! env belongs to whichever client started it, so it is never the source.
//!
//! T3 Code sets only `T3_*` vars for Prime (the MCP URL, its bearer token,
//! the tool list, the runtime mode), so the rule is the prefix. The daemon
//! re-filters on receipt (the socket peer is untrusted), as with
//! [`super::herdr_env`].

use std::collections::BTreeMap;
use std::ffi::OsString;

/// Whether a var belongs to one session rather than the daemon.
#[must_use]
pub fn is_session_scoped(key: &str) -> bool {
    key.starts_with("T3_")
}

/// A session's own env: only session-scoped keys with non-empty values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionEnv(BTreeMap<String, String>);

impl SessionEnv {
    /// Collect from a process env (`std::env::vars_os()`). A var that is
    /// not UTF-8 cannot ride the JSON wire, so it is skipped.
    #[must_use]
    pub fn collect(vars: impl IntoIterator<Item = (OsString, OsString)>) -> Self {
        Self(
            vars.into_iter()
                .filter_map(|(key, value)| {
                    Some((key.into_string().ok()?, value.into_string().ok()?))
                })
                .filter(|(key, value)| is_session_scoped(key) && !value.is_empty())
                .collect(),
        )
    }

    /// Re-filter a map received on the wire.
    #[must_use]
    pub fn filter(env: &BTreeMap<String, String>) -> Self {
        Self(
            env.iter()
                .filter(|(key, value)| is_session_scoped(key) && !value.is_empty())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        )
    }

    pub fn vars(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    /// The create's `launch_env`: absent when there is nothing to forward.
    #[must_use]
    pub fn into_wire(self) -> Option<BTreeMap<String, String>> {
        (!self.0.is_empty()).then_some(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn key() -> impl Strategy<Value = String> {
        prop_oneof![
            "T3_[A-Z_]{0,6}",
            "[A-Za-z0-9_]{0,8}",
            "t3_[A-Z]{0,3}",
            "[A-Z]{1,3}T3_",
        ]
    }

    fn env() -> impl Strategy<Value = BTreeMap<String, String>> {
        proptest::collection::btree_map(key(), "[a-z]{0,3}", 0..8)
    }

    fn expected(env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        env.iter()
            .filter(|(key, value)| key.starts_with("T3_") && !value.is_empty())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    proptest! {
        #[test]
        fn only_non_empty_t3_vars_are_session_scoped(env in env()) {
            let collected = SessionEnv::collect(
                env.iter().map(|(key, value)| (OsString::from(key), OsString::from(value))),
            );
            let expected = expected(&env);
            prop_assert_eq!(&collected, &SessionEnv::filter(&env));
            prop_assert_eq!(
                collected.into_wire(),
                (!expected.is_empty()).then_some(expected)
            );
        }

        #[test]
        fn filtering_is_idempotent(env in env()) {
            let once = SessionEnv::filter(&env);
            prop_assert_eq!(SessionEnv::filter(&once.0), once);
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_non_utf8_var_never_travels() {
        use std::os::unix::ffi::OsStringExt;
        let collected = SessionEnv::collect([
            (OsString::from("T3_MCP_URL"), OsString::from_vec(vec![0xff])),
            (
                OsString::from("T3_MCP_BEARER_TOKEN"),
                OsString::from("token"),
            ),
        ]);
        assert_eq!(
            collected.into_wire(),
            Some(BTreeMap::from([(
                "T3_MCP_BEARER_TOKEN".to_string(),
                "token".to_string()
            )]))
        );
    }
}
