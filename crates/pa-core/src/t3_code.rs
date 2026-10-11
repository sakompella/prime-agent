//! Whether a session runs inside T3 Code, decided from T3's MCP env.

/// The generic MCP server T3 Code registers itself as. T3 sessions reach it
/// through the kernel `t3` module instead.
pub(crate) const T3_CODE_MCP_SERVER: &str = "t3-code";

/// Whether the session has T3 Code's MCP endpoint. `Present` binds the kernel
/// `t3` module and adds the T3 prompt section.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum T3Code {
    #[default]
    Absent,
    Present,
}

impl T3Code {
    /// `Present` iff `T3_MCP_URL` and `T3_MCP_BEARER_TOKEN` are both non-empty,
    /// the same rule the kernel's `rlm.t3` applies.
    pub fn from_env(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let is_set = |key: &str| lookup(key).is_some_and(|value| !value.trim().is_empty());
        if is_set("T3_MCP_URL") && is_set("T3_MCP_BEARER_TOKEN") {
            Self::Present
        } else {
            Self::Absent
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn present_needs_both_t3_vars_non_empty() {
        let cases = [
            (None, None, T3Code::Absent),
            (Some("http://127.0.0.1:3773/mcp"), None, T3Code::Absent),
            (None, Some("token"), T3Code::Absent),
            (Some(""), Some("token"), T3Code::Absent),
            (
                Some("http://127.0.0.1:3773/mcp"),
                Some("  "),
                T3Code::Absent,
            ),
            (
                Some("http://127.0.0.1:3773/mcp"),
                Some("token"),
                T3Code::Present,
            ),
        ];
        for (url, token, expected) in cases {
            let lookup = |key: &str| match key {
                "T3_MCP_URL" => url.map(str::to_string),
                "T3_MCP_BEARER_TOKEN" => token.map(str::to_string),
                _ => None,
            };
            assert_eq!(T3Code::from_env(lookup), expected, "{url:?} {token:?}");
        }
    }
}
