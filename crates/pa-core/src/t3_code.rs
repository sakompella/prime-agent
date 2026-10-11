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
            (Some("http://127.0.0.1:3773/mcp"), Some("  "), T3Code::Absent),
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
