//! The baseline `default` and `default-raw` rules check, without a second copy of the body.
//! The rules only ask whether a path exists in the original request (`in_source`), and a
//! path's existence depends on keys, nesting, order and arrays, never on string contents.
//! [`Rules::baseline`] therefore keeps the body's shape with every string value emptied, and
//! only the top-level members the default paths can start at.

use super::{Rules, build_path, split_path};
use crate::json;

impl Rules {
    /// The request as the default rules read it, to keep in place of `body` until they run
    /// (pass it as [`super::Request::original`] with the same `root`); `None` without default
    /// rules. A path exists in it exactly when it exists in `body`.
    pub fn baseline(&self, root: &str, body: &[u8]) -> Option<Vec<u8>> {
        if !self.has_defaults() {
            return None;
        }
        if !json::valid(body) {
            // gjson's reads of a malformed document follow its bytes, so keep them.
            return Some(body.to_vec());
        }
        let paths: Vec<String> = (self.default.iter().chain(&self.default_raw))
            .flat_map(|rule| &rule.params)
            .map(|(path, _)| build_path(root, path))
            .filter(|path| !path.is_empty())
            .collect();
        let starts: Option<Vec<&str>> = paths.iter().map(|path| split_path(path).first().copied()).collect();
        let mut out = Vec::new();
        match starts.filter(|starts| starts.iter().all(|key| plain_key(key))) {
            Some(starts) if body.trim_ascii_start().first() == Some(&b'{') => {
                out.push(b'{');
                json::object_members(body, |key, escaped, value| {
                    // An escaped key may unescape to a start, so it stays.
                    if escaped || starts.iter().any(|start| start.as_bytes() == key) {
                        if out.len() > 1 {
                            out.push(b',');
                        }
                        out.push(b'"');
                        out.extend_from_slice(key);
                        out.extend_from_slice(b"\":");
                        shape(value, &mut out);
                    }
                    true
                });
                out.push(b'}');
            }
            _ => shape(body, &mut out),
        }
        Some(out)
    }
}

/// A path part gjson reads as one literal key: no wildcard, query, modifier, pipe or escape.
fn plain_key(part: &str) -> bool {
    !part.is_empty()
        && part
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}

/// Appends `json` without whitespace and with each string value as `""`; keys stay.
fn shape(json: &[u8], out: &mut Vec<u8>) {
    let mut i = 0;
    while i < json.len() {
        let c = json[i];
        if c != b'"' {
            if !c.is_ascii_whitespace() {
                out.push(c);
            }
            i += 1;
            continue;
        }
        let mut end = i + 1;
        while end < json.len() && json[end] != b'"' {
            end += if json[end] == b'\\' { 2 } else { 1 };
        }
        end = (end + 1).min(json.len());
        let key = json[end..].iter().find(|c| !c.is_ascii_whitespace()) == Some(&b':');
        out.extend_from_slice(if key { &json[i..end] } else { b"\"\"" });
        i = end;
    }
}

#[cfg(test)]
mod tests {
    use ck_core::config::Config;

    use super::*;

    fn rules(params: &str) -> std::sync::Arc<Rules> {
        let cfg = format!(
            "requests:\n  payload:\n    default:\n      - models: [{{name: \"m\"}}]\n        params: {params}\n"
        );
        Rules::of(&Config::parse(&cfg).unwrap())
    }

    #[test]
    fn keeps_the_shape_of_the_members_defaults_start_at() {
        let body = br#"{"messages":[{"role":"user","content":"a long \"text\""}],"reasoning":{"effort":"high"},"n":1}"#;
        let shaped = rules(r#"{"reasoning.effort": "low", "messages.#(role==\"user\").name": "x"}"#)
            .baseline("", body)
            .unwrap();
        assert_eq!(
            shaped,
            br#"{"messages":[{"role":"","content":""}],"reasoning":{"effort":""}}"#
        );
        // A wildcard first part may match any member, so all stay.
        let shaped = rules(r#"{"r*.effort": "low"}"#).baseline("", body).unwrap();
        assert_eq!(
            shaped,
            br#"{"messages":[{"role":"","content":""}],"reasoning":{"effort":""},"n":1}"#
        );
        assert_eq!(
            rules(r#"{"x": 1}"#).baseline("request", b" {\"request\" : {\"x\":\"\\\\\"} } "),
            Some(br#"{"request":{"x":""}}"#.to_vec())
        );
        assert!(Rules::default().baseline("", body).is_none());
    }

    /// For each path form the rules accept (wildcards, `#`, `@this`, pipes, escapes, duplicate
    /// keys, indexes), a default rule on it sees the same existence in the baseline as in the body.
    #[test]
    fn paths_exist_in_the_baseline_as_in_the_body() {
        let body = br#" {"a":{"b":"x","c":[1,"y"]},"a":{"d":2},"arr":[{"k":"v"},{"k":"w","z":null}],
            "e.f":"g","n":0,"s":"{\"x\":1}","q":" : "} "#;
        for path in [
            "a", "a.b", "a.d", "a.c.1", "a.c.#", "a.*", "a.?", "*.d", "arr.1.z", "arr.#", "arr.#.k", "arr.2.k",
            r"e\.f", "s.x", "n", "q", "@this", "a|d", "zz", "a.zz",
        ] {
            let baseline = rules(&format!("{{'{path}': 1}}")).baseline("", body).unwrap();
            assert_eq!(
                json::get(&baseline, path).exists(),
                json::get(body, path).exists(),
                "{path} in {}",
                String::from_utf8_lossy(&baseline)
            );
        }
    }

    /// A default rule, here for another model, keeps the body's shape until the rules ran,
    /// not a copy of the body.
    #[test]
    fn a_default_rule_for_another_model_keeps_no_body() {
        let rules = rules(r#"{"reasoning.effort": "low"}"#);
        let body = format!(r#"{{"model":"x","input":"{}"}}"#, "x".repeat(1 << 20)).into_bytes();
        let req = super::super::Request {
            model: "x",
            requested_model: "x",
            protocol: "openai",
            ..Default::default()
        };
        let input = body.clone();
        let before = crate::alloc_count::bytes();
        let out = super::super::apply(&rules, &req, body);
        let spent = crate::alloc_count::bytes() - before;
        assert_eq!(out, input);
        assert!(
            spent < 64 << 10,
            "{spent} bytes allocated for a {}-byte body",
            input.len()
        );
    }
}
