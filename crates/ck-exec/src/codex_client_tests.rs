//! Replays tests/fixtures/codex_client_translate_go.json, written by Go's
//! `TranslateRequestWithAPIKeyModelCompatibilityForExecutor` and
//! `OptimizeCodexMultiAgentV2RequestForAuth` at 6fecc6e
//! (tests/reference/codex_client/README.md).

use super::*;
use serde_json::Value;

fn headers(v: &Value) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (k, val) in v["headers"].as_object().into_iter().flatten() {
        out.insert(
            http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            val.as_str().unwrap().parse().unwrap(),
        );
    }
    out
}

fn flag(v: &Value, key: &str) -> bool {
    v[key].as_bool().unwrap_or(false)
}

fn settings(v: &Value) -> Settings {
    Settings {
        optimize_multi_agent_v2: flag(v, "optimize"),
        orphan_delegation: flag(v, "orphan"),
    }
}

#[test]
fn replays_go_translation_and_optimization() {
    let vectors: Vec<Value> =
        serde_json::from_str(include_str!("../tests/fixtures/codex_client_translate_go.json")).unwrap();
    let (mut translated, mut optimized) = (0, 0);
    for v in &vectors {
        let input = v["in"].as_str().unwrap().as_bytes();
        let want = v["out"].as_str().unwrap();
        let h = headers(v);
        match v["fn"].as_str().unwrap() {
            "translate" => {
                translated += 1;
                let from = Format::parse(v["from"].as_str().unwrap()).unwrap();
                let to = Format::parse(v["to"].as_str().unwrap()).unwrap();
                let client = Client {
                    headers: &h,
                    settings: settings(v),
                    target_executor: v["target"].as_str().unwrap_or_default(),
                    is_compat: flag(v, "compat"),
                };
                let ctx = RequestCtx {
                    model: "test-model",
                    stream: true,
                    tool_search: false,
                };
                let got = translate_request(from, to, &ctx, input, &client).unwrap();
                let want = match to {
                    Format::Claude => crate::claude::go_exec::without_guessed_max_tokens(from, input, want),
                    _ => want.to_owned(),
                };
                assert_eq!(
                    String::from_utf8(got).unwrap(),
                    want,
                    "{} -> {} compat={} target={:?} ua={}",
                    from.as_str(),
                    to.as_str(),
                    client.is_compat,
                    client.target_executor,
                    v["headers"]["User-Agent"]
                );
            }
            "optimize_auth" => {
                optimized += 1;
                let (got, renamed) = optimize_for_auth(&h, input, &settings(v), flag(v, "compat"), false);
                assert_eq!(String::from_utf8(got).unwrap(), want, "{v}");
                assert_eq!(renamed, flag(v, "bool"), "{v}");
            }
            other => panic!("unknown vector fn {other}"),
        }
    }
    assert_eq!((translated, optimized), (80, 8));
}

/// Orphan delegation is a shared `upstream.codex` setting (Go 52d5507d): API keys read it.
#[test]
fn orphan_delegation_is_shared_with_api_keys() {
    let cfg = Config::parse(
        "client:\n  codex:\n    optimize-multi-agent-v2: true\nupstream:\n  codex:\n    orphan-delegation-compatibility: true\n",
    )
    .unwrap();
    assert!(Settings::from_config(&cfg).orphan_delegation);
    let api_key = Settings::from_config(&cfg.for_api_key());
    assert!(api_key.orphan_delegation && api_key.optimize_multi_agent_v2);
}

/// The is-compat Claude translator skips the registered pair, so the summary intent read
/// from the client body is applied here: `thinking.display` decides `reasoning.summary`
/// exactly as on the ordinary path.
#[test]
fn compat_translation_applies_the_claude_summary_intent() {
    let cfg = Config::default();
    let headers = HeaderMap::new();
    let ctx = RequestCtx {
        model: "gpt-5.4",
        stream: false,
        tool_search: false,
    };
    for (display, want) in [("summarized", Value::from("auto")), ("omitted", Value::Null)] {
        let body = format!(
            r#"{{"model":"gpt-5.4","max_tokens":64,"thinking":{{"type":"adaptive","display":"{display}"}},"messages":[{{"role":"user","content":"hi"}}]}}"#
        );
        let summary = |compat| {
            let client = Client::new(&headers, &cfg, "codex", compat);
            let out = translate_request(Format::Claude, Format::Codex, &ctx, body.as_bytes(), &client).unwrap();
            serde_json::from_slice::<Value>(&out).unwrap()["reasoning"]["summary"].clone()
        };
        assert_eq!(summary(true), want, "compat, display {display}");
        assert_eq!(summary(false), want, "ordinary, display {display}");
    }
}
