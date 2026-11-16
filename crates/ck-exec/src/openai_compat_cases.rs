use super::*;

#[test]
fn provider_keys() {
    assert!(handles("openai-compatibility"));
    assert!(handles("openai-compatible-acme"));
    assert!(!handles("openai"));
    assert!(!handles("codex"));
}

fn b64(s: &Value) -> Vec<u8> {
    base64::engine::Engine::decode(&base64::engine::general_purpose::STANDARD, s.as_str().unwrap()).unwrap()
}

/// Go standard-library answers recorded by the generator (`vectors`).
#[test]
fn go_primitive_vectors() {
    use crate::openai_compat_go as go;
    use ck_wire::mime::{MediaType, parse_media_type};
    let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
    let v = &fixture["vectors"];
    for case in v["http_time"].as_array().unwrap() {
        let raw = case[0].as_str().unwrap();
        let got = go::parse_http_time(raw).map(|t| match t.duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => d.as_nanos() as i64,
            Err(e) => -(e.duration().as_nanos() as i64),
        });
        assert_eq!(got, case[1].as_i64(), "http.ParseTime({raw:?})");
    }
    for case in v["json_valid"].as_array().unwrap() {
        let input = b64(&case[0]);
        assert_eq!(
            go::json_valid(&input),
            case[1].as_bool().unwrap(),
            "json.Valid({:?})",
            String::from_utf8_lossy(&input[..input.len().min(40)])
        );
    }
    for case in v["gjson_int"].as_array().unwrap() {
        let raw = case[0].as_str().unwrap();
        assert_eq!(
            Some(gj::parse(raw.as_bytes()).int()),
            case[1].as_i64(),
            "gjson Int({raw})"
        );
    }
    for case in v["trim_space"].as_array().unwrap() {
        assert_eq!(go::trim_space(&b64(&case[0])), b64(&case[1]).as_slice());
    }
    for case in v["media_type"].as_array().unwrap() {
        let input = case["input"].as_str().unwrap();
        let (media, error) = (case["media"].as_str().unwrap(), case["error"].as_str().unwrap());
        let want = if error.is_empty() {
            let params = case["params"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
                .collect();
            MediaType::Ok(media.to_owned(), params)
        } else if media.is_empty() {
            match parse_media_type(input) {
                MediaType::Invalid(reason) => {
                    assert_eq!(reason, error, "mime.ParseMediaType({input:?})");
                    MediaType::Invalid(reason)
                }
                _ => MediaType::Invalid("<expected invalid>"),
            }
        } else {
            MediaType::BadParams(media.to_owned())
        };
        assert_eq!(parse_media_type(input), want, "mime.ParseMediaType({input:?})");
    }
    for case in v["token_counts"].as_array().unwrap() {
        let (model, payload) = (case[0].as_str().unwrap(), case[1].as_str().unwrap());
        assert_eq!(
            crate::openai_compat_payload::count_chat_tokens(model, payload.as_bytes()),
            Ok(case[2].as_i64().unwrap()),
            "CountOpenAIChatTokens({model:?}, {payload})"
        );
    }
}

/// A translator holding one unfinished Responses frame.
struct Pending;

impl StreamTranslator for Pending {
    fn event(&mut self, _: &[u8]) -> Result<Vec<Bytes>, ck_translate::Error> {
        Ok(vec![])
    }

    fn finish(&mut self) -> Result<Vec<Bytes>, ck_translate::Error> {
        Ok(vec![])
    }

    fn flush_frames(&mut self) -> Vec<Bytes> {
        vec![Bytes::from_static(b"event: pending\n\n")]
    }
}

#[tokio::test]
async fn terminal_errors_flush_pending_frames_first() {
    // Go's responsesSSEFramer flushes before the handler writes a terminal error.
    for (lines, status) in [
        (
            vec![
                Ok(Bytes::from_static(b"data: {\"error\":{\"status\":429}}")),
                Ok(Bytes::new()),
            ],
            429,
        ),
        (vec![Err(ExecError::local(502, FailureScope::Transport, "cut"))], 502),
    ] {
        let lines: ExecStream = futures_util::stream::iter(lines).boxed();
        let out: Vec<_> = frames(
            lines,
            Box::new(Pending),
            ClaudeInputTokens::new(Format::OpenAI, Format::OpenAI, Format::OpenAI, Bytes::new()),
            Format::OpenAIResponse,
            Default::default(),
            Default::default(),
            false,
        )
        .collect()
        .await;
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].as_ref().unwrap(), &Bytes::from_static(b"event: pending\n\n"));
        assert_eq!(out[1].as_ref().unwrap_err().status, status);
    }
}

/// Go 868e137 gives CountTokens the payload barrier: the rules run after thinking,
/// before the local tokenizer, so a configured message changes the count.
#[test]
fn count_tokens_sees_the_payload_rules() {
    let s = serde_json::json!({
        "op": "count", "source": "openai", "model": "gpt-4o", "provider": "openai-compatibility", "config_auth": -1,
        "payload": r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#,
    });
    let count = |config: &str| {
        let cfg = Config::parse(config).unwrap();
        let (req, _) = request(&s, Default::default());
        let out = super::count_tokens(&credential(&s, &cfg, "127.0.0.1:9"), &req, &cfg).unwrap();
        let ResponseBody::Buffered(bytes) = out.body else {
            panic!("count answers are buffered")
        };
        gj::get(&bytes, "usage.prompt_tokens").int()
    };
    let plain = count("");
    let configured = count(
        "requests:\n  payload:\n    override:\n      - models: [{name: gpt-4o}]\n        params: {messages.0.content: one two three four five six seven eight nine ten}\n",
    );
    assert!(plain > 0);
    assert!(configured > plain + 5, "{plain} -> {configured}");
}

/// Go 868e137: the chat rules run after the built-in stream options, so a filter on
/// `stream_options` removes the `include_usage` the executor sets.
#[tokio::test]
async fn chat_payload_rules_run_after_the_stream_options() {
    let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
    let mut s = fixture["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "stream_basic")
        .unwrap()
        .clone();
    let rule =
        "\nrequests:\n  payload:\n    filter:\n      - models: [{name: acme-chat}]\n        params: [stream_options]\n";
    s["config"] = Value::String(format!("{}{rule}", s["config"].as_str().unwrap()));
    let mock = Mock::start(&s["upstream"]).await;
    let cfg = Config::parse(&s["config"].as_str().unwrap().replace("UPSTREAM", &mock.addr)).unwrap();
    let cred = credential(&s, &cfg, &mock.addr);
    let (req, _) = request(&s, Default::default());
    let ResponseBody::Stream(mut stream) = OpenAICompatExecutor::default()
        .execute(&cred, req, &cfg)
        .await
        .unwrap()
        .body
    else {
        panic!("a stream")
    };
    while stream.next().await.is_some() {}
    let wire = String::from_utf8(mock.request().unwrap()).unwrap();
    assert!(wire.contains("\r\n\r\n{"), "{wire}");
    assert!(!wire.contains("stream_options"), "{wire}");
}

/// Go ignores a refusal only the original (baseline) body hits
/// (TranslateRequestPairWithAPIKeyModelCompatibilityAndUpdateIntent): the working body is
/// the one sent, and only its own refusal refuses the request.
#[tokio::test]
async fn a_refused_baseline_alone_does_not_refuse_the_request() {
    let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
    let mut s = fixture["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "claude_not_compat_drops_thinking")
        .unwrap()
        .clone();
    s["original"] = Value::String(
        r#"{"model":"chat","max_tokens":10,"messages":[{"role":"user","content":[{"type":"container_upload","source":{"type":"file","file_id":"f1"}}]}]}"#.into(),
    );
    let run = |s: Value| async move {
        let mock = Mock::start(&s["upstream"]).await;
        let cfg = Config::parse(&s["config"].as_str().unwrap().replace("UPSTREAM", &mock.addr)).unwrap();
        let cred = credential(&s, &cfg, &mock.addr);
        let (req, _) = request(&s, Default::default());
        let ok = OpenAICompatExecutor::default().execute(&cred, req, &cfg).await.is_ok();
        (ok, mock.request().map(|w| String::from_utf8(w).unwrap()))
    };
    let (ok, wire) = run(s.clone()).await;
    assert!(ok);
    assert!(wire.unwrap().contains(r#""content":"go""#));
    s["payload"] = s["original"].clone();
    assert_eq!(run(s).await, (false, None));
}

/// A base URL the relay refuses before sending (Go's parser, then its transport) names
/// neither its password nor its query key in the returned error or the captured error.
#[tokio::test]
async fn refused_base_urls_never_print_url_credentials() {
    use crate::openai_compat_usage::Captured;
    const SECRET: &str = "sk-review-secret";
    let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
    let s = fixture["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "claude_not_compat_drops_thinking")
        .unwrap();
    for (base, want) in [
        (
            format!("http://user:{SECRET}@127.0.0.1:bad/?api_key={SECRET}"),
            r#"parse "<redacted url>": invalid port after host"#,
        ),
        (
            format!("ftp://user:{SECRET}@127.0.0.1/?api_key={SECRET}"),
            r#"Post "ftp://127.0.0.1/?api_key=***": unsupported protocol scheme "ftp""#,
        ),
    ] {
        let cfg = Config::parse(&s["config"].as_str().unwrap().replace("http://UPSTREAM/v1", &base)).unwrap();
        let cred = credential(s, &cfg, "");
        let (_recorder, capture, sink) = crate::openai_compat_usage::sinks();
        let (req, _) = request(s, sink);
        let error = OpenAICompatExecutor::default()
            .execute(&cred, req, &cfg)
            .await
            .err()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&error.body), want, "{base}");
        // The request event carries the URL as sent; the log writer masks it.
        let errors: Vec<_> = capture
            .take()
            .into_iter()
            .filter(|e| matches!(e, Captured::Error(_)))
            .collect();
        assert!(!format!("{errors:?}").contains(SECRET), "{errors:?}");
        if base.starts_with("ftp:") {
            assert_eq!(errors, [Captured::Error(want.to_owned())]);
        }
    }
}
