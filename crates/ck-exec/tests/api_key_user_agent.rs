//! API-key identities on both execution paths, observed by a loopback upstream.
mod support;

use bytes::Bytes;
use ck_core::exec::{ExecRequest, Operation};
use ck_core::format::Format;
use ck_exec::Executors;
use ck_translate::codec::{RequestIngress, chat};
use support::upstream::{Reply, StreamUpstream};
use support::{Exchange, Streams, identity};

fn request(exchange: &Exchange) -> ExecRequest {
    ExecRequest {
        operation: Operation::Generate,
        source_format: Format::OpenAI,
        response_format: Format::OpenAI,
        model: exchange.model.clone(),
        requested_model: exchange.model.clone(),
        body: Bytes::from_static(
            br#"{"model":"test-model","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":true}"#,
        ),
        original_body: Bytes::new(),
        stream: true,
        alt: None,
        session: None,
        execution_session: None,
        derived_session: None,
        resolved_model: None,
        usage: Default::default(),
        request_path: exchange.route.clone(),
        headers: exchange.headers.clone(),
        caller: exchange.caller.clone(),
    }
}

async fn check(provider: &str) {
    for contract in [false, true] {
        if contract && provider == "devin" {
            continue;
        }
        for custom in [false, true] {
            let upstream =
                StreamUpstream::start([Reply::status(400, "application/json", r#"{"error":"test rejection"}"#)]).await;
            let credential = support::credential(provider, &upstream.url(), "test-api-key");
            let model = if provider == "codex" {
                "gpt-5.6-luna"
            } else {
                "test-model"
            };
            let mut exchange = Exchange::new(credential, model, "/v1/chat/completions");
            exchange.headers.insert("user-agent", "caller/1".parse().unwrap());
            if custom {
                exchange
                    .credential
                    .attributes
                    .insert("header:User-Agent".into(), "configured/1".into());
            }
            let executors = Executors {
                claude: ck_exec::claude::ClaudeExecutor::new(upstream.url()).unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            };
            let req = request(&exchange);
            if contract {
                let streams = Streams::new();
                let mut writer = streams.request();
                let response = streams.response(&writer);
                let attempt = exchange.attempt(&streams.store);
                let mut ingress = chat::request::Ingress::new(
                    &mut writer,
                    &streams.store,
                    identity(ck_translate::codec::event::Direction::Request),
                    ck_translate::codec::Operation::Generate,
                    1 << 20,
                )
                .unwrap();
                ingress.advance(&mut writer, &req.body).unwrap();
                let validated = executors.fence(&attempt, &mut ingress, &mut writer).unwrap();
                let request = exchange.request_with(validated, &req.body);
                let error = executors
                    .start(&attempt, &request, &mut writer, response)
                    .await
                    .err()
                    .unwrap();
                assert_eq!(error.status, 400, "{provider}: {error:?}");
            } else {
                let error = executors
                    .execute(&exchange.credential, req, &exchange.cfg)
                    .await
                    .err()
                    .unwrap();
                assert_eq!(error.status, 400, "{provider}: {error:?}");
            }
            let captured = upstream.captured();
            assert_eq!(captured.len(), 1, "{provider}, contract={contract}");
            assert_eq!(
                captured[0].header("user-agent"),
                Some("cooker"),
                "{provider}, contract={contract}, custom={custom}"
            );
        }
    }
}

macro_rules! provider_test {
    ($name:ident, $provider:literal) => {
        #[tokio::test]
        async fn $name() {
            check($provider).await;
        }
    };
}

provider_test!(openai_compat, "openai-compatibility");
provider_test!(claude, "claude");
provider_test!(codex, "codex");
provider_test!(gemini, "gemini");
provider_test!(vertex, "vertex");
provider_test!(xai, "xai");
provider_test!(kimi, "kimi");
provider_test!(meta, "meta");
provider_test!(devin, "devin");

#[tokio::test]
async fn xai_identity_uses_auth_kind_not_endpoint_mode() {
    for (kind, using_api, expected) in [("oauth", "true", "configured/1"), ("apikey", "false", "cooker")] {
        let upstream = StreamUpstream::start([Reply::status(400, "application/json", "{}")]).await;
        let mut credential = support::credential("xai", &upstream.url(), "test-key");
        for (key, value) in [
            ("auth_kind", kind),
            ("using_api", using_api),
            ("header:User-Agent", "configured/1"),
        ] {
            credential.attributes.insert(key.into(), value.into());
        }
        let exchange = Exchange::new(credential, "grok-4.3", "/v1/responses");
        let executor = ck_exec::xai::XaiExecutor::with_client(wreq::Client::new());
        let error = executor
            .execute(&exchange.credential, request(&exchange), &exchange.cfg, false)
            .await
            .err()
            .unwrap();
        assert_eq!(error.status, 400);
        assert_eq!(upstream.captured()[0].header("user-agent"), Some(expected));
    }
}

#[tokio::test]
async fn codex_alpha_search_identifies_cooker() {
    let upstream = StreamUpstream::start([Reply::json("{}")]).await;
    let mut credential = support::credential("codex", &upstream.url(), "test-key");
    credential
        .attributes
        .insert("header:User-Agent".into(), "configured/1".into());
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::USER_AGENT, "caller/1".parse().unwrap());
    ck_exec::codex::CodexExecutor::default()
        .alpha_search(
            &credential,
            br#"{"model":"gpt-5"}"#,
            &headers,
            "gpt-5",
            &Default::default(),
            &Default::default(),
        )
        .await
        .unwrap();
    let captured = upstream.captured();
    assert_eq!(captured[0].path, "/alpha/search");
    assert_eq!(captured[0].header("user-agent"), Some("cooker"));
}
