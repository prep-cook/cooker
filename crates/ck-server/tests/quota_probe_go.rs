//! Replays `fixtures/quota_probe_go.json` against the Rust management router: the
//! declarative `quota_probe` of `POST /v0/management/quota/fetch` (body binding, token
//! substitution, response mapping, malformed upstream answers, URL and header refusals)
//! and the `reset-quota` body binding, checking every response and every request the probe
//! sent to the local upstream.
//!
//! The fixture is a slice of the Go recording made by the removed plugin-route generator
//! at 6fecc6e: its upstream routes, credentials, plugin-free config and the steps those
//! credentials answered. No plugin handled them, so the slice needs no generator; edit the
//! fixture by hand when the contract changes. Replay drops the recorded `oauth.auth-dir`
//! line (`without_auth_dir`) and stages the credentials in `Config::auth_dir`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ck_exec::Executors;
use ck_exec::claude::ClaudeExecutor;
use ck_server::management::api::{self as management, Management};
use serde_json::Value;

/// The recording's upstream: fixed answers by path, every request recorded as Go's
/// handler sees it (canonical header names, without Host and Content-Length).
#[derive(Default)]
struct Upstream {
    base: std::sync::Mutex<String>,
    seen: Arc<std::sync::Mutex<Vec<Value>>>,
}

impl Upstream {
    async fn start(&self, routes: Value) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        *self.base.lock().unwrap() = format!("http://{}", listener.local_addr().unwrap());
        let seen = self.seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let (routes, seen) = (routes.clone(), seen.clone());
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let head_end = loop {
                        let mut chunk = [0u8; 4096];
                        let n = stream.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                    let mut lines = head.split("\r\n");
                    let mut request_line = lines.next().unwrap().split(' ');
                    let (method, uri) = (
                        request_line.next().unwrap().to_owned(),
                        request_line.next().unwrap().to_owned(),
                    );
                    let mut header = serde_json::Map::new();
                    let mut length = 0usize;
                    let mut host = String::new();
                    for line in lines {
                        let (name, value) = line.split_once(':').unwrap();
                        let name = ck_exec::proxy::canonical_header(name.trim());
                        let value = value.trim().to_owned();
                        if name == "Content-Length" {
                            length = value.parse().unwrap();
                            continue;
                        }
                        if name == "Host" {
                            host = value;
                            continue;
                        }
                        header
                            .entry(name)
                            .or_insert_with(|| Value::Array(Vec::new()))
                            .as_array_mut()
                            .unwrap()
                            .push(value.into());
                    }
                    let mut body = buf[head_end + 4..].to_vec();
                    while body.len() < length {
                        let mut chunk = [0u8; 4096];
                        let n = stream.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        body.extend_from_slice(&chunk[..n]);
                    }
                    let path = uri.split('?').next().unwrap_or_default().to_owned();
                    seen.lock().unwrap().push(serde_json::json!({
                        "method": method, "uri": uri, "host": host, "header": header, "body": String::from_utf8_lossy(&body),
                    }));
                    let route = &routes[path.as_str()];
                    let (status, text) = match route["Status"].as_u64() {
                        Some(s) => (s, route["Body"].as_str().unwrap_or_default().to_owned()),
                        None => (404, String::new()),
                    };
                    let mut response = format!(
                        "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n",
                        text.len()
                    );
                    if let Some(location) = route["Location"].as_str().filter(|l| !l.is_empty()) {
                        response.push_str(&format!("Location: {location}\r\n"));
                    }
                    response.push_str("\r\n");
                    response.push_str(&text);
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
    }
}

struct Dirs {
    auths: PathBuf,
    runtime_auths: std::sync::Mutex<Option<PathBuf>>,
    /// Auth file name to its `auth_index`.
    indexes: std::sync::Mutex<Vec<(String, String)>>,
    upstream: Upstream,
}

impl Dirs {
    fn placeholders(&self, s: &str) -> String {
        let mut s = s.replace("AUTHDIR", &self.auths.to_string_lossy());
        for (name, index) in self.indexes.lock().unwrap().iter() {
            s = s.replace(&format!("AUTHINDEX({name})"), index);
        }
        let upstream = self.upstream.base.lock().unwrap().clone();
        if !upstream.is_empty() {
            s = s.replace("UPSTREAM", &upstream);
        }
        s
    }

    fn normalize(&self, s: &str) -> String {
        let mut s = s.replace(&*self.auths.to_string_lossy(), "AUTHDIR");
        if let Some(auths) = self.runtime_auths.lock().unwrap().as_ref() {
            s = s.replace(&*auths.to_string_lossy(), "AUTHDIR");
        }
        for (name, index) in self.indexes.lock().unwrap().iter() {
            s = s.replace(index, &format!("AUTHINDEX({name})"));
        }
        let upstream = self.upstream.base.lock().unwrap().clone();
        if !upstream.is_empty() {
            s = s.replace(&upstream, "UPSTREAM");
            s = s.replace(upstream.trim_start_matches("http://"), "UPSTREAMHOST");
        }
        s
    }
}

/// The binary's app (API routes, then management) with every request coming from
/// `127.0.0.1:40000`.
async fn serve(rt: Arc<ck_server::Runtime>, state: Arc<Management>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = ck_server::app(rt, management::router(state)).layer(axum::middleware::from_fn(
        |mut req: axum::extract::Request, next: axum::middleware::Next| async move {
            let peer: SocketAddr = "127.0.0.1:40000".parse().unwrap();
            req.extensions_mut().insert(axum::extract::ConnectInfo(peer));
            next.run(req).await
        },
    ));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    base
}

async fn start(dirs: &Dirs, yaml: &str) -> String {
    let hash = bcrypt::hash("fake-secret", 4).unwrap();
    let store = Arc::new(ck_server::store::config_store::ConfigStore::memory_with(
        &ck_server::testing::without_auth_dir(&dirs.placeholders(yaml)).replace("$HASH", &hash),
    ));
    let mut config = store.load().unwrap().config;
    config.auth_dir = dirs.auths.clone();
    let credentials = ck_core::config::credentials::load(&config);
    let rt = Arc::new(ck_server::testing::runtime(
        config,
        credentials,
        Executors {
            claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: Default::default(),
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        },
    ));
    *dirs.indexes.lock().unwrap() = rt
        .store()
        .snapshot()
        .iter()
        .filter_map(|c| match &c.source {
            ck_core::credential::Source::File(path) => Some((
                path.file_name()?.to_string_lossy().into_owned(),
                ck_core::config::credentials::auth_index(c),
            )),
            _ => None,
        })
        .collect();
    *dirs.runtime_auths.lock().unwrap() = Some(rt.config().auth_dir.clone());
    serve(rt.clone(), Management::new(rt, store)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_probe_fetch_matches_go() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/quota_probe_go.json")).unwrap();
    let work = Path::new(env!("CARGO_TARGET_TMPDIR")).join("quota-probe-go");
    let _ = std::fs::remove_dir_all(&work);
    let dirs = Dirs {
        auths: work.join("auths"),
        runtime_auths: Default::default(),
        indexes: Default::default(),
        upstream: Upstream::default(),
    };
    std::fs::create_dir_all(&dirs.auths).unwrap();
    let client = wreq::Client::new();
    let mut base = String::new();
    let mut failures = Vec::new();
    for (i, step) in fixture["steps"].as_array().unwrap().iter().enumerate() {
        let args = &step["args"];
        let got = match step["op"].as_str().unwrap() {
            "config" => {
                base = start(&dirs, args.as_str().unwrap()).await;
                Value::Null
            }
            "auths" => {
                use std::os::unix::fs::PermissionsExt as _;
                assert!(base.is_empty(), "credentials are staged before the server starts");
                for (name, body) in args.as_object().unwrap() {
                    let path = dirs.auths.join(name);
                    std::fs::write(&path, dirs.placeholders(body.as_str().unwrap())).unwrap();
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
                }
                Value::Null
            }
            "upstream" => {
                dirs.upstream.start(args.clone()).await;
                Value::Null
            }
            "upstream_requests" => {
                let seen = std::mem::take(&mut *dirs.upstream.seen.lock().unwrap());
                serde_json::from_str(&dirs.normalize(&Value::Array(seen).to_string())).unwrap()
            }
            "http" => {
                let method = wreq::Method::from_bytes(args["method"].as_str().unwrap().as_bytes()).unwrap();
                let body = dirs.placeholders(args["body"].as_str().unwrap_or_default());
                let res = client
                    .request(
                        method,
                        format!("{base}{}", dirs.placeholders(args["path"].as_str().unwrap())),
                    )
                    .header("Authorization", "Bearer fake-secret")
                    .body(body)
                    .send()
                    .await
                    .unwrap();
                let status = res.status().as_u16();
                let content_type = res
                    .headers()
                    .get("content-type")
                    .map(|v| v.to_str().unwrap().to_owned())
                    .unwrap_or_default();
                let version = res.headers().contains_key("x-ck-version");
                let text = res.text().await.unwrap();
                // A probe URL carries the credential's token after substitution.
                assert!(!text.contains("sk-review-secret"), "step {i}: {text}");
                serde_json::json!({
                    "status": status,
                    "content_type": content_type,
                    "body": dirs.normalize(&text),
                    "version_headers": version,
                })
            }
            other => panic!("unknown step {other}"),
        };
        if step["result"] != got {
            failures.push(format!(
                "step {i} {} {}\n  go:   {}\n  rust: {}",
                step["op"], args, step["result"], got
            ));
        }
    }
    let _ = std::fs::remove_dir_all(&work);
    assert!(
        failures.is_empty(),
        "{} step(s) differ from Go:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
