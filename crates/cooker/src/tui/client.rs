//! Management API client with authenticated requests, URL redaction and bounded redirects.
use ck_core::gostr::GoStr;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use serde_json::{Map, Value};

/// A JSON object as Go decodes it into `map[string]any`.
pub type Object = Map<String, Value>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientKey {
    pub id: String,
    pub rev: u64,
}

/// An authorized original document and its content precondition; never debug-print it.
#[derive(Clone)]
pub struct CredentialSource {
    pub bytes: Vec<u8>,
    pub sha256: String,
}

pub struct CredentialSources {
    pub auth_dir: std::path::PathBuf,
    pub documents: BTreeMap<String, CredentialSource>,
}

/// Go `NewClientWithBaseURL`'s base URL rules.
pub fn normalize_base_url(raw: &str) -> String {
    let base = raw.trim();
    if base.is_empty() {
        return "http://127.0.0.1:8317".into();
    }
    let lower = base.go_lower();
    let base = if lower.starts_with("http://") || lower.starts_with("https://") {
        base.to_owned()
    } else {
        format!("http://{base}")
    };
    base.trim_end_matches('/').to_owned()
}

/// Go `url.QueryEscape`.
pub fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Go `json.Marshal` of a `map[string]any`: keys sorted, `<`, `>` and `&` escaped.
pub fn marshal_map(pairs: Vec<(&str, Value)>) -> String {
    let mut pairs = pairs;
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let map: Object = pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect();
    Value::Object(map)
        .to_string()
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
}

pub struct Client {
    base_url: String,
    secret: Mutex<String>,
    http: wreq::Client,
}

impl Client {
    pub fn new(base_url: &str, secret: &str) -> Self {
        Client {
            base_url: normalize_base_url(base_url),
            secret: Mutex::new(secret.trim().to_owned()),
            http: ck_exec::proxy::default_client(),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Go `SetSecretKey`.
    pub fn set_secret_key(&self, secret: &str) {
        *self.secret.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = secret.trim().to_owned();
    }

    /// Go `doRequest`: the status and the whole body, with `http.Client`'s rules: at
    /// most ten requests across redirects, 301-303 turn into a body-less GET, the
    /// Bearer key follows only to the same host or a subdomain, and the ten-second
    /// timeout covers the whole exchange. Errors read like Go's `url.Error`, with the
    /// URL redacted (`ck_wire::redact::url`). Atomic owner writes and privileged
    /// snapshot reads instead require the final management URL without redirects.
    // ponytail: the transport cause is the OS error text ("Connection refused (os error
    // 111)") where Go prints its dialer's ("dial tcp ...: connect: connection refused").
    async fn request(&self, method: wreq::Method, path: &str, body: Option<String>) -> Result<(u16, Vec<u8>), String> {
        self.request_at_revision(method, path, body, None).await
    }

    async fn request_at_revision(
        &self,
        method: wreq::Method,
        path: &str,
        body: Option<String>,
        revision: Option<u64>,
    ) -> Result<(u16, Vec<u8>), String> {
        let url = format!("{}{path}", self.base_url);
        let strict_owner_request =
            (path.starts_with("/admin/v1/") && method != wreq::Method::GET && method != wreq::Method::HEAD)
                || revision.is_some()
                || matches!(
                    path,
                    "/admin/v1/credentials/batch"
                        | "/admin/v1/apply?dry_run=true"
                        | "/admin/v1/credentials"
                        | "/admin/v1/export?include_secrets=true"
                        | "/admin/v1/r/setting/requests?include_secrets=true"
                        | "/admin/v1/r/setting/oauth"
                );
        let mut op = method.as_str().go_lower();
        op[..1].make_ascii_uppercase();
        // Go's url.Error names the request that failed; here its URL is redacted
        // (owner ruling Q18).
        let fail_at =
            |at: &str, cause: &str| format!("{op} {}: {cause}", ck_wire::gostr::quote(ck_wire::redact::url(at)));
        let secret = self
            .secret
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let headers_seen = std::sync::atomic::AtomicBool::new(false);
        // The request in flight, for a timeout's message (Go names the current hop).
        let in_flight = std::sync::Mutex::new(url.clone());
        let exchange = async {
            let initial = url::Url::parse(&url).map_err(|e| fail_at(&url, &e.to_string()))?;
            let (mut current, mut method, mut body) = (initial.clone(), method.clone(), body);
            let (mut with_auth, mut referer) = (!secret.is_empty(), None::<String>);
            let mut sent = 0;
            let res = loop {
                // wreq turns URL userinfo into Basic auth; Go does so only when no
                // Authorization header is set, so the Bearer key replaces it.
                let mut target = current.clone();
                if with_auth {
                    let _ = target.set_username("");
                    let _ = target.set_password(None);
                }
                let mut req = self
                    .http
                    .request(method.clone(), target.as_str())
                    .redirect(wreq::redirect::Policy::none());
                if with_auth {
                    req = req.header("Authorization", format!("Bearer {secret}"));
                }
                if let Some(revision) = revision {
                    req = req.header("If-Match", format!("\"{revision}\""));
                }
                if let Some(body) = &body {
                    req = req.header("Content-Type", "application/json").body(body.clone());
                }
                if let Some(referer) = &referer {
                    req = req.header("Referer", referer.clone());
                }
                let shown = current.to_string();
                *in_flight.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = shown.clone();
                let res = req.send().await.map_err(|e| fail_at(&shown, &cause(&e)))?;
                sent += 1;
                let location = res
                    .headers()
                    .get("location")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                let status = res.status().as_u16();
                if strict_owner_request && (300..400).contains(&status) {
                    return Err(
                        "owner snapshot reads and atomic writes must use the final management URL without redirects"
                            .into(),
                    );
                }
                if !matches!(status, 301 | 302 | 303 | 307 | 308) || location.is_empty() {
                    break res;
                }
                let next = current.join(&location).map_err(|e| {
                    let quoted = ck_wire::gostr::quote(ck_wire::redact::url(&location));
                    fail_at(&shown, &format!("failed to parse Location header {quoted}: {e}"))
                })?;
                if sent >= 10 {
                    // Go names the Location value here.
                    return Err(fail_at(&location, "stopped after 10 redirects"));
                }
                // Go builds the next request before draining, so a timeout from here on
                // names it.
                *in_flight.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = next.to_string();
                // Go drains a small or unsized redirect body (2 KiB at most) before
                // following it.
                if res.content_length().is_none_or(|n| n <= 2048) {
                    use futures_util::StreamExt;
                    let mut body = res.bytes_stream();
                    let mut drained = 0;
                    while drained < 2048 {
                        match body.next().await {
                            Some(Ok(chunk)) => drained += chunk.len(),
                            _ => break,
                        }
                    }
                }
                if status <= 303 {
                    body = None;
                    if method != wreq::Method::GET && method != wreq::Method::HEAD {
                        method = wreq::Method::GET;
                    }
                }
                if with_auth && !same_site(&initial, &next) {
                    with_auth = false;
                }
                referer = (!(current.scheme() == "https" && next.scheme() == "http")).then(|| {
                    let mut last = current.clone();
                    let _ = last.set_username("");
                    let _ = last.set_password(None);
                    last.to_string()
                });
                current = next;
            };
            headers_seen.store(true, std::sync::atomic::Ordering::Relaxed);
            let status = res.status().as_u16();
            let data = res.bytes().await.map_err(|e| cause(&e))?;
            Ok((status, data.to_vec()))
        };
        match tokio::time::timeout(Duration::from_secs(10), exchange).await {
            Ok(result) => result,
            Err(_) if headers_seen.load(std::sync::atomic::Ordering::Relaxed) => {
                Err("context deadline exceeded (Client.Timeout or context cancellation while reading body)".into())
            }
            Err(_) => {
                let at = in_flight
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                Err(fail_at(
                    &at,
                    "context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
                ))
            }
        }
    }

    /// Go `get`/`put`/`patch`: a status of 400 or more is `HTTP <code>: <body>`.
    /// Atomic owner applies additionally require a completed revision receipt.
    async fn checked(&self, method: wreq::Method, path: &str, body: Option<String>) -> Result<Vec<u8>, String> {
        let owner_commit = method == wreq::Method::POST && path == "/admin/v1/apply";
        let (code, data) = self.request(method, path, body).await?;
        if code >= 400 || (owner_commit && code != 200) {
            let text = String::from_utf8_lossy(ck_wire::gostr::trim_space(&data)).into_owned();
            return Err(format!("HTTP {code}: {text}"));
        }
        if owner_commit {
            let receipt: Value =
                serde_json::from_slice(&data).map_err(|_| "invalid configuration commit response".to_owned())?;
            if receipt.get("revision").and_then(Value::as_u64).is_none() {
                return Err("configuration commit response has no revision".into());
            }
        }
        Ok(data)
    }

    /// Go `getJSON`.
    pub async fn get_json(&self, path: &str) -> Result<Object, String> {
        let data = self.checked(wreq::Method::GET, path, None).await?;
        match serde_json::from_slice::<Value>(&data).map_err(|e| json_error(&data, &e))? {
            Value::Object(o) => Ok(o),
            Value::Null => Ok(Object::new()),
            other => Err(type_error(&other, "map[string]interface {}")),
        }
    }

    /// Go `postJSON`: a failure is `HTTP <code>` only.
    async fn post_json(&self, path: &str, body: String) -> Result<(), String> {
        let (code, _) = self.request(wreq::Method::POST, path, Some(body)).await?;
        if code >= 400 {
            return Err(format!("HTTP {code}"));
        }
        Ok(())
    }

    /// A DELETE whose failure is Go's `delete failed (HTTP <code>)`.
    async fn delete(&self, path: &str, failure: &str) -> Result<(), String> {
        let (code, _) = self.request(wreq::Method::DELETE, path, None).await?;
        if code >= 400 {
            return Err(failure.replace("{code}", &code.to_string()));
        }
        Ok(())
    }

    /// Submit a versioned configuration to the owner's atomic import endpoint.
    pub async fn import_config(&self, document: String) -> Result<u64, String> {
        let head = self.get_json("/admin/v1/revisions?limit=1").await?;
        let revisions = head
            .get("revisions")
            .and_then(Value::as_array)
            .ok_or_else(|| "invalid configuration revision response".to_owned())?;
        let revision = match revisions.first() {
            Some(latest) => latest
                .get("rev")
                .and_then(Value::as_u64)
                .ok_or_else(|| "invalid configuration revision response".to_owned())?,
            None => 0,
        };
        let (status, data) = self
            .request_at_revision(wreq::Method::POST, "/admin/v1/import", Some(document), Some(revision))
            .await?;
        if status >= 400 {
            return Err(format!("HTTP {status}"));
        }
        let response: Value =
            serde_json::from_slice(&data).map_err(|_| "invalid configuration import response".to_owned())?;
        response
            .get("revision")
            .and_then(Value::as_u64)
            .ok_or_else(|| "configuration import response has no revision".to_owned())
    }

    /// Original provider-rendered bytes, committed together by the running owner.
    pub async fn save_credentials(
        &self,
        documents: &[(String, Vec<u8>)],
        sources: Option<&BTreeMap<String, CredentialSource>>,
    ) -> Result<(), String> {
        self.apply_credential_changes(documents, &[], sources).await
    }

    pub async fn apply_credential_changes(
        &self,
        documents: &[(String, Vec<u8>)],
        deletions: &[String],
        sources: Option<&BTreeMap<String, CredentialSource>>,
    ) -> Result<(), String> {
        use base64::Engine as _;
        let deleted: Vec<_> = deletions
            .iter()
            .map(|name| {
                let source = sources
                    .and_then(|sources| sources.get(name))
                    .ok_or("credential deletion has no captured source")?;
                Ok(serde_json::json!({"name": name,"if_match":source.sha256}))
            })
            .collect::<Result<_, String>>()?;
        let mut body = serde_json::json!({"documents": documents.iter().map(|(name, bytes)| {
            let mut document = serde_json::json!({"name": name, "data": base64::engine::general_purpose::STANDARD.encode(bytes)});
            if let Some(sources) = sources {
                if let Some(source) = sources.get(name) {
                    document["if_match"] = source.sha256.clone().into();
                } else {
                    document["if_none_match"] = true.into();
                }
            }
            document
        }).collect::<Vec<_>>()});
        if !deletions.is_empty() {
            body["deletions"] = deleted.into();
        }
        let (status, bytes) = self
            .request(
                wreq::Method::POST,
                "/admin/v1/credentials/batch",
                Some(body.to_string()),
            )
            .await?;
        if status != 200 {
            return Err(format!("HTTP {status}"));
        }
        let response: Value =
            serde_json::from_slice(&bytes).map_err(|_| "invalid credential batch response".to_owned())?;
        if response.get("status").and_then(Value::as_str) != Some("ok")
            || response.get("saved").and_then(Value::as_u64) != Some(documents.len() as u64)
            || (!deletions.is_empty()
                && response.get("deleted").and_then(Value::as_u64) != Some(deletions.len() as u64))
        {
            return Err("credential batch was not acknowledged".into());
        }
        Ok(())
    }

    /// A normal dry-run command checks admission before any provider grant work.
    pub async fn credential_admission(&self) -> Result<(), String> {
        let (status, bytes) = self
            .request(
                wreq::Method::POST,
                "/admin/v1/apply?dry_run=true",
                Some(serde_json::json!({"ops": []}).to_string()),
            )
            .await?;
        if status != 200 {
            return Err(format!("HTTP {status}"));
        }
        let response: Value =
            serde_json::from_slice(&bytes).map_err(|_| "invalid owner admission response".to_owned())?;
        if response["dry_run"] != true || response["written"] != false {
            return Err("owner admission was not acknowledged".into());
        }
        Ok(())
    }

    pub async fn credential_sources(&self) -> Result<CredentialSources, String> {
        use base64::Engine as _;
        let response = self.get_json("/admin/v1/credentials").await?;
        let invalid = || "invalid credential source response".to_owned();
        let documents = response
            .get("documents")
            .and_then(Value::as_array)
            .ok_or_else(invalid)?;
        let mut sources = BTreeMap::new();
        for document in documents {
            let name = document.get("name").and_then(Value::as_str).ok_or_else(invalid)?;
            let hash = document.get("sha256").and_then(Value::as_str).ok_or_else(invalid)?;
            if name.is_empty()
                || name.contains(['/', '\\', ':'])
                || name.chars().any(char::is_control)
                || !name.go_lower().ends_with(".json")
                || hash.len() != 64
                || !hash.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(invalid());
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(document.get("data").and_then(Value::as_str).ok_or_else(invalid)?)
                .map_err(|_| invalid())?;
            if sources
                .insert(
                    name.to_owned(),
                    CredentialSource {
                        bytes,
                        sha256: hash.into(),
                    },
                )
                .is_some()
            {
                return Err(invalid());
            }
        }
        let auth_dir = response
            .get("auth_dir")
            .and_then(Value::as_str)
            .filter(|path| !path.is_empty())
            .ok_or_else(invalid)?;
        Ok(CredentialSources {
            auth_dir: auth_dir.into(),
            documents: sources,
        })
    }

    /// Reuse the SQL-free resource importer/compiler; never open a local store.
    /// The configuration a built-in login reads: the outbound proxy, whose URL can
    /// carry credentials, and the SIWC client identifier. Never the full export.
    pub async fn login_config(&self) -> Result<ck_core::config::Config, String> {
        let requests = self
            .setting("/admin/v1/r/setting/requests?include_secrets=true")
            .await?;
        let identifier = self.setting("/admin/v1/r/setting/oauth").await?.and_then(|oauth| {
            oauth
                .pointer("/providers/codex/signin-with-chatgpt-identifier")
                .cloned()
        });
        let mut document = ck_server::store::config_store::resources::export_json(0, &[], false);
        let resources = document["resources"].as_array_mut().expect("export envelope");
        if let Some(spec) = requests {
            resources.push(serde_json::json!({"kind": "setting", "id": "requests", "spec": spec}));
        }
        if let Some(identifier) = identifier {
            let spec = serde_json::json!({"providers": {"codex": {"signin-with-chatgpt-identifier": identifier}}});
            resources.push(serde_json::json!({"kind": "setting", "id": "oauth", "spec": spec}));
        }
        Self::compile(document)
    }

    fn compile(document: Value) -> Result<ck_core::config::Config, String> {
        let resources = ck_server::store::config_store::resources::import_json(&document.to_string())
            .map_err(|_| "invalid owner configuration export".to_owned())?;
        ck_server::store::config_store::resources::config(&resources)
            .map(|(config, _)| config)
            .map_err(|_| "invalid owner configuration".to_owned())
    }

    /// One setting's spec; `None` only for the owner's structured `not_found`.
    async fn setting(&self, path: &str) -> Result<Option<Value>, String> {
        let (code, data) = self.request(wreq::Method::GET, path, None).await?;
        let body: Option<Value> = serde_json::from_slice(&data).ok();
        if code == 404 && body.as_ref().and_then(|b| b.get("error")) == Some(&Value::from("not_found")) {
            return Ok(None);
        }
        if code >= 400 {
            let text = String::from_utf8_lossy(ck_wire::gostr::trim_space(&data)).into_owned();
            return Err(format!("HTTP {code}: {text}"));
        }
        body.and_then(|mut b| b.get_mut("spec").map(Value::take))
            .ok_or_else(|| "invalid owner setting response".to_owned())
            .map(Some)
    }

    pub async fn get_config(&self) -> Result<Object, String> {
        self.get_json("/admin/v1/effective/config").await
    }

    pub async fn get_auth_files(&self) -> Result<Vec<Object>, String> {
        extract_list(&self.get_json("/v0/management/auth-files").await?, "files")
    }

    pub async fn delete_auth_file(&self, name: &str) -> Result<(), String> {
        let path = format!("/v0/management/auth-files?name={}", query_escape(name));
        self.delete(&path, "delete failed (HTTP {code})").await
    }

    pub async fn toggle_auth_file(&self, name: &str, disabled: bool) -> Result<(), String> {
        let body = marshal_map(vec![("name", name.into()), ("disabled", disabled.into())]);
        self.checked(wreq::Method::PATCH, "/v0/management/auth-files/status", Some(body))
            .await
            .map(drop)
    }

    pub async fn patch_auth_file_fields(&self, name: &str, mut fields: Vec<(&str, Value)>) -> Result<(), String> {
        fields.push(("name", name.into()));
        let body = marshal_map(fields);
        self.checked(wreq::Method::PATCH, "/v0/management/auth-files/fields", Some(body))
            .await
            .map(drop)
    }

    pub async fn refresh_auth_file(&self, name: &str) -> Result<(), String> {
        let body = marshal_map(vec![("name", name.into())]);
        self.post_json("/v0/management/auth-files/refresh", body).await
    }

    /// Go `GetLogs`: new lines and the latest timestamp (never below `after`).
    pub async fn get_logs(&self, after: i64, limit: i64) -> Result<(Vec<String>, i64), String> {
        // url.Values.Encode sorts the keys.
        let mut query = Vec::new();
        if after > 0 {
            query.push(format!("after={after}"));
        }
        if limit > 0 {
            query.push(format!("limit={limit}"));
        }
        let mut path = "/v0/management/logs".to_owned();
        if !query.is_empty() {
            path = format!("{path}?{}", query.join("&"));
        }
        let wrapper = self.get_json(&path).await?;
        let lines = strings(wrapper.get("lines"))?;
        let latest = match wrapper.get("latest-timestamp").and_then(Value::as_f64) {
            Some(f) => (f as i64).max(after),
            None => after,
        };
        Ok((lines, latest))
    }

    pub async fn get_client_keys(&self) -> Result<Vec<ClientKey>, String> {
        extract_list(&self.get_json("/admin/v1/r/client-key").await?, "items")?
            .into_iter()
            .map(|item| {
                let id = item.get("id").and_then(Value::as_str).ok_or("missing client-key id")?;
                let rev = item
                    .get("rev")
                    .and_then(Value::as_u64)
                    .ok_or("missing client-key revision")?;
                Ok(ClientKey { id: id.to_owned(), rev })
            })
            .collect()
    }

    pub async fn add_client_key(&self, key: &str) -> Result<(), String> {
        if key == "$secret" {
            return Err("enter a new client key".into());
        }
        let id = format!(
            "tui-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_nanos()
        );
        let body = serde_json::json!({"ops": [{"op": "put", "kind": "client-key", "id": id, "spec": {"key": key}, "if_absent": true}]}).to_string();
        self.checked(wreq::Method::POST, "/admin/v1/apply", Some(body))
            .await
            .map(drop)
    }

    pub async fn edit_client_key(&self, key: &ClientKey, value: &str) -> Result<(), String> {
        if value == "$secret" {
            return Err("enter a new client key".into());
        }
        let body = serde_json::json!({"ops": [{"op": "patch", "kind": "client-key", "id": key.id, "patch": {"key": value}, "if_rev": key.rev}]}).to_string();
        self.checked(wreq::Method::POST, "/admin/v1/apply", Some(body))
            .await
            .map(drop)
    }

    pub async fn delete_client_key(&self, key: &ClientKey) -> Result<(), String> {
        let body =
            serde_json::json!({"ops": [{"op": "delete", "kind": "client-key", "id": key.id, "if_rev": key.rev}]})
                .to_string();
        self.checked(wreq::Method::POST, "/admin/v1/apply", Some(body))
            .await
            .map(drop)
    }

    /// Copy explicitly reveals one secret; ordinary reads never disclose key material.
    pub async fn reveal_client_key(&self, key: &ClientKey) -> Result<String, String> {
        let export = self.get_json("/admin/v1/export?include_secrets=true").await?;
        let resources = extract_list(&export, "resources")?;
        let resource = resources
            .iter()
            .find(|r| {
                r.get("kind").and_then(Value::as_str) == Some("client-key")
                    && r.get("id").and_then(Value::as_str) == Some(key.id.as_str())
            })
            .ok_or("client key no longer exists")?;
        let value = resource
            .get("spec")
            .and_then(|v| v.get("key"))
            .and_then(Value::as_str)
            .filter(|v| *v != "$secret")
            .ok_or("client key was not revealed")?;
        Ok(value.to_owned())
    }

    pub async fn get_key_list(&self, family: &str) -> Result<Vec<Object>, String> {
        let cfg = self.get_config().await?;
        let providers = value_at(&cfg, &format!("api-keys/{family}"));
        let mut keys = Vec::new();
        for provider in providers.and_then(Value::as_array).into_iter().flatten() {
            let Some(entry) = provider.as_object() else { continue };
            if family == "openai-compatibility" {
                keys.push(entry.clone());
                continue;
            }
            for key in entry.get("keys").and_then(Value::as_array).into_iter().flatten() {
                let mut item = entry.clone();
                if let Some(key) = key.as_object() {
                    item.extend(key.clone());
                }
                keys.push(item);
            }
        }
        Ok(keys)
    }

    /// Go `GetAuthStatus`: the status and error strings.
    pub async fn get_auth_status(&self, state: &str) -> Result<(String, String), String> {
        let path = format!("/v0/management/get-auth-status?state={}", query_escape(state));
        let wrapper = self.get_json(&path).await?;
        let text = |k: &str| wrapper.get(k).and_then(Value::as_str).unwrap_or_default().to_owned();
        Ok((text("status"), text("error")))
    }

    /// Go `CancelAuthSession`.
    pub async fn cancel_auth_session(&self, state: &str) -> Result<(), String> {
        let state = state.trim();
        if state.is_empty() {
            return Ok(());
        }
        let path = format!("/v0/management/oauth-session?state={}", query_escape(state));
        self.delete(&path, "HTTP {code}").await
    }

    pub async fn put_field(&self, path: &str, value: Value) -> Result<(), String> {
        let body = serde_json::json!({"ops": [{"op": "set", "path": path, "value": value}]}).to_string();
        self.checked(wreq::Method::POST, "/admin/v1/apply", Some(body))
            .await
            .map(drop)
    }

    /// The v0 OAuth callback submission (Go's `submitCallback`).
    pub async fn post_oauth_callback(&self, provider: &str, redirect_url: &str, state: &str) -> Result<(), String> {
        let body = marshal_map(vec![
            ("provider", provider.into()),
            ("redirect_url", redirect_url.into()),
            ("state", state.into()),
        ]);
        self.post_json("/v0/management/oauth-callback", body).await
    }
}

pub fn value_at<'a>(cfg: &'a Object, path: &str) -> Option<&'a Value> {
    let (head, rest) = path.split_once('/').unwrap_or((path, ""));
    let mut value = cfg.get(head)?;
    for part in rest.split('/').filter(|part| !part.is_empty()) {
        value = value.get(part)?;
    }
    Some(value)
}

/// The innermost cause of a request error.
fn cause(e: &wreq::Error) -> String {
    let mut cause: &dyn std::error::Error = e;
    while let Some(next) = cause.source() {
        cause = next;
    }
    cause.to_string()
}

/// Go `shouldCopyHeaderOnRedirect`: the same host name, or a subdomain of it.
fn same_site(initial: &url::Url, next: &url::Url) -> bool {
    let (parent, host) = (
        initial.host_str().unwrap_or_default(),
        next.host_str().unwrap_or_default(),
    );
    host == parent
        || (!host.contains([':', '%'])
            && host.len() > parent.len()
            && host.ends_with(parent)
            && host.as_bytes()[host.len() - parent.len() - 1] == b'.')
}

/// The JSON kind encoding/json names in an `UnmarshalTypeError`.
fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// encoding/json's `UnmarshalTypeError` text.
fn type_error(v: &Value, target: &str) -> String {
    format!("json: cannot unmarshal {} into Go value of type {target}", kind(v))
}

/// encoding/json's `quoteChar`: the byte as a rune, `strconv`-quoted in single quotes.
fn quote_char(b: u8) -> String {
    match b {
        b'\'' => "'\\''".into(),
        b'"' => "'\"'".into(),
        b => {
            let q = ck_wire::gostr::quote(char::from(b).to_string());
            format!("'{}'", &q[1..q.len() - 1])
        }
    }
}

/// What Go's scanner says when the input ends inside a top-level literal or number
/// (`scanner.eof` feeds it a space); `None` where it says "unexpected end of JSON input".
fn eof_error(value: &[u8]) -> Option<String> {
    for word in ["true", "false", "null"] {
        if !value.is_empty() && value.len() < word.len() && word.as_bytes().starts_with(value) {
            let expecting = word.as_bytes()[value.len()] as char;
            return Some(format!(
                "invalid character ' ' in literal {word} (expecting '{expecting}')"
            ));
        }
    }
    let numeric = value
        .iter()
        .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E'));
    let context = match (value.last()?, value.len()) {
        (b'-', 1) => "in numeric literal",
        (b'.', _) => "after decimal point in numeric literal",
        (b'e' | b'E' | b'+' | b'-', _) => "in exponent of numeric literal",
        _ => return None,
    };
    numeric.then(|| format!("invalid character ' ' {context}"))
}

/// encoding/json's syntax error text for the common cases.
// ponytail: an error inside a document keeps serde's wording.
fn json_error(data: &[u8], e: &serde_json::Error) -> String {
    let space = |b: &u8| matches!(b, b' ' | b'\t' | b'\n' | b'\r');
    let start = data.iter().position(|b| !space(b)).unwrap_or(data.len());
    let value = &data[start..];
    let Some(&first) = value.first() else {
        return "unexpected end of JSON input".into();
    };
    if !matches!(first, b'{' | b'[' | b'"' | b'-' | b'0'..=b'9' | b't' | b'f' | b'n') {
        return format!("invalid character {} looking for beginning of value", quote_char(first));
    }
    if e.is_eof() {
        let end = value.iter().rposition(|b| !space(b)).map_or(0, |i| i + 1);
        return eof_error(&value[..end]).unwrap_or_else(|| "unexpected end of JSON input".into());
    }
    if e.to_string().starts_with("trailing characters") {
        let mut stream = serde_json::Deserializer::from_slice(data).into_iter::<Value>();
        let _ = stream.next();
        if let Some(&b) = data[stream.byte_offset()..].iter().find(|b| !space(b)) {
            return format!("invalid character {} after top-level value", quote_char(b));
        }
    }
    e.to_string()
}

/// Go `extractList`: a missing or null member is no list; elements must be objects
/// (a null element is an empty map).
fn extract_list(wrapper: &Object, key: &str) -> Result<Vec<Object>, String> {
    match wrapper.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item {
                Value::Object(o) => Ok(o.clone()),
                Value::Null => Ok(Object::new()),
                other => Err(type_error(other, "map[string]interface {}")),
            })
            .collect(),
        Some(other) => Err(type_error(other, "[]map[string]interface {}")),
    }
}

/// A JSON `[]string` as Go decodes it (null elements are empty strings).
fn strings(v: Option<&Value>) -> Result<Vec<String>, String> {
    match v {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item {
                Value::String(s) => Ok(s.clone()),
                Value::Null => Ok(String::new()),
                other => Err(type_error(other, "string")),
            })
            .collect(),
        Some(other) => Err(type_error(other, "[]string")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Expected values: Go 1.26 json.Unmarshal into map[string]any.
    #[test]
    fn json_errors_read_like_go() {
        let err = |b: &[u8]| {
            let e = serde_json::from_slice::<Value>(b).unwrap_err();
            json_error(b, &e)
        };
        assert_eq!(err(b""), "unexpected end of JSON input");
        assert_eq!(err(b" {\"a\":"), "unexpected end of JSON input");
        assert_eq!(err(b"<html>"), "invalid character '<' looking for beginning of value");
        assert_eq!(err(b"{} x"), "invalid character 'x' after top-level value");
        assert_eq!(err(b"'a'"), "invalid character '\\'' looking for beginning of value");
        assert_eq!(err(b"tru"), "invalid character ' ' in literal true (expecting 'e')");
        assert_eq!(err(b"n"), "invalid character ' ' in literal null (expecting 'u')");
        assert_eq!(err(b"-"), "invalid character ' ' in numeric literal");
        assert_eq!(
            err(b"1."),
            "invalid character ' ' after decimal point in numeric literal"
        );
        assert_eq!(err(b"1e+"), "invalid character ' ' in exponent of numeric literal");
        assert_eq!(err(b"[1,"), "unexpected end of JSON input");
        assert_eq!(
            err("é".as_bytes()),
            "invalid character 'Ã' looking for beginning of value"
        );
        assert_eq!(err("{} é".as_bytes()), "invalid character 'Ã' after top-level value");
        assert_eq!(
            type_error(&serde_json::json!([1]), "map[string]interface {}"),
            "json: cannot unmarshal array into Go value of type map[string]interface {}"
        );
    }

    /// A refused connection reads like Go's url.Error, with the URL redacted: neither a
    /// password nor a query value such as the OAuth `state` shows (review of #127,
    /// finding 6).
    #[tokio::test]
    async fn request_errors_hide_url_credentials() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let client = Client::new(&format!("http://user:fake-url-password@127.0.0.1:{port}"), "k");
        let err = client.get_config().await.unwrap_err();
        assert!(
            err.starts_with(&format!("Get \"http://127.0.0.1:{port}/admin/v1/effective/config\": ")),
            "{err}"
        );
        assert!(!err.contains("fake-url-password") && !err.contains("user"), "{err}");
        let client = Client::new(&format!("http://127.0.0.1:{port}"), "");
        let err = client.get_auth_status("sk-review-secret").await.unwrap_err();
        assert!(
            err.starts_with(&format!(
                "Get \"http://127.0.0.1:{port}/v0/management/get-auth-status?state=***\": "
            )),
            "{err}"
        );
        assert!(!err.contains("sk-review-secret"), "{err}");
    }

    #[tokio::test]
    async fn config_import_preserves_its_condition_and_never_turns_a_redirect_into_success() {
        let followed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = followed.clone();
        let app = axum::Router::new()
            .route(
                "/admin/v1/revisions",
                axum::routing::get(|| async { axum::Json(serde_json::json!({"revisions": [{"rev": 7}]})) }),
            )
            .route(
                "/admin/v1/import",
                axum::routing::post(|headers: axum::http::HeaderMap| async move {
                    assert_eq!(headers.get("if-match").unwrap(), "\"7\"");
                    axum::response::Redirect::to("/not-an-import")
                }),
            )
            .route(
                "/not-an-import",
                axum::routing::get(move || {
                    let observed = observed.clone();
                    async move {
                        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        axum::Json(serde_json::json!({"revision": 7}))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let error = Client::new(&base, "fake-management-key")
            .import_config("{}".into())
            .await
            .unwrap_err();
        assert!(error.contains("without redirects"), "{error}");
        assert_eq!(followed.load(std::sync::atomic::Ordering::SeqCst), 0);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn login_config_reads_only_two_settings_and_tolerates_only_structured_absence() {
        let seen = std::sync::Arc::new(Mutex::new(Vec::<String>::new()));
        let record = seen.clone();
        let app = axum::Router::new()
            .route(
                "/admin/v1/r/setting/requests",
                axum::routing::get(|| async {
                    (
                        axum::http::StatusCode::NOT_FOUND,
                        axum::Json(serde_json::json!({"error": "not_found", "message": "no such resource"})),
                    )
                }),
            )
            .route(
                "/admin/v1/r/setting/oauth",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({"kind": "setting", "id": "oauth", "spec": {"providers": {"codex": {"signin-with-chatgpt-identifier": "fixture-id"}}}}))
                }),
            )
            .layer(axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let record = record.clone();
                    async move {
                        record.lock().unwrap().push(request.uri().to_string());
                        next.run(request).await
                    }
                },
            ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = Client::new(&base, "fake-management-key").login_config().await.unwrap();
        assert_eq!(ck_exec::codex_siwc::identifier(&config), "fixture-id");
        assert!(config.document.get("requests").is_none());
        assert_eq!(
            *seen.lock().unwrap(),
            [
                "/admin/v1/r/setting/requests?include_secrets=true",
                "/admin/v1/r/setting/oauth"
            ]
        );
        // A plain 404 (a wrong base URL) is an error, not an absent proxy.
        let error = Client::new(&format!("{base}/wrong"), "fake-management-key")
            .login_config()
            .await
            .unwrap_err();
        assert!(error.contains("HTTP 404"), "{error}");
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn credential_batches_reject_redirects_and_incomplete_acknowledgements() {
        let followed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = followed.clone();
        let app = axum::Router::new()
            .route(
                "/admin/v1/credentials/batch",
                axum::routing::post(|| async { axum::response::Redirect::to("/forged-receipt") }),
            )
            .route(
                "/admin/v1/apply",
                axum::routing::post(|| async { axum::response::Redirect::to("/forged-receipt") }),
            )
            .route(
                "/admin/v1/credentials",
                axum::routing::get(|| async { axum::response::Redirect::to("/forged-receipt") }),
            )
            .route(
                "/admin/v1/export",
                axum::routing::get(|| async { axum::response::Redirect::to("/forged-receipt") }),
            )
            .route(
                "/admin/v1/r/setting/{id}",
                axum::routing::get(|| async { axum::response::Redirect::to("/forged-receipt") }),
            )
            .route(
                "/forged-receipt",
                axum::routing::get(move || {
                    let observed = observed.clone();
                    async move {
                        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        axum::Json(serde_json::json!({"status": "ok", "saved": 1}))
                    }
                }),
            )
            .route(
                "/wrong/admin/v1/credentials/batch",
                axum::routing::post(|| async { axum::Json(serde_json::json!({"status": "ok", "saved": 0})) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let documents = vec![("test.json".into(), b"{}".to_vec())];
        let error = Client::new(&base, "fake-management-key")
            .save_credentials(&documents, None)
            .await
            .unwrap_err();
        assert!(error.contains("without redirects"), "{error}");
        assert_eq!(followed.load(std::sync::atomic::Ordering::SeqCst), 0);
        let error = Client::new(&base, "fake-management-key")
            .credential_admission()
            .await
            .unwrap_err();
        assert!(error.contains("without redirects"), "{error}");
        assert_eq!(followed.load(std::sync::atomic::Ordering::SeqCst), 0);
        let client = Client::new(&base, "fake-management-key");
        assert!(
            client
                .credential_sources()
                .await
                .err()
                .unwrap()
                .contains("without redirects")
        );
        assert!(client.login_config().await.err().unwrap().contains("without redirects"));
        assert_eq!(followed.load(std::sync::atomic::Ordering::SeqCst), 0);
        let error = Client::new(&format!("{base}/wrong"), "fake-management-key")
            .save_credentials(&documents, None)
            .await
            .unwrap_err();
        assert!(error.contains("not acknowledged"), "{error}");
        let sources = BTreeMap::from([(
            "test.json".into(),
            CredentialSource {
                bytes: b"{}".to_vec(),
                sha256: "00".repeat(32),
            },
        )]);
        let error = Client::new(&format!("{base}/wrong"), "fake-management-key")
            .apply_credential_changes(&[], &["test.json".into()], Some(&sources))
            .await
            .unwrap_err();
        assert!(
            error.contains("not acknowledged"),
            "a save receipt cannot acknowledge a deletion: {error}"
        );
        server.abort();
        let _ = server.await;
    }

    /// A one-shot HTTP server answering every request with `reply`; returns its port
    /// and the requests it saw.
    async fn record(reply: String) -> (u16, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let log = seen.clone();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = vec![0; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                log.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf[..n]).into_owned());
                let _ = sock.write_all(reply.as_bytes()).await;
            }
        });
        (port, seen)
    }

    #[tokio::test]
    async fn client_key_reads_keep_identity_not_secret_material() {
        let body =
            serde_json::json!({"items": [{"id": "client-one", "rev": 7, "spec": {"key": "$secret"}}]}).to_string();
        let (port, seen) = record(format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ))
        .await;
        let client = Client::new(&format!("http://127.0.0.1:{port}"), "key");
        assert_eq!(
            client.get_client_keys().await.unwrap(),
            [ClientKey {
                id: "client-one".into(),
                rev: 7
            }]
        );
        assert!(seen.lock().unwrap()[0].starts_with("GET /admin/v1/r/client-key "));
    }

    #[tokio::test]
    async fn redaction_markers_cannot_be_written_or_copied_as_keys() {
        let client = Client::new("http://127.0.0.1:1", "key");
        assert!(client.add_client_key("$secret").await.is_err());
        assert!(
            client
                .edit_client_key(
                    &ClientKey {
                        id: "client-one".into(),
                        rev: 7
                    },
                    "$secret"
                )
                .await
                .is_err()
        );
        let body =
            serde_json::json!({"resources": [{"kind":"client-key", "id":"client-one", "spec":{"key":"$secret"}}]})
                .to_string();
        let (port, seen) = record(format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ))
        .await;
        let client = Client::new(&format!("http://127.0.0.1:{port}"), "key");
        assert!(
            client
                .reveal_client_key(&ClientKey {
                    id: "client-one".into(),
                    rev: 7
                })
                .await
                .is_err()
        );
        assert!(seen.lock().unwrap()[0].starts_with("GET /admin/v1/export?include_secrets=true "));
    }

    #[tokio::test]
    async fn client_keys_patch_by_identity_and_revision() {
        let receipt = serde_json::json!({"revision":8}).to_string();
        let (port, seen) = record(format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{receipt}",
            receipt.len()
        ))
        .await;
        let client = Client::new(&format!("http://127.0.0.1:{port}"), "key");
        client
            .edit_client_key(
                &ClientKey {
                    id: "shown".into(),
                    rev: 7,
                },
                "edited",
            )
            .await
            .unwrap();
        let request = seen.lock().unwrap()[0].clone();
        assert!(request.starts_with("POST /admin/v1/apply "));
        let (_, body) = request.split_once("\r\n\r\n").unwrap();
        let body: Value = serde_json::from_str(body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"ops": [{"op":"patch", "kind":"client-key", "id":"shown", "if_rev":7, "patch":{"key":"edited"}}]})
        );
    }

    // Go TestClient_RemoteServerInteraction and TestNewAppWithBaseURL.
    #[tokio::test]
    async fn remote_get_config_sends_path_and_bearer() {
        let (port, seen) =
            record("HTTP/1.1 200 OK\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"status\":\"ok\"}".into())
                .await;
        let client = Client::new(&format!("http://127.0.0.1:{port}"), "remote-secret-key");
        let cfg = client.get_config().await.unwrap();
        assert_eq!(cfg.get("status"), Some(&Value::from("ok")));
        let request = seen.lock().unwrap()[0].clone();
        assert!(
            request.starts_with("GET /admin/v1/effective/config HTTP/1.1\r\n"),
            "{request}"
        );
        assert!(
            request
                .go_lower()
                .contains("authorization: bearer remote-secret-key\r\n"),
            "{request}"
        );
        let app = super::super::app::App::new("https://proxy.example.com", "secret", None);
        assert_eq!(app.base_url(), "https://proxy.example.com");
        // Go TestNewClient_BackwardsCompatibility: the standalone client's local URL.
        assert_eq!(
            normalize_base_url(&format!("http://127.0.0.1:{}", 8317)),
            "http://127.0.0.1:8317"
        );
    }

    /// URL userinfo never becomes a second Authorization header next to the Bearer key
    /// (Go sets Basic auth only when Authorization is unset).
    #[tokio::test]
    async fn bearer_replaces_url_basic_auth() {
        let (port, seen) = record("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".into()).await;
        let client = Client::new(&format!("http://u:fake-url-password@127.0.0.1:{port}"), "fake-bearer");
        client.get_config().await.unwrap();
        let request = seen.lock().unwrap()[0].go_lower();
        assert_eq!(request.matches("authorization:").count(), 1, "{request}");
        assert!(request.contains("authorization: bearer fake-bearer"));
    }

    /// Errors name the request that failed: the redirect target, or the Location value
    /// after ten redirects.
    #[tokio::test]
    async fn redirect_errors_name_the_failing_hop() {
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead = closed.local_addr().unwrap().port();
        drop(closed);
        let (port, _) = record(format!(
            "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{dead}/gone\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        ))
        .await;
        let err = Client::new(&format!("http://127.0.0.1:{port}"), "k")
            .get_config()
            .await
            .unwrap_err();
        assert!(
            err.starts_with(&format!("Get \"http://127.0.0.1:{dead}/gone\": ")),
            "{err}"
        );
        let (port, seen) =
            record("HTTP/1.1 302 Found\r\nLocation: /loop\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into())
                .await;
        let err = Client::new(&format!("http://127.0.0.1:{port}"), "k")
            .get_config()
            .await
            .unwrap_err();
        assert_eq!(err, "Get \"/loop\": stopped after 10 redirects");
        assert_eq!(seen.lock().unwrap().len(), 10);
        // Every URL a redirect error names is redacted (review of #127, finding 6).
        for (location, want) in [
            (
                format!("http://127.0.0.1:{dead}/gone?code=sk-review-secret"),
                format!("Get \"http://127.0.0.1:{dead}/gone?code=***\": "),
            ),
            (
                "/loop?code=sk-review-secret".to_owned(),
                "Get \"/loop?code=***\": stopped after 10 redirects".to_owned(),
            ),
            (
                "http://[::1/?code=sk-review-secret".to_owned(),
                "Get \"http://127.0.0.1:PORT/admin/v1/effective/config\": failed to parse Location header \"<redacted url>\": ".to_owned(),
            ),
        ] {
            let (port, _) = record(format!(
                "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ))
            .await;
            let err = Client::new(&format!("http://127.0.0.1:{port}"), "k")
                .get_config()
                .await
                .unwrap_err();
            assert!(err.starts_with(&want.replace("PORT", &port.to_string())), "{err}");
            assert!(!err.contains("sk-review-secret"), "{err}");
        }
    }

    #[tokio::test]
    async fn owner_setting_writes_require_a_direct_committed_receipt() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "owner-setting-receipt-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(ck_server::testing::config_store(&root.join("owner.db")).unwrap());
        store
            .import_yaml(
                "observability: {logs: {debug: false}}\n",
                "test",
                "direct owner receipt",
            )
            .unwrap();
        let db = ck_server::store::credential_store::CredentialDb::initialize(store.clone()).unwrap();
        let mut config = store.load().unwrap().config;
        config.auth_dir = db.auth_dir();
        let rt = Arc::new(ck_server::testing::runtime_with_db(
            config,
            Vec::new(),
            ck_exec::Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:9").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
            db,
        ));
        let state = ck_server::management::api::Management::with_options(
            rt.clone(),
            store.clone(),
            ck_server::management::api::Options {
                local_password: "fake-owner-key".into(),
                standalone: true,
                update_check_disabled: Some(true),
                ..Default::default()
            },
        );
        let seen = Arc::new(AtomicUsize::new(0));
        let app = ck_server::management::api::router(state.clone()).layer(axum::middleware::from_fn({
            let seen = seen.clone();
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                seen.fetch_add(1, Ordering::SeqCst);
                async move { next.run(request).await }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let owner = tokio::spawn(ck_server::net::listener::serve_with_resp(
            listener,
            app,
            None,
            Some(state.clone()),
        ));
        let revision = store.load().unwrap().rev;
        for (status, moved) in [
            (301, true),
            (302, true),
            (303, true),
            (307, true),
            (308, true),
            (301, false),
            (302, false),
            (303, false),
            (307, false),
            (308, false),
            (200, false),
            (202, false),
        ] {
            let location = moved.then(|| format!("{base}/admin/v1/effective/config"));
            let body = if status == 200 {
                "{}".to_owned()
            } else {
                serde_json::json!({"revision":revision}).to_string()
            };
            let entry = axum::Router::new().fallback(move || {
                let (location, body) = (location.clone(), body.clone());
                async move {
                    let mut reply = axum::response::Response::builder()
                        .status(status)
                        .header("content-type", "application/json");
                    if let Some(location) = location {
                        reply = reply.header("location", location);
                    }
                    reply.body(axum::body::Body::from(body)).unwrap()
                }
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let at = format!("http://{}", listener.local_addr().unwrap());
            let redirect = tokio::spawn(async move { axum::serve(listener, entry).await.unwrap() });
            let client = Client::new(&at, "fake-owner-key");
            let result = client.put_field("observability/logs/debug", Value::Bool(true)).await;
            drop(client);
            redirect.abort();
            assert!(redirect.await.unwrap_err().is_cancelled());
            assert!(
                result.is_err(),
                "uncommitted response {status} (Location={moved}) was acknowledged"
            );
            assert_eq!(seen.load(Ordering::SeqCst), 0, "owner write followed a redirect");
            assert_eq!(store.load().unwrap().rev, revision);
            assert_eq!(
                store.load().unwrap().config.document["observability"]["logs"]["debug"].as_bool(),
                Some(false)
            );
        }
        let client = Client::new(&base, "fake-owner-key");
        client
            .put_field("observability/logs/debug", Value::Bool(true))
            .await
            .unwrap();
        assert_eq!(
            store.load().unwrap().config.document["observability"]["logs"]["debug"].as_bool(),
            Some(true)
        );
        assert!(store.load().unwrap().rev > revision);
        drop(client);
        state.shutdown().await;
        owner.await.unwrap().unwrap();
        rt.finish_shutdown().await.unwrap();
        drop(state);
        drop(rt);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Go's redirect rules: a PUT that meets a 302 becomes a body-less GET, and the
    /// Bearer key follows to the same host on another port but not to another host.
    #[tokio::test]
    async fn redirects_follow_go_http_client() {
        use std::sync::{Arc, Mutex};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let serve = |reply: fn(u16) -> String| {
            let seen = seen.clone();
            async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = listener.local_addr().unwrap().port();
                tokio::spawn(async move {
                    while let Ok((mut sock, _)) = listener.accept().await {
                        let mut buf = vec![0; 4096];
                        let n = sock.read(&mut buf).await.unwrap_or(0);
                        seen.lock()
                            .unwrap()
                            .push(String::from_utf8_lossy(&buf[..n]).into_owned());
                        let _ = sock.write_all(reply(port).as_bytes()).await;
                    }
                });
                port
            }
        };
        let target = serve(|_| "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".into()).await;
        static TARGET: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
        TARGET.set(target).unwrap();
        let start = serve(|_| {
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{}/moved\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                TARGET.get().unwrap()
            )
        })
        .await;
        let client = Client::new(&format!("http://127.0.0.1:{start}"), "fake-bearer");
        // The generic transport keeps Go's behavior; owner writes are tested separately.
        client
            .checked(
                wreq::Method::PUT,
                "/legacy-setting",
                Some(
                    serde_json::json!({
                        "ops":[{"op":"set","path":"observability/logs/debug","value":true}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        let seen = seen.lock().unwrap().clone();
        assert!(
            seen[0].starts_with("PUT /legacy-setting ")
                && seen[0].contains("\"path\":\"observability/logs/debug\"")
                && seen[0].contains("\"value\":true"),
            "{}",
            seen[0]
        );
        assert!(seen[1].starts_with("GET /moved "), "{}", seen[1]);
        assert!(!seen[1].contains("value"), "the body is dropped: {}", seen[1]);
        assert!(
            seen[1].go_lower().contains("authorization: bearer fake-bearer"),
            "same host keeps the key"
        );
        assert!(
            seen[1]
                .go_lower()
                .contains(&format!("referer: http://127.0.0.1:{start}/legacy-setting"))
        );
    }

    // Expected values: Go TestNewClientWithBaseURL and net/url.
    #[test]
    fn base_urls_follow_go() {
        for (input, want) in [
            ("http://192.168.1.100:8317", "http://192.168.1.100:8317"),
            ("https://proxy.example.com/", "https://proxy.example.com"),
            ("HTTPS://proxy.example.com/", "HTTPS://proxy.example.com"),
            ("proxy.example.com:9000", "http://proxy.example.com:9000"),
            ("https://proxy.example.com/prefix/", "https://proxy.example.com/prefix"),
            ("", "http://127.0.0.1:8317"),
            ("  ", "http://127.0.0.1:8317"),
        ] {
            assert_eq!(normalize_base_url(input), want, "{input:?}");
        }
        assert_eq!(query_escape("a b/ü~.json"), "a+b%2F%C3%BC~.json");
        let host = |s: &str| url::Url::parse(s).unwrap();
        assert!(same_site(&host("http://a.example:1/x"), &host("http://a.example:2/y")));
        assert!(same_site(
            &host("http://example.invalid/"),
            &host("https://sub.example.invalid/")
        ));
        assert!(!same_site(
            &host("http://example.invalid/"),
            &host("http://badexample.invalid/")
        ));
        assert_eq!(
            marshal_map(vec![("old", Value::Null), ("new", "k<&>".into())]),
            r#"{"new":"k\u003c\u0026\u003e","old":null}"#
        );
    }
}
