//! `/admin/v1`: the config as resources ([`crate::store::config_store`]).
//!
//! - `GET /r/{kind}`, `GET|PUT|PATCH|DELETE /r/{kind}/{id}`: resources, with strong
//!   revision ETags. `If-Match` requires a matching revision (or existence with `*`);
//!   `If-None-Match: *` creates only. PATCH is an RFC 7396 merge patch of the spec.
//! - `POST /apply`: operations committed together as one revision.
//! - `GET /effective/config`: the annotated, secret-masked config document.
//! - `GET /revisions`, `GET /revisions/{rev}`, `POST /revisions/{rev}/restore`.
//! - `GET /export`, `POST /import`: a versioned resource export or a YAML config.
//!
//! - `POST /credentials/batch`: managed raw documents committed atomically.
//!
//! Configuration writes accept `?dry_run=true` and `?message=`. Import and restore require
//! a concrete `If-Match` revision from a collection read, effective config or export;
//! a wildcard is insufficient. The condition is checked inside the write transaction,
//! including dry runs.
//! Bodies are bounded and read only after management authentication. Responses never
//! expose parser or database error text. Resource reads mask secrets; the explicitly
//! authorized credential source view carries original bytes for client merge/CAS.

use std::collections::BTreeSet;
use std::sync::{Arc, PoisonError};

use axum::body::{Bytes, HttpBody as _};
use axum::extract::{FromRequest, FromRequestParts, Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header, request::Parts};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use ck_core::gostr::GoStr as _;
use futures_util::StreamExt as _;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value as Json, json};
use sha2::{Digest as _, Sha256};

use super::Management;
use crate::store::config_store::{CommitError, Committed, Kind, Op, Precondition, Resource, resources};

const ACTOR: &str = "admin-api";
/// A whole-store import may be larger than an individual resource (limited by SQL).
const BODY_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct WriteQuery {
    dry_run: bool,
    message: String,
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct PageQuery {
    limit: Option<u32>,
    before: Option<u64>,
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ExportQuery {
    include_secrets: bool,
}

fn send(state: &Management, status: StatusCode, body: &Json, revision: Option<u64>) -> Response {
    let mut response = (
        status,
        [(header::CONTENT_TYPE, HeaderValue::from_static("application/json"))],
        body.to_string(),
    )
        .into_response();
    response.extensions_mut().insert(super::access::AdminResponse);
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(rev) = revision {
        headers.insert("x-ck-revision", rev.into());
    }
    headers.insert("x-ck-published-revision", state.published_rev().into());
    response
}

fn error_at(
    state: &Management,
    status: u16,
    code: &'static str,
    message: &str,
    resource: Option<&str>,
    pointer: Option<&str>,
    pointers: &[String],
) -> Response {
    send(
        state,
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        &json!({"error": code, "resource": resource, "pointer": pointer, "pointers": pointers, "message": message}),
        None,
    )
}

pub(super) fn error(state: &Management, status: u16, code: &'static str, message: &str) -> Response {
    error_at(state, status, code, message, None, None, &[])
}

/// Extractor and handler failures share the guard's response contract.
pub(crate) struct AdminError(Box<Response>);

impl AdminError {
    fn new(state: &Management, status: u16, code: &'static str, message: &str) -> Self {
        Self(Box::new(error(state, status, code, message)))
    }

    fn store(state: &Management) -> Self {
        Self::new(
            state,
            500,
            "store_failed",
            "the config store could not complete the request",
        )
    }

    fn import(state: &Management, e: resources::ImportError) -> Self {
        let message = match e.code {
            "invalid_body" => "invalid JSON request body",
            "invalid_export" => "unsupported config export format or version",
            "unknown_kind" => "unknown resource kind",
            _ => "the configuration is invalid",
        };
        Self(Box::new(error_at(
            state,
            e.status,
            e.code,
            message,
            e.fault.resource.as_deref(),
            e.fault.pointer.as_deref(),
            &e.fault.pointers,
        )))
    }
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        *self.0
    }
}

fn fail(state: &Management, e: &CommitError) -> AdminError {
    // Neither parser diagnostics nor SQLite errors are safe to return or log: they
    // can include the submitted secret or a stored spec. Keep structured locations.
    if e.status >= 500 {
        tracing::error!(code = e.code, "admin config operation failed");
    }
    let message = match e.code {
        "precondition_failed" | "already_exists" => "a precondition did not match the stored configuration",
        "not_found" => "the resource or revision does not exist",
        "unknown_kind" => "unknown resource kind",
        "read_only_field" => "the request changes a read-only field",
        "invalid_path" => "the configuration path is invalid",
        "invalid_move" => "give either before or after, not both",
        "unordered_kind" => "only list resources can be reordered",
        "invalid_config" => "the configuration is invalid",
        "secret_destination_changed" => {
            "the secret destination changed; resend the secret values at the indicated pointers"
        }
        _ => "the config store could not complete the request",
    };
    AdminError(Box::new(error_at(
        state,
        e.status,
        e.code,
        message,
        e.fault.resource.as_deref(),
        e.fault.pointer.as_deref(),
        &e.fault.pointers,
    )))
}

fn shown(r: &Resource) -> Json {
    shown_as(&resources::redacted(r))
}

fn shown_as(r: &Resource) -> Json {
    json!({
        "kind": r.kind,
        "id": r.id,
        "ord": r.ord,
        "rev": r.rev,
        "created_rev": r.created_rev,
        "description": r.description,
        "spec": r.spec,
    })
}

fn tagged(mut response: Response, rev: u64) -> Response {
    let etag = HeaderValue::try_from(format!("\"{rev}\"")).expect("digits");
    response.headers_mut().insert(header::ETAG, etag);
    response
}

/// Accept canonical revision tags or a lone `*`; weak tags never strongly match.
/// Multiple field lines have the same meaning as a comma-separated list.
fn if_match(headers: &HeaderMap) -> Result<Option<Precondition>, ()> {
    if !headers.contains_key(header::IF_MATCH) {
        return Ok(None);
    }
    let mut revisions = Vec::new();
    let mut wildcard = false;
    let mut tags = 0;
    for field in headers.get_all(header::IF_MATCH) {
        for token in field.to_str().map_err(drop)?.split(',') {
            let token = token.trim_matches([' ', '\t']);
            if token == "*" {
                if wildcard || tags != 0 {
                    return Err(());
                }
                wildcard = true;
                continue;
            }
            if wildcard {
                return Err(());
            }
            tags += 1;
            let weak = token.starts_with("W/");
            let token = token.strip_prefix("W/").unwrap_or(token);
            let digits = token.strip_prefix('"').and_then(|v| v.strip_suffix('"')).ok_or(())?;
            if digits.is_empty()
                || !digits.bytes().all(|c| c.is_ascii_digit())
                || (digits.len() > 1 && digits.starts_with('0'))
            {
                return Err(());
            }
            let revision = digits.parse::<u64>().map_err(drop)?;
            if !weak {
                revisions.push(revision);
            }
        }
    }
    if wildcard {
        Ok(Some(Precondition::Exists))
    } else if revisions.len() == 1 {
        Ok(Some(Precondition::Revision(revisions[0])))
    } else if tags == 0 {
        Err(())
    } else {
        Ok(Some(Precondition::AnyRevision(revisions)))
    }
}

pub(crate) struct Conditions<const REQUIRED: bool, const ALLOW_ABSENT: bool> {
    condition: Option<Precondition>,
    if_absent: bool,
}

impl<const REQUIRED: bool, const ALLOW_ABSENT: bool> FromRequestParts<Arc<Management>>
    for Conditions<REQUIRED, ALLOW_ABSENT>
{
    type Rejection = AdminError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<Management>) -> Result<Self, AdminError> {
        let headers = &parts.headers;
        let condition = if_match(headers).map_err(|()| {
            AdminError::new(
                state,
                400,
                "invalid_if_match",
                "If-Match must be a strong quoted revision, a list of them, or *",
            )
        })?;
        if REQUIRED && matches!(condition.as_ref(), None | Some(Precondition::Exists)) {
            return Err(AdminError::new(
                state,
                428,
                "precondition_required",
                "If-Match with a concrete current global revision ETag is required",
            ));
        }
        let if_absent = headers.contains_key(header::IF_NONE_MATCH);
        if if_absent {
            let mut fields = headers.get_all(header::IF_NONE_MATCH).iter();
            if !ALLOW_ABSENT
                || headers.contains_key(header::IF_MATCH)
                || fields.next().and_then(|v| v.to_str().ok()).map(str::trim) != Some("*")
                || fields.next().is_some()
            {
                return Err(AdminError::new(
                    state,
                    400,
                    "invalid_if_none_match",
                    "only a PUT without If-Match accepts If-None-Match: *",
                ));
            }
        }
        Ok(Self { condition, if_absent })
    }
}

async fn path<T: DeserializeOwned + Send>(parts: &mut Parts, state: &Arc<Management>) -> Result<T, AdminError> {
    Path::<T>::from_request_parts(parts, state)
        .await
        .map(|Path(value)| value)
        .map_err(|_| AdminError::new(state, 400, "invalid_path", "invalid resource or revision path"))
}

fn kind_of(state: &Management, name: &str) -> Result<Kind, AdminError> {
    Kind::parse(name).ok_or_else(|| AdminError::new(state, 404, "unknown_kind", "unknown resource kind"))
}

pub(crate) struct KindPath(Kind);

impl FromRequestParts<Arc<Management>> for KindPath {
    type Rejection = AdminError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<Management>) -> Result<Self, AdminError> {
        let name: String = path(parts, state).await?;
        Ok(Self(kind_of(state, &name)?))
    }
}

pub(crate) struct ResourcePath(Kind, String);

impl FromRequestParts<Arc<Management>> for ResourcePath {
    type Rejection = AdminError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<Management>) -> Result<Self, AdminError> {
        let (name, id): (String, String) = path(parts, state).await?;
        Ok(Self(kind_of(state, &name)?, id))
    }
}

pub(crate) struct RevisionPath(u64);

impl FromRequestParts<Arc<Management>> for RevisionPath {
    type Rejection = AdminError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<Management>) -> Result<Self, AdminError> {
        Ok(Self(path(parts, state).await?))
    }
}

pub(crate) struct AdminQuery<T>(T);

impl<T: DeserializeOwned + Send> FromRequestParts<Arc<Management>> for AdminQuery<T> {
    type Rejection = AdminError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<Management>) -> Result<Self, AdminError> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(value)| Self(value))
            .map_err(|_| AdminError::new(state, 400, "invalid_query", "invalid query parameters"))
    }
}

/// Validate import media before reading its body, after query and conditions.
pub(crate) struct ImportType(bool);

impl FromRequestParts<Arc<Management>> for ImportType {
    type Rejection = AdminError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<Management>) -> Result<Self, AdminError> {
        let media_type = parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            .unwrap_or("")
            .trim();
        let json = media_type.eq_ignore_ascii_case("application/json");
        if !json
            && ![
                "application/yaml",
                "application/x-yaml",
                "text/yaml",
                "text/x-yaml",
                "text/plain",
            ]
            .iter()
            .any(|allowed| media_type.eq_ignore_ascii_case(allowed))
        {
            return Err(AdminError::new(
                state,
                415,
                "unsupported_media_type",
                "use application/json for an export or application/yaml for a config",
            ));
        }
        Ok(Self(json))
    }
}

pub(crate) struct LimitedBody(Bytes);

impl FromRequest<Arc<Management>> for LimitedBody {
    type Rejection = AdminError;

    async fn from_request(request: Request, state: &Arc<Management>) -> Result<Self, AdminError> {
        if request.body().size_hint().lower() > BODY_LIMIT as u64 {
            return Err(AdminError::new(
                state,
                413,
                "body_too_large",
                "request body exceeds the 16 MiB limit",
            ));
        }
        let mut stream = request.into_body().into_data_stream();
        let mut bytes = bytes::BytesMut::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                if crate::http::body_limit::is_too_large(&e) {
                    AdminError::new(state, 413, "body_too_large", "request body exceeds the limit")
                } else {
                    AdminError::new(state, 400, "invalid_body", "could not read request body")
                }
            })?;
            if chunk.len() > BODY_LIMIT - bytes.len() {
                return Err(AdminError::new(
                    state,
                    413,
                    "body_too_large",
                    "request body exceeds the 16 MiB limit",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(Self(bytes.freeze()))
    }
}

impl LimitedBody {
    fn json<T: DeserializeOwned>(&self, state: &Management, resource: Option<&str>) -> Result<T, AdminError> {
        serde_json::from_slice(&self.0).map_err(|_| {
            AdminError(Box::new(error_at(
                state,
                400,
                "invalid_body",
                "invalid JSON request body",
                resource,
                None,
                &[],
            )))
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CredentialBatchQuery {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialBatch {
    #[serde(default)]
    documents: Vec<CredentialDocument>,
    #[serde(default)]
    deletions: Vec<CredentialDeletion>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialDeletion {
    name: String,
    if_match: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialDocument {
    name: String,
    data: String,
    /// SHA-256 of the current raw document, not a configuration revision.
    if_match: Option<String>,
    #[serde(default)]
    if_none_match: bool,
}

/// One authorized immutable source view, including documents not yet parsed.
pub(crate) async fn credential_sources(State(state): State<Arc<Management>>) -> Result<Response, AdminError> {
    blocking(state, |state| {
        let db = state.rt.store().credential_backend();
        let root = db.auth_dir();
        let documents = db
            .documents()
            .map_err(|_| AdminError::store(state))?
            .into_iter()
            .filter_map(|(path, bytes)| {
                let name = path.file_name()?.to_str()?;
                if path.parent() != Some(root.as_path()) || !super::auth_files::managed_name(name) {
                    return None;
                }
                Some(
                    json!({"name": name, "data": base64::engine::general_purpose::STANDARD.encode(&bytes),
                    "sha256": format!("{:x}", Sha256::digest(&bytes))}),
                )
            })
            .collect::<Vec<_>>();
        let auth_dir = root.to_str().ok_or_else(|| AdminError::store(state))?;
        Ok(send(
            state,
            StatusCode::OK,
            &json!({"auth_dir": auth_dir, "documents": documents}),
            None,
        ))
    })
    .await
}

pub(crate) async fn credential_batch(
    State(state): State<Arc<Management>>,
    AdminQuery(_): AdminQuery<CredentialBatchQuery>,
    body: LimitedBody,
) -> Result<Response, AdminError> {
    let batch: CredentialBatch = body.json(&state, None)?;
    let invalid = || AdminError::new(&state, 400, "invalid_credential_batch", "invalid credential batch");
    if batch.documents.is_empty() && batch.deletions.is_empty() {
        return Err(invalid());
    }
    let db = state.rt.store().credential_backend();
    let mut names = BTreeSet::new();
    let mut documents = Vec::with_capacity(batch.documents.len());
    for document in batch.documents {
        let name = &document.name;
        if !super::auth_files::managed_name(name)
            || !names.insert(if cfg!(windows) { name.go_lower() } else { name.clone() })
            || document.if_match.as_ref().is_some_and(|hash| {
                document.if_none_match || hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit())
            })
        {
            return Err(invalid());
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&document.data)
            .map_err(|_| invalid())?;
        let path = db.auth_dir().join(name);
        documents.push((document, path, bytes));
    }
    let mut deletions = Vec::with_capacity(batch.deletions.len());
    for deletion in batch.deletions {
        let name = &deletion.name;
        if !super::auth_files::managed_name(name)
            || !names.insert(if cfg!(windows) { name.go_lower() } else { name.clone() })
            || deletion.if_match.len() != 64
            || !deletion.if_match.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(invalid());
        }
        let path = db.auth_dir().join(name);
        deletions.push((deletion, path));
    }
    let batch = Arc::new((documents, deletions));
    for _ in 0..16 {
        let attempt = batch.clone();
        let busy = match blocking(state.clone(), move |state| credential_batch_attempt(state, &attempt)).await? {
            BatchAttempt::Done(response) => return Ok(response),
            BatchAttempt::Busy(path) => path,
        };
        // Wait off the store for this document alone, then repeat every check: whatever
        // its holder committed meanwhile is the source the conditions now apply to.
        let lock = state
            .rt
            .lock_codex_file(&busy)
            .await
            .map_err(|_| AdminError::store(&state))?;
        ck_exec::codex_siwc_bind::unlock(&lock).map_err(|_| AdminError::store(&state))?;
    }
    Err(AdminError::new(
        &state,
        500,
        "store_failed",
        "the credential lock kept changing hands",
    ))
}

type CredentialBatchParts = (
    Vec<(CredentialDocument, std::path::PathBuf, Vec<u8>)>,
    Vec<(CredentialDeletion, std::path::PathBuf)>,
);

enum BatchAttempt {
    Done(Response),
    /// A refresh holds this document's lock; nothing was written.
    Busy(std::path::PathBuf),
}

/// One all-or-nothing batch attempt under `disk` and the store command.
fn credential_batch_attempt(
    state: &Management,
    (documents, deletions): &CredentialBatchParts,
) -> Result<BatchAttempt, AdminError> {
    let db = state.rt.store().credential_backend();
    let _disk = state.disk.lock().unwrap_or_else(PoisonError::into_inner);
    let mut command = db.command().map_err(|_| AdminError::store(state))?;
    // Every refresh of an existing document takes its lock and only then restores the
    // document through the store command, keeping the lock until its result commits. A
    // free lock probed under this command therefore means no refresh of that document
    // has started or can start before this commit, so the probe releases each lock at
    // once instead of keeping one descriptor per document. A held lock means a refresh
    // is in flight: give up the attempt and the store so that refresh can commit.
    for path in documents
        .iter()
        .map(|(_, path, _)| path)
        .chain(deletions.iter().map(|(_, path)| path))
    {
        let lock = ck_exec::codex_siwc_bind::open_lock_file(path).map_err(|_| AdminError::store(state))?;
        if !ck_exec::codex_siwc_bind::try_lock(&lock).map_err(|_| AdminError::store(state))? {
            return Ok(BatchAttempt::Busy(path.clone()));
        }
        ck_exec::codex_siwc_bind::unlock(&lock).map_err(|_| AdminError::store(state))?;
    }
    #[cfg(test)]
    crate::testing::installation_effects::run(
        &db.auth_dir(),
        crate::testing::installation_effects::Stage::CredentialBatchProbed,
    )
    .map_err(|_| AdminError::store(state))?;
    let mut prepared = Vec::with_capacity(documents.len());
    for (document, path, bytes) in documents {
        let (target, _, synthesized, _) =
            super::auth_files::prepare_upload(state, &document.name, bytes).map_err(|(invalid, _)| {
                if invalid {
                    AdminError::new(state, 400, "invalid_credential", "invalid credential document")
                } else {
                    AdminError::store(state)
                }
            })?;
        if target != *path {
            return Err(AdminError::new(
                state,
                400,
                "invalid_credential_target",
                "invalid managed target",
            ));
        }
        if document.if_none_match || document.if_match.is_some() {
            let current = command.read(path).map_err(|_| AdminError::store(state))?;
            let conflict = document.if_none_match && current.is_some()
                || document.if_match.as_ref().is_some_and(|expected| {
                    current
                        .as_ref()
                        .is_none_or(|bytes| !format!("{:x}", Sha256::digest(bytes)).eq_ignore_ascii_case(expected))
                });
            if conflict {
                return Err(AdminError::new(
                    state,
                    412,
                    "precondition_failed",
                    "a credential precondition failed",
                ));
            }
        }
        prepared.push((path.clone(), synthesized.is_some()));
    }
    for (deletion, path) in deletions {
        let current = command.read(path).map_err(|_| AdminError::store(state))?;
        if current
            .as_ref()
            .is_none_or(|bytes| !format!("{:x}", Sha256::digest(bytes)).eq_ignore_ascii_case(&deletion.if_match))
        {
            return Err(AdminError::new(
                state,
                412,
                "precondition_failed",
                "a credential precondition failed",
            ));
        }
    }
    let mut changes: Vec<_> = documents
        .iter()
        .map(|(_, path, bytes)| (path.clone(), Some(bytes.as_slice())))
        .collect();
    changes.extend(deletions.iter().map(|(_, path)| (path.clone(), None)));
    // Validation is shared with upload, but this port preserves original bytes.
    command
        .apply_documents(&changes)
        .map_err(|_| AdminError::store(state))?;
    drop(command);
    for path in prepared
        .iter()
        .map(|(path, _)| path)
        .chain(deletions.iter().map(|(_, path)| path))
    {
        state.oauth.invalidate_siwc(path);
    }
    {
        let mut fallbacks = state.fallbacks.lock().unwrap_or_else(PoisonError::into_inner);
        for (_, path) in deletions {
            fallbacks.remove(path);
        }
        for (path, claimed) in prepared {
            if claimed {
                fallbacks.remove(&path);
            } else {
                fallbacks.insert(path);
            }
        }
    }
    state.publish((*state.rt.config()).clone(), None);
    state.credential_changes.notify_one();
    let mut receipt = json!({"status": "ok", "saved": documents.len()});
    if !deletions.is_empty() {
        receipt["deleted"] = deletions.len().into();
    }
    Ok(BatchAttempt::Done(send(state, StatusCode::OK, &receipt, None)))
}

fn checked(kind: Kind, id: &str, condition: Option<Precondition>, op: Op) -> Vec<Op> {
    let revisions = match condition {
        Some(Precondition::Revision(rev)) => Some(Some(vec![rev])),
        Some(Precondition::AnyRevision(revs)) => Some(Some(revs)),
        Some(Precondition::Exists) => Some(None),
        None => None,
    };
    let mut ops = Vec::with_capacity(if revisions.is_some() { 2 } else { 1 });
    if let Some(revisions) = revisions {
        ops.push(Op::Check {
            kind: kind.name().into(),
            id: id.into(),
            revisions,
        });
    }
    ops.push(op);
    ops
}

fn answer(state: &Management, done: &Committed, restart: bool, resource: Option<&Resource>, dry_run: bool) -> Response {
    let mut body = json!({
        "revision": done.rev,
        "written": done.written,
        "dry_run": dry_run,
        "changes": done.changes,
        "restart_required": restart,
    });
    if let Some(r) = resource {
        body["resource"] = shown(r);
    }
    let response = send(state, StatusCode::OK, &body, Some(done.rev));
    if dry_run {
        // The candidate body is not the persisted representation of its revision.
        response
    } else {
        tagged(response, resource.map_or(done.rev, |r| r.rev))
    }
}

/// Runs synchronous store work off the async runtime.
async fn blocking<T: Send + 'static>(
    state: Arc<Management>,
    f: impl FnOnce(&Management) -> Result<T, AdminError> + Send + 'static,
) -> Result<T, AdminError> {
    let fallback = state.clone();
    state
        .work
        .clone()
        .spawn_blocking(move || f(&state))
        .await
        .map_err(|_| AdminError::store(&fallback))?
}

async fn write(
    state: Arc<Management>,
    query: WriteQuery,
    ops: Vec<Op>,
    target: Option<(Kind, String)>,
    condition: Option<Precondition>,
) -> Result<Response, AdminError> {
    blocking(state, move |state| {
        let (done, restart) = state
            .commit_with(|store| store.commit_if(&ops, ACTOR, &query.message, query.dry_run, condition))
            .map_err(|mut e| {
                if e.fault.resource.is_none() {
                    e.fault.resource = target.as_ref().map(|(kind, id)| format!("{}/{id}", kind.name()));
                }
                fail(state, &e)
            })?;
        let resource = target
            .as_ref()
            .and_then(|(kind, id)| done.resources.iter().find(|r| r.kind == *kind && r.id == *id));
        Ok(answer(state, &done, restart, resource, query.dry_run))
    })
    .await
}

pub(crate) async fn list(
    State(state): State<Arc<Management>>,
    KindPath(kind): KindPath,
) -> Result<Response, AdminError> {
    blocking(state, move |state| {
        let (rev, all) = state.config.snapshot().map_err(|_| AdminError::store(state))?;
        let items: Vec<Json> = all.iter().filter(|r| r.kind == kind).map(shown).collect();
        Ok(tagged(
            send(state, StatusCode::OK, &json!({ "items": items }), Some(rev)),
            rev,
        ))
    })
    .await
}

/// `?include_secrets=true` reveals only this resource, for a client that needs one
/// secret-bearing setting rather than the whole privileged export.
pub(crate) async fn get(
    State(state): State<Arc<Management>>,
    ResourcePath(kind, id): ResourcePath,
    AdminQuery(query): AdminQuery<ExportQuery>,
) -> Result<Response, AdminError> {
    blocking(state, move |state| {
        let (rev, all) = state.config.snapshot().map_err(|_| AdminError::store(state))?;
        let r = all.iter().find(|r| r.kind == kind && r.id == id).ok_or_else(|| {
            AdminError(Box::new(error_at(
                state,
                404,
                "not_found",
                "no such resource",
                Some(&format!("{}/{id}", kind.name())),
                None,
                &[],
            )))
        })?;
        let body = if query.include_secrets {
            tracing::warn!("admin api: {} {id} read with secrets", kind.name());
            shown_as(r)
        } else {
            shown(r)
        };
        Ok(tagged(send(state, StatusCode::OK, &body, Some(rev)), r.rev))
    })
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    spec: Json,
    #[serde(default)]
    ord: Option<i64>,
    #[serde(default)]
    description: Option<String>,
}

pub(crate) async fn put(
    State(state): State<Arc<Management>>,
    ResourcePath(kind, id): ResourcePath,
    AdminQuery(query): AdminQuery<WriteQuery>,
    Conditions { condition, if_absent }: Conditions<false, true>,
    bytes: LimitedBody,
) -> Result<Response, AdminError> {
    let key = format!("{}/{id}", kind.name());
    let body: Body = bytes.json(&state, Some(&key))?;
    let op = Op::Put {
        kind: kind.name().into(),
        id: id.clone(),
        spec: body.spec,
        ord: body.ord,
        description: body.description,
        if_rev: None,
        if_absent,
    };
    write(state, query, checked(kind, &id, condition, op), Some((kind, id)), None).await
}

pub(crate) async fn patch(
    State(state): State<Arc<Management>>,
    ResourcePath(kind, id): ResourcePath,
    AdminQuery(query): AdminQuery<WriteQuery>,
    Conditions { condition, .. }: Conditions<false, false>,
    bytes: LimitedBody,
) -> Result<Response, AdminError> {
    let key = format!("{}/{id}", kind.name());
    let patch = bytes.json(&state, Some(&key))?;
    let op = Op::Patch {
        kind: kind.name().into(),
        id: id.clone(),
        patch,
        if_rev: None,
    };
    write(state, query, checked(kind, &id, condition, op), Some((kind, id)), None).await
}

pub(crate) async fn delete(
    State(state): State<Arc<Management>>,
    ResourcePath(kind, id): ResourcePath,
    AdminQuery(query): AdminQuery<WriteQuery>,
    Conditions { condition, .. }: Conditions<false, false>,
) -> Result<Response, AdminError> {
    let op = Op::Delete {
        kind: kind.name().into(),
        id: id.clone(),
        if_rev: None,
    };
    write(state, query, checked(kind, &id, condition, op), Some((kind, id)), None).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Apply {
    ops: Vec<Op>,
    #[serde(default)]
    message: String,
    #[serde(default)]
    dry_run: bool,
}

pub(crate) async fn apply(
    State(state): State<Arc<Management>>,
    AdminQuery(mut query): AdminQuery<WriteQuery>,
    Conditions { condition, .. }: Conditions<false, false>,
    bytes: LimitedBody,
) -> Result<Response, AdminError> {
    let apply: Apply = bytes.json(&state, None)?;
    query.dry_run |= apply.dry_run;
    if query.message.is_empty() {
        query.message = apply.message;
    }
    write(state, query, apply.ops, None, condition).await
}

pub(crate) async fn effective_config(State(state): State<Arc<Management>>) -> Result<Response, AdminError> {
    blocking(state, |state| {
        let (rev, all) = state.config.snapshot().map_err(|_| AdminError::store(state))?;
        let mut doc = resources::document(&all, true);
        if let Ok((cfg, _)) = resources::config(&all) {
            super::auth_index::inject(&mut doc, &cfg, &super::live_auth_indexes(state));
        }
        resources::redact_document(&mut doc);
        Ok(tagged(send(state, StatusCode::OK, &doc, Some(rev)), rev))
    })
    .await
}

pub(crate) async fn revisions(
    State(state): State<Arc<Management>>,
    AdminQuery(page): AdminQuery<PageQuery>,
) -> Result<Response, AdminError> {
    blocking(state, move |state| {
        let limit = page.limit.unwrap_or(50).clamp(1, 500);
        let list = state
            .config
            .revisions(limit, page.before)
            .map_err(|_| AdminError::store(state))?;
        Ok(send(state, StatusCode::OK, &json!({ "revisions": list }), None))
    })
    .await
}

pub(crate) async fn revision(
    State(state): State<Arc<Management>>,
    RevisionPath(rev): RevisionPath,
) -> Result<Response, AdminError> {
    blocking(state, move |state| {
        let (revision, changes) = state
            .config
            .revision(rev)
            .map_err(|_| AdminError::store(state))?
            .ok_or_else(|| AdminError::new(state, 404, "not_found", "no such revision"))?;
        Ok(send(
            state,
            StatusCode::OK,
            &json!({ "revision": revision, "changes": changes }),
            None,
        ))
    })
    .await
}

/// Restores historical nonsecret values while retaining secrets still stored now.
pub(crate) async fn restore(
    State(state): State<Arc<Management>>,
    RevisionPath(rev): RevisionPath,
    AdminQuery(query): AdminQuery<WriteQuery>,
    Conditions { condition, .. }: Conditions<true, false>,
) -> Result<Response, AdminError> {
    blocking(state, move |state| {
        let then = state
            .config
            .state_at(rev)
            .map_err(|_| AdminError::new(state, 404, "not_found", "no such revision"))?;
        let message = if query.message.is_empty() {
            format!("restore revision {rev}")
        } else {
            query.message
        };
        let (done, restart) = state
            .commit_with(|store| store.replace_if(then, ACTOR, &message, query.dry_run, condition))
            .map_err(|e| fail(state, &e))?;
        Ok(answer(state, &done, restart, None, query.dry_run))
    })
    .await
}

pub(crate) async fn export(
    State(state): State<Arc<Management>>,
    AdminQuery(query): AdminQuery<ExportQuery>,
) -> Result<Response, AdminError> {
    blocking(state, move |state| {
        let (rev, all) = state.config.snapshot().map_err(|_| AdminError::store(state))?;
        if query.include_secrets {
            tracing::warn!("admin api: config exported with secrets");
        }
        let document = resources::export_json(rev, &all, query.include_secrets);
        Ok(tagged(send(state, StatusCode::OK, &document, Some(rev)), rev))
    })
    .await
}

/// Replaces the config with a versioned JSON export or an explicitly typed YAML file.
pub(crate) async fn import(
    State(state): State<Arc<Management>>,
    AdminQuery(query): AdminQuery<WriteQuery>,
    Conditions { condition, .. }: Conditions<true, false>,
    ImportType(json): ImportType,
    LimitedBody(bytes): LimitedBody,
) -> Result<Response, AdminError> {
    let parsed = if json {
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| AdminError::new(&state, 400, "invalid_body", "invalid JSON request body"))?;
        // The export's source revision is not the destination's If-Match condition.
        resources::import_json(text).map_err(|e| AdminError::import(&state, e))?
    } else {
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| AdminError::new(&state, 400, "invalid_body", "a config file must be UTF-8"))?;
        resources::import_yaml(text)
            .map_err(|_| AdminError::new(&state, 422, "invalid_config", "invalid YAML configuration"))?
    };
    blocking(state, move |state| {
        let message = if query.message.is_empty() {
            "import".to_owned()
        } else {
            query.message
        };
        let (done, restart) = state
            .commit_with(|store| store.replace_if(parsed, ACTOR, &message, query.dry_run, condition))
            .map_err(|e| fail(state, &e))?;
        Ok(answer(state, &done, restart, None, query.dry_run))
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::installation_effects::{self, Stage};
    use std::time::Duration;

    /// The probe releases each document lock before the batch commits. A refresh that
    /// takes the lock in that window must read the batch's bytes, never the source they
    /// replace: its restore waits for the store command the batch still holds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_locking_after_the_batch_probe_reads_the_batch() {
        let root = std::env::temp_dir().join(format!("cooker-batch-probe-{}", uuid::Uuid::new_v4()));
        let config = Arc::new(crate::testing::config_store(&root.join("cooker.db")).unwrap());
        let db = crate::store::credential_store::CredentialDb::initialize(config.clone()).unwrap();
        let path = db.auth_dir().join("probed.json");
        let seed = br#"{"type":"claude","access_token":"probe-seed-fixture"}"#;
        let batch = br#"{"type":"claude","access_token":"probe-batch-fixture"}"#;
        db.save(&path, seed).unwrap();
        let mut cfg = config.load().unwrap().config;
        cfg.auth_dir = db.auth_dir();
        let rt = Arc::new(crate::testing::runtime_with_db(
            cfg,
            Vec::new(),
            ck_exec::Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:9").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
            db.clone(),
        ));
        let state = Management::new(rt, config);

        let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let _parked = installation_effects::once(&db.auth_dir(), Stage::CredentialBatchProbed, move || {
            parked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        });
        let document = CredentialDocument {
            name: "probed.json".into(),
            data: base64::engine::general_purpose::STANDARD.encode(batch),
            if_match: None,
            if_none_match: false,
        };
        let parts: CredentialBatchParts = (vec![(document, path.clone(), batch.to_vec())], Vec::new());
        let committing = std::thread::spawn({
            let state = state.clone();
            move || matches!(credential_batch_attempt(&state, &parts), Ok(BatchAttempt::Done(_)))
        });
        let shutdown = tokio_util::sync::CancellationToken::new();
        // The timeouts only bound a deadlock's failure.
        let bounded = Duration::from_secs(10);
        tokio::time::timeout(bounded, parked_rx).await.unwrap().unwrap();
        let reading = tokio::spawn({
            let (path, db, shutdown) = (path.clone(), db.clone(), shutdown.clone());
            async move { crate::runtime::state::test_read_locked_codex_file(&path, &shutdown, db).await }
        });
        // Let the refresh take the free lock and reach its read before the batch commits.
        let held = ck_exec::codex_siwc_bind::open_lock_file(&path).unwrap();
        tokio::time::timeout(bounded, async {
            while ck_exec::codex_siwc_bind::try_lock(&held).unwrap() {
                ck_exec::codex_siwc_bind::unlock(&held).unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the refresh takes the probed lock");
        drop(held);
        tokio::time::sleep(Duration::from_millis(100)).await;
        release_tx.send(()).unwrap();
        let (lock, read) = tokio::time::timeout(bounded, reading).await.unwrap().unwrap().unwrap();
        assert!(committing.join().unwrap(), "the batch commits");
        assert_eq!(
            Json::Object(read),
            serde_json::from_slice::<Json>(batch).unwrap(),
            "a refresh locking after the probe read the replaced source"
        );
        assert_eq!(db.read(&path).unwrap().unwrap(), batch);
        drop(lock);
        drop((state, db));
        std::fs::remove_dir_all(root).unwrap();
    }
}
