use std::collections::HashSet;

use serde_json::{Map, Value as Json};

use super::{Fault, Kind, Resource, SECRET, bounded, document, strip_provider_read_annotations, valid_id};

fn secret_field(name: &str) -> bool {
    matches!(
        name,
        "api-key"
            | "key"
            | "secret-key"
            | "github-token"
            | "credential"
            | "username"
            | "password"
            | "token"
            | "access-token"
            | "refresh-token"
            | "client-secret"
            | "authorization"
            | "cookie"
            | "relay-keys"
    )
}

fn credential_url(text: &str) -> bool {
    text.contains('@') && text.contains(':')
        || url::Url::parse(text).is_ok_and(|u| {
            !u.username().is_empty()
                || u.password().is_some()
                || u.query().is_some_and(|q| !q.is_empty())
                || u.fragment().is_some_and(|f| !f.is_empty())
        })
}

fn mask(value: &mut Json, field: &str, headers: bool) {
    if (headers || secret_field(field) || value.as_str().is_some_and(credential_url))
        && !value.is_null()
        && value.as_str() != Some("")
    {
        *value = SECRET.into();
        return;
    }
    match value {
        Json::Object(map) => {
            for (name, child) in map {
                mask(child, name, field == "headers" || field == "header-defaults");
            }
        }
        Json::Array(items) => {
            for child in items {
                mask(child, field, headers);
            }
        }
        _ => {}
    }
}

fn mask_annotated_entry(value: &mut Json) {
    const ANNOTATIONS: [&str; 3] = ["@id", "auth_index", "auth-index"];
    let saved = ANNOTATIONS.map(|field| value.as_object_mut().and_then(|map| map.remove(field)));
    mask(value, "", false);
    if let Some(map) = value.as_object_mut() {
        for (field, value) in ANNOTATIONS.into_iter().zip(saved) {
            if let Some(value) = value {
                map.insert(field.into(), value);
            }
        }
    }
}

fn mask_provider(value: &mut Json) {
    let mut keys = value.as_object_mut().and_then(|map| map.remove("keys"));
    mask_annotated_entry(value);
    if let Some(keys) = &mut keys {
        if let Some(entries) = keys.as_array_mut() {
            entries.iter_mut().for_each(mask_annotated_entry);
        } else {
            mask(keys, "keys", false);
        }
    }
    if let (Some(map), Some(keys)) = (value.as_object_mut(), keys) {
        map.insert("keys".into(), keys);
    }
}

/// Every ordinary read and history representation uses the same secret policy.
pub fn redacted(r: &Resource) -> Resource {
    let mut r = r.clone();
    if r.kind == Kind::Provider {
        mask_provider(&mut r.spec);
    } else {
        mask(&mut r.spec, "", false);
    }
    r
}

pub fn document_for_read(resources: &[Resource]) -> Json {
    let mut doc = document(resources, true);
    redact_document(&mut doc);
    doc
}

/// Redacts an annotated document without stripping resource or auth-index identity.
pub fn redact_document(doc: &mut Json) {
    let mut providers = doc.as_object_mut().and_then(|map| map.remove("api-keys"));
    let annotated =
        ["/access/api-keys", "/model-routers"].map(|path| (path, doc.pointer_mut(path).map(std::mem::take)));
    mask(doc, "", false);
    for (path, value) in annotated {
        if let Some(mut value) = value {
            if let Some(entries) = value.as_array_mut() {
                entries.iter_mut().for_each(mask_annotated_entry);
            } else {
                mask(&mut value, "", false);
            }
            *doc.pointer_mut(path).expect("annotated collection remains present") = value;
        }
    }
    if let Some(providers) = &mut providers {
        if let Some(families) = providers.as_object_mut() {
            for entries in families.values_mut().filter_map(Json::as_array_mut) {
                entries.iter_mut().for_each(mask_provider);
            }
        } else {
            mask(providers, "key", false);
        }
    }
    if let (Some(map), Some(providers)) = (doc.as_object_mut(), providers) {
        map.insert("api-keys".into(), providers);
    }
}

fn same_with_placeholders(value: Option<&Json>, stored: Option<&Json>) -> bool {
    match (value, stored) {
        (Some(Json::String(value)), Some(_)) if value == SECRET => true,
        (Some(Json::Object(value)), Some(Json::Object(stored))) => {
            value.len() == stored.len()
                && value
                    .iter()
                    .all(|(key, child)| same_with_placeholders(Some(child), stored.get(key)))
        }
        (Some(Json::Array(value)), Some(Json::Array(stored))) => {
            value.len() == stored.len()
                && value
                    .iter()
                    .zip(stored)
                    .all(|(a, b)| same_with_placeholders(Some(a), Some(b)))
        }
        (value, stored) => value == stored,
    }
}

fn destination_field(field: &str) -> bool {
    matches!(
        field,
        "type"
            | "url"
            | "urls"
            | "headers"
            | "header-defaults"
            | "host"
            | "port"
            | "public-ip"
            | "udp-port-min"
            | "udp-port-max"
            | "header-name"
    ) || field.ends_with("-url")
}

fn destination_changed(value: &Map<String, Json>, stored: Option<&Json>) -> bool {
    let old = stored.and_then(Json::as_object);
    value
        .keys()
        .chain(old.into_iter().flat_map(|map| map.keys()))
        .filter(|field| destination_field(field))
        .any(|field| !same_with_placeholders(value.get(field), old.and_then(|map| map.get(field))))
}

pub(super) fn secret_pointers(value: &Json, at: &str, out: &mut Vec<String>) {
    match value {
        Json::String(value) if value == SECRET => out.push(at.to_owned()),
        Json::Object(map) => {
            for (key, value) in map {
                secret_pointers(
                    value,
                    &format!("{at}/{}", key.replace('~', "~0").replace('/', "~1")),
                    out,
                );
            }
        }
        Json::Array(items) => {
            for (i, value) in items.iter().enumerate() {
                secret_pointers(value, &format!("{at}/{i}"), out);
            }
        }
        _ => {}
    }
}

fn contains_secret(value: &Json) -> bool {
    match value {
        Json::String(s) => s == SECRET,
        Json::Array(a) => a.iter().any(contains_secret),
        Json::Object(m) => m.values().any(contains_secret),
        _ => false,
    }
}

fn resolve(
    value: &mut Json,
    stored: Option<&Json>,
    at: &str,
    changed_destination: bool,
    resend: &mut Vec<String>,
) -> Result<(), String> {
    let changed_destination =
        changed_destination || value.as_object().is_some_and(|map| destination_changed(map, stored));
    if changed_destination && contains_secret(value) {
        secret_pointers(value, at, resend);
        return Ok(());
    }
    if value.as_str() == Some(SECRET) {
        *value = stored
            .filter(|v| v.as_str() != Some(SECRET))
            .cloned()
            .ok_or_else(|| at.to_owned())?;
        return Ok(());
    }
    match value {
        Json::Object(map) => {
            for (key, child) in map {
                resolve(
                    child,
                    stored.and_then(|s| s.get(key)),
                    &format!("{at}/{}", key.replace('~', "~0").replace('/', "~1")),
                    changed_destination,
                    resend,
                )?;
            }
        }
        Json::Array(items) => {
            let old = stored.and_then(Json::as_array);
            let mut used = HashSet::new();
            for (i, child) in items.iter_mut().enumerate() {
                let matched = if contains_secret(child) {
                    let id = child.get("@id").and_then(Json::as_str);
                    let mut public = child.clone();
                    mask(&mut public, "", false);
                    let candidates: Vec<_> = old
                        .into_iter()
                        .flatten()
                        .enumerate()
                        .filter(|(_, candidate)| {
                            if let Some(id) = id {
                                return candidate.get("@id").and_then(Json::as_str) == Some(id);
                            }
                            let mut candidate = Json::clone(candidate);
                            if let Some(m) = candidate.as_object_mut() {
                                m.remove("@id");
                            }
                            mask(&mut candidate, "", false);
                            candidate == public
                        })
                        .collect();
                    if candidates.len() != 1 || !used.insert(candidates[0].0) {
                        return Err(format!("{at}/{i}"));
                    }
                    Some(candidates[0].1)
                } else {
                    None
                };
                resolve(child, matched, &format!("{at}/{i}"), changed_destination, resend)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Placeholders in arrays resolve by stable entry identity, never by array position.
pub fn resolve_secrets(r: &mut Resource, stored: Option<&Resource>) -> Result<(), Fault> {
    if !bounded(&r.spec) {
        return Err(Fault::new(Some(r.key()), "resource exceeds depth limit"));
    }
    if r.kind == Kind::Provider
        && let Some(map) = r.spec.as_object_mut()
    {
        strip_provider_read_annotations(map);
    }
    let mut resend = Vec::new();
    resolve(&mut r.spec, stored.map(|s| &s.spec), "", false, &mut resend).map_err(|at| {
        let mut fault = Fault::new(
            Some(r.key()),
            format!("{at}: masked value has no unambiguous stored identity; send the value"),
        );
        fault.pointer = Some(at);
        fault
    })?;
    if !resend.is_empty() {
        let mut fault = Fault::new(Some(r.key()), "send the secret again to change where it is sent");
        fault.pointer = resend.first().cloned();
        fault.pointers = resend;
        fault.code = Some("secret_destination_changed");
        return Err(fault);
    }
    if r.kind == Kind::Provider {
        let key = r.key();
        let mut seen = HashSet::new();
        for entry in r
            .spec
            .get_mut("keys")
            .and_then(Json::as_array_mut)
            .into_iter()
            .flatten()
        {
            let map = entry
                .as_object_mut()
                .ok_or_else(|| Fault::new(Some(key.clone()), "provider key must be a mapping"))?;
            match map.get("@id") {
                Some(id) if !id.as_str().is_some_and(|id| valid_id(id) && seen.insert(id.to_owned())) => {
                    return Err(Fault::new(Some(key), "invalid or duplicate provider key @id"));
                }
                Some(_) => {}
                None => {
                    let id = stored
                        .and_then(|r| r.spec.get("keys"))
                        .and_then(Json::as_array)
                        .and_then(|keys| {
                            let mut matches = keys.iter().filter(|key| {
                                let mut key = Json::clone(key);
                                if let Some(m) = key.as_object_mut() {
                                    m.remove("@id");
                                }
                                key == Json::Object(map.clone())
                            });
                            let key = matches.next()?;
                            if matches.next().is_some() {
                                return None;
                            }
                            key.get("@id").and_then(Json::as_str).map(str::to_owned)
                        })
                        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                    if !seen.insert(id.clone()) {
                        return Err(Fault::new(Some(key), "duplicate provider key @id"));
                    }
                    map.insert("@id".into(), id.into());
                }
            }
        }
    }
    Ok(())
}
/// Hashes a plaintext management key with bcrypt, so the store never holds it.
pub fn hash_management_key(r: &mut Resource) -> Result<(), Fault> {
    if r.kind != Kind::Setting || r.id != "management" {
        return Ok(());
    }
    let Some(Json::String(key)) = r.spec.get_mut("secret-key") else {
        return Ok(());
    };
    if key.is_empty() || ck_core::config::is_bcrypt(key) || key == SECRET {
        return Ok(());
    }
    // bcrypt refuses longer keys rather than truncating them.
    if key.len() > 72 {
        return Err(Fault::new(Some(r.key()), "/secret-key: longer than 72 bytes"));
    }
    *key =
        bcrypt::hash(key.as_bytes(), 10).map_err(|e| Fault::new(Some("setting/management".into()), e.to_string()))?;
    Ok(())
}
