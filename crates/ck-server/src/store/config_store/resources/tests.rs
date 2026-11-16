use super::*;
use ck_core::config::Config;
use serde_json::json;

/// Fields the Go config-diff fixture sets on a family whose current schema does
/// not have them: (what the document sets, family header, field). Strict import
/// rejects these documents on purpose; `go_only_fields_are_rejected_not_dropped`
/// pins that, and every other fixture document must load.
const GO_ONLY: &[(&str, &str, &str)] = &[
    ("claude keys carry no websockets", "\n  claude:\n", "websockets:"),
    ("gemini keys carry no websockets", "\n  gemini:\n", "websockets:"),
    (
        "interactions keys carry no websockets",
        "\n  interactions:\n",
        "websockets:",
    ),
    ("vertex keys carry no websockets", "\n  vertex:\n", "websockets:"),
    ("vertex models carry no is-compat", "\n  vertex:\n", "is-compat:"),
    ("the AI Studio relay has no ws-auth switch", "aistudio:", "ws-auth:"),
];

fn go_only(text: &str) -> Option<&'static str> {
    GO_ONLY
        .iter()
        .find(|(_, family, field)| text.contains(family) && text.contains(field))
        .map(|(name, ..)| *name)
}

/// Every config the Go config-diff fixture holds, plus one with every split-out
/// list and OAuth-only provider settings, in the current layout. The documents
/// named in `GO_ONLY` are left out; any other document that strict import
/// rejects fails the test that loads it instead of being skipped.
fn corpus() -> Vec<String> {
    all_documents()
        .into_iter()
        .filter(|text| go_only(text).is_none())
        .collect()
}

fn all_documents() -> Vec<String> {
    let fixture: Json =
        serde_json::from_str(include_str!("../../../../tests/fixtures/config_diff_go.json")).expect("fixture JSON");
    let mut out: Vec<String> = fixture
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|case| [&case["old"], &case["new"]])
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect();
    out.push(
        r#"
server:
  port: 8317
access:
  api-keys: [client-a, client-b]
oauth:
  providers:
    codex:
      header-defaults: {user-agent: "codex-cli/1"}
upstream:
  claude: {tool-search: true}
  codex:
    disable-codex-cloaking: true
    response-steering: true
api-keys:
  claude:
  - name: claude-1
    base-url: https://a.example.invalid
    models: [{name: claude-x, alias: x}]
    keys:
      - api-key: sk-a
  openai-compatibility:
  - name: router
    base-url: https://r.example.invalid/v1
    keys: [{api-key: sk-r}]
model-routers:
  - {name: smart, judge: router/jev}
requests:
  payload:
    default:
      - models: [{name: "gpt-*"}]
        params: {"reasoning.effort": high, when: 2001-12-14}
"#
        .to_owned(),
    );
    out
}

#[test]
fn an_imported_config_compiles_to_what_the_file_loads_to() {
    let corpus = corpus();
    for text in &corpus {
        let original = Config::parse(text).unwrap_or_else(|e| panic!("corpus document does not parse: {e:#}\n{text}"));
        ck_core::config::validate_config_fields(&original.document, true)
            .unwrap_or_else(|e| panic!("corpus document fails validation: {e:#}\n{text}"));
        let resources = import_yaml(text).unwrap_or_else(|e| panic!("import failed: {e:#}\n{text}"));
        let (compiled, _) = config(&resources).unwrap_or_else(|e| panic!("compile failed: {e}\n{text}"));
        let mut want = original.clone();
        let mut got = compiled;
        // A plaintext management key is stored hashed; everything else is the same.
        if !want.management.secret_key.is_empty() && !ck_core::config::is_bcrypt(&want.management.secret_key) {
            assert!(bcrypt::verify(&want.management.secret_key, &got.management.secret_key).unwrap());
            want.management.secret_key.clear();
            got.management.secret_key.clear();
            for c in [&mut want, &mut got] {
                if let Some(m) = c
                    .document
                    .get_mut("management")
                    .and_then(serde_yaml_ng::Value::as_mapping_mut)
                {
                    m.remove("secret-key");
                }
            }
        }
        assert_eq!(got.oauth_only, want.oauth_only, "{text}");
        assert_eq!(got.api_keys, want.api_keys, "{text}");
        assert_eq!(got.routing, want.routing, "{text}");
        assert_eq!((got.host.clone(), got.port), (want.host.clone(), want.port), "{text}");
        assert_eq!(got.auth_dir, want.auth_dir, "{text}");
        let known = serde_json::to_value(ck_core::config::AsJson(&want.document)).unwrap();
        assert_eq!(
            serde_json::to_value(ck_core::config::AsJson(&got.document)).unwrap(),
            known,
            "{text}"
        );
    }
    eprintln!("compiled and compared {} corpus documents", corpus.len());
    assert!(corpus.len() > 100, "the corpus lost its fixture documents");
}

#[test]
fn a_document_splits_back_into_the_same_resources() {
    let corpus = corpus();
    for text in &corpus {
        let resources = import_yaml(text).unwrap_or_else(|e| panic!("import failed: {e:#}\n{text}"));
        let doc = document(&resources, true);
        let again = split(&doc, &resources).unwrap();
        assert_eq!(again, resources, "{text}");
    }
    eprintln!("split {} corpus documents back", corpus.len());
    assert!(corpus.len() > 100, "the corpus lost its fixture documents");
}

#[test]
fn go_only_fields_are_rejected_not_dropped() {
    let mut seen = std::collections::BTreeSet::new();
    for text in all_documents() {
        let Some(name) = go_only(&text) else { continue };
        seen.insert(name);
        assert!(import_yaml(&text).is_err(), "{name}: import must reject it\n{text}");
    }
    assert_eq!(
        seen.len(),
        GO_ONLY.len(),
        "a GO_ONLY entry matches no fixture document: {seen:?}"
    );
}

#[test]
fn removed_layouts_are_rejected_instead_of_dropped() {
    for (name, text) in [
        ("root auth-dir", "auth-dir: /tmp/a\n"),
        ("flat provider list", "gemini-api-key: [{api-key: k}]\n"),
        (
            "codex steering under oauth",
            "oauth: {providers: {codex: {response-steering: true}}}\n",
        ),
    ] {
        assert!(import_yaml(text).is_err(), "{name} must be rejected: {text}");
    }
}

fn provider(id: &str, family: &str, key: &str) -> Resource {
    Resource::new(
        Kind::Provider,
        id,
        0,
        json!({"type": family, "base-url": "https://x.example.invalid", "keys": [{"api-key": key}]}),
    )
}

#[test]
fn entries_keep_their_ids_through_an_edited_document() {
    let mut a = provider("main", "claude", "sk-1");
    let mut b = provider("spare", "claude", "sk-2");
    b.ord = 1;
    a.description = "primary".into();
    let key = Resource::new(Kind::ClientKey, "laptop", 0, json!({"key": "ck-1"}));
    let old = vec![a, b, key];
    let mut doc = document(&old, true);
    // Swap the two providers and add a client key in front.
    let list = doc.pointer_mut("/api-keys/claude").unwrap().as_array_mut().unwrap();
    list.swap(0, 1);
    let keys = doc.pointer_mut("/access/api-keys").unwrap().as_array_mut().unwrap();
    keys.insert(0, json!("ck-0"));
    let split = split(&doc, &old).unwrap();
    let find = |kind, id: &str| split.iter().find(|r| r.kind == kind && r.id == id).unwrap();
    assert_eq!(find(Kind::Provider, "spare").ord, 0);
    assert_eq!(find(Kind::Provider, "main").ord, 1);
    assert_eq!(find(Kind::Provider, "main").description, "primary");
    assert_eq!(find(Kind::ClientKey, "laptop").ord, 1);
    assert_eq!(find(Kind::ClientKey, "key-1").spec, json!({"key": "ck-0"}));
}

#[test]
fn secrets_are_hidden_in_history_and_kept_on_write_back() {
    let stored = provider("main", "claude", "sk-live");
    let hidden = redacted(&stored);
    assert_eq!(hidden.spec["keys"][0]["api-key"], SECRET);
    let mut back = hidden.clone();
    back.spec["base-url"] = "https://y.example.invalid".into();
    let err = resolve_secrets(&mut back, Some(&stored)).unwrap_err();
    assert_eq!(err.code, Some("secret_destination_changed"));
    assert_eq!(err.pointers, vec!["/keys/0/api-key"]);
    back.spec["keys"][0]["api-key"] = "sk-live".into();
    resolve_secrets(&mut back, Some(&stored)).unwrap();
    assert_eq!(back.spec["keys"][0]["api-key"], "sk-live");
    // A placeholder where nothing is stored is refused, naming the place.
    let mut two = hidden;
    two.spec["keys"] = json!([{"api-key": SECRET}, {"api-key": SECRET}]);
    let err = resolve_secrets(&mut two, Some(&stored)).unwrap_err();
    assert!(
        err.pointer.as_deref().is_some_and(|p| p.starts_with("/keys/")),
        "{}",
        err.message
    );
}

#[test]
fn the_management_key_is_hashed_and_hidden_on_read() {
    let mut m = Resource::new(Kind::Setting, "management", 0, json!({"secret-key": "hunter2"}));
    hash_management_key(&mut m).unwrap();
    let hash = m.spec["secret-key"].as_str().unwrap().to_owned();
    assert!(bcrypt::verify("hunter2", &hash).unwrap());
    assert_eq!(redacted(&m).spec["secret-key"], SECRET);
    // Hashing an existing hash or the placeholder changes nothing.
    hash_management_key(&mut m).unwrap();
    assert_eq!(m.spec["secret-key"], hash.as_str());
}

#[test]
fn set_path_edits_mappings_and_lists() {
    let mut doc = json!({"routing": {"strategy": "fill-first"}, "list": [1, 2]});
    set_path(&mut doc, &["routing", "retry", "request-retry"], Some(json!(2))).unwrap();
    set_path(&mut doc, &["list", "2"], Some(json!(3))).unwrap();
    set_path(&mut doc, &["list", "0"], None).unwrap();
    set_path(&mut doc, &["routing", "strategy"], None).unwrap();
    assert_eq!(doc, json!({"routing": {"retry": {"request-retry": 2}}, "list": [2, 3]}));
    assert!(set_path(&mut doc, &["list", "9"], Some(json!(0))).is_err());
}

#[test]
fn resources_that_cannot_compile_are_named() {
    let bad = Resource::new(Kind::Provider, "x", 0, json!({"type": "nope"}));
    assert_eq!(config(&[bad]).unwrap_err().resource.as_deref(), Some("provider/x"));
    let keys_in_access = Resource::new(Kind::Setting, "access", 0, json!({"api-keys": ["k"]}));
    assert!(config(&[keys_in_access]).is_err());
    let unknown = Resource::new(Kind::Setting, "routing", 0, json!({"no-such-setting": 1}));
    assert!(config(&[unknown]).is_err());
}

#[test]
fn legacy_and_mixed_oauth_scopes_survive_import_and_unrelated_edits() {
    let source = "oauth:\n  providers:\n    codex:\n      header-defaults: {user-agent: scoped-agent}\n      chatgpt-keep-alive: true\n";
    let original = Config::parse(source).unwrap();
    let resources = import_yaml(source).unwrap();
    let (compiled, _) = config(&resources).unwrap();
    assert_eq!(compiled.oauth_only, original.oauth_only);
    assert_eq!(compiled.for_api_key().document, original.for_api_key().document);
    let mut doc = document(&resources, true);
    set_path(&mut doc, &["server", "port"], Some(json!(9001))).unwrap();
    let next = split(&doc, &resources).unwrap();
    assert_eq!(config(&next).unwrap().0.oauth_only, original.oauth_only);
    set_path(
        &mut doc,
        &["oauth", "providers", "codex", "chatgpt-keep-alive"],
        Some(json!(false)),
    )
    .unwrap();
    let next = split(&doc, &resources).unwrap();
    assert!(
        config(&next)
            .unwrap()
            .0
            .oauth_only
            .contains("oauth.providers.codex.chatgpt-keep-alive")
    );
}

#[test]
fn unknown_sections_and_nested_fields_are_rejected_not_discarded() {
    for source in [
        "unknown-section: true\n",
        "server: {unknown-field: 1}\n",
        "api-keys: {claude: [{keys: [{api-key: x, unknown-field: true}]}]}\n",
    ] {
        assert!(import_yaml(source).is_err(), "{source}");
    }
}

#[test]
fn masked_nested_keys_follow_identity_when_reordered_and_reject_ambiguity() {
    let mut stored = provider("main", "claude", "sk-a");
    stored.spec["keys"] = json!([{"api-key":"sk-a"},{"api-key":"sk-b"}]);
    resolve_secrets(&mut stored, None).unwrap();
    let mut edit = redacted(&stored);
    edit.spec["keys"].as_array_mut().unwrap().swap(0, 1);
    resolve_secrets(&mut edit, Some(&stored)).unwrap();
    assert_eq!(edit.spec["keys"][0]["api-key"], "sk-b");
    assert_eq!(edit.spec["keys"][1]["api-key"], "sk-a");
    let mut ambiguous = redacted(&stored);
    for key in ambiguous.spec["keys"].as_array_mut().unwrap() {
        key.as_object_mut().unwrap().remove("@id");
    }
    assert!(resolve_secrets(&mut ambiguous, Some(&stored)).is_err());
    let mut duplicate = redacted(&stored);
    duplicate.spec["keys"][1]["@id"] = duplicate.spec["keys"][0]["@id"].clone();
    assert!(resolve_secrets(&mut duplicate, Some(&stored)).is_err());
}

#[test]
fn all_secret_carriers_are_masked_and_annotation_stripping_is_positioned() {
    let mut r = provider("main", "claude", "sk-a");
    r.spec["headers"] = json!({"Authorization":"Bearer hidden", "X-Custom":"hidden-header", "@id":"hidden-id-header"});
    r.spec["proxy-url"] = "https://user:password@proxy.invalid".into();
    r.spec["base-url"] = "https://api.invalid/v1?token=hidden-query".into();
    let shown = redacted(&r);
    for pointer in [
        "/keys/0/api-key",
        "/headers/Authorization",
        "/headers/X-Custom",
        "/headers/@id",
        "/proxy-url",
        "/base-url",
    ] {
        assert_eq!(shown.spec.pointer(pointer).unwrap(), SECRET);
    }
    let doc = json!({"api-keys":{"claude":[{"auth_index":"drop", "keys":[{"auth-index":"drop", "api-key":"x"}], "payload":{"auth_index":"keep"}}]}, "requests":{"payload":{"auth_index":"keep","auth-index":"keep"}}});
    let clean = strip_read_annotations_at(doc, &[]);
    assert!(clean["api-keys"]["claude"][0].get("auth_index").is_none());
    assert_eq!(clean["api-keys"]["claude"][0]["payload"]["auth_index"], "keep");
    assert_eq!(clean["requests"]["payload"]["auth_index"], "keep");
}

#[test]
fn duplicate_and_invalid_document_annotations_are_errors() {
    for ids in [
        [json!("same"), json!("same")],
        [json!("bad/id"), json!("ok")],
        [json!(4), json!("ok")],
    ] {
        let doc = json!({"api-keys":{"claude":[{"@id":ids[0],"keys":[{"api-key":"a"}]},{"@id":ids[1],"keys":[{"api-key":"b"}]}]}});
        assert!(split(&doc, &[]).is_err());
    }
    assert!(split(&json!({"model-routers": {"not":"a list"}}), &[]).is_err());
    assert!(split(&json!({"access": {"api-keys": [SECRET]}}), &[]).is_err());
}

#[test]
fn resource_and_depth_limits_are_checked_before_compilation() {
    let huge = Resource::new(Kind::Setting, "huge", 0, json!({"value":"x".repeat(MAX_SPEC_BYTES)}));
    assert!(check(&huge).is_err());
    let mut deep = json!(0);
    for _ in 0..=MAX_DEPTH {
        deep = json!({"nested":deep});
    }
    assert!(!bounded(&deep));
}

#[test]
fn expected_collection_annotations_are_stripped_without_touching_payloads() {
    let annotated =
        json!([{"auth_index":"drop","keys":[{"auth-index":"drop","api-key":SECRET}],"payload":{"auth_index":"keep"}}]);
    let clean = strip_read_annotations_at(annotated, &["api-keys", "claude"]);
    assert!(clean[0].get("auth_index").is_none());
    assert!(clean[0]["keys"][0].get("auth-index").is_none());
    assert_eq!(clean[0]["payload"]["auth_index"], "keep");
    let payload = json!({"auth_index":"keep", "auth-index":"keep"});
    assert_eq!(
        strip_read_annotations_at(payload.clone(), &["requests", "payload"]),
        payload
    );
}

#[test]
fn effective_redaction_keeps_injected_identity_and_hides_opaque_configs() {
    let mut doc = json!({"api-keys":{"gemini":[{"@id":"provider:owner@example.invalid","auth_index":"index:owner@example.invalid","keys":[{"@id":"key:owner@example.invalid","auth_index":"key-index","api-key":"sk-live"}],"headers":{"Authorization":"Bearer live"},"payload":{"auth_index":"https://user:password@example.invalid"}}]}});
    doc["access"] = json!({"api-keys":[{"@id":"client:alice@example.com","key":"fake-key"}]});
    doc["model-routers"] = json!([{"@id":"router:alice@example.com","name":"route"}]);
    redact_document(&mut doc);
    assert_eq!(
        doc["api-keys"]["gemini"][0]["auth_index"],
        "index:owner@example.invalid"
    );
    assert_eq!(doc["api-keys"]["gemini"][0]["@id"], "provider:owner@example.invalid");
    assert_eq!(doc["api-keys"]["gemini"][0]["keys"][0]["auth_index"], "key-index");
    assert_eq!(
        doc["api-keys"]["gemini"][0]["keys"][0]["@id"],
        "key:owner@example.invalid"
    );
    assert_eq!(doc["api-keys"]["gemini"][0]["keys"][0]["api-key"], SECRET);
    assert_eq!(doc["api-keys"]["gemini"][0]["headers"]["Authorization"], SECRET);
    assert_eq!(doc["api-keys"]["gemini"][0]["payload"]["auth_index"], SECRET);
    assert_eq!(doc["access"]["api-keys"][0]["@id"], "client:alice@example.com");
    assert_eq!(doc["access"]["api-keys"][0]["key"], SECRET);
    assert_eq!(doc["model-routers"][0]["@id"], "router:alice@example.com");
}
