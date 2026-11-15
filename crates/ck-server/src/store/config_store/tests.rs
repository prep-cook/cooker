use super::db::{SCHEMA, SCHEMA_VERSION, user_version};
use super::*;
use serde_json::json;

mod database;
mod encoding;
mod keyring;
mod operation_resources;
mod ordered;

impl ConfigStore {
    fn commit(&self, ops: &[Op], actor: &str, message: &str, dry_run: bool) -> Result<Committed, CommitError> {
        self.commit_if(ops, actor, message, dry_run, None)
    }

    fn head(&self) -> anyhow::Result<u64> {
        Ok(self.snapshot()?.0)
    }

    fn resources(&self) -> anyhow::Result<Vec<Resource>> {
        Ok(self.snapshot()?.1)
    }

    fn get(&self, kind: Kind, id: &str) -> anyhow::Result<Option<Resource>> {
        Ok(self.resources()?.into_iter().find(|r| r.kind == kind && r.id == id))
    }
}

fn put(kind: &str, id: &str, spec: Json) -> Op {
    Op::Put {
        kind: kind.into(),
        id: id.into(),
        spec,
        ord: None,
        description: None,
        if_rev: None,
        if_absent: false,
    }
}

fn provider(key: &str) -> Json {
    json!({"type": "claude", "base-url": "https://a.example.invalid", "keys": [{"api-key": key}]})
}

#[test]
fn a_commit_is_one_revision_and_an_invalid_one_changes_nothing() {
    let store = ConfigStore::memory();
    let first = store
        .commit(
            &[
                put("setting", "routing", json!({"strategy": "fill-first"})),
                put("provider", "main", provider("sk-1")),
            ],
            "test",
            "",
            false,
        )
        .unwrap();
    assert_eq!((first.rev, first.written, first.changes.len()), (1, true, 2));
    assert_eq!(first.config.routing.strategy, "fill-first");
    // The second op fails validation: neither is applied.
    let err = store
        .commit(
            &[
                put("setting", "routing", json!({"strategy": "round-robin"})),
                put("setting", "routing2", json!({})),
            ],
            "test",
            "",
            false,
        )
        .unwrap_err();
    assert_eq!(err.status, 422);
    assert_eq!(store.head().unwrap(), 1);
    assert_eq!(store.load().unwrap().config.routing.strategy, "fill-first");
    // Writing the same content again makes no revision.
    let same = store
        .commit(
            &[put("setting", "routing", json!({"strategy": "fill-first"}))],
            "test",
            "",
            false,
        )
        .unwrap();
    assert_eq!((same.rev, same.written), (1, false));
}

#[test]
fn preconditions_are_per_resource() {
    let store = ConfigStore::memory();
    store
        .commit(
            &[
                put("provider", "a", provider("sk-a")),
                put("provider", "b", provider("sk-b")),
            ],
            "t",
            "",
            false,
        )
        .unwrap();
    let a = store.get(Kind::Provider, "a").unwrap().unwrap();
    // Another writer changes b; a write to a that read a before still succeeds.
    store
        .commit(&[put("provider", "b", provider("sk-b2"))], "t", "", false)
        .unwrap();
    let ok = Op::Patch {
        kind: "provider".into(),
        id: "a".into(),
        patch: json!({"prefix": "team"}),
        if_rev: Some(a.rev),
    };
    store.commit(std::slice::from_ref(&ok), "t", "", false).unwrap();
    // The same precondition is now stale.
    assert_eq!(store.commit(&[ok], "t", "", false).unwrap_err().status, 412);
    let exists = Op::Put {
        kind: "provider".into(),
        id: "a".into(),
        spec: provider("sk"),
        ord: None,
        description: None,
        if_rev: None,
        if_absent: true,
    };
    assert_eq!(store.commit(&[exists], "t", "", false).unwrap_err().status, 412);
}

#[test]
fn history_hides_secrets_and_restore_keeps_the_current_ones() {
    let store = ConfigStore::memory();
    store
        .commit(&[put("provider", "main", provider("sk-old"))], "t", "", false)
        .unwrap();
    store
        .commit(
            &[Op::Patch {
                kind: "provider".into(),
                id: "main".into(),
                patch: json!({"prefix": "team"}),
                if_rev: None,
            }],
            "t",
            "",
            false,
        )
        .unwrap();
    let (_, changes) = store.revision(1).unwrap().unwrap();
    assert_eq!(
        changes[0].after.as_ref().unwrap()["spec"]["keys"][0]["api-key"],
        resources::SECRET
    );
    // Restore the old non-destination fields with the key stored now.
    let state = store.state_at(1).unwrap();
    store.replace(state, "restore", "", false).unwrap();
    let main = store.get(Kind::Provider, "main").unwrap().unwrap();
    assert_eq!(main.spec["base-url"], "https://a.example.invalid");
    assert!(main.spec.get("prefix").is_none());
    assert_eq!(main.spec["keys"][0]["api-key"], "sk-old");
    // Once the provider is gone, its history cannot bring the key back.
    store
        .commit(
            &[Op::Delete {
                kind: "provider".into(),
                id: "main".into(),
                if_rev: None,
            }],
            "t",
            "",
            false,
        )
        .unwrap();
    let err = store
        .replace(store.state_at(1).unwrap(), "restore", "", false)
        .unwrap_err();
    assert_eq!(err.status, 422);
}

#[test]
fn a_deleted_key_leaves_no_trace_in_the_files() {
    let dir = std::env::temp_dir().join(format!("cooker-config-store-{}", uuid::Uuid::new_v4()));
    let path = dir.join("cooker.db");
    let store = crate::testing::config_store(&path).unwrap();
    let secret = "sk-forget-me-0123456789";
    store
        .commit(&[put("provider", "main", provider(secret))], "t", "", false)
        .unwrap();
    store
        .commit(
            &[Op::Delete {
                kind: "provider".into(),
                id: "main".into(),
                if_rev: None,
            }],
            "t",
            "",
            false,
        )
        .unwrap();
    drop(store);
    let bytes = std::fs::read(&path).unwrap();
    assert!(
        !bytes.windows(secret.len()).any(|w| w == secret.as_bytes()),
        "published snapshot keeps the key"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_path_set_keeps_ids_and_checks_what_was_read() {
    let store = ConfigStore::memory();
    store
        .commit(
            &[
                put("provider", "main", provider("sk-1")),
                put("client-key", "laptop", json!({"key": "ck"})),
            ],
            "t",
            "",
            false,
        )
        .unwrap();
    let doc = resources::document_for_read(&store.resources().unwrap());
    let mut list = doc["api-keys"]["claude"].clone();
    list[0]["prefix"] = "team".into();
    let set = Op::Set {
        path: "api-keys/claude".into(),
        value: list,
        expect: Some(doc["api-keys"]["claude"].clone()),
    };
    store.commit(std::slice::from_ref(&set), "t", "", false).unwrap();
    let main = store.get(Kind::Provider, "main").unwrap().unwrap();
    assert_eq!(main.spec["prefix"], "team");
    assert!(main.spec.get("@id").is_none());
    // The same expectation no longer holds.
    assert_eq!(store.commit(&[set], "t", "", false).unwrap_err().status, 412);
    // An absent value matches an expected empty one.
    let first = Op::Set {
        path: "routing/strategy".into(),
        value: json!("fill-first"),
        expect: Some(Json::Null),
    };
    store.commit(&[first], "t", "", false).unwrap();
    assert_eq!(store.load().unwrap().config.routing.strategy, "fill-first");
}

#[test]
fn moves_reorder_list_entries() {
    let store = ConfigStore::memory();
    let keys = ["a", "b", "c"].map(|k| put("client-key", k, json!({ "key": format!("ck-{k}") })));
    store.commit(&keys, "t", "", false).unwrap();
    let mv = Op::Move {
        kind: "client-key".into(),
        id: "c".into(),
        before: Some("a".into()),
        after: None,
        if_rev: None,
    };
    let done = store.commit(&[mv], "t", "", false).unwrap();
    assert_eq!(done.config.api_keys, ["ck-c", "ck-a", "ck-b"]);
}

#[test]
fn owner_mutations_publish_current_memory_without_reloading() {
    let dir = std::env::temp_dir().join(format!("cooker-config-owner-{}", uuid::Uuid::new_v4()));
    let server = std::sync::Arc::new(crate::testing::config_store(&dir.join("cooker.db")).unwrap());
    server
        .commit(&[put("setting", "routing", json!({}))], "t", "", false)
        .unwrap();
    let retained = server.load().unwrap();
    let adapter = server.clone();
    adapter
        .commit(
            &[put("setting", "routing", json!({"strategy": "fill-first"}))],
            "adapter",
            "",
            false,
        )
        .unwrap();
    assert_eq!(server.load().unwrap().rev, 2);
    assert_eq!(retained.rev, 1);
    assert_eq!(retained.resources[0].spec, json!({}));
    assert_eq!(server.load().unwrap().resources[0].spec["strategy"], "fill-first");
    drop((adapter, server));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn an_empty_first_import_is_durable_initialization() {
    let store = ConfigStore::memory();
    assert!(store.is_empty().unwrap());
    let first = store.import_yaml("", "test", "empty import").unwrap();
    assert_eq!((first.rev, first.written), (1, true));
    assert!(!store.is_empty().unwrap());
    assert_eq!(store.import_yaml("", "test", "").unwrap().rev, 1);
    assert!(!store.import_yaml("", "test", "").unwrap().written);
}

#[test]
fn collection_and_resource_preconditions_are_checked_inside_the_write() {
    let store = ConfigStore::memory();
    assert_eq!(
        store
            .replace_if(vec![], "t", "", false, Some(Precondition::Exists))
            .unwrap_err()
            .status,
        412
    );
    let first = store
        .commit(&[put("setting", "routing", json!({}))], "t", "", false)
        .unwrap();
    assert_eq!(first.resources[0].rev, 1);
    assert_eq!(first.resources[0].created_rev, 1);
    store
        .commit(
            &[put("setting", "routing", json!({"strategy":"fill-first"}))],
            "t",
            "",
            false,
        )
        .unwrap();
    assert_eq!(
        store
            .replace_if(first.resources, "t", "", false, Some(Precondition::Revision(1)))
            .unwrap_err()
            .status,
        412
    );
    let check = Op::Check {
        kind: "setting".into(),
        id: "routing".into(),
        revisions: Some(vec![1]),
    };
    assert_eq!(store.commit(&[check], "t", "", false).unwrap_err().status, 412);
    let missing = Op::Check {
        kind: "setting".into(),
        id: "missing".into(),
        revisions: None,
    };
    assert_eq!(store.commit(&[missing], "t", "", false).unwrap_err().status, 412);
    let then = store.head().unwrap();
    let done = store
        .commit_if(
            &[put("setting", "server", json!({"port":9001}))],
            "t",
            "",
            false,
            Some(Precondition::AnyRevision(vec![1, then])),
        )
        .unwrap();
    assert!(
        done.resources
            .iter()
            .find(|r| r.id == "server")
            .is_some_and(|r| r.rev == done.rev && r.created_rev == done.rev)
    );
}

#[test]
fn unsupported_database_kinds_are_rejected_before_bootstrap() {
    let dir = std::env::temp_dir().join(format!("cooker-config-unsupported-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let original = dir.join("original.db");
    let store = crate::testing::config_store(&original).unwrap();
    store.import_yaml("", "t", "").unwrap();
    let bytes = std::fs::read(&original).unwrap();
    for (name, sql) in [
        (
            "resource",
            "INSERT INTO resource(kind,id,spec,rev,created_rev) VALUES ('future','x','{}',1,1)",
        ),
        (
            "history",
            "INSERT INTO change(rev,kind,id,after) VALUES (1,'future','x','{\"ord\":0,\"spec\":{}}')",
        ),
    ] {
        let path = dir.join(format!("{name}.db"));
        std::fs::write(&path, &bytes).unwrap();
        let conn = Connection::open(&path).unwrap();
        conn.execute(sql, []).unwrap();
        drop(conn);
        let unsupported = std::fs::read(&path).unwrap();
        assert!(
            crate::testing::config_store(&path).is_err(),
            "unsupported {name} was admitted"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            unsupported,
            "failed bootstrap changed the input artifact"
        );
    }
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn public_digests_do_not_fingerprint_secrets() {
    let store = ConfigStore::memory();
    let first = store
        .commit(&[put("provider", "main", provider("short"))], "t", "", false)
        .unwrap();
    let second = store.commit(&[
        Op::Patch {kind:"provider".into(),id:"main".into(),patch:json!({"keys":[{"@id":first.resources.iter().find(|r| r.kind == Kind::Provider).unwrap().spec["keys"][0]["@id"],"api-key":"another-secret"}]}),if_rev:None}
    ], "t", "", false).unwrap();
    assert_eq!(first.digest, second.digest);
    assert_eq!(store.load().unwrap().digest, second.digest);
    let history = serde_json::to_string(&store.revision(2).unwrap().unwrap().1).unwrap();
    for secret in ["short", "another-secret"] {
        assert!(!history.contains(secret));
    }
}

#[test]
fn snapshot_revision_and_rows_never_cross_owner_commits() {
    let dir = std::env::temp_dir().join(format!("cooker-config-snapshot-{}", uuid::Uuid::new_v4()));
    let path = dir.join("cooker.db");
    let store = Arc::new(crate::testing::config_store(&path).unwrap());
    store
        .commit(&[put("setting", "server", json!({"port":9000}))], "t", "", false)
        .unwrap();
    let writer_path = path.clone();
    let owner = store.clone();
    let writer = std::thread::spawn(move || {
        assert!(crate::testing::config_store(&writer_path).is_err());
        let writer = owner;
        for port in 9001..9020 {
            writer
                .commit(&[put("setting", "server", json!({"port":port}))], "t", "", false)
                .unwrap();
        }
    });
    for _ in 0..100 {
        let (rev, resources) = store.snapshot().unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].rev, rev);
        let loaded = store.load().unwrap();
        assert_eq!(loaded.resources[0].rev, loaded.rev);
    }
    writer.join().unwrap();
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn limits_reject_oversized_states_and_operation_batches_atomically() {
    let store = ConfigStore::memory();
    let ops = vec![put("setting", "routing", json!({})); 1001];
    assert_eq!(store.commit(&ops, "t", "", false).unwrap_err().status, 422);
    let resources = (0..=resources::MAX_RESOURCES)
        .map(|i| Resource::new(Kind::ClientKey, format!("key-{i}"), i as i64, json!({"key":"x"})))
        .collect();
    assert_eq!(store.replace(resources, "t", "", false).unwrap_err().status, 422);
    assert_eq!(store.head().unwrap(), 0);
}

#[test]
fn online_backup_is_consistent_and_never_overwrites_a_path() {
    let dir = std::env::temp_dir().join(format!("cooker-config-backup-{}", uuid::Uuid::new_v4()));
    let store = crate::testing::config_store(&dir.join("cooker.db")).unwrap();
    store.import_yaml("server: {port: 9001}\n", "t", "").unwrap();
    let destination = dir.join("backup.db");
    store.backup(&destination).unwrap();
    assert!(store.backup(&destination).is_err());
    assert!(store.backup(store.path().unwrap()).is_err());
    let backup = crate::testing::config_store(&destination).unwrap();
    assert_eq!(backup.load().unwrap().config.port, 9001);
    drop(backup);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn versioned_exports_preserve_scope_with_explicit_secret_access() {
    let store = ConfigStore::memory();
    store
        .import_yaml("upstream:\n  codex:\n    disable-codex-cloaking: true\n", "t", "")
        .unwrap();
    let loaded = store.load().unwrap();
    let private = resources::export_json(loaded.rev, &loaded.resources, true).to_string();
    let copy = ConfigStore::memory();
    copy.replace(resources::import_json(&private).unwrap(), "t", "", false)
        .unwrap();
    let again = copy.load().unwrap();
    assert_eq!(again.config.oauth_only, loaded.config.oauth_only);
    let safe = resources::export_json(loaded.rev, &loaded.resources, false).to_string();
    let restored = store
        .replace(resources::import_json(&safe).unwrap(), "t", "", false)
        .unwrap();
    assert!(!restored.written);
    let mut malformed: Json = serde_json::from_str(&private).unwrap();
    malformed.as_object_mut().unwrap().remove("revision");
    assert!(resources::import_json(&malformed.to_string()).is_err());
    malformed["revision"] = json!(1);
    malformed["unknown"] = json!(true);
    assert!(resources::import_json(&malformed.to_string()).is_err());
}

/// Decision 2: a database holding a kind this build removed (plugins) is refused as a
/// whole, whether the row is current or only in history, and the refusal names the kind.
#[test]
fn a_removed_resource_kind_refuses_the_database_and_a_clean_one_reopens() {
    let dir = std::env::temp_dir().join(format!("cooker-config-removed-kind-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let clean = dir.join("clean.db");
    let store = crate::testing::config_store(&clean).unwrap();
    store
        .commit(&[put("provider", "main", provider("sk-kept"))], "t", "", false)
        .unwrap();
    let (rev, rows) = store.snapshot().unwrap();
    let before = resources::export_json(rev, &rows, false);
    drop(store);
    let reopened = crate::testing::config_store(&clean).unwrap();
    let (reopened_rev, reopened_rows) = reopened.snapshot().unwrap();
    assert_eq!(reopened_rev, rev);
    assert_eq!(resources::export_json(reopened_rev, &reopened_rows, false), before);
    assert!(reopened.revision(rev).unwrap().is_some());
    drop(reopened);

    let plugin = json!({"ord": 0, "spec": {"enabled": true}}).to_string();
    for (name, sql) in [
        (
            "current.db",
            format!(
                "INSERT INTO revision VALUES ({next},'then','t','','');
                 INSERT INTO change VALUES ({next},'plugin','demo',NULL,'{plugin}');
                 INSERT INTO resource VALUES ('plugin','demo',0,'{{\"enabled\":true}}','',{next},{next});",
                next = rev + 1
            ),
        ),
        (
            "history.db",
            format!(
                "INSERT INTO revision VALUES ({next},'then','t','','');
                 INSERT INTO change VALUES ({next},'plugin','demo','{plugin}',NULL);",
                next = rev + 1
            ),
        ),
    ] {
        let path = dir.join(name);
        std::fs::copy(&clean, &path).unwrap();
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(&sql).unwrap();
        drop(conn);
        let error = crate::testing::config_store(&path).err().expect(name);
        assert!(
            error.to_string().contains(
                "unknown stored resource kind \"plugin\"; this build cannot open the database, start from a new one"
            ),
            "{name}: {error:#}"
        );
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn stored_history_is_masked_on_read() {
    let dir = std::env::temp_dir().join(format!("cooker-config-history-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("cooker.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(SCHEMA).unwrap();
    conn.pragma_update(None, "user_version", SCHEMA_VERSION).unwrap();
    conn.execute("INSERT INTO revision VALUES (1,'then','t','','')", [])
        .unwrap();
    let after = json!({"ord":0,"spec":{"type":"claude","headers":{"Authorization":"Bearer stored-secret"},"keys":[{"api-key":"$secret"}]}}).to_string();
    conn.execute("INSERT INTO change VALUES (1,'provider','main',NULL,?1)", [&after])
        .unwrap();
    drop(conn);
    let store = crate::testing::config_store(&path).unwrap();
    let (_, changes) = store.revision(1).unwrap().unwrap();
    assert!(!serde_json::to_string(&changes).unwrap().contains("stored-secret"));
    assert_eq!(
        store.state_at(1).unwrap()[0].spec["headers"]["Authorization"],
        resources::SECRET
    );
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn importing_masked_keys_cannot_redirect_them_to_another_destination() {
    let store = ConfigStore::memory();
    store
        .commit(&[put("provider", "main", provider("sk-live"))], "t", "", false)
        .unwrap();
    let (rev, rows) = store.snapshot().unwrap();
    let mut export = resources::export_json(rev, &rows, false);
    let spec = &mut export["resources"][0]["spec"];
    spec["base-url"] = "https://attacker.example.invalid".into();
    spec["keys"] = json!([{"api-key":resources::SECRET}]);
    let err = store
        .replace_if(
            resources::import_json(&export.to_string()).unwrap(),
            "import",
            "",
            false,
            Some(Precondition::Revision(rev)),
        )
        .unwrap_err();
    assert_eq!(err.code, "secret_destination_changed");
    assert_eq!(err.fault.resource.as_deref(), Some("provider/main"));
    assert_eq!(err.fault.pointers, vec!["/keys/0/api-key"]);
    assert_eq!(store.head().unwrap(), rev);
}

#[test]
fn restoring_a_previous_destination_requires_resending_the_secret() {
    let store = ConfigStore::memory();
    store
        .commit(&[put("provider", "main", provider("sk-old"))], "t", "", false)
        .unwrap();
    let mut current = provider("sk-current");
    current["base-url"] = "https://new.example.invalid".into();
    store
        .commit(&[put("provider", "main", current)], "t", "", false)
        .unwrap();
    let old = store.state_at(1).unwrap();
    let err = store.replace(old.clone(), "restore", "", false).unwrap_err();
    assert_eq!(err.code, "secret_destination_changed");
    assert_eq!(err.fault.pointers, vec!["/keys/0/api-key"]);
    assert_eq!(store.head().unwrap(), 2);
    let mut explicit = old;
    explicit[0].spec["keys"][0]["api-key"] = "sk-current".into();
    store.replace(explicit, "restore", "", false).unwrap();
    assert_eq!(
        store.get(Kind::Provider, "main").unwrap().unwrap().spec["base-url"],
        "https://a.example.invalid"
    );
}

#[test]
fn a_nested_key_proxy_change_cannot_keep_its_masked_key() {
    let store = ConfigStore::memory();
    store
        .commit(&[put("provider", "main", provider("sk-live"))], "t", "", false)
        .unwrap();
    let current = store.get(Kind::Provider, "main").unwrap().unwrap();
    let op = Op::Patch {
        kind: "provider".into(),
        id: "main".into(),
        patch: json!({"keys":[{"@id":current.spec["keys"][0]["@id"],"api-key":resources::SECRET,"proxy-url":"https://attacker.example.invalid"}]}),
        if_rev: Some(current.rev),
    };
    let err = store.commit(&[op], "t", "", false).unwrap_err();
    assert_eq!(err.code, "secret_destination_changed");
    assert_eq!(err.fault.pointers, vec!["/keys/0/api-key"]);
    assert_eq!(store.head().unwrap(), current.rev);
}

#[test]
fn a_header_change_lists_every_secret_that_must_be_resent() {
    let store = ConfigStore::memory();
    let mut original = provider("sk-live");
    original["headers"] = json!({"Authorization":"Bearer live", "X-Route":"before"});
    store
        .commit(&[put("provider", "main", original)], "t", "", false)
        .unwrap();
    let err = store
        .commit(
            &[Op::Patch {
                kind: "provider".into(),
                id: "main".into(),
                patch: json!({"headers":{"X-Route":"after"}}),
                if_rev: Some(1),
            }],
            "t",
            "",
            false,
        )
        .unwrap_err();
    assert_eq!(err.code, "secret_destination_changed");
    assert_eq!(err.fault.code, Some("secret_destination_changed"));
    assert_eq!(err.fault.pointers.len(), 2);
    assert!(err.fault.pointers.iter().any(|p| p == "/keys/0/api-key"));
    assert!(err.fault.pointers.iter().any(|p| p == "/headers/Authorization"));
    assert_eq!(err.fault.pointer, err.fault.pointers.first().cloned());
    assert_eq!(store.head().unwrap(), 1);
}

#[test]
fn patch_and_set_cannot_bypass_secret_resends_by_omitting_the_key() {
    let store = ConfigStore::memory();
    store
        .commit(&[put("provider", "main", provider("sk-live"))], "t", "", false)
        .unwrap();
    let ops = [
        Op::Patch {
            kind: "provider".into(),
            id: "main".into(),
            patch: json!({"base-url":"https://attacker.example.invalid"}),
            if_rev: None,
        },
        Op::Set {
            path: "api-keys/claude/0/base-url".into(),
            value: json!("https://attacker.example.invalid"),
            expect: None,
        },
    ];
    for op in ops {
        let err = store.commit(&[op], "t", "", false).unwrap_err();
        assert_eq!(err.code, "secret_destination_changed");
        assert_eq!(err.fault.pointers, vec!["/keys/0/api-key"]);
        assert_eq!(store.head().unwrap(), 1);
    }
}

/// The removed Home settings (the `home` section and the credential settings) and the
/// removed `plugins` section are unknown keys: YAML import and management writes refuse
/// them in every spelling and publish no revision.
#[test]
fn removed_home_settings_are_rejected_without_a_revision() {
    let store = ConfigStore::memory();
    store.import_yaml("server: {port: 9001}\n", "t", "").unwrap();
    let head = store.head().unwrap();
    for yaml in [
        "credentials: {concurrency: {max-limit: 5}}",
        "credentials: {in-flight: {max-details: 9}}",
        "credentials: {concurrency: {}}",
        "credentials: {in-flight: null}",
        "credential-concurrency: {max-limit: 5}",
        "credential-in-flight: {max-details: 9}",
        "credential-concurrency: {}",
        "credential-in-flight: null",
        "home: {enabled: true}",
        "home: {enabled: false}",
        "home: {}",
        "home: null",
        "plugins: {auth-revision: 10}",
        "plugins: {auth-revision: null}",
        "plugins: {enabled: true}",
        "plugins: {configs: {acme: {enabled: true, ratio: 1.5}}}",
    ] {
        assert!(store.import_yaml(&format!("{yaml}\n"), "t", "").is_err(), "{yaml}");
        assert_eq!(store.head().unwrap(), head, "{yaml}");
    }
    // Decision 1: a `plugins:` section fails with the ordinary unknown-key error.
    let err = store.import_yaml("plugins: {enabled: true}\n", "t", "").unwrap_err();
    assert!(format!("{err:#}").contains("unknown setting config.plugins"), "{err:#}");
    for (setting, spec) in [
        ("credentials", json!({"concurrency": {"max-limit": 5}})),
        ("credentials", json!({"in-flight": {"max-details": 9}})),
        ("credentials", json!({"concurrency": {}})),
        ("credentials", json!({"in-flight": null})),
        ("home", json!({"enabled": true})),
        ("home", json!({})),
        ("plugins", json!({"auth-revision": 9})),
        ("plugins", json!({"configs": {"acme": {"enabled": true}}})),
    ] {
        assert!(
            store
                .commit(&[put("setting", setting, spec.clone())], "t", "", false)
                .is_err(),
            "{setting}: {spec}"
        );
        assert_eq!(store.head().unwrap(), head, "{setting}: {spec}");
    }
    for (path, value) in [
        ("home/enabled", json!(true)),
        ("home", json!({"enabled": true})),
        ("plugins/auth-revision", json!(9)),
        ("plugins/configs/acme/enabled", json!(true)),
    ] {
        let op = Op::Set {
            path: path.into(),
            value: value.clone(),
            expect: None,
        };
        assert!(store.commit(&[op], "t", "", false).is_err(), "{path}: {value}");
        assert_eq!(store.head().unwrap(), head, "{path}: {value}");
    }
}

#[test]
fn credential_like_resource_ids_survive_effective_reads_and_unrelated_sets() {
    let store = ConfigStore::memory();
    let id = "client:alice@example.com";
    store
        .commit(
            &[
                put("client-key", id, json!({"key":"fake-client-key"})),
                put(
                    "setting",
                    "requests",
                    json!({"proxy-url":"http://user:password@example.invalid"}),
                ),
            ],
            "t",
            "",
            false,
        )
        .unwrap();
    let shown = resources::document_for_read(&store.resources().unwrap());
    assert_eq!(shown["access"]["api-keys"][0]["@id"], id);
    assert_eq!(shown["access"]["api-keys"][0]["key"], resources::SECRET);
    assert_eq!(shown["requests"]["proxy-url"], resources::SECRET);
    store
        .commit(
            &[Op::Set {
                path: "server/port".into(),
                value: json!(8443),
                expect: None,
            }],
            "t",
            "",
            false,
        )
        .unwrap();
    assert_eq!(
        store.get(Kind::ClientKey, id).unwrap().unwrap().spec["key"],
        "fake-client-key"
    );
}

#[test]
fn schema_failures_carry_resource_relative_pointers() {
    let store = ConfigStore::memory();
    for (op, resource, pointer) in [
        (
            put(
                "setting",
                "observability",
                json!({"logs":{"debug":["submitted-private-value"]}}),
            ),
            "setting/observability",
            "/logs/debug",
        ),
        (
            put(
                "provider",
                "main",
                json!({"type":"claude","keys":[{"api-key":"fake", "models":false}]}),
            ),
            "provider/main",
            "/keys/0/models",
        ),
        (
            put("setting", "observability", json!({"unknown/~field":false})),
            "setting/observability",
            "/unknown~1~0field",
        ),
    ] {
        let error = store.commit(&[op], "t", "", false).unwrap_err();
        assert_eq!(error.status, 422);
        assert_eq!(error.fault.resource.as_deref(), Some(resource));
        assert_eq!(error.fault.pointer.as_deref(), Some(pointer));
        assert!(!error.fault.message.contains("submitted-private-value"));
    }
}

#[test]
fn changing_provider_type_requires_resending_masked_secrets() {
    let store = ConfigStore::memory();
    store
        .commit(&[put("provider", "main", provider("fake-live-key"))], "t", "", false)
        .unwrap();
    let mut spec = resources::redacted(&store.get(Kind::Provider, "main").unwrap().unwrap()).spec;
    spec["type"] = "codex".into();
    let error = store
        .commit(&[put("provider", "main", spec.clone())], "t", "", false)
        .unwrap_err();
    assert_eq!(error.code, "secret_destination_changed");
    assert_eq!(error.fault.pointers, vec!["/keys/0/api-key"]);
    spec["keys"][0]["api-key"] = "fake-live-key".into();
    store.commit(&[put("provider", "main", spec)], "t", "", false).unwrap();
    assert_eq!(
        store.get(Kind::Provider, "main").unwrap().unwrap().spec["type"],
        "codex"
    );
}
