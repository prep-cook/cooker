use super::*;
use crate::store::config_store::keyring::{test_hmac, test_secret_hex};

fn database(name: &str) -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!("cooker-keyring-{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("cooker.db");
    (dir, path)
}

fn states(store: &ConfigStore) -> Vec<(String, KeyState)> {
    store
        .envelope_keys()
        .states()
        .map(|(kid, state)| (kid.to_owned(), state))
        .collect()
}

fn sign(store: &ConfigStore, message: &[u8]) -> (String, [u8; 32]) {
    let ring = store.envelope_keys();
    let signer = ring.signer().expect("an active key");
    (signer.kid().to_owned(), signer.mac(message))
}

fn stored_rows(path: &Path) -> Vec<(String, String)> {
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut query = conn
        .prepare("SELECT name, value FROM credential_meta ORDER BY name")
        .unwrap();
    query
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn hmac_matches_rfc_4231() {
    let hex = |bytes: [u8; 32]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    assert_eq!(
        hex(test_hmac(&[0x0b; 20], b"Hi There")),
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
    assert_eq!(
        hex(test_hmac(b"Jefe", b"what do ya want for nothing?")),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
}

#[test]
fn an_empty_store_commits_one_key_that_verifies_after_restart() {
    let (dir, path) = database("restart");
    let store = crate::testing::config_store(&path).unwrap();
    assert!(store.envelope_keys().signer().is_none());
    let kid = store.ensure_envelope_key().unwrap();
    assert_eq!(states(&store), [(kid.clone(), KeyState::Active)]);
    assert_eq!(store.ensure_envelope_key().unwrap(), kid, "a signing key is kept");
    assert_eq!(states(&store).len(), 1);
    assert_eq!(store.head().unwrap(), 0, "the key ring is not a config revision");
    let message = format!("ck1.{kid}.payload");
    let (signed_by, tag) = sign(&store, message.as_bytes());
    assert_eq!(signed_by, kid);
    drop(store);

    let store = crate::testing::config_store(&path).unwrap();
    assert_eq!(store.ensure_envelope_key().unwrap(), kid, "restart loads the same key");
    assert_eq!(store.envelope_keys().verify(&kid, message.as_bytes(), &tag), Ok(()));
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn rotation_keeps_old_envelopes_valid_and_revocation_rejects_them() {
    let (dir, path) = database("rotate");
    let store = crate::testing::config_store(&path).unwrap();
    let first = store.ensure_envelope_key().unwrap();
    let (_, old) = sign(&store, b"old item");
    let second = store.rotate_envelope_key().unwrap();
    assert_ne!(first, second);
    assert_eq!(
        states(&store),
        BTreeMap::from([(first.clone(), KeyState::Verify), (second.clone(), KeyState::Active)])
            .into_iter()
            .collect::<Vec<_>>()
    );
    assert_eq!(sign(&store, b"new item").0, second, "the new key signs");
    let ring = store.envelope_keys();
    assert_eq!(
        ring.verify(&first, b"old item", &old),
        Ok(()),
        "rotation keeps old envelopes"
    );
    assert_eq!(ring.verify(&first, b"edited item", &old), Err(Refused::Mismatch));
    assert_eq!(ring.verify("00000000", b"old item", &old), Err(Refused::Unknown));

    let error = store.revoke_envelope_key(&second).unwrap_err();
    assert!(error.to_string().contains("rotate"), "{error}");
    store.revoke_envelope_key(&first).unwrap();
    assert_eq!(
        store.envelope_keys().verify(&first, b"old item", &old),
        Err(Refused::Revoked)
    );
    assert_eq!(store.head().unwrap(), 0);
    drop(store);

    let rows = stored_rows(&path);
    let revoked = &rows.iter().find(|(name, _)| name.ends_with(&first)).unwrap().1;
    assert!(!revoked.contains("secret"), "a revoked secret is erased: {revoked}");
    let store = crate::testing::config_store(&path).unwrap();
    assert_eq!(store.ensure_envelope_key().unwrap(), second);
    assert_eq!(
        store.envelope_keys().verify(&first, b"old item", &old),
        Err(Refused::Revoked),
        "revocation survives restart"
    );
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn restore_and_import_never_revive_a_revoked_key() {
    let store = ConfigStore::memory_with("server: {port: 8700}\n");
    let first = store.ensure_envelope_key().unwrap();
    let (_, old) = sign(&store, b"item");
    let second = store.rotate_envelope_key().unwrap();
    store.revoke_envelope_key(&first).unwrap();
    store
        .import_yaml("server: {port: 8701}\n", "test", "second revision")
        .unwrap();
    let after = states(&store);
    let check = |how: &str| {
        assert_eq!(states(&store), after, "{how} changed the key ring");
        assert_eq!(
            store.envelope_keys().verify(&first, b"item", &old),
            Err(Refused::Revoked),
            "{how}"
        );
        assert_eq!(sign(&store, b"item").0, second, "{how}");
    };
    store
        .replace(store.state_at(1).unwrap(), "test", "restore revision 1", false)
        .unwrap();
    check("history restore");
    store.import_yaml("server: {port: 8702}\n", "test", "import").unwrap();
    check("YAML import");
    let (rev, all) = store.snapshot().unwrap();
    let export = resources::export_json(rev, &all, true).to_string();
    store
        .replace(resources::import_json(&export).unwrap(), "test", "import", false)
        .unwrap();
    check("JSON import");
}

#[test]
fn no_candidate_moves_a_key_back_or_drops_it() {
    let store = ConfigStore::memory();
    let first = store.ensure_envelope_key().unwrap();
    let second = store.rotate_envelope_key().unwrap();
    store.revoke_envelope_key(&first).unwrap();
    let after = states(&store);
    let secret = "ab".repeat(32);
    for (sql, why) in [
        (
            format!(
                "UPDATE credential_meta SET value = '{{\"state\":\"verify\",\"created\":\"x\",\"secret\":\"{secret}\"}}' \
                 WHERE name = 'envelope-key/{first}'"
            ),
            "cannot return",
        ),
        (
            format!("DELETE FROM credential_meta WHERE name = 'envelope-key/{second}'"),
            "would disappear",
        ),
    ] {
        let error = store.fixture_execute(&sql).unwrap_err();
        assert!(error.to_string().contains(why), "{error}");
        assert_eq!(states(&store), after);
    }
    let row =
        |state: &str, secret: &str| format!("{{\"state\":\"{state}\",\"created\":\"x\",\"secret\":\"{secret}\"}}");
    for (sql, why) in [
        (
            format!(
                "UPDATE credential_meta SET value = '{}' WHERE name = 'envelope-key/{second}'",
                row("active", &secret)
            ),
            "cannot change its secret",
        ),
        (
            format!(
                "UPDATE credential_meta SET value = '{}' WHERE name = 'envelope-key/{first}'",
                row("revoked", &secret)
            ),
            "does not match its state",
        ),
        (
            format!(
                "UPDATE credential_meta SET value = '{}' WHERE name = 'envelope-key/{second}'",
                row("active", "ab")
            ),
            "does not match its state",
        ),
        (
            format!(
                "INSERT INTO credential_meta VALUES ('envelope-key/0000000f', '{}')",
                row("bogus", &secret)
            ),
            "unreadable row",
        ),
        (
            format!(
                "INSERT INTO credential_meta VALUES ('envelope-key/0000000g', '{}')",
                row("verify", &secret)
            ),
            "invalid kid",
        ),
        (
            format!(
                "INSERT INTO credential_meta VALUES ('envelope-key/00000000', '{}')",
                row("active", &secret)
            ),
            "more than one active",
        ),
    ] {
        let error = format!("{:#}", store.fixture_execute(&sql).unwrap_err());
        assert!(error.contains(why), "{error}");
        assert!(!error.contains(&secret), "{error}");
        assert_eq!(states(&store), after);
    }
}

#[test]
fn a_stale_candidate_cannot_undo_a_revocation() {
    let store = ConfigStore::memory();
    let first = store.ensure_envelope_key().unwrap();
    store.rotate_envelope_key().unwrap();
    let stale = store.current().as_ref().clone();
    store.revoke_envelope_key(&first).unwrap();
    let after = states(&store);
    let mut pending = store.writer().unwrap();
    let error = store.accept(stale.clone(), &mut pending).unwrap_err();
    assert!(error.to_string().contains("cannot return"), "{error}");
    let error = store.retain_grants(stale, &[], &mut pending).unwrap_err();
    assert!(error.to_string().contains("cannot return"), "{error}");
    assert!(pending.is_none());
    drop(pending);
    assert_eq!(states(&store), after);
}

#[test]
fn secrets_stay_out_of_logs_exports_reads_and_history() {
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer({
            let captured = captured.clone();
            move || captured.clone()
        })
        .finish();
    let store = ConfigStore::memory_with("server: {port: 8700}\n");
    let (first, second) = tracing::subscriber::with_default(subscriber, || {
        let first = store.ensure_envelope_key().unwrap();
        let second = store.rotate_envelope_key().unwrap();
        tracing::info!(ring = ?store.envelope_keys(), "ring");
        (first, second)
    });
    let ring = store.envelope_keys();
    let secrets: Vec<String> = [&first, &second]
        .into_iter()
        .map(|kid| test_secret_hex(&ring, kid).unwrap())
        .collect();
    let logs = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains(&second), "the kid is logged: {logs}");
    let (rev, all) = store.snapshot().unwrap();
    let revisions = serde_json::to_string(&store.revisions(100, None).unwrap()).unwrap();
    let changes = serde_json::to_string(&store.revision(rev).unwrap().unwrap().1).unwrap();
    let listed: Vec<String> = states(&store)
        .iter()
        .map(|(kid, state)| format!("{kid:?}: {state:?}"))
        .collect();
    assert_eq!(
        format!("{ring:?}"),
        format!("{{{}}}", listed.join(", ")),
        "Debug shows kids and states only"
    );
    let shown = [
        ("logs", logs),
        ("export", resources::export_json(rev, &all, true).to_string()),
        ("read", resources::document_for_read(&all).to_string()),
        ("config text", store.load().unwrap().text),
        ("revisions", revisions),
        ("changes", changes),
    ];
    for (what, text) in shown {
        for secret in &secrets {
            assert!(!text.contains(secret.as_str()), "{what} shows a secret");
            assert!(!text.contains(&secret.to_uppercase()), "{what} shows a secret");
        }
    }
}

#[test]
fn a_second_owner_is_refused_and_the_ring_stays() {
    let (dir, path) = database("owner");
    let store = crate::testing::config_store(&path).unwrap();
    let kid = store.ensure_envelope_key().unwrap();
    let error = crate::testing::config_store(&path)
        .err()
        .expect("a second owner of the same database");
    assert!(error.to_string().contains("already owned"), "{error}");
    assert_eq!(states(&store), [(kid, KeyState::Active)]);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_retained_candidate_is_the_base_after_failed_publication() {
    let (dir, path) = database("retained");
    let store = crate::testing::config_store(&path).unwrap();
    store.ensure_envelope_key().unwrap();
    std::fs::rename(&path, dir.join("old.db")).unwrap();
    std::fs::create_dir(&path).unwrap();
    store.rotate_envelope_key().unwrap_err();
    let mut pending = store.grant_writer().unwrap();
    let retained = pending.as_ref().expect("the rotation is retained").state.clone();
    assert_eq!(retained.envelope_keys.states().count(), 2);
    let error = store
        .accept(store.current().as_ref().clone(), &mut pending)
        .unwrap_err()
        .to_string();
    assert!(error.contains("would disappear") || error.contains("cannot return"), "{error}");
    drop(pending);
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(dir.join("old.db"), &path).unwrap();
    store.ensure_envelope_key().unwrap();
    assert_eq!(states(&store).len(), 2, "the retained rotation is published");
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}
