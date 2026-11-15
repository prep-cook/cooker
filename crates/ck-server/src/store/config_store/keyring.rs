//! The envelope key ring: the HMAC-SHA256 keys that authenticate the opaque items cooker
//! hands to clients (`ck1.` envelopes, D2-PLAN §2.4.1–2.4.2).
//!
//! The ring is owner state beside the config resources, like credential documents. It is
//! stored as `envelope-key/<kid>` rows of the store's `credential_meta` table and is
//! never part of a config document, revision, change, read or export, so a config-history
//! restore or an import cannot touch it. Key states only advance (active, verify,
//! revoked); [`KeyRing::follows`] refuses any candidate state that moves one back.
//! Secrets have no `Debug`, `Display` or `Serialize` form outside the stored row, and a
//! revoked key's secret is erased.

use subtle::ConstantTimeEq as _;

use super::*;

const ROW_PREFIX: &str = "envelope-key/";
const SECRET_BYTES: usize = 32;

/// Where a key is in its life. The order is the only allowed direction of change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyState {
    /// Signs new envelopes and verifies; exactly one key is active once the owner starts.
    Active,
    /// Verifies envelopes clients already hold.
    Verify,
    /// Rejects its envelopes; its secret is gone.
    Revoked,
}

#[derive(Clone, PartialEq, Eq)]
struct Secret([u8; SECRET_BYTES]);

#[derive(Clone, PartialEq, Eq)]
struct Key {
    state: KeyState,
    created: String,
    /// `None` exactly when revoked.
    secret: Option<Secret>,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct KeyRing {
    keys: BTreeMap<String, Key>,
}

impl std::fmt::Debug for KeyRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.keys.iter().map(|(kid, key)| (kid, key.state)))
            .finish()
    }
}

/// Why an envelope did not verify. Request ingress answers each with 400
/// `invalid_request_error` and sends nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    Unknown,
    Revoked,
    Mismatch,
}

/// The active key, lent for signing; the secret never leaves this type.
pub struct Signer<'a> {
    kid: &'a str,
    secret: &'a Secret,
}

impl Signer<'_> {
    pub fn kid(&self) -> &str {
        self.kid
    }

    /// HMAC-SHA256 of the complete authenticated message, which names the kid itself.
    pub fn mac(&self, message: &[u8]) -> [u8; 32] {
        hmac_sha256(&self.secret.0, message)
    }
}

/// The stored form of one key, the value of its `credential_meta` row.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    state: KeyState,
    created: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    secret: Option<String>,
}

impl KeyRing {
    /// The active key, if one exists.
    pub fn signer(&self) -> Option<Signer<'_>> {
        self.keys.iter().find_map(|(kid, key)| match (&key.state, &key.secret) {
            (KeyState::Active, Some(secret)) => Some(Signer { kid, secret }),
            _ => None,
        })
    }

    /// Checks `tag` against the key `kid` in constant time.
    pub fn verify(&self, kid: &str, message: &[u8], tag: &[u8]) -> Result<(), Refused> {
        let key = self.keys.get(kid).ok_or(Refused::Unknown)?;
        // Revoked is exactly the state without a secret (`decode` and `revoke`).
        let secret = key.secret.as_ref().ok_or(Refused::Revoked)?;
        if bool::from(hmac_sha256(&secret.0, message)[..].ct_eq(tag)) {
            Ok(())
        } else {
            Err(Refused::Mismatch)
        }
    }

    /// Every kid and its state, in kid order.
    pub fn states(&self) -> impl Iterator<Item = (&str, KeyState)> {
        self.keys.iter().map(|(kid, key)| (kid.as_str(), key.state))
    }

    /// Adds a new active key and moves the previous active key to `verify`.
    fn generate(&mut self) -> String {
        let mut kid = [0u8; 4];
        let kid = loop {
            getrandom::fill(&mut kid).expect("system randomness");
            let kid = hex(&kid);
            if !self.keys.contains_key(&kid) {
                break kid;
            }
        };
        let mut secret = [0u8; SECRET_BYTES];
        getrandom::fill(&mut secret).expect("system randomness");
        for key in self.keys.values_mut().filter(|key| key.state == KeyState::Active) {
            key.state = KeyState::Verify;
        }
        self.keys.insert(
            kid.clone(),
            Key {
                state: KeyState::Active,
                created: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                secret: Some(Secret(secret)),
            },
        );
        kid
    }

    /// Revokes a verifying key and erases its secret. The active key must be rotated
    /// out first, so a key always signs.
    fn revoke(&mut self, kid: &str) -> anyhow::Result<()> {
        let key = self
            .keys
            .get_mut(kid)
            .ok_or_else(|| anyhow::anyhow!("no envelope key {kid}"))?;
        anyhow::ensure!(
            key.state != KeyState::Active,
            "envelope key {kid} signs new envelopes; rotate before revoking it"
        );
        key.state = KeyState::Revoked;
        key.secret = None;
        Ok(())
    }

    /// Whether `self` may replace `previous`: no key disappears or changes its secret, and
    /// no state moves back (a revoked kid never verifies again).
    pub(super) fn follows(&self, previous: &KeyRing) -> anyhow::Result<()> {
        for (kid, before) in &previous.keys {
            let after = self
                .keys
                .get(kid)
                .ok_or_else(|| anyhow::anyhow!("envelope key {kid} would disappear"))?;
            anyhow::ensure!(
                after.state >= before.state,
                "envelope key {kid} cannot return from {:?} to {:?}",
                before.state,
                after.state
            );
            anyhow::ensure!(
                after.secret.is_none() || after.secret == before.secret,
                "envelope key {kid} cannot change its secret"
            );
        }
        Ok(())
    }

    /// Splits the key rows out of the `credential_meta` rows.
    pub(super) fn decode(rows: BTreeMap<String, String>) -> anyhow::Result<(Self, BTreeMap<String, String>)> {
        let mut ring = Self::default();
        let mut other = BTreeMap::new();
        for (name, value) in rows {
            let Some(kid) = name.strip_prefix(ROW_PREFIX) else {
                other.insert(name, value);
                continue;
            };
            // The parse error is not shown: it could quote the stored secret.
            let row: Row =
                serde_json::from_str(&value).map_err(|_| anyhow::anyhow!("envelope key {kid:?}: unreadable row"))?;
            let secret = match (row.state, row.secret.as_deref().map(unhex)) {
                (KeyState::Revoked, None) => None,
                (KeyState::Active | KeyState::Verify, Some(Some(secret))) => Some(Secret(secret)),
                _ => anyhow::bail!("envelope key {kid:?}: secret does not match its state"),
            };
            anyhow::ensure!(valid_kid(kid), "envelope key {kid:?}: invalid kid");
            let key = Key {
                state: row.state,
                created: row.created,
                secret,
            };
            ring.keys.insert(kid.to_owned(), key);
        }
        anyhow::ensure!(
            ring.states().filter(|(_, state)| *state == KeyState::Active).count() <= 1,
            "more than one active envelope key"
        );
        Ok((ring, other))
    }

    /// The `credential_meta` rows of the ring.
    pub(super) fn rows(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.keys.iter().map(|(kid, key)| {
            let row = Row {
                state: key.state,
                created: key.created.clone(),
                secret: key.secret.as_ref().map(|secret| hex(&secret.0)),
            };
            (
                format!("{ROW_PREFIX}{kid}"),
                serde_json::to_string(&row).expect("JSON row"),
            )
        })
    }
}

impl ConfigStore {
    /// The accepted key ring.
    pub fn envelope_keys(&self) -> Arc<KeyRing> {
        self.current().envelope_keys.clone()
    }

    /// Startup, before the listener opens: makes sure a key signs, committing a new one
    /// when none does. Returns the active kid.
    pub fn ensure_envelope_key(&self) -> anyhow::Result<String> {
        let mut pending = self.writer()?;
        if let Some(signer) = self.current().envelope_keys.signer() {
            return Ok(signer.kid().to_owned());
        }
        let kid = self.change_keys(&mut pending, |ring| Ok(ring.generate()))?;
        tracing::info!(kid = %kid, "generated the envelope signing key");
        Ok(kid)
    }

    /// Commits a new active key; the previous one keeps verifying. Returns the new kid.
    pub fn rotate_envelope_key(&self) -> anyhow::Result<String> {
        let mut pending = self.writer()?;
        let kid = self.change_keys(&mut pending, |ring| Ok(ring.generate()))?;
        tracing::info!(kid = %kid, "rotated the envelope signing key");
        Ok(kid)
    }

    /// Commits the revocation of a verifying key; its envelopes stop verifying.
    pub fn revoke_envelope_key(&self, kid: &str) -> anyhow::Result<()> {
        let mut pending = self.writer()?;
        self.change_keys(&mut pending, |ring| ring.revoke(kid))?;
        tracing::info!(kid = %kid, "revoked an envelope key");
        Ok(())
    }

    fn change_keys<T>(
        &self,
        pending: &mut Option<Pending>,
        change: impl FnOnce(&mut KeyRing) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let mut candidate = self.current().as_ref().clone();
        let value = change(Arc::make_mut(&mut candidate.envelope_keys))?;
        self.accept(candidate, pending)?;
        Ok(value)
    }

    /// Every accepted candidate passes here: its ring must follow the one it replaces.
    pub(super) fn check_keys(&self, candidate: &State, pending: &Option<Pending>) -> anyhow::Result<()> {
        let previous = pending.as_ref().map_or_else(|| self.current(), |p| p.state.clone());
        candidate.envelope_keys.follows(&previous.envelope_keys)
    }
}

fn valid_kid(kid: &str) -> bool {
    kid.len() == 8 && kid.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Option<[u8; SECRET_BYTES]> {
    if text.len() != SECRET_BYTES * 2 || !text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let mut out = [0u8; SECRET_BYTES];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

/// HMAC-SHA256 (RFC 2104) for keys of at most one block, which every ring key is.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    assert!(key.len() <= BLOCK, "HMAC key longer than one block");
    let mut inner_pad = [0x36u8; BLOCK];
    let mut outer_pad = [0x5cu8; BLOCK];
    for (i, byte) in key.iter().enumerate() {
        inner_pad[i] ^= byte;
        outer_pad[i] ^= byte;
    }
    let inner = Sha256::new().chain_update(inner_pad).chain_update(message).finalize();
    Sha256::new()
        .chain_update(outer_pad)
        .chain_update(inner)
        .finalize()
        .into()
}

#[cfg(test)]
pub(super) fn test_hmac(key: &[u8], message: &[u8]) -> [u8; 32] {
    hmac_sha256(key, message)
}

#[cfg(test)]
pub(super) fn test_secret_hex(ring: &KeyRing, kid: &str) -> Option<String> {
    ring.keys.get(kid)?.secret.as_ref().map(|secret| hex(&secret.0))
}
