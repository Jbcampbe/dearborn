//! PAT encryption at rest (T-102).
//!
//! Per-project GitHub PATs are encrypted with **AES-256-GCM** before they touch
//! the database (`project.pat_encrypted` BLOB) and are only ever decrypted on the
//! crate-internal path that T-103's `git clone`/`git fetch` uses. A PAT never
//! appears in an API response or a log line.
//!
//! ## Key derivation
//!
//! The 256-bit AES key is derived from the `DEERBORN_MASTER_KEY` env material by
//! **SHA-256**: `key = SHA-256(DEERBORN_MASTER_KEY_bytes)`. This accepts master
//! key material of any length/format and deterministically produces exactly the
//! 32 bytes AES-256 requires. The only invalid input is empty material, which is
//! already rejected by config loading and is re-checked here so key derivation
//! fails fast at boot rather than at first encryption. Rotating
//! `DEERBORN_MASTER_KEY` changes the derived key, so PATs encrypted under the old
//! value stop decrypting (they must be re-entered) — an intentional, safe
//! failure (a wrong/rotated key yields a GCM auth-tag error, never plaintext).
//!
//! ## Storage layout
//!
//! Each encryption generates a fresh random **96-bit (12-byte) nonce**. The value
//! stored in `pat_encrypted` is `nonce || ciphertext` (the nonce prepended to the
//! AES-GCM ciphertext, which already includes the 128-bit auth tag). Decryption
//! splits the first 12 bytes back off as the nonce.

use std::fmt;

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Length of the AES-GCM nonce we generate and prepend, in bytes (96 bits).
const NONCE_LEN: usize = 12;

/// Errors from key derivation or PAT encrypt/decrypt.
#[derive(Debug, Error)]
pub enum CryptoError {
    /// `DEERBORN_MASTER_KEY` was empty — cannot derive a key.
    #[error("master key material must not be empty")]
    EmptyKeyMaterial,
    /// AES-GCM encryption failed (should not happen for valid inputs).
    #[error("encryption failed")]
    Encrypt,
    /// Ciphertext was too short to contain a nonce.
    #[error("stored ciphertext is malformed (too short)")]
    Malformed,
    /// AES-GCM decryption/authentication failed — wrong key or tampered bytes.
    #[error("decryption failed (wrong key or corrupt ciphertext)")]
    Decrypt,
}

/// A 256-bit AES key derived from `DEERBORN_MASTER_KEY`.
///
/// Deliberately does **not** derive `Debug`/`Serialize`: the key bytes must never
/// be logged or serialised. The manual [`fmt::Debug`] impl redacts them.
#[derive(Clone)]
pub struct MasterKey([u8; 32]);

impl fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MasterKey(<redacted>)")
    }
}

impl MasterKey {
    /// Derive the AES-256 key from raw `DEERBORN_MASTER_KEY` material via SHA-256.
    ///
    /// Fails only on empty material, so this doubles as the boot-time validation
    /// that the configured key can form a valid 256-bit key (see [`crate::main`]).
    pub fn derive(material: &str) -> Result<MasterKey, CryptoError> {
        if material.is_empty() {
            return Err(CryptoError::EmptyKeyMaterial);
        }
        let digest = Sha256::digest(material.as_bytes());
        let mut key = [0u8; 32];
        key.copy_from_slice(&digest);
        Ok(MasterKey(key))
    }

    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.0))
    }

    /// Encrypt `plaintext`, returning `nonce || ciphertext` for storage.
    ///
    /// A fresh random 96-bit nonce is generated per call, so encrypting the same
    /// PAT twice yields different bytes.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let cipher = self.cipher();
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|_| CryptoError::Encrypt)?;
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(nonce.as_slice());
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt a `nonce || ciphertext` blob produced by [`encrypt`](Self::encrypt).
    ///
    /// Returns [`CryptoError::Decrypt`] on any authentication failure (wrong key,
    /// tampered bytes) — never partial/garbage plaintext.
    pub fn decrypt(&self, blob: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if blob.len() < NONCE_LEN {
            return Err(CryptoError::Malformed);
        }
        let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);
        self.cipher()
            .decrypt(nonce, ciphertext)
            .map_err(|_| CryptoError::Decrypt)
    }

    /// Encrypt a PAT string to storage bytes. Convenience over [`encrypt`].
    pub fn encrypt_pat(&self, pat: &str) -> Result<Vec<u8>, CryptoError> {
        self.encrypt(pat.as_bytes())
    }

    /// Decrypt storage bytes back to a PAT string.
    pub fn decrypt_pat(&self, blob: &[u8]) -> Result<String, CryptoError> {
        let bytes = self.decrypt(blob)?;
        String::from_utf8(bytes).map_err(|_| CryptoError::Decrypt)
    }
}

/// A secret string (e.g. a PAT) whose `Debug` is redacted so it can never leak
/// into a log line, even if a request DTO containing it is `Debug`-formatted.
#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    /// The plaintext secret. Named `expose` to make call-sites conspicuous.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_encrypt_then_decrypt() {
        let key = MasterKey::derive("some-master-key-material").unwrap();
        let pat = "ghp_exampleTokenABC123";
        let blob = key.encrypt_pat(pat).unwrap();
        assert_eq!(key.decrypt_pat(&blob).unwrap(), pat);
    }

    #[test]
    fn nonce_is_random_so_ciphertext_differs_each_time() {
        let key = MasterKey::derive("k").unwrap();
        let a = key.encrypt_pat("same").unwrap();
        let b = key.encrypt_pat("same").unwrap();
        assert_ne!(a, b, "fresh nonce must make ciphertexts differ");
        // ...yet both decrypt back to the original.
        assert_eq!(key.decrypt_pat(&a).unwrap(), "same");
        assert_eq!(key.decrypt_pat(&b).unwrap(), "same");
    }

    #[test]
    fn stored_bytes_are_ciphertext_not_plaintext() {
        let key = MasterKey::derive("k").unwrap();
        let pat = "ghp_secretPlaintext";
        let blob = key.encrypt_pat(pat).unwrap();
        assert!(!blob.is_empty());
        // The plaintext must not appear anywhere in the stored bytes.
        assert!(
            !blob.windows(pat.len()).any(|w| w == pat.as_bytes()),
            "plaintext PAT must not appear in stored ciphertext"
        );
        // Layout: 12-byte nonce + ciphertext + 16-byte GCM tag.
        assert!(blob.len() >= NONCE_LEN + pat.len() + 16);
    }

    #[test]
    fn wrong_key_fails_safely() {
        let good = MasterKey::derive("correct-key").unwrap();
        let bad = MasterKey::derive("different-key").unwrap();
        let blob = good.encrypt_pat("ghp_topSecret").unwrap();
        // Decrypting with the wrong derived key is an error, never garbage/plaintext.
        assert!(matches!(bad.decrypt(&blob), Err(CryptoError::Decrypt)));
    }

    #[test]
    fn malformed_material_and_ciphertext_fail_safely() {
        // Empty master key material cannot derive a key.
        assert!(matches!(
            MasterKey::derive(""),
            Err(CryptoError::EmptyKeyMaterial)
        ));
        // A truncated blob (shorter than the nonce) is rejected as malformed.
        let key = MasterKey::derive("k").unwrap();
        assert!(matches!(key.decrypt(&[0u8; 4]), Err(CryptoError::Malformed)));
        // A tampered ciphertext (valid length, wrong bytes) fails the auth check.
        let mut blob = key.encrypt_pat("hello").unwrap();
        let last = blob.len() - 1;
        blob[last] ^= 0xff;
        assert!(matches!(key.decrypt(&blob), Err(CryptoError::Decrypt)));
    }

    #[test]
    fn secret_debug_is_redacted() {
        let s = Secret("ghp_shouldNeverAppear".to_string());
        assert_eq!(format!("{s:?}"), "Secret(<redacted>)");
        assert!(!format!("{s:?}").contains("ghp_"));
    }

    #[test]
    fn master_key_debug_is_redacted() {
        let key = MasterKey::derive("super-secret").unwrap();
        assert_eq!(format!("{key:?}"), "MasterKey(<redacted>)");
    }
}
