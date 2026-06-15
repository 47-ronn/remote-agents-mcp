//! Optional end-to-end encryption for command payloads (AES-GCM-256).
//!
//! The relay is a blind passthrough, so MCP and agent can share a secret and
//! encrypt payloads such that the relay never sees plaintext. The 256-bit key
//! is derived from a shared passphrase via SHA-256. Each message uses a fresh
//! random 96-bit nonce, prepended to the ciphertext and base64-encoded.
//!
//! Wire format (after base64-decode): `nonce(12) || ciphertext || tag(16)`.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use rand::RngCore;
use sha2::{Digest, Sha256};

const NONCE_LEN: usize = 12;

/// A symmetric cipher derived from a shared passphrase.
#[derive(Clone)]
pub struct Cipher {
    key: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("encryption failed")]
    Encrypt,
    #[error("decryption failed (wrong key or corrupt data)")]
    Decrypt,
    #[error("invalid ciphertext encoding")]
    Encoding,
    #[error("ciphertext too short")]
    TooShort,
}

impl Cipher {
    /// Derive the transport cipher used for end-to-end encryption.
    ///
    /// By default the key is derived from the room token (which both the MCP
    /// server and the agent already share), namespaced so the encryption key is
    /// never literally the auth token. An explicit `override_key` (e.g. an agent
    /// `security.encryption_key`) takes precedence for stronger separation.
    /// Both ends MUST use the same derivation to interoperate.
    pub fn for_transport(token: &str, override_key: Option<&str>) -> Self {
        match override_key {
            Some(k) if !k.is_empty() => Cipher::from_passphrase(k),
            _ => Cipher::from_passphrase(&format!("remote-agents/v1:{}", token)),
        }
    }

    /// Derive a key from a shared passphrase (SHA-256).
    pub fn from_passphrase(passphrase: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(passphrase.as_bytes());
        let digest = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&digest);
        Self { key }
    }

    fn aead(&self) -> Aes256Gcm {
        Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.key))
    }

    /// Encrypt plaintext, returning a base64 string (`nonce || ciphertext`).
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<String, CryptoError> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .aead()
            .encrypt(nonce, plaintext)
            .map_err(|_| CryptoError::Encrypt)?;

        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(B64.encode(out))
    }

    /// Decrypt a base64 string produced by [`Cipher::encrypt`].
    pub fn decrypt(&self, encoded: &str) -> Result<Vec<u8>, CryptoError> {
        let raw = B64.decode(encoded).map_err(|_| CryptoError::Encoding)?;
        if raw.len() <= NONCE_LEN {
            return Err(CryptoError::TooShort);
        }
        let (nonce_bytes, ciphertext) = raw.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);
        self.aead()
            .decrypt(nonce, ciphertext)
            .map_err(|_| CryptoError::Decrypt)
    }

    /// Convenience: encrypt a string.
    pub fn encrypt_str(&self, s: &str) -> Result<String, CryptoError> {
        self.encrypt(s.as_bytes())
    }

    /// Convenience: decrypt to a UTF-8 string.
    pub fn decrypt_str(&self, encoded: &str) -> Result<String, CryptoError> {
        let bytes = self.decrypt(encoded)?;
        String::from_utf8(bytes).map_err(|_| CryptoError::Decrypt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let c = Cipher::from_passphrase("correct horse battery staple");
        let ct = c.encrypt_str("hello, fleet").unwrap();
        assert_eq!(c.decrypt_str(&ct).unwrap(), "hello, fleet");
    }

    #[test]
    fn distinct_nonces_produce_distinct_ciphertext() {
        let c = Cipher::from_passphrase("k");
        let a = c.encrypt_str("same").unwrap();
        let b = c.encrypt_str("same").unwrap();
        assert_ne!(a, b, "nonce reuse would make these equal");
        assert_eq!(c.decrypt_str(&a).unwrap(), "same");
        assert_eq!(c.decrypt_str(&b).unwrap(), "same");
    }

    #[test]
    fn wrong_key_fails() {
        let a = Cipher::from_passphrase("key-a");
        let b = Cipher::from_passphrase("key-b");
        let ct = a.encrypt_str("secret").unwrap();
        assert!(b.decrypt_str(&ct).is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let c = Cipher::from_passphrase("k");
        let mut ct = c.encrypt_str("secret").unwrap();
        // Flip a character in the middle of the base64 to corrupt the tag.
        let mid = ct.len() / 2;
        let bytes = unsafe { ct.as_bytes_mut() };
        bytes[mid] = if bytes[mid] == b'A' { b'B' } else { b'A' };
        assert!(c.decrypt_str(&ct).is_err());
    }
}
