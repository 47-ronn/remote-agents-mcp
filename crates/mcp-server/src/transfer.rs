//! Host↔host file transfer: the progress registry and the receiver-side write.
//!
//! The bytes ride the ordinary peer command path (`relay_controller`), which
//! prefers the direct UDP channel and falls back to the relay — so there is no
//! bespoke UDP file protocol here. The *sender* orchestration lives in
//! `relay_controller` (it needs the connection's send primitives); this module
//! holds the shared progress store, the receiver write, and the checksum.

use crate::config::SecurityConfig;
use crate::safety;
use anyhow::{bail, Context, Result};
use base64::Engine;
use remote_agents_shared::{TransferState, TransferStatus};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::RwLock;

/// In-memory registry of host↔host transfers this node initiated, polled via
/// `TransferGet`. Cheap to clone (shared `Arc` in `AgentState`).
#[derive(Default)]
pub struct TransferStore {
    inner: RwLock<HashMap<String, TransferStatus>>,
}

impl TransferStore {
    /// Register a new transfer of `total` bytes in the `Queued` state.
    pub fn start(&self, id: &str, total: u64) {
        self.inner.write().unwrap().insert(
            id.to_string(),
            TransferStatus {
                id: id.to_string(),
                state: TransferState::Queued,
                bytes: 0,
                total,
                error: None,
            },
        );
    }

    /// Record progress (and flip to `Running`).
    pub fn progress(&self, id: &str, bytes: u64) {
        if let Some(t) = self.inner.write().unwrap().get_mut(id) {
            t.state = TransferState::Running;
            t.bytes = bytes;
        }
    }

    /// Mark the transfer complete.
    pub fn done(&self, id: &str) {
        if let Some(t) = self.inner.write().unwrap().get_mut(id) {
            t.state = TransferState::Done;
            t.bytes = t.total;
        }
    }

    /// Mark the transfer failed with a reason.
    pub fn fail(&self, id: &str, error: impl Into<String>) {
        if let Some(t) = self.inner.write().unwrap().get_mut(id) {
            t.state = TransferState::Failed;
            t.error = Some(error.into());
        }
    }

    /// Look up a transfer's status.
    pub fn get(&self, id: &str) -> Option<TransferStatus> {
        self.inner.read().unwrap().get(id).cloned()
    }
}

/// Write one received slice to `dest_path` at `offset` (the destination side of
/// `SendFileTo`). At `offset == 0` the file is created/truncated; later slices
/// seek and overwrite in place. On `eof`, the whole file is hashed and compared
/// to `expected_sha256`. Path access is policy-gated; the caller enforces write
/// mode. Blocking — call from `spawn_blocking`.
pub fn receive_chunk(
    dest_path: &str,
    offset: u64,
    bytes_b64: &str,
    eof: bool,
    expected_sha256: Option<&str>,
    sec: &SecurityConfig,
) -> Result<()> {
    safety::check_path_allowed(dest_path, sec)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(bytes_b64)
        .context("decode transfer chunk")?;

    if sec.max_transfer_size > 0 && offset + bytes.len() as u64 > sec.max_transfer_size {
        bail!("transfer exceeds limit of {} bytes", sec.max_transfer_size);
    }

    let mut f = if offset == 0 {
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dest_path)
            .with_context(|| format!("create {dest_path}"))?
    } else {
        std::fs::OpenOptions::new()
            .write(true)
            .open(dest_path)
            .with_context(|| format!("open {dest_path}"))?
    };
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(&bytes)?;

    if eof {
        f.flush()?;
        drop(f);
        if let Some(expected) = expected_sha256 {
            let actual = sha256_file(dest_path)?;
            if !actual.eq_ignore_ascii_case(expected) {
                bail!("sha256 mismatch (got {actual}, expected {expected})");
            }
        }
    }
    Ok(())
}

/// Lowercase-hex SHA-256 of a whole file (streamed, constant memory).
pub fn sha256_file(path: &str) -> Result<String> {
    let mut f = std::fs::File::open(path).with_context(|| format!("open {path}"))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex(&hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sec() -> SecurityConfig {
        SecurityConfig::default()
    }

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn store_tracks_lifecycle() {
        let s = TransferStore::default();
        s.start("t1", 100);
        assert_eq!(s.get("t1").unwrap().state, TransferState::Queued);
        s.progress("t1", 40);
        let p = s.get("t1").unwrap();
        assert_eq!(p.state, TransferState::Running);
        assert_eq!(p.bytes, 40);
        s.done("t1");
        let d = s.get("t1").unwrap();
        assert_eq!(d.state, TransferState::Done);
        assert_eq!(d.bytes, 100);
        s.fail("t1", "boom");
        assert_eq!(s.get("t1").unwrap().state, TransferState::Failed);
    }

    #[test]
    fn receive_reassembles_chunks_and_verifies_sha() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        let dp = dest.to_string_lossy().to_string();

        // Binary payload across two chunks.
        let data: Vec<u8> = (0u8..=255).cycle().take(700).collect();
        let sha = {
            let mut h = Sha256::new();
            h.update(&data);
            hex(&h.finalize())
        };

        receive_chunk(&dp, 0, &b64(&data[0..400]), false, None, &sec()).unwrap();
        receive_chunk(&dp, 400, &b64(&data[400..700]), true, Some(&sha), &sec()).unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), data);
    }

    #[test]
    fn receive_rejects_sha_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        let dp = dest.to_string_lossy().to_string();
        let r = receive_chunk(&dp, 0, &b64(b"hello"), true, Some("deadbeef"), &sec());
        assert!(r.is_err());
    }
}
