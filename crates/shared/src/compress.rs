//! Transparent zstd compression for end-to-end payloads (provider history,
//! file-transfer chunks, fleet results, …).
//!
//! Compression happens INSIDE the encrypted envelope — compress *then* encrypt,
//! since ciphertext is incompressible — and is gated by a size threshold so
//! small commands aren't touched. The result is **self-describing**: a payload
//! is either a raw JSON byte string or a zstd frame, distinguished on read by
//! the zstd magic bytes. JSON never starts with those bytes, so detection has no
//! false positives, and a reader transparently accepts uncompressed payloads
//! (small messages, or peers that don't compress). Pure-Rust via `ruzstd`.

use ruzstd::decoding::StreamingDecoder;
use ruzstd::encoding::{compress_to_vec, CompressionLevel};
use std::io::Read;

/// zstd frame magic, little-endian `0xFD2FB528`.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Payloads below this size aren't worth compressing (frame overhead + CPU).
const COMPRESS_THRESHOLD: usize = 512;

/// Whether `data` looks like a zstd frame (i.e. was produced by
/// [`maybe_compress`]).
fn is_zstd(data: &[u8]) -> bool {
    data.len() >= 4 && data[..4] == ZSTD_MAGIC
}

/// Compress `data` when it's large enough AND compression actually shrinks it;
/// otherwise return it unchanged. Output is self-describing (zstd frame or raw).
pub fn maybe_compress(data: &[u8]) -> Vec<u8> {
    if data.len() < COMPRESS_THRESHOLD {
        return data.to_vec();
    }
    let compressed = compress_to_vec(data, CompressionLevel::Fastest);
    if compressed.len() < data.len() {
        compressed
    } else {
        data.to_vec()
    }
}

/// Inverse of [`maybe_compress`]: decompress a zstd frame, or pass raw bytes
/// through unchanged.
pub fn maybe_decompress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    if !is_zstd(data) {
        return Ok(data.to_vec());
    }
    let mut decoder = StreamingDecoder::new(data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_payload_is_left_raw() {
        let data = b"{\"cmd\":\"get_info\"}";
        let out = maybe_compress(data);
        assert_eq!(out, data, "below threshold → unchanged");
        assert!(!is_zstd(&out));
        // And it round-trips through decompress untouched.
        assert_eq!(maybe_decompress(&out).unwrap(), data);
    }

    #[test]
    fn large_compressible_payload_round_trips_and_shrinks() {
        // Highly compressible (repetitive JSON-ish text), well over threshold.
        let data = "{\"role\":\"assistant\",\"text\":\"hello fleet \"}"
            .repeat(500)
            .into_bytes();
        let compressed = maybe_compress(&data);
        assert!(is_zstd(&compressed), "should be a zstd frame");
        assert!(compressed.len() < data.len() / 2, "should shrink substantially");
        assert_eq!(maybe_decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn incompressible_payload_stays_raw() {
        // High-entropy bytes (xorshift64) over the threshold: zstd can't shrink
        // them, so we keep the raw form (no wasted frame overhead).
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let data: Vec<u8> = (0..4096)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect();
        let out = maybe_compress(&data);
        assert!(!is_zstd(&out), "incompressible → kept raw");
        assert_eq!(out, data);
        assert_eq!(maybe_decompress(&out).unwrap(), data);
    }

    #[test]
    fn raw_json_passes_through_decompress() {
        // A reader must accept an uncompressed payload (legacy / small peer).
        let json = b"[1,2,3]";
        assert_eq!(maybe_decompress(json).unwrap(), json);
    }
}
