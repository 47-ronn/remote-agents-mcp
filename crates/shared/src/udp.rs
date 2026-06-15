//! UDP peer-to-peer data channel for direct host communication.
//!
//! The WebSocket relay is used only for signaling (exchanging public endpoints);
//! actual data flows directly between peers over UDP. If UDP hole-punching fails
//! (e.g. symmetric NAT), the system falls back to WebSocket transport.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐   WS (signaling)   ┌─────────────┐
//! │  MCP Server │ ◄──────────────────►│    Relay    │
//! └──────┬──────┘                     └──────┬──────┘
//!        │                                   │
//!        │ UDP (data)                        │ WS
//!        │                                   │
//!        ▼                                   ▼
//! ┌─────────────┐   UDP (data)       ┌─────────────┐
//! │   Agent 1   │ ◄──────────────────►│   Agent 2   │
//! └─────────────┘                     └─────────────┘
//! ```
//!
//! # Hole Punching Protocol
//!
//! 1. Both peers bind a local UDP socket and determine their public endpoint
//!    via STUN or by having the relay echo back their source address.
//! 2. Peers exchange `UdpOffer` messages through the WS relay (signaling).
//! 3. Both peers simultaneously send UDP probe packets to each other's
//!    public endpoint to punch holes in NATs.
//! 4. Once bidirectional connectivity is confirmed, the channel is established.
//! 5. If probing fails after timeout, fall back to WS transport.
//!
//! # Reliable Transport
//!
//! For command/result payloads that need reliability, we implement a simple
//! ACK-based protocol on top of UDP with retransmission.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// A network endpoint (IP + port) for UDP connectivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Endpoint {
    /// IPv4 or IPv6 address as string
    pub addr: std::net::IpAddr,
    /// UDP port
    pub port: u16,
}

impl Endpoint {
    pub fn new(addr: std::net::IpAddr, port: u16) -> Self {
        Self { addr, port }
    }

    pub fn to_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.addr, self.port)
    }
}

/// Build our reflexive (public) UDP endpoint by combining the IP the relay
/// observed for our connection (`reflected`, from a `YourEndpoint` message —
/// only its IP is meaningful; the relay sees our TCP port, not the UDP one)
/// with `local`, the actual port our UDP socket bound. Returns `None` when the
/// relay never reflected an address, so offers fall back to local-only.
///
/// Correct for hosts with a public IP or behind a full-cone NAT — the common
/// case for the remote servers this project manages.
pub fn reflexive_endpoint(reflected: Option<Endpoint>, local: Endpoint) -> Option<Endpoint> {
    reflected.map(|r| Endpoint::new(r.addr, local.port))
}

impl From<SocketAddr> for Endpoint {
    fn from(addr: SocketAddr) -> Self {
        Self {
            addr: addr.ip(),
            port: addr.port(),
        }
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.addr, self.port)
    }
}

/// UDP channel offer sent through WebSocket signaling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpOffer {
    /// Unique channel ID (both peers must use the same ID)
    pub channel_id: String,
    /// Session ID of the peer making the offer
    pub from_session: String,
    /// Target agent or MCP session ID
    pub to_session: String,
    /// Local endpoint (behind NAT, may not be directly reachable)
    pub local_endpoint: Endpoint,
    /// Public endpoint as seen by the relay/STUN (reflexive address)
    pub public_endpoint: Option<Endpoint>,
    /// Random nonce for this offer (used in hole-punching probes)
    pub nonce: [u8; 16],
}

/// UDP channel answer sent in response to an offer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpAnswer {
    /// Channel ID (must match the offer)
    pub channel_id: String,
    /// Session ID of the answering peer
    pub from_session: String,
    /// Local endpoint
    pub local_endpoint: Endpoint,
    /// Public endpoint
    pub public_endpoint: Option<Endpoint>,
    /// Nonce for this answer
    pub nonce: [u8; 16],
    /// Whether the peer accepts UDP (false = force WS fallback)
    pub accepted: bool,
}

/// Result of UDP channel establishment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpChannelResult {
    /// Channel ID
    pub channel_id: String,
    /// Whether UDP channel was successfully established
    pub success: bool,
    /// Reason for failure (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// UDP packet types for the data channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UdpPacketType {
    /// Hole-punching probe (contains nonce)
    Probe = 0x01,
    /// Probe acknowledgment
    ProbeAck = 0x02,
    /// Keepalive ping
    Ping = 0x03,
    /// Keepalive pong
    Pong = 0x04,
    /// Reliable data packet (needs ACK)
    Data = 0x10,
    /// Acknowledgment for data packet
    DataAck = 0x11,
    /// Unreliable data packet (no ACK needed)
    DataUnreliable = 0x12,
    /// One fragment of a reliable message larger than `UDP_MAX_PAYLOAD`
    /// (needs ACK, like `Data`). Payload is prefixed with a [`FragmentHeader`].
    DataFragment = 0x13,
    /// Channel close
    Close = 0xFF,
}

impl TryFrom<u8> for UdpPacketType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Probe),
            0x02 => Ok(Self::ProbeAck),
            0x03 => Ok(Self::Ping),
            0x04 => Ok(Self::Pong),
            0x10 => Ok(Self::Data),
            0x11 => Ok(Self::DataAck),
            0x12 => Ok(Self::DataUnreliable),
            0x13 => Ok(Self::DataFragment),
            0xFF => Ok(Self::Close),
            _ => Err(()),
        }
    }
}

/// Wire format for UDP packets.
///
/// ```text
/// ┌───────────┬───────────┬──────────────┬─────────────────┐
/// │  Type(1)  │  Seq(4)   │  Length(2)   │  Payload(var)   │
/// └───────────┴───────────┴──────────────┴─────────────────┘
/// ```
///
/// All multi-byte integers are big-endian.
pub const UDP_HEADER_SIZE: usize = 7; // type(1) + seq(4) + len(2)
pub const UDP_MAX_PAYLOAD: usize = 1200; // Safe for most MTUs
pub const UDP_MAX_PACKET: usize = UDP_HEADER_SIZE + UDP_MAX_PAYLOAD;

/// Parsed UDP packet header.
#[derive(Debug, Clone, Copy)]
pub struct UdpPacketHeader {
    pub packet_type: UdpPacketType,
    pub sequence: u32,
    pub payload_len: u16,
}

impl UdpPacketHeader {
    /// Parse header from bytes. Returns None if invalid.
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < UDP_HEADER_SIZE {
            return None;
        }
        let packet_type = UdpPacketType::try_from(buf[0]).ok()?;
        let sequence = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        let payload_len = u16::from_be_bytes([buf[5], buf[6]]);
        Some(Self {
            packet_type,
            sequence,
            payload_len,
        })
    }

    /// Write header to buffer. Buffer must be at least UDP_HEADER_SIZE bytes.
    pub fn write(&self, buf: &mut [u8]) {
        buf[0] = self.packet_type as u8;
        buf[1..5].copy_from_slice(&self.sequence.to_be_bytes());
        buf[5..7].copy_from_slice(&self.payload_len.to_be_bytes());
    }
}

/// State of a UDP channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChannelState {
    /// Initial state, no connectivity yet
    #[default]
    Disconnected,
    /// Sent offer, waiting for answer
    Offering,
    /// Received offer, sent answer, probing
    Answering,
    /// Both peers probing for connectivity
    Probing,
    /// UDP channel established
    Connected,
    /// Hole-punching failed, using WS fallback
    Fallback,
    /// Channel closed
    Closed,
}

/// Configuration for UDP channel.
#[derive(Debug, Clone)]
pub struct UdpConfig {
    /// Timeout for hole-punching probes (ms)
    pub probe_timeout_ms: u64,
    /// Interval between probe packets (ms)
    pub probe_interval_ms: u64,
    /// Maximum probe attempts before giving up
    pub max_probe_attempts: u32,
    /// Keepalive interval (ms)
    pub keepalive_interval_ms: u64,
    /// Retransmission timeout for reliable packets (ms)
    pub rto_ms: u64,
    /// Maximum retransmissions before failure
    pub max_retransmissions: u32,
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            probe_timeout_ms: 5000,
            probe_interval_ms: 100,
            max_probe_attempts: 50,
            keepalive_interval_ms: 10000,
            rto_ms: 200,
            max_retransmissions: 5,
        }
    }
}

// ============================================================================
// Fragmentation
//
// Reliable messages whose (encrypted) body exceeds `UDP_MAX_PAYLOAD` are split
// into several `DataFragment` packets. Each fragment's UDP payload begins with
// a fixed [`FragmentHeader`] so the receiver can reassemble them by message id,
// independent of arrival order or duplicate retransmissions.
// ============================================================================

/// Size of the per-fragment sub-header: msg_id(4) + index(2) + count(2).
pub const FRAGMENT_HEADER_SIZE: usize = 8;
/// Max bytes of message body carried by a single fragment.
pub const FRAGMENT_CHUNK_SIZE: usize = UDP_MAX_PAYLOAD - FRAGMENT_HEADER_SIZE;

/// Per-fragment sub-header, prepended to each `DataFragment` payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentHeader {
    /// Identifies the logical message all fragments belong to.
    pub msg_id: u32,
    /// 0-based index of this fragment.
    pub index: u16,
    /// Total number of fragments in the message.
    pub count: u16,
}

impl FragmentHeader {
    /// Parse the sub-header from the start of a fragment payload, returning the
    /// header and the remaining chunk bytes. `None` if the buffer is too short.
    pub fn parse(buf: &[u8]) -> Option<(Self, &[u8])> {
        if buf.len() < FRAGMENT_HEADER_SIZE {
            return None;
        }
        let msg_id = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let index = u16::from_be_bytes([buf[4], buf[5]]);
        let count = u16::from_be_bytes([buf[6], buf[7]]);
        Some((Self { msg_id, index, count }, &buf[FRAGMENT_HEADER_SIZE..]))
    }

    fn write(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.msg_id.to_be_bytes());
        buf[4..6].copy_from_slice(&self.index.to_be_bytes());
        buf[6..8].copy_from_slice(&self.count.to_be_bytes());
    }
}

/// Application-level frame carried over an established UDP data channel.
///
/// Lets a command (with its bulk partition `data`) travel directly to an agent
/// over UDP instead of through the WS relay, and a result travel back. The
/// `payload`/`result` strings are the SAME AES-GCM E2E envelopes used on the WS
/// path ([`Command::encrypt`]/[`CommandResult::encrypt`]), so confidentiality is
/// identical regardless of transport. The UDP path is an optimization; WS is the
/// fallback when no channel is established.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UdpFrame {
    /// A command for the agent. `payload` is an encrypted [`crate::Command`].
    Command {
        request_id: String,
        from_session: String,
        payload: String,
    },
    /// A command result. `result` is an encrypted [`crate::CommandResult`].
    Result { request_id: String, result: String },
    /// A command error (decrypt/exec failure) for `request_id`.
    Error { request_id: String, error: String },
}

impl UdpFrame {
    /// Serialize to JSON bytes for transmission over a UDP channel.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Parse from bytes received over a UDP channel; `None` if malformed.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

/// Split `body` into fragment payloads (each = [`FragmentHeader`] + chunk),
/// chunking at `FRAGMENT_CHUNK_SIZE`. Always returns at least one fragment.
pub fn split_into_fragments(body: &[u8], msg_id: u32) -> Vec<Vec<u8>> {
    let chunks: Vec<&[u8]> = if body.is_empty() {
        vec![&body[0..0]]
    } else {
        body.chunks(FRAGMENT_CHUNK_SIZE).collect()
    };
    let count = chunks.len() as u16;
    chunks
        .into_iter()
        .enumerate()
        .map(|(i, chunk)| {
            let header = FragmentHeader { msg_id, index: i as u16, count };
            let mut out = vec![0u8; FRAGMENT_HEADER_SIZE + chunk.len()];
            header.write(&mut out);
            out[FRAGMENT_HEADER_SIZE..].copy_from_slice(chunk);
            out
        })
        .collect()
}

/// Reassembles `DataFragment` payloads into complete messages. Tolerates
/// out-of-order arrival and duplicate fragments (retransmissions).
#[derive(Debug, Default)]
pub struct FragmentReassembler {
    /// msg_id -> (expected count, index -> chunk)
    partial: std::collections::HashMap<u32, (u16, std::collections::HashMap<u16, Vec<u8>>)>,
}

impl FragmentReassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of messages currently being reassembled (for bookkeeping/tests).
    pub fn pending_len(&self) -> usize {
        self.partial.len()
    }

    /// Feed one fragment payload (sub-header + chunk). Returns the fully
    /// reassembled message body once the final missing fragment arrives,
    /// otherwise `None`. Malformed or inconsistent fragments are ignored.
    pub fn insert(&mut self, payload: &[u8]) -> Option<Vec<u8>> {
        let (header, chunk) = FragmentHeader::parse(payload)?;
        if header.count == 0 || header.index >= header.count {
            return None; // nonsensical framing
        }

        let entry = self
            .partial
            .entry(header.msg_id)
            .or_insert_with(|| (header.count, std::collections::HashMap::new()));
        // A mismatched count for the same id means corruption — reset it.
        if entry.0 != header.count {
            *entry = (header.count, std::collections::HashMap::new());
        }
        entry.1.insert(header.index, chunk.to_vec());

        if entry.1.len() as u16 != header.count {
            return None;
        }

        // All present — concatenate in index order and drop the buffer.
        let (count, parts) = self.partial.remove(&header.msg_id)?;
        let mut body = Vec::new();
        for i in 0..count {
            body.extend_from_slice(parts.get(&i)?);
        }
        Some(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn endpoint_roundtrip() {
        let ep = Endpoint::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 12345);
        let json = serde_json::to_string(&ep).unwrap();
        let parsed: Endpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(ep, parsed);
    }

    #[test]
    fn packet_header_roundtrip() {
        let header = UdpPacketHeader {
            packet_type: UdpPacketType::Data,
            sequence: 0x12345678,
            payload_len: 100,
        };
        let mut buf = [0u8; UDP_HEADER_SIZE];
        header.write(&mut buf);
        let parsed = UdpPacketHeader::parse(&buf).unwrap();
        assert_eq!(parsed.packet_type, UdpPacketType::Data);
        assert_eq!(parsed.sequence, 0x12345678);
        assert_eq!(parsed.payload_len, 100);
    }

    #[test]
    fn packet_type_conversion() {
        assert_eq!(UdpPacketType::try_from(0x01), Ok(UdpPacketType::Probe));
        assert_eq!(UdpPacketType::try_from(0x10), Ok(UdpPacketType::Data));
        assert_eq!(
            UdpPacketType::try_from(0x13),
            Ok(UdpPacketType::DataFragment)
        );
        assert!(UdpPacketType::try_from(0x99).is_err());
    }

    #[test]
    fn fragment_header_roundtrip() {
        let h = FragmentHeader { msg_id: 0xDEADBEEF, index: 3, count: 9 };
        let mut buf = vec![0u8; FRAGMENT_HEADER_SIZE + 2];
        h.write(&mut buf);
        buf[FRAGMENT_HEADER_SIZE..].copy_from_slice(b"hi");
        let (parsed, chunk) = FragmentHeader::parse(&buf).unwrap();
        assert_eq!(parsed, h);
        assert_eq!(chunk, b"hi");
    }

    #[test]
    fn fragment_header_parse_rejects_short_buffer() {
        assert!(FragmentHeader::parse(&[0u8; FRAGMENT_HEADER_SIZE - 1]).is_none());
    }

    #[test]
    fn single_fragment_for_small_body() {
        let frags = split_into_fragments(b"small", 1);
        assert_eq!(frags.len(), 1);
        let (h, chunk) = FragmentHeader::parse(&frags[0]).unwrap();
        assert_eq!(h.count, 1);
        assert_eq!(h.index, 0);
        assert_eq!(chunk, b"small");
    }

    #[test]
    fn split_then_reassemble_roundtrip() {
        // ~3.5 chunks worth of data.
        let body: Vec<u8> = (0..FRAGMENT_CHUNK_SIZE * 3 + 100)
            .map(|i| (i % 251) as u8)
            .collect();
        let frags = split_into_fragments(&body, 42);
        assert_eq!(frags.len(), 4);

        let mut r = FragmentReassembler::new();
        // Feed all but the last → still incomplete.
        for f in &frags[..frags.len() - 1] {
            assert!(r.insert(f).is_none());
        }
        let done = r.insert(&frags[frags.len() - 1]).expect("completes");
        assert_eq!(done, body);
        assert_eq!(r.pending_len(), 0);
    }

    #[test]
    fn reassemble_out_of_order_and_duplicates() {
        let body: Vec<u8> = (0..FRAGMENT_CHUNK_SIZE * 2 + 5).map(|i| i as u8).collect();
        let frags = split_into_fragments(&body, 7);
        assert_eq!(frags.len(), 3);

        let mut r = FragmentReassembler::new();
        // Reverse order, with a duplicate thrown in (simulating a retransmit).
        assert!(r.insert(&frags[2]).is_none());
        assert!(r.insert(&frags[2]).is_none()); // duplicate, harmless
        assert!(r.insert(&frags[0]).is_none());
        let done = r.insert(&frags[1]).expect("completes after last unique");
        assert_eq!(done, body);
    }

    #[test]
    fn udp_frame_roundtrips_each_variant() {
        let cases = vec![
            UdpFrame::Command {
                request_id: "r1".into(),
                from_session: "mcp-1".into(),
                payload: "ENC".into(),
            },
            UdpFrame::Result { request_id: "r1".into(), result: "ENCRES".into() },
            UdpFrame::Error { request_id: "r1".into(), error: "boom".into() },
        ];
        for f in cases {
            let bytes = f.to_bytes();
            assert!(!bytes.is_empty());
            assert_eq!(UdpFrame::from_bytes(&bytes), Some(f));
        }
    }

    #[test]
    fn reflexive_endpoint_combines_reflected_ip_with_local_port() {
        let reflected = Endpoint::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), 55000); // TCP port
        let local = Endpoint::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 41234); // bound UDP port
        let public = reflexive_endpoint(Some(reflected), local).unwrap();
        assert_eq!(public.addr, IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))); // reflected IP
        assert_eq!(public.port, 41234); // our UDP port, NOT the reflected TCP port
        // No reflection → None (offer falls back to local endpoint only).
        assert!(reflexive_endpoint(None, local).is_none());
    }

    #[test]
    fn udp_frame_from_bytes_rejects_garbage() {
        assert!(UdpFrame::from_bytes(b"not json").is_none());
        assert!(UdpFrame::from_bytes(b"{\"kind\":\"bogus\"}").is_none());
    }

    #[test]
    fn reassembler_ignores_nonsensical_framing() {
        let mut r = FragmentReassembler::new();
        // count = 0 is invalid.
        let bad = {
            let mut b = vec![0u8; FRAGMENT_HEADER_SIZE];
            FragmentHeader { msg_id: 1, index: 0, count: 0 }.write(&mut b);
            b
        };
        assert!(r.insert(&bad).is_none());
        assert_eq!(r.pending_len(), 0);
        // index >= count is invalid.
        let bad2 = {
            let mut b = vec![0u8; FRAGMENT_HEADER_SIZE];
            FragmentHeader { msg_id: 1, index: 5, count: 3 }.write(&mut b);
            b
        };
        assert!(r.insert(&bad2).is_none());
        assert_eq!(r.pending_len(), 0);
    }
}
