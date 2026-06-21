//! Shared types and protocol for remote-agents
//!
//! This crate contains the WebSocket message protocol and common types
//! used by both the agent and MCP server.

pub mod compress;
pub mod crypto;
mod protocol;
mod types;
pub mod udp;

/// Protobuf wire types generated from `proto/remote_agents.proto` (prost). The
/// single source of truth for the on-wire protocol; the idiomatic domain types
/// in [`protocol`]/[`types`] convert to/from these at the transport boundary
/// (see [`proto_convert`]). TS (worker + panel) generate from the same `.proto`.
#[allow(clippy::all, clippy::pedantic, missing_docs)]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/remote_agents.v1.rs"));
}
pub mod proto_convert;
pub use proto_convert::{ConvertError, WireError};
#[cfg(feature = "udp")]
pub mod quic;
#[cfg(feature = "udp")]
pub mod udp_channel;

pub use crypto::Cipher;
pub use protocol::*;
pub use types::*;
pub use udp::{
    candidate_addrs, candidate_addrs_multi, local_candidate_ips, local_egress_ip,
    reflexive_endpoint, ChannelState, Endpoint, UdpAnswer, UdpChannelResult, UdpConfig, UdpFrame,
    UdpOffer, UdpPacketHeader, UdpPacketType, UDP_HEADER_SIZE, UDP_MAX_PACKET, UDP_MAX_PAYLOAD,
    STUN_SERVERS,
};
#[cfg(feature = "udp")]
pub use udp::stun_discover;

#[cfg(feature = "udp")]
pub use udp_channel::*;
