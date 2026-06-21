//! Shared types and protocol for remote-agents
//!
//! This crate contains the WebSocket message protocol and common types
//! used by both the agent and MCP server.

pub mod compress;
pub mod crypto;
mod protocol;
mod types;
pub mod udp;
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
