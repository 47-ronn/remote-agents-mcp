//! Shared types and protocol for remote-agents
//!
//! This crate contains the WebSocket message protocol and common types
//! used by both the agent and MCP server.

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
    reflexive_endpoint, ChannelState, Endpoint, UdpAnswer, UdpChannelResult, UdpConfig, UdpFrame,
    UdpOffer, UdpPacketHeader, UdpPacketType, UDP_HEADER_SIZE, UDP_MAX_PACKET, UDP_MAX_PAYLOAD,
};

#[cfg(feature = "udp")]
pub use udp_channel::*;
