//! Remote Agents MCP Server
//!
//! Unified MCP server that can:
//! 1. Execute commands locally on this machine
//! 2. Control remote agents connected to a relay room
//!
//! All tools accept an optional `agent_id` parameter:
//! - If omitted → execute locally
//! - If provided → forward to remote agent via relay

pub mod autonomous;
pub mod config;
pub mod connection;
pub mod daemon;
pub mod executor;
pub mod mapreduce;
pub mod mcp_server;
pub mod relay_api;
pub mod relay_controller;
pub mod relay_udp;
pub mod safety;
pub mod scheduler;
pub mod state;
pub mod udp_transport;
