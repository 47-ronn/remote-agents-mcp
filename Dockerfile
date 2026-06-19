# Multi-stage build for the remote-agents MCP server.
# Used by the Docker MCP Catalog (docker/mcp-registry builds & signs the image)
# and usable standalone: `docker build -t remote-agents . && docker run -i \
#   -e REMOTE_AGENTS_RELAY=wss://host -e REMOTE_AGENTS_ROOM=room remote-agents`.

FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
# Only the MCP server binary is needed for the stdio MCP transport.
RUN cargo build --release -p remote-agent --bin remote-agent

FROM debian:bookworm-slim
# ca-certificates: the relay is reached over wss:// (TLS) by default.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/remote-agent /usr/local/bin/remote-agent

# MCP hosts speak to the server over stdio; `mcp` is the stdio transport mode.
# Connection settings come from REMOTE_AGENTS_* env vars (relay/room/token/name)
# or extra CLI args appended by the host.
ENTRYPOINT ["remote-agent", "mcp"]
