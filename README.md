# kutl

Real-time collaborative document sync engine.

- **kutl-core** — CRDT engine and shared types
- **kutl-relay** — WebSocket sync relay server (AGPL-3.0)
- **kutl-daemon** — File watcher and sync client
- **kutl** — CLI binary

## License

All crates are licensed under MIT OR Apache-2.0, except kutl-relay which is
licensed under AGPL-3.0. See individual crate LICENSE files and SPDX headers.

## Running the relay in Docker

A container image for `kutl-relay` is published to GitHub Container Registry on
each release:

```
ghcr.io/kutl-io/kutl-relay:latest
ghcr.io/kutl-io/kutl-relay:0.1
ghcr.io/kutl-io/kutl-relay:0.1.5
```

Multi-arch (`linux/amd64`, `linux/arm64`).

The image binds `0.0.0.0:9100` with auth disabled by default — the right
defaults for a container. The relay then refuses to start until you tell it
which mode you intend, so accidental open-relay exposure is impossible.

**Local development** (open relay; explicit operator acknowledgement):

```sh
docker run --rm -p 9100:9100 \
  -e KUTL_RELAY_ALLOW_OPEN_RELAY=true \
  ghcr.io/kutl-io/kutl-relay
```

**Network-reachable** (file-based DID allowlist, persistent storage):

```sh
docker run -d --name kutl-relay -p 9100:9100 \
  -e KUTL_RELAY_REQUIRE_AUTH=true \
  -e KUTL_RELAY_AUTHORIZED_KEYS_FILE=/etc/kutl/authorized_keys \
  -e KUTL_RELAY_DATA_DIR=/var/lib/kutl \
  -e KUTL_RELAY_EXTERNAL_URL=https://relay.example.com \
  -v "$PWD/authorized_keys:/etc/kutl/authorized_keys:ro" \
  -v kutl-data:/var/lib/kutl \
  ghcr.io/kutl-io/kutl-relay
```

See [kutl.io/docs/relay](https://kutl.io/docs/relay) for the full env-var
reference and TLS guidance.

## Links

- [kutl.io](https://kutl.io) — pre-built binaries and documentation
- [kutlhub.com](https://kutlhub.com) — hosted collaborative editing

This repository is a read-only mirror. Issues and pull requests are disabled.
