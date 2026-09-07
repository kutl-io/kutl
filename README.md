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

The image binds `0.0.0.0:9100` and persists to `/var/lib/kutl` (declared as a
volume) — the right defaults for a container. Authentication is mandatory:
there is no auth-off toggle, and the relay refuses to start without a did:key
allowlist file. The allowlist is a plain text file you edit like SSH's
`authorized_keys` — one DID per line, kept in version control if you like;
the relay live-reloads it, so later grants need no restart:

```sh
# your DID is printed by `kutl status`
echo 'did:key:z6Mk...' >> ./authorized_keys

docker run -d --name kutl-relay -p 9100:9100 \
  -e KUTL_RELAY_AUTHORIZED_KEYS_FILE=/etc/kutl/authorized_keys \
  -e KUTL_RELAY_EXTERNAL_URL=https://relay.example.com \
  -v "$PWD/authorized_keys:/etc/kutl/authorized_keys:ro" \
  -v kutl-data:/var/lib/kutl \
  ghcr.io/kutl-io/kutl-relay
```

Clients authenticate automatically with their local DID identity — no login
step. Authorize each additional machine by appending its DID as a line to the
relay's `authorized_keys` file (`kutl status` prints a machine's DID).

See [kutl.io/docs/relay](https://kutl.io/docs/relay) for the full env-var
reference and TLS guidance.

## Links

- [kutl.io](https://kutl.io) — pre-built binaries and documentation

This repository is a read-only mirror. Issues and pull requests are disabled.
