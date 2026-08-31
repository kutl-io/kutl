# syntax=docker/dockerfile:1.7

# Builder pinned to Debian 13 (trixie) so glibc matches the runtime.
FROM rust:1-trixie AS builder
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release -p kutl-relay --locked && \
    cp target/release/kutl-relay /tmp/kutl-relay

# The distroless runtime has no shell, so the data directory can't be created
# with `RUN mkdir` there. Stage it here (owned by distroless's nonroot uid 65532)
# and copy it into the final image.
RUN mkdir -p /tmp/kutl-data && chown 65532:65532 /tmp/kutl-data

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=builder /tmp/kutl-relay /usr/local/bin/kutl-relay
COPY --from=builder --chown=65532:65532 /tmp/kutl-data /var/lib/kutl

ARG VERSION=unknown
ARG REVISION=unknown
LABEL org.opencontainers.image.title="kutl-relay"
LABEL org.opencontainers.image.description="OSS sync relay for kutl"
LABEL org.opencontainers.image.source="https://github.com/kutl-io/kutl"
LABEL org.opencontainers.image.licenses="AGPL-3.0-only"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${REVISION}"

# Container-appropriate default: bind all interfaces (loopback inside a
# container is unreachable through Docker port mapping).
#
# Auth is mandatory — there is no auth-off toggle. The relay
# refuses to start unless it can authenticate clients, so the operator MUST
# provide one of:
#   * a membership backend (KUTL_DATABASE_URL, as kutlhub-relay does), or
#   * a did:key allowlist file via KUTL_RELAY_AUTHORIZED_KEYS_FILE.
# We deliberately do NOT bake a keys-file path into the image: the operator
# mounts one and points KUTL_RELAY_AUTHORIZED_KEYS_FILE at it (an empty file
# boots the relay; `kutl relay authorize` appends DIDs, live-reloaded).
#
# The relay data dir is always present; default it to a stable path
# inside the image and declare it a volume so registry/signal state survives
# container restarts when the operator mounts a volume there.
ENV KUTL_RELAY_HOST=0.0.0.0 \
    KUTL_RELAY_DATA_DIR=/var/lib/kutl

VOLUME /var/lib/kutl

EXPOSE 9100

ENTRYPOINT ["/usr/local/bin/kutl-relay"]
