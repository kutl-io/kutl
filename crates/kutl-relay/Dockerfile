# syntax=docker/dockerfile:1.7

# Builder pinned to Debian 13 (trixie) so glibc matches the runtime.
FROM rust:1-trixie AS builder
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release -p kutl-relay --locked && \
    cp target/release/kutl-relay /tmp/kutl-relay

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=builder /tmp/kutl-relay /usr/local/bin/kutl-relay

ARG VERSION=unknown
ARG REVISION=unknown
LABEL org.opencontainers.image.title="kutl-relay"
LABEL org.opencontainers.image.description="OSS sync relay for kutl"
LABEL org.opencontainers.image.source="https://github.com/kutl-io/kutl"
LABEL org.opencontainers.image.licenses="AGPL-3.0-only"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${REVISION}"

# Container-appropriate defaults: bind all interfaces (loopback inside a
# container is unreachable through Docker port mapping) and don't auto-flip
# auth on. The relay refuses to start with this combination unless the
# operator explicitly sets KUTL_RELAY_ALLOW_OPEN_RELAY=true (dev) or
# configures KUTL_RELAY_AUTHORIZED_KEYS_FILE + KUTL_RELAY_REQUIRE_AUTH=true
# (prod), so accidental open-relay exposure is impossible.
ENV KUTL_RELAY_HOST=0.0.0.0 \
    KUTL_RELAY_REQUIRE_AUTH=false

EXPOSE 9100

ENTRYPOINT ["/usr/local/bin/kutl-relay"]
