# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.97.1

FROM rust:${RUST_VERSION}-alpine AS builder

ARG VERSION=dev

RUN apk add --no-cache build-base cmake perl pkgconf
WORKDIR /app

COPY fujin-rs/Cargo.toml fujin-rs/Cargo.lock ./fujin-rs/
COPY fujin-rs/crates ./fujin-rs/crates
COPY fujin-rs/plugins ./fujin-rs/plugins

RUN FUJIN_VERSION="${VERSION}" cargo build \
    --manifest-path fujin-rs/Cargo.toml \
    --release \
    -p fujin \
    --features full \
 && ./fujin-rs/target/release/fujin --version

FROM alpine:3.22 AS runtime

RUN apk add --no-cache ca-certificates libgcc libstdc++ \
 && addgroup -S fujin \
 && adduser -S -G fujin -h /nonexistent -s /sbin/nologin fujin

COPY --from=builder /app/fujin-rs/target/release/fujin /fujin

USER fujin
ENV FUJIN_CONFIG=/config/config.yaml \
    RUST_LOG=info
STOPSIGNAL SIGTERM
EXPOSE 4850/tcp 4848/udp 4849/tcp 4851/tcp 8080/tcp

ENTRYPOINT ["/fujin"]
