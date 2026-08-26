# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.97.1

FROM rust:${RUST_VERSION}-alpine AS builder

ARG VERSION=dev
ARG FUJIN_BUILD_MANIFEST=deploy/docker/fujin.build.toml
ARG FUJIN_BUILD_LOCK=Cargo.lock

RUN apk add --no-cache build-base ca-certificates cmake curl-dev perl pkgconf protobuf-dev
WORKDIR /app

COPY . .

RUN set -eux; \
    set --; \
    if [ -n "${FUJIN_BUILD_LOCK}" ]; then \
        set -- --lockfile "${FUJIN_BUILD_LOCK}"; \
    fi; \
    FUJIN_BUILD_VERSION="${VERSION}" cargo run --release --locked -p cargo-fujin -- \
        --manifest "${FUJIN_BUILD_MANIFEST}" \
        build --profile release --output /runtime/fujin --clean-after "$@"; \
    /runtime/fujin --version; \
    install -d -m 0755 /runtime/etc/ssl/certs; \
    install -d -m 0700 -o 65532 -g 65532 /runtime/run/fujin; \
    cp /etc/ssl/certs/ca-certificates.crt /runtime/etc/ssl/certs/

FROM scratch AS runtime

COPY --from=builder /runtime/ /

USER 65532:65532
ENV FUJIN_CONFIGURATOR=file \
    FUJIN_CONFIGURATOR_FILE_PATHS=/config/config.yaml \
    FUJIN_LOG_LEVEL=INFO \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
STOPSIGNAL SIGTERM
EXPOSE 4850/tcp 8080/tcp

ENTRYPOINT ["/fujin"]
