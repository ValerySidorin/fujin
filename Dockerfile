# syntax=docker/dockerfile:1

ARG GO_VERSION=1.25.1

FROM golang:${GO_VERSION}-alpine AS builder

ARG FUJIN_CONFIGURATORS
ARG FUJIN_CONNECTORS
ARG FUJIN_TRANSPORTS
ARG FUJIN_BIND_MIDDLEWARES
ARG FUJIN_CONNECTOR_MIDDLEWARES
ARG FUJIN_GO_TAGS=fujin,grpc
ARG VERSION=dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN apk add --no-cache bash dos2unix

RUN chmod +x build.sh

# For Windows compatibility
RUN dos2unix build.sh

RUN FUJIN_CONFIGURATORS="${FUJIN_CONFIGURATORS:-github.com/fujin-io/fujin/public/plugins/configurator/all}" \
    FUJIN_CONNECTORS="${FUJIN_CONNECTORS:-github.com/fujin-io/fujin/public/plugins/connector/kafka/franz}" \
    FUJIN_TRANSPORTS="${FUJIN_TRANSPORTS:-github.com/fujin-io/fujin/public/plugins/transport/all}" \
    FUJIN_BIND_MIDDLEWARES="${FUJIN_BIND_MIDDLEWARES:-}" \
    FUJIN_CONNECTOR_MIDDLEWARES="${FUJIN_CONNECTOR_MIDDLEWARES:-}" \
    FUJIN_GO_TAGS="${FUJIN_GO_TAGS}" \
    FUJIN_VERSION="${VERSION}" \
    ./build.sh

FROM scratch AS runtime

WORKDIR /

COPY --from=builder /app/bin/fujin /fujin

STOPSIGNAL SIGTERM
EXPOSE 4850/tcp 4848/udp 4849/tcp 8080/tcp

ENTRYPOINT ["/fujin"]
