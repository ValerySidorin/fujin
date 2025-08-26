ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-alpine AS builder

ARG GO_BUILD_TAGS=kafka,nats_core,amqp091,amqp10,resp_pubsub,resp_streams,mqtt,nsq

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download && apk add git make

COPY . .

RUN make build GO_BUILD_TAGS=${GO_BUILD_TAGS}

FROM scratch

WORKDIR /

COPY --from=builder app/bin/fujin /fujin

STOPSIGNAL SIGTERM

ENTRYPOINT ["/fujin"]