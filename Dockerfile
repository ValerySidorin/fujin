FROM golang:1.24-alpine AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 && go build -o /fujin -trimpath -ldflags "-s -w" ./cmd

FROM scratch

WORKDIR /

COPY --from=build /fujin /fujin

STOPSIGNAL SIGTERM

ENTRYPOINT ["/fujin"]