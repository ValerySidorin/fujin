# ZeroMQ connector via `pebbe/zmq4`

Opt-in Fujin connector backed by native `libzmq`.

**Registered name:** `zeromq_pebbe`

## Build requirements

- Go build tag: `zeromq_pebbe`
- CGO enabled (`cmd/builder -cgo`)
- `github.com/pebbe/zmq4 v1.4.0`
- `libzmq >= 4.3.5` and libsodium development files at build time
- Dynamic `libzmq` and libsodium libraries at runtime

The plugin is intentionally absent from `public/plugins/connector/all`, so default and scratch builds remain CGO-free.

```sh
go run ./cmd/builder \
  -local -cgo \
  -tags fujin,grpc,zeromq_pebbe \
  -configurator github.com/fujin-io/fujin/public/plugins/configurator/yaml \
  -connector github.com/fujin-io/fujin/public/plugins/connector/zeromq/pebbe \
  -transport github.com/fujin-io/fujin/public/plugins/transport/all \
  -output ./bin/fujin-zeromq
```

## Capabilities

| Pattern | Fujin API | Distribution | Produce guarantee |
|---|---|---|---|
| `pub` | Produce | ZeroMQ fan-out to matching SUB peers | `local_accept` |
| `sub` | Subscribe | Every local Fujin subscriber receives every matching message | n/a |
| `push` | Produce | ZeroMQ round-robin across PULL peers | `local_accept` |
| `pull` | Subscribe | One local Fujin subscriber receives each message, round-robin | n/a |

Fetch, manual ACK/NACK, transactions, replay, and durability are unsupported. Writer success means that the local ZeroMQ socket accepted the message; it does not prove peer delivery.

## Configuration

```yaml
connectors:
  events:
    type: zeromq_pebbe
    settings:
      common:
        io_threads: 1
        send_hwm: 1000
        receive_hwm: 1000
        send_timeout: 5s
        ready_timeout: 10s
        receive_poll_interval: 100ms
        reconnect_interval: 100ms
        reconnect_interval_max: 5s
        linger: 0s
        max_message_bytes: 4194304
        subscriber_queue_capacity: 256
      routes:
        events_out:
          pattern: pub
          endpoint: tcp://events.example:5555
          mode: connect
          framing: fujin_v1
          topic: events.
          security:
            mechanism: curve
            public_key: CLIENT_PUBLIC_Z85
            secret_key_path: /run/secrets/zmq-client-secret
            server_public_key: SERVER_PUBLIC_Z85
        events_in:
          pattern: sub
          endpoint: tcp://*:5556
          mode: bind
          framing: fujin_v1
          subscriptions: [events.]
          security:
            mechanism: curve
            public_key: SERVER_PUBLIC_Z85
            secret_key_path: /run/secrets/zmq-server-secret
            allowed_client_public_keys:
              - CLIENT_PUBLIC_Z85
```

Supported endpoints are `tcp://` and `ipc://`. `mode` defaults to `connect`; `framing` defaults to `fujin_v1`; security defaults to `null`.

## Multipart framing

`raw`:

- PUB/SUB: `[topic, payload]`
- PUSH/PULL: `[payload]`
- Header-aware Fujin operations are rejected.

`fujin_v1`:

- PUB/SUB: `[topic, "fujin.v1", header_count_u16_be, header_0, ..., payload]`
- PUSH/PULL: `["fujin.v1", header_count_u16_be, header_0, ..., payload]`

The header count is the number of raw key/value fields and must be even. Duplicate keys, arbitrary binary values, and empty payloads round-trip losslessly. Malformed and oversized input is counted, rate-limited in logs, and dropped without stopping the route.

## Runtime behavior

One connector runtime owns one ZeroMQ context and one socket actor per route. Socket access is serialized. SUB broadcasts through bounded per-session queues; PULL distributes locally round-robin. A full subscriber queue detaches only that subscriber with `ErrSlowConsumer`.

Bind sockets are opened before a generation is published. Unchanged settings reuse the active runtime across generation overlap. A changed configuration that claims the same bind endpoint returns `connector.ErrRuntimeDrainRequired`; remove the route, wait for the old generation to retire, then add the changed route.

CURVE secret keys are loaded from files during runtime preflight. Each ZeroMQ context runs its own ZAP actor and immutable domain allowlists; no process-global `pebbe/zmq4` authentication state is used.

## Verification

```sh
make test-zeromq-pebbe
FUJIN_ZEROMQ_PYTHON=/path/to/python-with-pyzmq make e2e-zeromq-pebbe
make bench-zeromq-pebbe
```

The E2E matrix uses the independent `testdata/pyzmq_peer.py` fixture in both directions for PUB/SUB and PUSH/PULL, bind/connect ownership, duplicate headers, empty payloads, and 1 MiB payloads.
