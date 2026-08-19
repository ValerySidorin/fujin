# WebSocket Transport

Fujin native binary protocol over WebSocket. This transport is intended for browser clients and networks where HTTP upgrade traffic is easier to operate than a raw TCP connection.

**Registered name:** `websocket` | **Default address:** `:4851` | **Default path:** `/fujin`

## Configuration

```yaml
fujin:
  transports:
    - type: websocket
      enabled: true
      settings:
        addr: ":4851"
        path: /fujin
        allowed_origins:
          - https://app.example.com
        max_message_bytes: 4194304
        tls:
          enabled: true
          server_cert_pem_path: ./certs/server.pem
          server_key_pem_path: ./certs/server-key.pem
          # client_certs_dir: ./certs/clients
          # require_and_verify_client_cert: true
        fujin:
          ping_interval: 2s
          ping_timeout: 5s
          write_deadline: 10s
          force_terminate_timeout: 15s
```

| Field | Default | Description |
|---|---:|---|
| `addr` | `:4851` | TCP listen address used by the HTTP/WebSocket server |
| `path` | `/fujin` | HTTP upgrade path; must start with `/` |
| `allowed_origins` | same-origin | Origins allowed during the WebSocket handshake |
| `max_message_bytes` | `4194304` | Maximum size of one inbound WebSocket message |
| `tls.enabled` | `false` | Serve `wss://` instead of `ws://` |
| `tls.server_cert_pem_path` | — | PEM server certificate path |
| `tls.server_key_pem_path` | — | PEM server private-key path |
| `tls.client_certs_dir` | — | Directory containing trusted client certificates |
| `tls.require_and_verify_client_cert` | `false` | Require and verify client certificates |
| `fujin.ping_interval` | `2s` | Fujin protocol ping interval |
| `fujin.ping_timeout` | `5s` | Time allowed for the Fujin protocol ping response |
| `fujin.write_deadline` | `10s` | Deadline applied to protocol writes |
| `fujin.force_terminate_timeout` | `15s` | Session force-termination timeout |

## Wire Semantics

The WebSocket carries the same Fujin binary protocol used by the TCP and Unix transports. It does not define a JSON or WebSocket-specific application protocol.

- Clients must send **binary** WebSocket messages. Text messages terminate the session.
- WebSocket message boundaries are not Fujin frame boundaries. A message may contain a partial frame or multiple frames, and a Fujin frame may span multiple WebSocket messages.
- Clients must process received messages as one continuous byte stream.
- Server-side protocol batches are coalesced into binary WebSocket messages where possible.
- Fujin PING/PONG messages are application-protocol messages, not WebSocket control-frame pings.
- WebSocket compression (`permessage-deflate`) is not enabled by this transport.

## Origin Policy

When `allowed_origins` is empty, Gorilla WebSocket's same-origin policy is used: browser requests with an `Origin` host different from the request `Host` are rejected.

Configured origins are exact scheme/host/port matches, ignoring a trailing slash:

```yaml
allowed_origins:
  - https://app.example.com
  - https://admin.example.com:8443
```

Use `"*"` to accept every origin:

```yaml
allowed_origins: ["*"]
```

Requests without an `Origin` header are accepted. Prefer an explicit allowlist for browser-facing deployments; origin checks are not a replacement for authentication.

## TLS and Proxies

With TLS disabled, connect using `ws://host:4851/fujin`. With TLS enabled, use `wss://host:4851/fujin`.

Reverse proxies must:

- forward the HTTP `Upgrade` and `Connection` headers;
- preserve binary WebSocket messages;
- allow messages up to `max_message_bytes`;
- use an idle timeout longer than the configured Fujin ping interval and timeout.

TLS can terminate either in Fujin or at the reverse proxy. If TLS terminates at the proxy, keep `tls.enabled: false` for the internal hop.

## Limits and Performance

`max_message_bytes` limits each inbound WebSocket message, not the total lifetime of a session. Protocol-level payload and batch limits still apply independently.

WebSocket adds framing and userspace processing. It is appropriate for browser compatibility and HTTP-aware infrastructure; use raw TCP when sustained bulk throughput and minimum allocation count are the primary requirements.

## Lifecycle

The transport supports inherited TCP listener file descriptors for graceful binary upgrade. On shutdown, existing sessions are allowed to drain for up to 30 seconds before their WebSocket connections are forcibly closed.
