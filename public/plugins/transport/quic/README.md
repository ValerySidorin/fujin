# QUIC Transport

QUIC transport with built-in TLS and multiplexed streams. Best for environments needing connection migration or multiple concurrent streams.

**Registered name:** `quic` | **Default port:** `:4848`

## Configuration

```yaml
fujin:
  transports:
    - type: quic
      enabled: true
      settings:
        addr: ":4848"
        max_incoming_streams: 1000
        max_idle_timeout: 5s
        keepalive_period: 2s
        handshake_idle_timeout: 10s
        tls:
          enabled: true
          server_cert_pem_path: ./certs/server.pem
          server_key_pem_path: ./certs/server-key.pem
        fujin:
          ping_interval: 5s
          ping_timeout: 10s
          ping_stream: false
          ping_max_retries: 5
          write_deadline: 10s
          force_terminate_timeout: 15s
```
