# TCP Transport

TCP transport with optional TLS. Best for maximum single-stream throughput.

**Registered name:** `tcp` | **Default port:** `:4850`

## Configuration

```yaml
fujin:
  transports:
    - type: tcp
      enabled: true
      settings:
        addr: ":4850"
        tls:
          enabled: false
          server_cert_pem_path: ./certs/server.pem
          server_key_pem_path: ./certs/server-key.pem
        fujin:
          ping_interval: 5s
          ping_timeout: 10s
          ping_max_retries: 5
          write_deadline: 10s
          force_terminate_timeout: 15s
```
