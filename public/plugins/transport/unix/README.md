# Unix Socket Transport

Unix domain socket transport. Lowest latency, ideal for same-host IPC (sidecars, local services). Unix-only — disabled automatically on Windows.

**Registered name:** `unix` | **Default path:** `/tmp/fujin.sock`

## Configuration

```yaml
fujin:
  transports:
    - type: unix
      enabled: true
      settings:
        path: /tmp/fujin.sock
        fujin:
          ping_interval: 5s
          ping_timeout: 10s
          ping_max_retries: 5
          write_deadline: 10s
          force_terminate_timeout: 15s
```
