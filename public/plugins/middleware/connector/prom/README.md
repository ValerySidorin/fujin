# Prometheus Metrics Middleware

Connector middleware that collects Prometheus metrics. Starts an HTTP server with a `/metrics` endpoint exposing operation counts, error counts, and produce latency histograms — all labeled by connector name.

**Registered name:** `prom`

## Configuration

```yaml
connectors:
  my_connector:
    connector_middlewares:
      - name: prom
        addr: ":9090"
        path: /metrics
```

| Field | Default | Description |
|-------|---------|-------------|
| `addr` | `:9090` | HTTP server listen address |
| `path` | `/metrics` | Metrics endpoint path |
